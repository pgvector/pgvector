#include "postgres.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/read_stream.h"
#include "storage/bufmgr.h"
#include "access/genam.h"
#include <float.h>
#include "nodes/pg_list.h"
#include "access/relscan.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "lib/pairingheap.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "portability/instr_time.h"
#include "access/heapam.h"
#include "catalog/index.h"
#include <sys/mman.h>
#include <unistd.h>
#include "storage/procsignal.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"

#define GetScanList(ptr) pairingheap_container(IvfflatScanList, ph_node, ptr)
#define GetScanListConst(ptr) pairingheap_const_container(IvfflatScanList, ph_node, ptr)

extern int max_parallel_workers_per_gather;
extern int max_worker_processes;

PGDLLEXPORT void ivf_prefetch_worker_main(Datum main_arg);

static HTAB *centroid_access_counts = NULL;
static HTAB *page_hits = NULL;

static int NUM_WORKERS;

// The total number of workers depends on the max_worker_processes available ar runtime as well as the max_parallel_workers_per_gather knob
void init_num_workers(void)
{
	NUM_WORKERS = Min(max_worker_processes, max_parallel_workers_per_gather);
}

typedef struct ResultItem
{
	double distance;
	ItemPointerData tid;
} ResultItem;

// This struct is shared between all the background workers
typedef struct IvfSharedVector
{
	Oid relid;
	Oid distfunc_oid;
	Oid collation;
	Oid dbid;
	Oid userid;
	//  directly store a full varlena Vector here
	bool is_null;
	char vector_data[FLEXIBLE_ARRAY_MEMBER];

} IvfSharedVector;

// This struct is allocated per worker.
typedef struct IvfParallelWorkerTask
{
	// How many blocks have been assigned for this worker
	int num_blocks;
	// Semaphore to indicate the finite state of the corresponding worker. 0 = not finished, 1 = finished
	pg_atomic_uint32 worker_done;
	// How many results written
	int results_count;
	// Cursor used to return results directly from the shared memory
	int return_result_pos;
	// Allocated space for the block list assigned to the corresponding worker and the results
	char data[FLEXIBLE_ARRAY_MEMBER];
} IvfParallelWorkerTask;

static IvfParallelWorkerTask **tasks;
static dsm_segment **segments;
static int used_workers;
static dsm_segment *shared_info_seg;

// This struct contains a list of all the block numbers (pages) that need to be scanned during query execution
typedef struct IvfStaticScanState
{
	int index;
	int total;
	BlockNumber *blocknos;
} IvfStaticScanState;

IvfStaticScanState *scan_state;

//
typedef struct MetaBlockData
{
	int count;
	BlockNumber blocknos[FLEXIBLE_ARRAY_MEMBER]; // Flexible array member
} MetaBlockData;

int result_cmp(const void *a, const void *b)
{
	const ResultItem *ra = (const ResultItem *)a;
	const ResultItem *rb = (const ResultItem *)b;
	return (ra->distance > rb->distance) - (ra->distance < rb->distance);
}

// This function is executed per worker
void ivf_prefetch_worker_main(Datum main_arg)
{

	BackgroundWorkerUnblockSignals();

	// Access the shared segment to get information about the relation and the query vector
	dsm_handle vec_handle;
	memcpy(&vec_handle, MyBgworkerEntry->bgw_extra, sizeof(dsm_handle));
	dsm_segment *vec_seg = dsm_attach(vec_handle);
	IvfSharedVector *shared_vec = dsm_segment_address(vec_seg);

	// Connect to the same DB/user as the launcher
	BackgroundWorkerInitializeConnectionByOid(shared_vec->dbid, shared_vec->userid, 0);

	// Access the query vector value
	Vector *vec = (Vector *)shared_vec->vector_data;
	Datum value;

	if (!shared_vec->is_null)
	{
		value = PointerGetDatum(vec);
	}

	// Access the per-worker assigned task shared memory region
	dsm_handle handle = DatumGetUInt32(main_arg);
	dsm_segment *seg = dsm_attach(handle);
	IvfParallelWorkerTask *task = dsm_segment_address(seg);

	StartTransactionCommand();
	Relation rel = relation_open(shared_vec->relid, AccessShareLock);
	int result_index = 0;

	FmgrInfo procinfo;
	fmgr_info_cxt(shared_vec->distfunc_oid, &procinfo, CurrentMemoryContext);
	BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

	BlockNumber *blocknos = (BlockNumber *)task->data;
	ResultItem *results = (ResultItem *)(blocknos + task->num_blocks);

	// Scan the assigned pages and extract the embeddings from each page
	for (int i = 0; i < task->num_blocks; i++)
	{
		BlockNumber blkno = blocknos[i];
		Buffer buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		Page page = BufferGetPage(buf);
		OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
		for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno++)
		{
			IndexTuple itup;
			Datum datum;
			bool isnull;

			ItemId itemid = PageGetItemId(page, offno);
			itup = (IndexTuple)PageGetItem(page, itemid);
			datum = index_getattr(itup, 1, RelationGetDescr(rel), &isnull);

			double dist = 0.0;
			if (!shared_vec->is_null)
			{
				dist = DatumGetFloat8(FunctionCall2Coll(&procinfo, shared_vec->collation, datum, value));
			}
			results[result_index].distance = dist;
			results[result_index].tid = itup->t_tid;
			result_index++;
		}
		UnlockReleaseBuffer(buf);
	}

	// Sort the results
	qsort(results, result_index, sizeof(ResultItem), result_cmp);

	task->results_count = result_index;
	pg_atomic_write_u32(&task->worker_done, 1);

	relation_close(rel, AccessShareLock);
	CommitTransactionCommand();

	proc_exit(0);
}

/*
 * Compare list distances
 */
static int
CompareLists(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (GetScanListConst(a)->distance > GetScanListConst(b)->distance)
		return 1;

	if (GetScanListConst(a)->distance < GetScanListConst(b)->distance)
		return -1;

	return 0;
}

/*
 * Get lists and sort by distance
 */
static void
GetScanLists(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
	BlockNumber nextblkno = IVFFLAT_HEAD_BLKNO;
	int listCount = 0;
	double maxDistance = DBL_MAX;

	/* Search all list pages */
	while (BlockNumberIsValid(nextblkno))
	{

		instr_time start, end, diff;
		instr_time start_overhead, end_overhead, elapsed;

		Buffer cbuf;
		Page cpage;
		OffsetNumber maxoffno;

		cbuf = ReadBuffer(scan->indexRelation, nextblkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);

		maxoffno = PageGetMaxOffsetNumber(cpage);

		for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			IvfflatList list = (IvfflatList)PageGetItem(cpage, PageGetItemId(cpage, offno));
			double distance;

			/* Use procinfo from the index instead of scan key for performance */
			distance = DatumGetFloat8(so->distfunc(so->procinfo, so->collation, PointerGetDatum(&list->center), value));

			if (listCount < so->maxProbes)
			{
				IvfflatScanList *scanlist;

				scanlist = &so->lists[listCount];
				scanlist->startPage = list->startPage;
				scanlist->numBlocks = list->numBlocks;

				scanlist->distance = distance;
				listCount++;

				/* Add to heap */
				pairingheap_add(so->listQueue, &scanlist->ph_node);

				/* Calculate max distance */
				if (listCount == so->maxProbes)
					maxDistance = GetScanList(pairingheap_first(so->listQueue))->distance;
			}
			else if (distance < maxDistance)
			{
				IvfflatScanList *scanlist;

				/* Remove */
				scanlist = GetScanList(pairingheap_remove_first(so->listQueue));

				/* Reuse */
				scanlist->startPage = list->startPage;
				scanlist->distance = distance;
				scanlist->numBlocks = list->numBlocks;
				pairingheap_add(so->listQueue, &scanlist->ph_node);

				/* Update max distance */
				maxDistance = GetScanList(pairingheap_first(so->listQueue))->distance;
			}
		}

		nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

		UnlockReleaseBuffer(cbuf);
	}

	int num_blocks = 0;
	for (int i = listCount - 1; i >= 0; i--)
	{
		IvfflatScanList *scanlist = GetScanList(pairingheap_remove_first(so->listQueue));
		so->listPages[i] = scanlist->startPage;
		num_blocks += scanlist->numBlocks;
	}

	scan_state = palloc(sizeof(IvfStaticScanState));
	scan_state->index = 0;
	scan_state->total = 0;
	if (num_blocks > 0)
		scan_state->blocknos = palloc(sizeof(BlockNumber) * num_blocks);
	else
		scan_state->blocknos = NULL;

	for (int i = 0; i < listCount; i++)
	{
		BlockNumber searchPage = so->listPages[i];

		// Walk chain of metadata pages. Each metapage contains an item which in turn contains a list of block numbers
		while (BlockNumberIsValid(searchPage))
		{
			Buffer metabuf;
			Page metapage;

			metabuf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, so->bas);
			LockBuffer(metabuf, BUFFER_LOCK_SHARE);
			metapage = BufferGetPage(metabuf);

			ItemId itemid = PageGetItemId(metapage, FirstOffsetNumber);
			MetaBlockData *metaDataItem = (MetaBlockData *)PageGetItem(metapage, itemid);

			// Copy the block numbers to the scan_state list
			for (int j = 0; j < metaDataItem->count; j++)
			{
				scan_state->blocknos[scan_state->total++] = metaDataItem->blocknos[j];
			}
			searchPage = IvfflatPageGetOpaque(metapage)->nextblkno;
			UnlockReleaseBuffer(metabuf);
		}
	}

	Assert(pairingheap_is_empty(so->listQueue));
}

// This function represent the serial execution. The main worker scans the list of block numbers, extracts the embeddings from each page and
// computes the distance between each embedding and the query vector
static void GetScanItems(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
	TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
	TupleTableSlot *slot = so->vslot;
	int batchProbes = 0;

	tuplesort_reset(so->sortstate);

	for (int i = 0; i < scan_state->total; i++)
	{
		Buffer buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, scan_state->blocknos[i], RBM_NORMAL, so->bas);
		if (!BufferIsValid(buf))
			break;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		Page page = BufferGetPage(buf);
		OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);

		for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			IndexTuple itup;
			Datum datum;
			bool isnull;

			ItemId itemid = PageGetItemId(page, offno);

			itup = (IndexTuple)PageGetItem(page, itemid);
			datum = index_getattr(itup, 1, tupdesc, &isnull);
			ExecClearTuple(slot);
			slot->tts_values[0] = so->distfunc(so->procinfo, so->collation, datum, value);

			slot->tts_isnull[0] = false;
			slot->tts_values[1] = PointerGetDatum(&itup->t_tid);
			slot->tts_isnull[1] = false;
			ExecStoreVirtualTuple(slot);

			tuplesort_puttupleslot(so->sortstate, slot);
		}

		UnlockReleaseBuffer(buf);
	}

	tuplesort_performsort(so->sortstate);

#if defined(IVFFLAT_MEMORY)
	elog(INFO, "memory: %zu MB", MemoryContextMemAllocated(CurrentMemoryContext, true) / (1024 * 1024));
#endif
}

static void
ParallelGetScanItems(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
	TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
	TupleTableSlot *slot = so->vslot;
	int batchProbes = 0;

	tuplesort_reset(so->sortstate);

	// Create a shared structure that contains information shared to all the background workers
	Vector *qvec = NULL;
	if (DatumGetPointer(value) != NULL)
	{
		qvec = DatumGetVector(value);
	}
	int dim = so->dimensions;

	// Compute shared memory size and create the DSM segment
	Size sz = offsetof(IvfSharedVector, vector_data) + VECTOR_SIZE(dim);
	shared_info_seg = dsm_create(sz, 0);
	dsm_handle vec_handle = dsm_segment_handle(shared_info_seg);

	// Get pointer to shared memory
	IvfSharedVector *shared_vec = dsm_segment_address(shared_info_seg);
	shared_vec->relid = RelationGetRelid(scan->indexRelation);
	shared_vec->distfunc_oid = so->procinfo->fn_oid;
	shared_vec->collation = so->collation;
	shared_vec->dbid = MyDatabaseId;
	shared_vec->userid = GetUserId();

	Vector *vec_in_dsm = (Vector *)shared_vec->vector_data;

	if (qvec != NULL)
	{
		// Copy the query vector to the shared structure
		shared_vec->is_null = false;
		SET_VARSIZE(vec_in_dsm, VECTOR_SIZE(dim));
		vec_in_dsm->dim = dim;
		memcpy(vec_in_dsm->x, qvec->x, sizeof(float4) * dim);
	}
	else
	{
		// Notify the background workers in case the query vector is NULL
		shared_vec->is_null = true;
	}

	tasks = (IvfParallelWorkerTask **)palloc(NUM_WORKERS * sizeof(IvfParallelWorkerTask *));
	segments = (dsm_segment **)palloc(NUM_WORKERS * sizeof(dsm_segment *));

	int chunk_size = (scan_state->total + NUM_WORKERS - 1) / NUM_WORKERS;
	BackgroundWorkerHandle *handles[NUM_WORKERS];

	// Generate NUM_WORKERS background processes. Assign a chunk of the block list to each worker.
	for (int w = 0; w < NUM_WORKERS; w++)
	{
		int start = w * chunk_size;
		int end = Min(start + chunk_size, scan_state->total);
		int count = end - start;

		if (count <= 0)
		{
			break;
		}
		used_workers++;
		int max_results = count * ((BLCKSZ - SizeOfPageHeaderData) / (dim * sizeof(float)));
		Size sz = offsetof(IvfParallelWorkerTask, data) + (sizeof(BlockNumber) * count) + (sizeof(ResultItem) * max_results);
		segments[w] = dsm_create(sz, 0);
		tasks[w] = dsm_segment_address(segments[w]);
		tasks[w]->num_blocks = count;
		tasks[w]->return_result_pos = 0;
		pg_atomic_init_u32(&tasks[w]->worker_done, 0);
		memcpy(tasks[w]->data, &scan_state->blocknos[start], sizeof(BlockNumber) * count);
		tasks[w]->results_count = 0;

		// Launch worker
		BackgroundWorker worker;
		memset(&worker, 0, sizeof(worker));
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_ConsistentState;
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		// Pass a pointer to the struct that is shared between all the workers
		memcpy(worker.bgw_extra, &vec_handle, sizeof(vec_handle));
		snprintf(worker.bgw_name, BGW_MAXLEN, "ivfflat worker %d", w);
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "vector");
		snprintf(worker.bgw_function_name, BGW_MAXLEN, "ivf_prefetch_worker_main");
		worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(segments[w]));

		if (!RegisterDynamicBackgroundWorker(&worker, &handles[w]))
			elog(INFO, "could not register worker %d", w);
	}

	// Wait until all the workers have been finished
	bool all_done = false;
	while (!all_done)
	{
		all_done = true;
		for (int w = 0; w < used_workers; w++)
		{
			if (pg_atomic_read_u32(&tasks[w]->worker_done) == 0)
			{
				all_done = false;
				break;
			}
		}
		if (!all_done)
			pg_usleep(1000);
	}

	tuplesort_performsort(so->sortstate);
#if defined(IVFFLAT_MEMORY)
	elog(INFO, "memory: %zu MB", MemoryContextMemAllocated(CurrentMemoryContext, true) / (1024 * 1024));
#endif
}

/*
 * Zero distance
 */
static Datum
ZeroDistance(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
{
	return Float8GetDatum(0.0);
}

/*
 * Get scan value
 */
static Datum
GetScanValue(IndexScanDesc scan)
{

	IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
	Datum value;

	if (scan->orderByData->sk_flags & SK_ISNULL)
	{
		value = PointerGetDatum(NULL);
		so->distfunc = ZeroDistance;
	}
	else
	{
		value = scan->orderByData->sk_argument;
		so->distfunc = FunctionCall2Coll;

		/* Value should not be compressed or toasted */
		Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
		Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

		/* Normalize if needed */
		if (so->normprocinfo != NULL)
		{
			MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

			value = IvfflatNormValue(so->typeInfo, so->collation, value);

			MemoryContextSwitchTo(oldCtx);
		}
	}

	return value;
}

/*
 * Initialize scan sort state
 */
static Tuplesortstate *
InitScanSortState(TupleDesc tupdesc)
{
	AttrNumber attNums[] = {1};
	Oid sortOperators[] = {Float8LessOperator};
	Oid sortCollations[] = {InvalidOid};
	bool nullsFirstFlags[] = {false};

	return tuplesort_begin_heap(tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, work_mem, NULL, false);
}

/*
 * Prepare for an index scan
 */
IndexScanDesc
ivfflatbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	IvfflatScanOpaque so;
	int lists;
	int dimensions;
	int probes = ivfflat_probes;
	int maxProbes;
	MemoryContext oldCtx;

	init_num_workers();
	used_workers = 0;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	/* Get lists and dimensions from metapage */
	IvfflatGetMetaPageInfo(index, &lists, &dimensions);

	if (ivfflat_iterative_scan != IVFFLAT_ITERATIVE_SCAN_OFF)
		maxProbes = Max(ivfflat_max_probes, probes);
	else
		maxProbes = probes;

	if (probes > lists)
		probes = lists;

	if (maxProbes > lists)
		maxProbes = lists;

	so = (IvfflatScanOpaque)palloc(sizeof(IvfflatScanOpaqueData));
	so->typeInfo = IvfflatGetTypeInfo(index);
	so->first = true;
	so->probes = probes;
	so->maxProbes = maxProbes;
	so->dimensions = dimensions;

	/* Set support functions */
	so->procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
	so->normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	so->collation = index->rd_indcollation[0];

	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Ivfflat scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);

	/* Create tuple description for sorting */
	so->tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(so->tupdesc, (AttrNumber)1, "distance", FLOAT8OID, -1, 0);
	TupleDescInitEntry(so->tupdesc, (AttrNumber)2, "heaptid", TIDOID, -1, 0);

	/* Prep sort */
	so->sortstate = InitScanSortState(so->tupdesc);

	/* Need separate slots for puttuple and gettuple */
	so->vslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
	so->mslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);

	/*
	 * Reuse same set of shared buffers for scan
	 *
	 * See postgres/src/backend/storage/buffer/README for description
	 */
	so->bas = GetAccessStrategy(BAS_BULKREAD);

	so->listQueue = pairingheap_allocate(CompareLists, scan);
	so->listPages = palloc(maxProbes * sizeof(BlockNumber));
	so->listIndex = 0;
	so->lists = palloc(maxProbes * sizeof(IvfflatScanList));

	MemoryContextSwitchTo(oldCtx);

	scan->opaque = so;

	return scan;
}

/*
 * Start or restart an index scan
 */
void ivfflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;

	so->first = true;
	pairingheap_reset(so->listQueue);
	so->listIndex = 0;

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));

	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
}

/*
 * Fetch the next tuple in the given scan
 */
bool ivfflatgettuple(IndexScanDesc scan, ScanDirection dir)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
	ItemPointer heaptid;
	bool isnull;

	/*
	 * Index can be used to scan backward, but Postgres doesn't support
	 * backward scan on operators
	 */
	Assert(ScanDirectionIsForward(dir));

	if (so->first)
	{

		Datum value;

		/* Count index scan for stats */
		pgstat_count_index_scan(scan->indexRelation);

		/* Safety check */
		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan ivfflat index without order");

		/* Requires MVCC-compliant snapshot as not able to pin during sorting */
		/* https://www.postgresql.org/docs/current/index-locking.html */
		if (!IsMVCCSnapshot(scan->xs_snapshot))
			elog(ERROR, "non-MVCC snapshots are not supported with ivfflat");

		value = GetScanValue(scan);

		struct timespec start, end;
		double elapsed;

		GetScanLists(scan, value);
		if (NUM_WORKERS > 0)
		{
			// Apply parallel index scan
			ParallelGetScanItems(scan, value);
		}
		else
		{
			// Apply sequential index scan
			GetScanItems(scan, value);
		}

		so->first = false;
		so->value = value;
	}

	if (NUM_WORKERS > 0)
	{
		// Keep a cursor to the result list of each worker in the shared memory.
		// Perform at most #worker comparisons and return the result from the worker that has the smallest distance to the query vector
		int best_worker = -1;
		double best_distance = DBL_MAX;

		for (int w = 0; w < used_workers; w++)
		{
			if (tasks[w]->return_result_pos < tasks[w]->results_count)
			{
				BlockNumber *blocknos = (BlockNumber *)tasks[w]->data;
				ResultItem *results = (ResultItem *)(blocknos + tasks[w]->num_blocks);

				double dist = results[tasks[w]->return_result_pos].distance;
				if (dist < best_distance)
				{
					best_distance = dist;
					best_worker = w;
				}
			}
		}

		// No more results
		if (best_worker == -1)
		{
			return false;
		}

		BlockNumber *blocknos = (BlockNumber *)tasks[best_worker]->data;
		ResultItem *results = (ResultItem *)(blocknos + tasks[best_worker]->num_blocks);
		ResultItem *item = &results[tasks[best_worker]->return_result_pos++];

		scan->xs_heaptid = item->tid;
		scan->xs_recheck = false;
		scan->xs_recheckorderby = false;
	}
	else
	{
		// No more results
		if (!tuplesort_gettupleslot(so->sortstate, true, false, so->mslot, NULL))
		{
			return false;
		}

		Datum distDatum = slot_getattr(so->mslot, 1, &isnull);
		heaptid = (ItemPointer)DatumGetPointer(slot_getattr(so->mslot, 2, &isnull));

		scan->xs_heaptid = *heaptid;
		scan->xs_recheck = false;
		scan->xs_recheckorderby = false;
	}

	return true;
}

/*
 * End a scan and release resources
 */
void ivfflatendscan(IndexScanDesc scan)
{
	// Deallocate the reserved space in case parallel algorithm executed
	if (NUM_WORKERS > 0)
	{
		for (int w = 0; w < used_workers; w++)
		{
			dsm_detach(segments[w]);
		}
		dsm_detach(shared_info_seg);

		pfree(tasks);
		pfree(segments);
	}

	IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;

	/* Free any temporary files */
	tuplesort_end(so->sortstate);

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}
