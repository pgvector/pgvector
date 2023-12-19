#include "postgres.h"

#include <math.h>

#include "access/parallel.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "hnsw.h"
#include "miscadmin.h"
#include "lib/pairingheap.h"
#include "nodes/pg_list.h"
#include "storage/bufmgr.h"
#include "tcop/tcopprot.h"
#include "utils/datum.h"
#include "utils/memutils.h"

#if PG_VERSION_NUM >= 140000
#include "utils/backend_progress.h"
#elif PG_VERSION_NUM >= 120000
#include "pgstat.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "access/tableam.h"
#include "commands/progress.h"
#else
#define PROGRESS_CREATEIDX_TUPLES_DONE 0
#endif

#if PG_VERSION_NUM >= 130000
#define CALLBACK_ITEM_POINTER ItemPointer tid
#else
#define CALLBACK_ITEM_POINTER HeapTuple hup
#endif

#if PG_VERSION_NUM >= 120000
#define UpdateProgress(index, val) pgstat_progress_update_param(index, val)
#else
#define UpdateProgress(index, val) ((void)val)
#endif

#if PG_VERSION_NUM >= 140000
#include "utils/backend_status.h"
#include "utils/wait_event.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#include "optimizer/optimizer.h"
#else
#include "access/heapam.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#endif

#define PARALLEL_KEY_HNSW_SHARED		UINT64CONST(0xA000000000000001)
#define PARALLEL_KEY_QUERY_TEXT			UINT64CONST(0xA000000000000002)

#define LIST_MAX_LENGTH ((1 << 26) - 1)

static long HnswElementMemory(HnswElement e, int m);

/*
 * Create the metapage
 */
static void
CreateMetaPage(HnswBuildState * buildstate)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	HnswMetaPage metap;

	buf = HnswNewBuffer(index, forkNum);
	HnswInitRegisterPage(index, &buf, &page, &state);

	/* Set metapage data */
	metap = HnswPageGetMeta(page);
	metap->magicNumber = HNSW_MAGIC_NUMBER;
	metap->version = HNSW_VERSION;
	metap->dimensions = buildstate->dimensions;
	metap->m = buildstate->m;
	metap->efConstruction = buildstate->efConstruction;
	metap->entryBlkno = InvalidBlockNumber;
	metap->entryOffno = InvalidOffsetNumber;
	metap->entryLevel = -1;
	metap->insertPage = InvalidBlockNumber;
	((PageHeader) page)->pd_lower =
		((char *) metap + sizeof(HnswMetaPageData)) - (char *) page;

	HnswCommitBuffer(buf, state);
}

/*
 * Add a new page
 */
static void
HnswBuildAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum)
{
	/* Add a new page */
	Buffer		newbuf = HnswNewBuffer(index, forkNum);

	/* Update previous page */
	HnswPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

	/* Commit */
	GenericXLogFinish(*state);
	UnlockReleaseBuffer(*buf);

	/* Can take a while, so ensure we can interrupt */
	/* Needs to be called when no buffer locks are held */
	LockBuffer(newbuf, BUFFER_LOCK_UNLOCK);
	CHECK_FOR_INTERRUPTS();
	LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);

	/* Prepare new page */
	*buf = newbuf;
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	HnswInitPage(*buf, *page);
}

/*
 * Create element pages
 */
static void
CreateElementPages(HnswBuildState * buildstate)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	Size		etupAllocSize;
	Size		maxSize;
	HnswElementTuple etup;
	HnswNeighborTuple ntup;
	BlockNumber insertPage;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	ListCell   *lc;

	/* Calculate sizes */
	etupAllocSize = BLCKSZ;
	maxSize = HNSW_MAX_SIZE;

	/* Allocate once */
	etup = palloc0(etupAllocSize);
	ntup = palloc0(BLCKSZ);

	/* Prepare first page */
	buf = HnswNewBuffer(index, forkNum);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
	HnswInitPage(buf, page);

	foreach(lc, buildstate->elements)
	{
		HnswElement element = lfirst(lc);
		Size		etupSize;
		Size		ntupSize;
		Size		combinedSize;

		/* Zero memory for each element */
		MemSet(etup, 0, etupAllocSize);

		/* Calculate sizes */
		etupSize = HNSW_ELEMENT_TUPLE_SIZE(VARSIZE_ANY(DatumGetPointer(element->value)));
		ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(element->level, buildstate->m);
		combinedSize = etupSize + ntupSize + sizeof(ItemIdData);

		/* Initial size check */
		if (etupSize > etupAllocSize)
			elog(ERROR, "index tuple too large");

		HnswSetElementTuple(etup, element);

		/* Keep element and neighbors on the same page if possible */
		if (PageGetFreeSpace(page) < etupSize || (combinedSize <= maxSize && PageGetFreeSpace(page) < combinedSize))
			HnswBuildAppendPage(index, &buf, &page, &state, forkNum);

		/* Calculate offsets */
		element->blkno = BufferGetBlockNumber(buf);
		element->offno = OffsetNumberNext(PageGetMaxOffsetNumber(page));
		if (combinedSize <= maxSize)
		{
			element->neighborPage = element->blkno;
			element->neighborOffno = OffsetNumberNext(element->offno);
		}
		else
		{
			element->neighborPage = element->blkno + 1;
			element->neighborOffno = FirstOffsetNumber;
		}

		ItemPointerSet(&etup->neighbortid, element->neighborPage, element->neighborOffno);

		/* Add element */
		if (PageAddItem(page, (Item) etup, etupSize, InvalidOffsetNumber, false, false) != element->offno)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		/* Add new page if needed */
		if (PageGetFreeSpace(page) < ntupSize)
			HnswBuildAppendPage(index, &buf, &page, &state, forkNum);

		/* Add placeholder for neighbors */
		if (PageAddItem(page, (Item) ntup, ntupSize, InvalidOffsetNumber, false, false) != element->neighborOffno)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
	}

	insertPage = BufferGetBlockNumber(buf);

	/* Commit */
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	HnswUpdateMetaPage(index, HNSW_UPDATE_ENTRY_ALWAYS, buildstate->entryPoint, insertPage, forkNum);

	pfree(etup);
	pfree(ntup);
}

/*
 * Create neighbor pages
 */
static void
CreateNeighborPages(HnswBuildState * buildstate)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	int			m = buildstate->m;
	ListCell   *lc;
	HnswNeighborTuple ntup;

	/* Allocate once */
	ntup = palloc0(BLCKSZ);

	foreach(lc, buildstate->elements)
	{
		HnswElement e = lfirst(lc);
		Buffer		buf;
		Page		page;
		GenericXLogState *state;
		Size		ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(e->level, m);

		/* Can take a while, so ensure we can interrupt */
		/* Needs to be called when no buffer locks are held */
		CHECK_FOR_INTERRUPTS();

		buf = ReadBufferExtended(index, forkNum, e->neighborPage, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		HnswSetNeighborTuple(ntup, e, m);

		if (!PageIndexTupleOverwrite(page, e->neighborOffno, (Item) ntup, ntupSize))
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		/* Commit */
		GenericXLogFinish(state);
		UnlockReleaseBuffer(buf);
	}

	pfree(ntup);
}

/*
 * Flush pages
 */
static void
FlushPages(HnswBuildState * buildstate)
{
	elog(LOG, "memoryUsed: %lu", maintenance_work_mem * 1024L - buildstate->memoryLeft);
	MemoryContextStats(buildstate->memGraphCtx);

	CreateMetaPage(buildstate);
	CreateElementPages(buildstate);
	CreateNeighborPages(buildstate);

	/* Free the in-memory graph. It's all held in 'memGraphCtx'. */
	MemoryContextDelete(buildstate->memGraphCtx);
	buildstate->memGraphCtx = NULL;
	buildstate->elements = NIL;

	buildstate->flushed = true;
}

/*
 * Insert tuple
 */
static bool
InsertTupleInMemory(Relation index, ItemPointer heaptid, Datum *values, HnswBuildState * buildstate)
{
	FmgrInfo   *procinfo = buildstate->procinfo;
	Oid			collation = buildstate->collation;
	HnswElement entryPoint = buildstate->entryPoint;
	int			efConstruction = buildstate->efConstruction;
	int			m = buildstate->m;
	HnswElement element;
	HnswElement dup;
	Datum		value;

	/*
	 * Detoast once for all calls. Detoast can allocate, so switch to temp
	 * memory context first.
	 */
	MemoryContextSwitchTo(buildstate->tmpCtx);
	value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

	/* Normalize if needed */
	if (buildstate->normprocinfo != NULL)
	{
		if (!HnswNormValue(buildstate->normprocinfo, collation, &value, buildstate->normvec))
			return false;
	}

	/* Allocate element in the long-lived memory context */
	MemoryContextSwitchTo(buildstate->memGraphCtx);
	element = HnswInitElement(heaptid, buildstate->m, buildstate->ml, buildstate->maxLevel);
	element->value = datumCopy(value, false, -1);
	MemoryContextSwitchTo(buildstate->tmpCtx);

	/* Insert element in graph */
	HnswInsertElement(element, entryPoint, NULL, procinfo, collation, m, efConstruction, false);

	/* Look for duplicate */
	dup = HnswFindDuplicate(element);

	/* Update neighbors if needed */
	if (dup == NULL)
	{
		for (int lc = element->level; lc >= 0; lc--)
		{
			int			lm = HnswGetLayerM(m, lc);
			HnswNeighborArray *neighbors = &element->neighbors[lc];

			for (int i = 0; i < neighbors->length; i++)
				HnswUpdateConnection(element, &neighbors->items[i], lm, lc, NULL, NULL, procinfo, collation);
		}
	}

	/* Update entry point if needed */
	if (dup == NULL && (entryPoint == NULL || element->level > entryPoint->level))
		buildstate->entryPoint = element;

	/* Add to buildstate or free */
	MemoryContextSwitchTo(buildstate->memGraphCtx);
	if (dup != NULL)
	{
		HnswAddHeapTid(dup, heaptid);
		buildstate->memoryLeft -= sizeof(ItemPointerData);
		HnswFreeElement(element);
		return false;
	}
	else
	{
		buildstate->elements = lappend(buildstate->elements, element);
		buildstate->memoryLeft -= HnswElementMemory(element, buildstate->m);
		return true;
	}
}

/*
 * Get the memory used by an element
 */
static long
HnswElementMemory(HnswElement e, int m)
{
	long		elementSize = sizeof(HnswElementData);

	elementSize += sizeof(HnswNeighborArray) * (e->level + 1);
	elementSize += sizeof(HnswCandidate) * (m * (e->level + 2));
	elementSize += sizeof(ItemPointerData);
	elementSize += VARSIZE_ANY(DatumGetPointer(e->value));
	return elementSize;
}

/*
 * Callback for table_index_build_scan
 */
static void
BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values,
			  bool *isnull, bool tupleIsAlive, void *state)
{
	HnswBuildState *buildstate = (HnswBuildState *) state;
	MemoryContext oldCtx = CurrentMemoryContext;

#if PG_VERSION_NUM < 130000
	ItemPointer tid = &hup->t_self;
#endif

	/* Skip nulls */
	if (isnull[0])
		return;

	/* Is it time to switch from in-memory to on-disk build? */
	if (!buildstate->flushed && (buildstate->memoryLeft <= 0 || list_length(buildstate->elements) == LIST_MAX_LENGTH))
	{
		if (buildstate->memoryLeft <= 0)
			ereport(NOTICE,
					(errmsg("hnsw graph no longer fits into maintenance_work_mem after " INT64_FORMAT " tuples", (int64) buildstate->indtuples),
					 errdetail("Building will take significantly more time."),
					 errhint("Increase maintenance_work_mem to speed up builds.")));

		FlushPages(buildstate);
	}

	if (buildstate->flushed)
	{
		oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

		if (HnswInsertTuple(buildstate->index, values, isnull, tid, buildstate->heap))
		{
			if (buildstate->hnswshared)
			{
				HnswShared *hnswshared = buildstate->hnswshared;

				SpinLockAcquire(&hnswshared->mutex);
				UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++hnswshared->indtuples);
				SpinLockRelease(&hnswshared->mutex);
			}
			else
				UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++buildstate->indtuples);
		}
	}
	else
	{
		if (InsertTupleInMemory(index, tid, values, buildstate))
			UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++buildstate->indtuples);
	}

	/* Reset memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Initialize the build state
 */
static void
InitBuildState(HnswBuildState * buildstate, Relation heap, Relation index, IndexInfo *indexInfo, ForkNumber forkNum)
{
	buildstate->heap = heap;
	buildstate->index = index;
	buildstate->indexInfo = indexInfo;
	buildstate->forkNum = forkNum;

	buildstate->m = HnswGetM(index);
	buildstate->efConstruction = HnswGetEfConstruction(index);
	buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

	/* Require column to have dimensions to be indexed */
	if (buildstate->dimensions < 0)
		elog(ERROR, "column does not have dimensions");

	if (buildstate->dimensions > HNSW_MAX_DIM)
		elog(ERROR, "column cannot have more than %d dimensions for hnsw index", HNSW_MAX_DIM);

	if (buildstate->efConstruction < 2 * buildstate->m)
		elog(ERROR, "ef_construction must be greater than or equal to 2 * m");

	buildstate->reltuples = 0;
	buildstate->indtuples = 0;

	/* Get support functions */
	buildstate->procinfo = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
	buildstate->normprocinfo = HnswOptionalProcInfo(index, HNSW_NORM_PROC);
	buildstate->collation = index->rd_indcollation[0];

	buildstate->elements = NIL;
	buildstate->entryPoint = NULL;
	buildstate->ml = HnswGetMl(buildstate->m);
	buildstate->maxLevel = HnswGetMaxLevel(buildstate->m);
	buildstate->memoryLeft = maintenance_work_mem * 1024L;
	buildstate->flushed = false;

	/* Reuse for each tuple */
	buildstate->normvec = InitVector(buildstate->dimensions);

	buildstate->memGraphCtx = AllocSetContextCreate(CurrentMemoryContext,
											   "Hnsw in-memory build context",
											   ALLOCSET_DEFAULT_SIZES);
	buildstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											   "Hnsw build temporary context",
											   ALLOCSET_DEFAULT_SIZES);

	buildstate->hnswleader = NULL;
	buildstate->hnswshared = NULL;
}

/*
 * Free resources
 */
static void
FreeBuildState(HnswBuildState * buildstate)
{
	pfree(buildstate->normvec);
	MemoryContextDelete(buildstate->tmpCtx);
	if (buildstate->memGraphCtx)
		MemoryContextDelete(buildstate->memGraphCtx);
}

/*
 * Within leader, wait for end of heap scan
 */
static double
ParallelHeapScan(HnswBuildState * buildstate)
{
	HnswShared *hnswshared = buildstate->hnswleader->hnswshared;
	int			nparticipanttuplesorts;
	double		reltuples;

	nparticipanttuplesorts = buildstate->hnswleader->nparticipanttuplesorts;
	for (;;)
	{
		SpinLockAcquire(&hnswshared->mutex);
		if (hnswshared->nparticipantsdone == nparticipanttuplesorts)
		{
			buildstate->indtuples = hnswshared->indtuples;
			reltuples = hnswshared->reltuples;
			SpinLockRelease(&hnswshared->mutex);
			break;
		}
		SpinLockRelease(&hnswshared->mutex);

		ConditionVariableSleep(&hnswshared->workersdonecv,
							   WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
	}

	ConditionVariableCancelSleep();

	return reltuples;
}

/*
 * Perform a worker's portion of a parallel insert
 */
static void
HnswParallelScanAndInsert(HnswSpool * hnswspool, HnswShared * hnswshared, bool progress)
{
	HnswBuildState buildstate;
#if PG_VERSION_NUM >= 120000
	TableScanDesc scan;
#else
	HeapScanDesc scan;
#endif
	double		reltuples;
	IndexInfo  *indexInfo;

	/* Join parallel scan */
	indexInfo = BuildIndexInfo(hnswspool->index);
	indexInfo->ii_Concurrent = hnswshared->isconcurrent;
	InitBuildState(&buildstate, hnswspool->heap, hnswspool->index, indexInfo, MAIN_FORKNUM);
	/* TODO Support in-memory builds */
	buildstate.memoryLeft = 0;
	buildstate.flushed = true;
	buildstate.hnswshared = hnswshared;
#if PG_VERSION_NUM >= 120000
	scan = table_beginscan_parallel(hnswspool->heap,
									ParallelTableScanFromHnswShared(hnswshared));
	reltuples = table_index_build_scan(hnswspool->heap, hnswspool->index, indexInfo,
									   true, progress, BuildCallback,
									   (void *) &buildstate, scan);
#else
	scan = heap_beginscan_parallel(hnswspool->heap, &hnswshared->heapdesc);
	reltuples = IndexBuildHeapScan(hnswspool->heap, hnswspool->index, indexInfo,
								   true, BuildCallback,
								   (void *) &buildstate, scan);
#endif

	/* Record statistics */
	SpinLockAcquire(&hnswshared->mutex);
	hnswshared->nparticipantsdone++;
	hnswshared->reltuples += reltuples;
	SpinLockRelease(&hnswshared->mutex);

	/* Log statistics */
	if (progress)
		ereport(DEBUG1, (errmsg("leader processed " INT64_FORMAT " tuples", (int64) reltuples)));
	else
		ereport(DEBUG1, (errmsg("worker processed " INT64_FORMAT " tuples", (int64) reltuples)));

	/* Notify leader */
	ConditionVariableSignal(&hnswshared->workersdonecv);

	FreeBuildState(&buildstate);
}

/*
 * Perform work within a launched parallel process
 */
void
HnswParallelBuildMain(dsm_segment *seg, shm_toc *toc)
{
	char	   *sharedquery;
	HnswSpool  *hnswspool;
	HnswShared *hnswshared;
	Relation	heapRel;
	Relation	indexRel;
	LOCKMODE	heapLockmode;
	LOCKMODE	indexLockmode;

	/* Set debug_query_string for individual workers first */
	sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true);
	debug_query_string = sharedquery;

	/* Report the query string from leader */
	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	/* Look up shared state */
	hnswshared = shm_toc_lookup(toc, PARALLEL_KEY_HNSW_SHARED, false);

	/* Open relations using lock modes known to be obtained by index.c */
	if (!hnswshared->isconcurrent)
	{
		heapLockmode = ShareLock;
		indexLockmode = AccessExclusiveLock;
	}
	else
	{
		heapLockmode = ShareUpdateExclusiveLock;
		indexLockmode = RowExclusiveLock;
	}

	/* Open relations within worker */
#if PG_VERSION_NUM >= 120000
	heapRel = table_open(hnswshared->heaprelid, heapLockmode);
#else
	heapRel = heap_open(hnswshared->heaprelid, heapLockmode);
#endif
	indexRel = index_open(hnswshared->indexrelid, indexLockmode);

	/* Initialize worker's own spool */
	hnswspool = (HnswSpool *) palloc0(sizeof(HnswSpool));
	hnswspool->heap = heapRel;
	hnswspool->index = indexRel;

	/* Perform inserts */
	HnswParallelScanAndInsert(hnswspool, hnswshared, false);

	/* Close relations within worker */
	index_close(indexRel, indexLockmode);
#if PG_VERSION_NUM >= 120000
	table_close(heapRel, heapLockmode);
#else
	heap_close(heapRel, heapLockmode);
#endif
}

/*
 * End parallel build
 */
static void
HnswEndParallel(HnswLeader * hnswleader)
{
	/* Shutdown worker processes */
	WaitForParallelWorkersToFinish(hnswleader->pcxt);

	/* Free last reference to MVCC snapshot, if one was used */
	if (IsMVCCSnapshot(hnswleader->snapshot))
		UnregisterSnapshot(hnswleader->snapshot);
	DestroyParallelContext(hnswleader->pcxt);
	ExitParallelMode();
}

/*
 * Return size of shared memory required for parallel index build
 */
static Size
ParallelEstimateShared(Relation heap, Snapshot snapshot)
{
#if PG_VERSION_NUM >= 120000
	return add_size(BUFFERALIGN(sizeof(HnswShared)), table_parallelscan_estimate(heap, snapshot));
#else
	if (!IsMVCCSnapshot(snapshot))
	{
		Assert(snapshot == SnapshotAny);
		return sizeof(HnswShared);
	}

	return add_size(offsetof(HnswShared, heapdesc) +
					offsetof(ParallelHeapScanDescData, phs_snapshot_data),
					EstimateSnapshotSpace(snapshot));
#endif
}

/*
 * Within leader, participate as a parallel worker
 */
static void
HnswLeaderParticipateAsWorker(HnswBuildState * buildstate)
{
	HnswLeader *hnswleader = buildstate->hnswleader;
	HnswSpool  *leaderworker;

	/* Allocate memory and initialize private spool */
	leaderworker = (HnswSpool *) palloc0(sizeof(HnswSpool));
	leaderworker->heap = buildstate->heap;
	leaderworker->index = buildstate->index;

	/* Perform work common to all participants */
	HnswParallelScanAndInsert(leaderworker, hnswleader->hnswshared, true);
}

/*
 * Begin parallel build
 */
static void
HnswBeginParallel(HnswBuildState * buildstate, bool isconcurrent, int request)
{
	ParallelContext *pcxt;
	int			scantuplesortstates;
	Snapshot	snapshot;
	Size		esthnswshared;
	HnswShared *hnswshared;
	HnswLeader *hnswleader = (HnswLeader *) palloc0(sizeof(HnswLeader));
	bool		leaderparticipates = true;
	int			querylen;

#ifdef DISABLE_LEADER_PARTICIPATION
	leaderparticipates = false;
#endif

	/* Enter parallel mode and create context */
	EnterParallelMode();
	Assert(request > 0);
#if PG_VERSION_NUM >= 120000
	pcxt = CreateParallelContext("vector", "HnswParallelBuildMain", request);
#else
	pcxt = CreateParallelContext("vector", "HnswParallelBuildMain", request, true);
#endif

	scantuplesortstates = leaderparticipates ? request + 1 : request;

	/* Get snapshot for table scan */
	if (!isconcurrent)
		snapshot = SnapshotAny;
	else
		snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/* Estimate size of workspaces */
	esthnswshared = ParallelEstimateShared(buildstate->heap, snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, esthnswshared);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Finally, estimate PARALLEL_KEY_QUERY_TEXT space */
	if (debug_query_string)
	{
		querylen = strlen(debug_query_string);
		shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
	else
		querylen = 0;			/* keep compiler quiet */

	/* Everyone's had a chance to ask for space, so now create the DSM */
	InitializeParallelDSM(pcxt);

	/* If no DSM segment was available, back out (do serial build) */
	if (pcxt->seg == NULL)
	{
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return;
	}

	/* Store shared build state, for which we reserved space */
	hnswshared = (HnswShared *) shm_toc_allocate(pcxt->toc, esthnswshared);
	/* Initialize immutable state */
	hnswshared->heaprelid = RelationGetRelid(buildstate->heap);
	hnswshared->indexrelid = RelationGetRelid(buildstate->index);
	hnswshared->isconcurrent = isconcurrent;
	hnswshared->scantuplesortstates = scantuplesortstates;
	ConditionVariableInit(&hnswshared->workersdonecv);
	SpinLockInit(&hnswshared->mutex);
	/* Initialize mutable state */
	hnswshared->nparticipantsdone = 0;
	hnswshared->reltuples = 0;
	hnswshared->indtuples = 0;
#if PG_VERSION_NUM >= 120000
	table_parallelscan_initialize(buildstate->heap,
								  ParallelTableScanFromHnswShared(hnswshared),
								  snapshot);
#else
	heap_parallelscan_initialize(&hnswshared->heapdesc, buildstate->heap, snapshot);
#endif

	shm_toc_insert(pcxt->toc, PARALLEL_KEY_HNSW_SHARED, hnswshared);

	/* Store query string for workers */
	if (debug_query_string)
	{
		char	   *sharedquery;

		sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
		memcpy(sharedquery, debug_query_string, querylen + 1);
		shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, sharedquery);
	}

	/* Launch workers, saving status for leader/caller */
	LaunchParallelWorkers(pcxt);
	hnswleader->pcxt = pcxt;
	hnswleader->nparticipanttuplesorts = pcxt->nworkers_launched;
	if (leaderparticipates)
		hnswleader->nparticipanttuplesorts++;
	hnswleader->hnswshared = hnswshared;
	hnswleader->snapshot = snapshot;

	/* If no workers were successfully launched, back out (do serial build) */
	if (pcxt->nworkers_launched == 0)
	{
		HnswEndParallel(hnswleader);
		return;
	}

	/* Log participants */
	ereport(DEBUG1, (errmsg("using %d parallel workers", pcxt->nworkers_launched)));

	/* Save leader state now that it's clear build will be parallel */
	buildstate->hnswleader = hnswleader;

	/* Join heap scan ourselves */
	if (leaderparticipates)
		HnswLeaderParticipateAsWorker(buildstate);

	/* Wait for all launched workers */
	WaitForParallelWorkersToAttach(pcxt);
}

/*
 * Build graph
 */
static void
BuildGraph(HnswBuildState * buildstate, ForkNumber forkNum)
{
	int			parallel_workers = 0;

	UpdateProgress(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_HNSW_PHASE_LOAD);

	/* Calculate parallel workers */
	if (hnsw_enable_parallel_build)
		parallel_workers = plan_create_index_workers(RelationGetRelid(buildstate->heap), RelationGetRelid(buildstate->index));

	/* Attempt to launch parallel worker scan when required */
	if (parallel_workers > 0)
	{
		/* TODO Support in-memory builds */
		FlushPages(buildstate);
		HnswBeginParallel(buildstate, buildstate->indexInfo->ii_Concurrent, parallel_workers);
	}

	/* Add tuples to sort */
	if (buildstate->hnswleader)
		buildstate->reltuples = ParallelHeapScan(buildstate);
	else
	{
#if PG_VERSION_NUM >= 120000
		buildstate->reltuples = table_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
													   true, true, BuildCallback, (void *) buildstate, NULL);
#else
		buildstate->reltuples = IndexBuildHeapScan(buildstate->heap, buildstate->index, buildstate->indexInfo,
												   true, BuildCallback, (void *) buildstate, NULL);
#endif
	}

	/* End parallel build */
	if (buildstate->hnswleader)
		HnswEndParallel(buildstate->hnswleader);
}

/*
 * Build the index
 */
static void
BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo,
		   HnswBuildState * buildstate, ForkNumber forkNum)
{
	InitBuildState(buildstate, heap, index, indexInfo, forkNum);

	if (buildstate->heap != NULL)
		BuildGraph(buildstate, forkNum);

	if (!buildstate->flushed)
		FlushPages(buildstate);

	FreeBuildState(buildstate);
}

/*
 * Build the index for a logged table
 */
IndexBuildResult *
hnswbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	HnswBuildState buildstate;

	BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}

/*
 * Build the index for an unlogged table
 */
void
hnswbuildempty(Relation index)
{
	IndexInfo  *indexInfo = BuildIndexInfo(index);
	HnswBuildState buildstate;

	BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}
