#include "postgres.h"

#include <float.h>
#include <math.h>

#include "access/generic_xlog.h"
#include "access/parallel.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"	/* plan_create_index_workers */
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/lmgr.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "tqivf.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tuplesort.h"
#include "vector.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

#if PG_VERSION_NUM >= 140000
#include "utils/backend_status.h"
#include "utils/wait_event.h"
#endif

#define PARALLEL_KEY_TQIVF_SHARED		UINT64CONST(0xB000000000000001)
#define PARALLEL_KEY_TUPLESORT			UINT64CONST(0xB000000000000002)
#define PARALLEL_KEY_TQIVF_CENTERS		UINT64CONST(0xB000000000000003)
#define PARALLEL_KEY_TQIVF_MODEL		UINT64CONST(0xB000000000000004)
#define PARALLEL_KEY_QUERY_TEXT			UINT64CONST(0xB000000000000005)

/*
 * Per-list streaming cursor for emitting one list's TurboQuant block stream.
 *
 * tqflat keeps a single global code/side cursor in its TqBuildState; tqivf
 * needs one cursor per list (lists are written contiguously, one at a time, so
 * a single reused cursor structure that is reset per list suffices).  The
 * on-disk byte layout produced here is byte-for-byte identical to tqflat's
 * (same MAXALIGN chunking via TqIvfCodeAppend, same per-block side records via
 * PageAddItem), so the scan path can read it back with TqReadBytes +
 * TqScoreBlockRange exactly as for a tqflat index.
 */
typedef struct TqivfListCursor
{
	/* Code-plane chain */
	BlockNumber codeStart;
	Buffer		codeBuf;
	Page		codePage;
	GenericXLogState *codeState;

	/* Side-record chain */
	BlockNumber sideStart;
	Buffer		sideBuf;
	Page		sidePage;
	GenericXLogState *sideState;

	/* Block staging */
	uint8	   *codeStage;		/* TQ_BLOCK_CODE_BYTES(dimCodes) */
	TqBlockSideRec sideStage;
	int			slot;			/* next free lane 0..TQ_BLOCK_WIDTH-1 */
	uint32		blockCount;
	uint32		nvectors;
} TqivfListCursor;

/*
 * Shared state for a parallel tqivf build (mirror IvfflatShared).  Lives in the
 * DSM segment; the embedded ParallelTableScanDesc follows this struct.
 */
typedef struct TqivfShared
{
	/* Immutable state */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isconcurrent;
	int			scantuplesortstates;

	/* Worker progress */
	ConditionVariable workersdonecv;

	/* Mutex for mutable state */
	slock_t		mutex;

	/* Mutable state */
	int			nparticipantsdone;
	double		reltuples;
	double		indtuples;
} TqivfShared;

#define ParallelTableScanFromTqivfShared(shared) \
	(ParallelTableScanDesc) ((char *) (shared) + BUFFERALIGN(sizeof(TqivfShared)))

typedef struct TqivfSpool
{
	Relation	heap;
	Relation	index;
	Tuplesortstate *sortstate;
} TqivfSpool;

typedef struct TqivfLeader
{
	ParallelContext *pcxt;
	int			nparticipanttuplesorts;
	TqivfShared *tqivfshared;
	Sharedsort *sharedsort;
	Snapshot	snapshot;
	char	   *tqivfcenters;
	char	   *tqivfmodel;
} TqivfLeader;

typedef struct TqivfBuildState
{
	Relation	heap;
	Relation	index;
	IndexInfo  *indexInfo;
	ForkNumber	forkNum;

	/* Model parameters */
	int			dim;
	int			bits;
	int			nLevels;
	TqMetric	metric;
	bool		fastRotation;
	int			dimPadded;
	int			dimCodes;		/* dim, or next_pow2(dim) in fast mode */
	TqModel		model;
	const TqTypeInfo *typeInfo; /* tqivf type-info vtable (extractor + metric) */
	float	   *vecScratch;		/* dim floats, reused per tuple at encode */

	/* Clustering (full-precision, un-rotated centers) */
	int			lists;

	/* Assignment / sorting */
	FmgrInfo   *procinfo;		/* exact distance (proc 1) for assignment */
	FmgrInfo   *normprocinfo;	/* proc 2 (ip/cosine); NULL for l2 */
	Oid			collation;
	Tuplesortstate *sortstate;
	TupleDesc	sortdesc;
	TupleDesc	tupdesc;
	TupleTableSlot *slot;
	VectorArray centers;

	/* List directory locations for back-patching */
	ListInfo   *listInfo;

	/* Counters */
	double		reltuples;
	double		indtuples;

	/* Set only during a parallel build (NULL for serial). */
	TqivfLeader *tqivfleader;

	MemoryContext tmpCtx;
} TqivfBuildState;

/*
 * Append raw bytes to a list's streaming code-plane chain (replicated from
 * tqbuild.c's TqCodeAppend, operating on the per-list cursor).  Keep the
 * chunking logic byte-identical so the reassembled stream matches tqflat's.
 */
static void
TqIvfCodeAppend(Relation index, ForkNumber forkNum, TqivfListCursor *cur,
				const char *bytes, Size nbytes)
{
	Size		offset = 0;

	/* Lazily open the code chain on the first block flush. */
	if (cur->codePage == NULL)
	{
		cur->codeBuf = TqNewBuffer(index, forkNum);
		TqInitRegisterPage(index, &cur->codeBuf, &cur->codePage, &cur->codeState,
						   TQIVF_PAGE_ID);
		cur->codeStart = BufferGetBlockNumber(cur->codeBuf);
	}

	while (offset < nbytes)
	{
		Size		avail = PageGetFreeSpace(cur->codePage);
		Size		chunk;
		OffsetNumber offno;

		if (avail <= sizeof(ItemIdData))
		{
			TqAppendPage(index, &cur->codeBuf, &cur->codePage, &cur->codeState, forkNum,
						 TQIVF_PAGE_ID);
			continue;
		}

		chunk = avail - sizeof(ItemIdData);
		chunk = chunk - (chunk % MAXIMUM_ALIGNOF);
		if (chunk == 0)
		{
			TqAppendPage(index, &cur->codeBuf, &cur->codePage, &cur->codeState, forkNum,
						 TQIVF_PAGE_ID);
			continue;
		}
		if (chunk > nbytes - offset)
			chunk = nbytes - offset;

		offno = PageAddItem(cur->codePage, (Pointer) (bytes + offset), chunk,
							InvalidOffsetNumber, false, false);
		if (offno == InvalidOffsetNumber)
			elog(ERROR, "failed to add code-plane item to \"%s\"", RelationGetRelationName(index));

		offset += chunk;
	}
}

/*
 * Append one TqBlockSideRec to a list's side chain (replicated from tqbuild.c's
 * TqAppendSideRec, operating on the per-list cursor).
 */
static void
TqIvfAppendSideRec(Relation index, ForkNumber forkNum, TqivfListCursor *cur,
				   const TqBlockSideRec *rec)
{
	OffsetNumber offno;

	if (cur->sidePage == NULL)
	{
		cur->sideBuf = TqNewBuffer(index, forkNum);
		TqInitRegisterPage(index, &cur->sideBuf, &cur->sidePage, &cur->sideState,
						   TQIVF_PAGE_ID);
		cur->sideStart = BufferGetBlockNumber(cur->sideBuf);
	}

	if (PageGetFreeSpace(cur->sidePage) < sizeof(TqBlockSideRec))
		TqAppendPage(index, &cur->sideBuf, &cur->sidePage, &cur->sideState, forkNum,
					 TQIVF_PAGE_ID);

	offno = PageAddItem(cur->sidePage, (Pointer) rec, sizeof(TqBlockSideRec),
						InvalidOffsetNumber, false, false);
	if (offno == InvalidOffsetNumber)
		elog(ERROR, "failed to add tqivf side record to \"%s\"",
			 RelationGetRelationName(index));
}

/*
 * Flush the currently-staged block of a list cursor (replicated from
 * tqbuild.c's TqFlushBlock).
 */
static void
TqIvfFlushBlock(TqivfBuildState *buildstate, TqivfListCursor *cur)
{
	int			dc = buildstate->dimCodes;

	cur->sideStage.nvecs = (uint16) cur->slot;
	cur->sideStage.deletedMask = 0;
	cur->sideStage.pad = 0;

	TqIvfCodeAppend(buildstate->index, buildstate->forkNum, cur,
					(char *) cur->codeStage, TQ_BLOCK_CODE_BYTES(dc));
	TqIvfAppendSideRec(buildstate->index, buildstate->forkNum, cur, &cur->sideStage);

	memset(cur->codeStage, 0, TQ_BLOCK_CODE_BYTES(dc));
	cur->slot = 0;
	cur->blockCount++;
	MemSet(&cur->sideStage, 0, sizeof(cur->sideStage));
}

/*
 * Serialized TurboQuant model header for broadcasting to parallel workers.
 * Followed by float boundaries[nLevels-1], float centroids[nLevels], and (only
 * when !fastRotation) float rotation[dim*dim].
 */
typedef struct TqivfModelHeader
{
	int			dim;
	int			bits;
	int			nLevels;
	int			metric;			/* TqMetric */
	int			fastRotation;	/* bool */
	int			dimPadded;
	int			dimCodes;
	uint64		rotSeed;
	uint64		qjlSeed;
} TqivfModelHeader;

/* Size of the serialized model for the given model. */
static Size
TqivfModelSerializedSize(const TqModel *model)
{
	Size		sz = MAXALIGN(sizeof(TqivfModelHeader));

	sz += (Size) sizeof(float) * ((model->nLevels - 1) + model->nLevels);
	if (!model->fastRotation)
		sz += (Size) sizeof(float) * model->dim * model->dim;
	return sz;
}

/* Serialize model into buf (must be >= TqivfModelSerializedSize). */
static void
TqivfSerializeModel(const TqModel *model, char *buf)
{
	TqivfModelHeader *hdr = (TqivfModelHeader *) buf;
	char	   *p = buf + MAXALIGN(sizeof(TqivfModelHeader));

	hdr->dim = model->dim;
	hdr->bits = model->bits;
	hdr->nLevels = model->nLevels;
	hdr->metric = (int) model->metric;
	hdr->fastRotation = model->fastRotation ? 1 : 0;
	hdr->dimPadded = model->dimPadded;
	hdr->dimCodes = model->dimCodes;
	hdr->rotSeed = model->rotSeed;
	hdr->qjlSeed = model->qjlSeed;

	memcpy(p, model->boundaries, sizeof(float) * (model->nLevels - 1));
	p += sizeof(float) * (model->nLevels - 1);
	memcpy(p, model->centroids, sizeof(float) * model->nLevels);
	p += sizeof(float) * model->nLevels;
	if (!model->fastRotation)
		memcpy(p, model->rotation, (Size) sizeof(float) * model->dim * model->dim);
}

/*
 * Reconstruct a TqModel from a serialized buffer into model-> fields, palloc'ing
 * boundaries/centroids/rotation in the current memory context.  Sets tqProd off
 * and qjl NULL (tqivf blocked layout).
 */
static void
TqivfDeserializeModel(const char *buf, TqModel *model)
{
	const TqivfModelHeader *hdr = (const TqivfModelHeader *) buf;
	const char *p = buf + MAXALIGN(sizeof(TqivfModelHeader));

	model->dim = hdr->dim;
	model->bits = hdr->bits;
	model->nLevels = hdr->nLevels;
	model->metric = (TqMetric) hdr->metric;
	model->tqProd = false;
	model->fastRotation = hdr->fastRotation != 0;
	model->dimPadded = hdr->dimPadded;
	model->dimCodes = hdr->dimCodes;
	model->rotSeed = hdr->rotSeed;
	model->qjlSeed = hdr->qjlSeed;
	model->qjl = NULL;
	model->qjlScale = 0.0f;

	model->boundaries = palloc(sizeof(float) * (model->nLevels - 1));
	memcpy(model->boundaries, p, sizeof(float) * (model->nLevels - 1));
	p += sizeof(float) * (model->nLevels - 1);
	model->centroids = palloc(sizeof(float) * model->nLevels);
	memcpy(model->centroids, p, sizeof(float) * model->nLevels);
	p += sizeof(float) * model->nLevels;
	if (!model->fastRotation)
	{
		model->rotation = palloc((Size) sizeof(float) * model->dim * model->dim);
		memcpy(model->rotation, p, (Size) sizeof(float) * model->dim * model->dim);
	}
	else
		model->rotation = NULL;
}

/* Forward declarations for static helpers used by the parallel functions. */
static void TqivfInitBuildState(TqivfBuildState *buildstate, Relation heap,
								Relation index, IndexInfo *indexInfo,
								ForkNumber forkNum);
static void TqivfFreeBuildState(TqivfBuildState *buildstate);
static void TqivfBuildCallback(Relation index, ItemPointer tid, Datum *values,
							   bool *isnull, bool tupleIsAlive, void *state);

/* Size of shared memory for the parallel build (shared struct + parallel scan). */
static Size
ParallelEstimateShared(Relation heap, Snapshot snapshot)
{
	return add_size(BUFFERALIGN(sizeof(TqivfShared)),
					table_parallelscan_estimate(heap, snapshot));
}

/*
 * Within the leader, wait until all participants finish their scan+sort, then
 * collect the tuple counts.  Mirrors ivfbuild.c's ParallelHeapScan.
 */
static double
ParallelHeapScan(TqivfBuildState *buildstate)
{
	TqivfShared *tqivfshared = buildstate->tqivfleader->tqivfshared;
	int			nparticipanttuplesorts;
	double		reltuples;

	nparticipanttuplesorts = buildstate->tqivfleader->nparticipanttuplesorts;
	for (;;)
	{
		SpinLockAcquire(&tqivfshared->mutex);
		if (tqivfshared->nparticipantsdone == nparticipanttuplesorts)
		{
			buildstate->indtuples = tqivfshared->indtuples;
			reltuples = tqivfshared->reltuples;
			SpinLockRelease(&tqivfshared->mutex);
			break;
		}
		SpinLockRelease(&tqivfshared->mutex);

		ConditionVariableSleep(&tqivfshared->workersdonecv,
							   WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
	}
	ConditionVariableCancelSleep();

	return reltuples;
}

/*
 * Perform one participant's portion of the parallel scan + sort.  Used by both
 * the real workers and the leader-as-worker.  Reconstructs a local build state,
 * overrides centers + model from shared memory (so no side-page reads and no
 * model rebuild happen here), then runs the shared TqivfBuildCallback.
 */
static void
TqivfParallelScanAndSort(TqivfSpool *spool, TqivfShared *tqivfshared,
						 Sharedsort *sharedsort, char *tqivfcenters,
						 char *tqivfmodel, int sortmem, bool progress)
{
	SortCoordinate coordinate;
	TqivfBuildState buildstate;
	TableScanDesc scan;
	double		reltuples;
	IndexInfo  *indexInfo;
	AttrNumber	attNums[] = {1};
	Oid			sortOperators[] = {Int4LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};

	/* Local tuplesort coordination state (this participant is a worker). */
	coordinate = palloc0(sizeof(SortCoordinateData));
	coordinate->isWorker = true;
	coordinate->nParticipants = -1;
	coordinate->sharedsort = sharedsort;

	indexInfo = BuildIndexInfo(spool->index);
	indexInfo->ii_Concurrent = tqivfshared->isconcurrent;

	MemSet(&buildstate, 0, sizeof(buildstate));
	TqivfInitBuildState(&buildstate, spool->heap, spool->index, indexInfo, MAIN_FORKNUM);

	/* Override centers from shared memory (no k-means in the worker). */
	memcpy(buildstate.centers->items, tqivfcenters,
		   buildstate.centers->itemsize * buildstate.centers->maxlen);
	buildstate.centers->length = buildstate.centers->maxlen;

	/* Populate the model from shared memory (no rebuild, no side-page reads). */
	TqivfDeserializeModel(tqivfmodel, &buildstate.model);
	buildstate.dimPadded = buildstate.model.dimPadded;
	buildstate.dimCodes = buildstate.model.dimCodes;

	spool->sortstate = tuplesort_begin_heap(buildstate.sortdesc, 1, attNums,
											sortOperators, sortCollations,
											nullsFirstFlags, sortmem, coordinate, false);
	buildstate.sortstate = spool->sortstate;

	scan = table_beginscan_parallel(spool->heap,
									ParallelTableScanFromTqivfShared(tqivfshared)
#if PG_VERSION_NUM >= 190000
									,SO_NONE
#endif
		);
	reltuples = table_index_build_scan(spool->heap, spool->index, indexInfo,
									   true, progress, TqivfBuildCallback,
									   (void *) &buildstate, scan);

	tuplesort_performsort(spool->sortstate);

	SpinLockAcquire(&tqivfshared->mutex);
	tqivfshared->nparticipantsdone++;
	tqivfshared->reltuples += reltuples;
	tqivfshared->indtuples += buildstate.indtuples;
	SpinLockRelease(&tqivfshared->mutex);

	ConditionVariableSignal(&tqivfshared->workersdonecv);

	tuplesort_end(spool->sortstate);

	TqivfFreeBuildState(&buildstate);
}

/*
 * Entry point for a parallel build worker.  Looked up by name via
 * CreateParallelContext("vector", "TqivfParallelBuildMain", ...).
 */
void
TqivfParallelBuildMain(dsm_segment *seg, shm_toc *toc)
{
	char	   *sharedquery;
	TqivfSpool *spool;
	TqivfShared *tqivfshared;
	Sharedsort *sharedsort;
	char	   *tqivfcenters;
	char	   *tqivfmodel;
	Relation	heapRel;
	Relation	indexRel;
	LOCKMODE	heapLockmode;
	LOCKMODE	indexLockmode;
	int			sortmem;

	sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true);
	debug_query_string = sharedquery;
	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	tqivfshared = shm_toc_lookup(toc, PARALLEL_KEY_TQIVF_SHARED, false);

	if (!tqivfshared->isconcurrent)
	{
		heapLockmode = ShareLock;
		indexLockmode = AccessExclusiveLock;
	}
	else
	{
		heapLockmode = ShareUpdateExclusiveLock;
		indexLockmode = RowExclusiveLock;
	}

	heapRel = table_open(tqivfshared->heaprelid, heapLockmode);
	indexRel = index_open(tqivfshared->indexrelid, indexLockmode);

	spool = (TqivfSpool *) palloc0(sizeof(TqivfSpool));
	spool->heap = heapRel;
	spool->index = indexRel;

	sharedsort = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT, false);
	tuplesort_attach_shared(sharedsort, seg);

	tqivfcenters = shm_toc_lookup(toc, PARALLEL_KEY_TQIVF_CENTERS, false);
	tqivfmodel = shm_toc_lookup(toc, PARALLEL_KEY_TQIVF_MODEL, false);

	sortmem = maintenance_work_mem / tqivfshared->scantuplesortstates;
	TqivfParallelScanAndSort(spool, tqivfshared, sharedsort, tqivfcenters,
							 tqivfmodel, sortmem, false);

	index_close(indexRel, indexLockmode);
	table_close(heapRel, heapLockmode);
}

/* Leader runs one participant's share of the scan + sort. */
static void
TqivfLeaderParticipateAsWorker(TqivfBuildState *buildstate)
{
	TqivfLeader *leader = buildstate->tqivfleader;
	TqivfSpool *leaderworker;
	int			sortmem;

	leaderworker = (TqivfSpool *) palloc0(sizeof(TqivfSpool));
	leaderworker->heap = buildstate->heap;
	leaderworker->index = buildstate->index;

	sortmem = maintenance_work_mem / leader->nparticipanttuplesorts;
	TqivfParallelScanAndSort(leaderworker, leader->tqivfshared, leader->sharedsort,
							 leader->tqivfcenters, leader->tqivfmodel, sortmem, true);
}

/* Tear down the parallel context. */
static void
TqivfEndParallel(TqivfLeader *leader)
{
	WaitForParallelWorkersToFinish(leader->pcxt);
	if (IsMVCCSnapshot(leader->snapshot))
		UnregisterSnapshot(leader->snapshot);
	DestroyParallelContext(leader->pcxt);
	ExitParallelMode();
}

/*
 * Begin a parallel build: create the parallel context, broadcast shared state,
 * centers, and the TurboQuant model, launch workers, and have the leader join.
 * Leaves buildstate->tqivfleader NULL if no workers could be launched.
 */
static void
TqivfBeginParallel(TqivfBuildState *buildstate, bool isconcurrent, int request)
{
	ParallelContext *pcxt;
	int			scantuplesortstates;
	Snapshot	snapshot;
	Size		estshared;
	Size		estsort;
	Size		estcenters;
	Size		estmodel;
	TqivfShared *tqivfshared;
	Sharedsort *sharedsort;
	char	   *tqivfcenters;
	char	   *tqivfmodel;
	TqivfLeader *leader = (TqivfLeader *) palloc0(sizeof(TqivfLeader));
	bool		leaderparticipates = true;
	int			querylen;

	EnterParallelMode();
	Assert(request > 0);
	pcxt = CreateParallelContext("vector", "TqivfParallelBuildMain", request);

	scantuplesortstates = leaderparticipates ? request + 1 : request;

	if (!isconcurrent)
		snapshot = SnapshotAny;
	else
		snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/* Estimate DSM space: shared+scan, tuplesort, centers, model, query text. */
	estshared = ParallelEstimateShared(buildstate->heap, snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, estshared);
	estsort = tuplesort_estimate_shared(scantuplesortstates);
	shm_toc_estimate_chunk(&pcxt->estimator, estsort);
	estcenters = buildstate->centers->itemsize * buildstate->centers->maxlen;
	shm_toc_estimate_chunk(&pcxt->estimator, estcenters);
	estmodel = TqivfModelSerializedSize(&buildstate->model);
	shm_toc_estimate_chunk(&pcxt->estimator, estmodel);
	shm_toc_estimate_keys(&pcxt->estimator, 4);

	if (debug_query_string)
	{
		querylen = strlen(debug_query_string);
		shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
	else
		querylen = 0;

	InitializeParallelDSM(pcxt);

	/* No DSM available: back out, run serial. */
	if (pcxt->seg == NULL)
	{
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return;
	}

	tqivfshared = (TqivfShared *) shm_toc_allocate(pcxt->toc, estshared);
	tqivfshared->heaprelid = RelationGetRelid(buildstate->heap);
	tqivfshared->indexrelid = RelationGetRelid(buildstate->index);
	tqivfshared->isconcurrent = isconcurrent;
	tqivfshared->scantuplesortstates = scantuplesortstates;
	ConditionVariableInit(&tqivfshared->workersdonecv);
	SpinLockInit(&tqivfshared->mutex);
	tqivfshared->nparticipantsdone = 0;
	tqivfshared->reltuples = 0;
	tqivfshared->indtuples = 0;
	table_parallelscan_initialize(buildstate->heap,
								  ParallelTableScanFromTqivfShared(tqivfshared),
								  snapshot);

	sharedsort = (Sharedsort *) shm_toc_allocate(pcxt->toc, estsort);
	tuplesort_initialize_shared(sharedsort, scantuplesortstates, pcxt->seg);

	tqivfcenters = shm_toc_allocate(pcxt->toc, estcenters);
	memcpy(tqivfcenters, buildstate->centers->items, estcenters);

	tqivfmodel = shm_toc_allocate(pcxt->toc, estmodel);
	TqivfSerializeModel(&buildstate->model, tqivfmodel);

	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TQIVF_SHARED, tqivfshared);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLESORT, sharedsort);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TQIVF_CENTERS, tqivfcenters);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TQIVF_MODEL, tqivfmodel);

	if (debug_query_string)
	{
		char	   *sharedquery;

		sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
		memcpy(sharedquery, debug_query_string, querylen + 1);
		shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, sharedquery);
	}

	LaunchParallelWorkers(pcxt);

	/*
	 * If no workers launched, back out and run serial.  No
	 * WaitForParallelWorkersToFinish is needed (none were launched); leaving
	 * buildstate->tqivfleader NULL signals the caller to fall back to serial.
	 */
	if (pcxt->nworkers_launched == 0)
	{
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return;
	}

	ereport(DEBUG1, (errmsg("using %d parallel workers", pcxt->nworkers_launched)));

	leader->pcxt = pcxt;
	leader->nparticipanttuplesorts = pcxt->nworkers_launched;
	if (leaderparticipates)
		leader->nparticipanttuplesorts++;
	leader->tqivfshared = tqivfshared;
	leader->sharedsort = sharedsort;
	leader->snapshot = snapshot;
	leader->tqivfcenters = tqivfcenters;
	leader->tqivfmodel = tqivfmodel;

	buildstate->tqivfleader = leader;

	if (leaderparticipates)
		TqivfLeaderParticipateAsWorker(buildstate);

	WaitForParallelWorkersToAttach(pcxt);
}

/*
 * Build the global model (rotation + codebook) and write the codebook (and, in
 * dense mode, the rotation) to side pages.  This is TurboQuant's
 * data-OBLIVIOUS, list-independent model: ONE rotation + ONE codebook shared by
 * every list.  Replicated from tqbuild.c's TqBuildModelAndSidePages (the model
 * fields it writes into TqBuildState live in TqivfBuildState here, and it uses
 * the now-exported TqWriteBytes).  tqProd / QJL are unsupported in the blocked
 * blocked layout, so the QJL branch is omitted.
 */
static void
TqivfBuildModelAndSidePages(TqivfBuildState *buildstate, BlockNumber *rotStart,
							BlockNumber *cbStart)
{
	int			dim = buildstate->dim;
	int			bits = buildstate->bits;
	int			nLevels = buildstate->nLevels;
	int			nBnd = nLevels - 1;
	bool		fastRotation = buildstate->fastRotation;
	int			dimPadded = fastRotation ? TqNextPow2(dim) : dim;
	int			dimCodes = fastRotation ? dimPadded : dim;
	Size		rotBytes = (Size) sizeof(float) * dim * dim;
	Size		cbBytes = (Size) sizeof(float) * (nBnd + nLevels);
	char	   *cbBuf;

	buildstate->model.dim = dim;
	buildstate->model.bits = bits;
	buildstate->model.nLevels = nLevels;
	buildstate->model.metric = buildstate->metric;
	buildstate->model.tqProd = false;
	buildstate->model.qjl = NULL;
	buildstate->model.qjlScale = 0.0f;
	buildstate->model.fastRotation = fastRotation;
	buildstate->model.dimPadded = dimPadded;
	buildstate->model.dimCodes = dimCodes;
	buildstate->model.rotation = NULL;
	buildstate->model.rotSeed = TQ_ROTATION_SEED;
	buildstate->model.qjlSeed = TQ_QJL_SEED;
	buildstate->model.boundaries = palloc(sizeof(float) * nBnd);
	buildstate->model.centroids = palloc(sizeof(float) * nLevels);

	buildstate->dimPadded = dimPadded;
	buildstate->dimCodes = dimCodes;

	TqBuildCodebook(dimCodes, bits, buildstate->model.boundaries, buildstate->model.centroids);

	if (fastRotation)
	{
		*rotStart = InvalidBlockNumber;
	}
	else
	{
		buildstate->model.rotation = palloc(rotBytes);
		TqBuildRotation(dim, TQ_ROTATION_SEED, buildstate->model.rotation);
		*rotStart = TqWriteBytes(buildstate->index, buildstate->forkNum,
								 (const char *) buildstate->model.rotation, rotBytes,
								 TQIVF_PAGE_ID);
	}

	cbBuf = palloc(cbBytes);
	memcpy(cbBuf, buildstate->model.boundaries, sizeof(float) * nBnd);
	memcpy(cbBuf + sizeof(float) * nBnd, buildstate->model.centroids, sizeof(float) * nLevels);
	*cbStart = TqWriteBytes(buildstate->index, buildstate->forkNum, cbBuf, cbBytes,
							TQIVF_PAGE_ID);
	pfree(cbBuf);
}

/*
 * Initialize the build state from index relation / options.  Mirrors
 * TqInitBuildState (model params) + InitBuildState (clustering params).
 */
static void
TqivfInitBuildState(TqivfBuildState *buildstate, Relation heap, Relation index,
					IndexInfo *indexInfo, ForkNumber forkNum)
{
	TqivfOptions *opts = (TqivfOptions *) index->rd_options;
	const IvfflatTypeInfo *typeInfo;

	buildstate->heap = heap;
	buildstate->index = index;
	buildstate->indexInfo = indexInfo;
	buildstate->forkNum = forkNum;

	/*
	 * Rerank fetches the raw heap column (indkey.values[0]); an expression
	 * index has no backing attribute (attno 0) and cannot be reranked.
	 */
	if (indexInfo->ii_Expressions != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("tqivf indexes do not support expression index columns")));

	/* Dimensions from the index column typmod (mirror tqflat/ivfflat) */
	buildstate->dim = TupleDescAttr(index->rd_att, 0)->atttypmod;
	if (buildstate->dim < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("column does not have dimensions")));
	if (buildstate->dim < 3)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("tqivf index requires at least 3 dimensions")));

	/* Options.  bits is FIXED at 4 (the blocked layout). */
	buildstate->bits = 4;
	buildstate->fastRotation = opts ? opts->fastRotation : TQ_DEFAULT_FAST_ROTATION;
	buildstate->lists = opts ? opts->lists : TQIVF_DEFAULT_LISTS;
	buildstate->nLevels = 1 << buildstate->bits;

	/* Type-info vtable from the opclass support proc (default vector/L2). */
	buildstate->typeInfo = TqGetTypeInfo(index, TQIVF_TYPE_INFO_PROC);
	buildstate->metric = buildstate->typeInfo->metric;

	if (buildstate->dim > buildstate->typeInfo->maxDimensions)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("column cannot have more than %d dimensions for tqivf index",
						buildstate->typeInfo->maxDimensions)));

	/*
	 * vecScratch lives in this (parent) context, NOT tmpCtx, so it persists
	 * and is reused across all build-callback tuples (tmpCtx is reset per
	 * tuple).
	 */
	buildstate->vecScratch = palloc(sizeof(float) * buildstate->dim);

	/* Assignment / probe support functions (FUNCTION 1 = exact distance) */
	buildstate->procinfo = index_getprocinfo(index, 1, TQIVF_DISTANCE_PROC);
	buildstate->normprocinfo = IvfflatOptionalProcInfo(index, TQIVF_NORM_PROC);
	buildstate->collation = index->rd_indcollation[0];

	/*
	 * Clustering typeInfo from the opclass: the vector opclasses omit
	 * FUNCTION slot 5 so IvfflatGetTypeInfo() returns the default Vector
	 * typeInfo, while the halfvec opclasses register slot 5
	 * (ivfflat_halfvec_support) so sampling / k-means / centroid storage run
	 * natively on halfvec.  Centers are sized to `lists` via
	 * typeInfo->itemSize; IvfflatKmeans reads k from centers->maxlen /
	 * centers->length and never touches reloptions.
	 */
	typeInfo = IvfflatGetTypeInfo(index);

	/*
	 * Each list-directory record stores the full-precision centroid inline in
	 * a single page item; validate that up front rather than failing
	 * mid-build with a generic "failed to add tqivf list item" (ivfflat
	 * avoids this with its fixed IVFFLAT_MAX_DIM cap).
	 */
	if (MAXALIGN(offsetof(TqivfListData, center) + typeInfo->itemSize(buildstate->dim)) >
		TqPageCapacity() - sizeof(ItemIdData))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("tqivf centroid for %d dimensions does not fit on an index page",
						buildstate->dim)));

	buildstate->centers = VectorArrayInit(buildstate->lists, buildstate->dim,
										  typeInfo->itemSize(buildstate->dim));
	buildstate->listInfo = palloc(sizeof(ListInfo) * buildstate->lists);

	/*
	 * Sort tuple descriptor: (list int4, entry bytea) keyed by list.  The
	 * callback encodes each vector into a row-major TqEntry and carries it as
	 * a bytea payload; TqivfEmitLists unwraps it and packs blocks (no
	 * encode). The 4-bit codes are far smaller than the source float vector,
	 * so the sort volume is much smaller than ivfflat's (which sorts the raw
	 * vectors).
	 */
	buildstate->tupdesc = RelationGetDescr(index);
	buildstate->sortdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(buildstate->sortdesc, (AttrNumber) 1, "list", INT4OID, -1, 0);
	TupleDescInitEntry(buildstate->sortdesc, (AttrNumber) 2, "entry", BYTEAOID, -1, 0);
#if PG_VERSION_NUM >= 190000
	TupleDescFinalize(buildstate->sortdesc);
#endif
	buildstate->slot = MakeSingleTupleTableSlot(buildstate->sortdesc, &TTSOpsVirtual);

	buildstate->reltuples = 0;
	buildstate->indtuples = 0;

	buildstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											   "Tqivf build temporary context",
											   ALLOCSET_DEFAULT_SIZES);
}

static void
TqivfFreeBuildState(TqivfBuildState *buildstate)
{
	VectorArrayFree(buildstate->centers);
	pfree(buildstate->listInfo);
	MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Run sample + k-means to produce full-precision, un-rotated centers.  Reuses
 * ivfflat's IvfflatSampleRows + IvfflatKmeans via a locally-populated
 * IvfflatBuildState (only the fields those routines read are set).
 */
static void
TqivfComputeCenters(TqivfBuildState *buildstate)
{
	IvfflatBuildState ivfstate;
	const IvfflatTypeInfo *typeInfo = IvfflatGetTypeInfo(buildstate->index);
	int			numSamples;

	MemSet(&ivfstate, 0, sizeof(ivfstate));
	ivfstate.heap = buildstate->heap;
	ivfstate.index = buildstate->index;
	ivfstate.indexInfo = buildstate->indexInfo;
	ivfstate.typeInfo = typeInfo;
	ivfstate.dimensions = buildstate->dim;
	ivfstate.lists = buildstate->lists;
	ivfstate.centers = buildstate->centers;
	ivfstate.collation = buildstate->collation;

	/* k-means uses proc 3; spherical norm proc 4 (ip/cosine only) */
	ivfstate.procinfo = index_getprocinfo(buildstate->index, 1, TQIVF_KMEANS_DISTANCE_PROC);
	ivfstate.normprocinfo = IvfflatOptionalProcInfo(buildstate->index, TQIVF_NORM_PROC);
	ivfstate.kmeansnormprocinfo = IvfflatOptionalProcInfo(buildstate->index, TQIVF_KMEANS_NORM_PROC);

	ivfstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											"Tqivf kmeans sample context",
											ALLOCSET_DEFAULT_SIZES);

	/*
	 * Target 50 samples per list, with at least 10000 samples (mirror
	 * ivfflat)
	 */
	numSamples = buildstate->lists * 50;
	if (numSamples < 10000)
		numSamples = 10000;
	if (buildstate->heap == NULL)
		numSamples = 1;

	ivfstate.samples = VectorArrayInit(numSamples, buildstate->dim, buildstate->centers->itemsize);
	if (buildstate->heap != NULL)
	{
		IvfflatSampleRows(&ivfstate);

		if (ivfstate.samples->length < buildstate->lists)
			ereport(NOTICE,
					(errmsg("tqivf index created with little data"),
					 errdetail("This will cause low recall."),
					 errhint("Drop the index until the table has more data.")));
	}

	IvfflatKmeans(buildstate->index, ivfstate.samples, buildstate->centers, typeInfo, ivfstate.memoryUsed);

	VectorArrayFree(ivfstate.samples);
	MemoryContextDelete(ivfstate.tmpCtx);
}

/*
 * Write the `lists` list-directory records up front (Invalid chain heads, zero
 * counts), recording each record's (blkno, offno) for later back-patching.
 * Mirrors ivfbuild.c's CreateListPages, sizing each list item via the type's
 * itemSize (offsetof(center) + itemSize(dim)).
 */
static BlockNumber
TqivfCreateListPages(TqivfBuildState *buildstate)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	BlockNumber listStart;
	const IvfflatTypeInfo *ivfTypeInfo = IvfflatGetTypeInfo(buildstate->index);
	Size		listSize = MAXALIGN(offsetof(TqivfListData, center) +
									ivfTypeInfo->itemSize(buildstate->dim));
	TqivfList	list = palloc0(listSize);

	buf = TqNewBuffer(index, forkNum);
	TqInitRegisterPage(index, &buf, &page, &state, TQIVF_PAGE_ID);
	listStart = BufferGetBlockNumber(buf);

	for (int i = 0; i < buildstate->lists; i++)
	{
		OffsetNumber offno;
		Pointer		center = VectorArrayGet(buildstate->centers, i);

		MemSet(list, 0, listSize);
		list->codeStart = InvalidBlockNumber;
		list->sideStart = InvalidBlockNumber;
		list->tailStart = InvalidBlockNumber;
		list->tailInsertPage = InvalidBlockNumber;
		list->blockCount = 0;
		list->nvectors = 0;
		memcpy(&list->center, center, VARSIZE_ANY(center));

		if (PageGetFreeSpace(page) < listSize)
			TqAppendPage(index, &buf, &page, &state, forkNum, TQIVF_PAGE_ID);

		offno = PageAddItem(page, (Pointer) list, listSize, InvalidOffsetNumber, false, false);
		if (offno == InvalidOffsetNumber)
			elog(ERROR, "failed to add tqivf list item to \"%s\"", RelationGetRelationName(index));

		buildstate->listInfo[i].blkno = BufferGetBlockNumber(buf);
		buildstate->listInfo[i].offno = offno;
	}

	TqCommitBuffer(buf, state);
	pfree(list);

	return listStart;
}

/*
 * Assignment callback: find the nearest center (FUNCTION 1 distance over
 * full-precision, un-rotated centers) and push (list, tid, vector) into the
 * tuplesort keyed by list.  Mirrors ivfbuild.c's AddTupleToSort/BuildCallback.
 */
static void
TqivfBuildCallback(Relation index, ItemPointer tid, Datum *values,
				   bool *isnull, bool tupleIsAlive, void *state)
{
	TqivfBuildState *buildstate = (TqivfBuildState *) state;
	MemoryContext oldCtx;
	Datum		value;
	double		minDistance = DBL_MAX;
	int			closestCenter = 0;
	VectorArray centers = buildstate->centers;
	TupleTableSlot *slot = buildstate->slot;

	if (isnull[0])
		return;

	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

	/* Normalize for spherical assignment (ip/cosine), mirror ivfflat */
	if (buildstate->normprocinfo != NULL)
	{
		if (!IvfflatCheckNorm(buildstate->normprocinfo, buildstate->collation, value))
		{
			MemoryContextSwitchTo(oldCtx);
			MemoryContextReset(buildstate->tmpCtx);
			return;
		}

		value = IvfflatNormValue(IvfflatGetTypeInfo(index), buildstate->collation, value);
	}

	for (int i = 0; i < centers->length; i++)
	{
		double		distance = DatumGetFloat8(FunctionCall2Coll(buildstate->procinfo,
																buildstate->collation, value,
																PointerGetDatum(VectorArrayGet(centers, i))));

		if (distance < minDistance)
		{
			minDistance = distance;
			closestCenter = i;
		}
	}

	{
		const float *fv = buildstate->typeInfo->toFloat(value,
														buildstate->vecScratch,
														buildstate->dim);
		Size		entrySize = TqEntrySize(buildstate->model.dimCodes,
											buildstate->model.bits, false);
		Size		byteaSize = VARHDRSZ + entrySize;
		bytea	   *payload = (bytea *) palloc0(byteaSize);
		TqEntry    *entry = (TqEntry *) VARDATA(payload);

		SET_VARSIZE(payload, byteaSize);
		entry->heaptid = *tid;
		entry->deleted = 0;
		TqEncode(&buildstate->model, fv, entry);

		ExecClearTuple(slot);
		slot->tts_values[0] = Int32GetDatum(closestCenter);
		slot->tts_isnull[0] = false;
		slot->tts_values[1] = PointerGetDatum(payload);
		slot->tts_isnull[1] = false;
		ExecStoreVirtualTuple(slot);

		tuplesort_puttupleslot(buildstate->sortstate, slot);

		buildstate->indtuples++;
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Emit the sorted tuples into per-list block streams, back-patching each list
 * directory record.  Replicates tqflat's per-block flush over a per-list cursor.
 */
static void
TqivfEmitLists(TqivfBuildState *buildstate)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	int			dc = buildstate->dimCodes;
	TupleTableSlot *sortSlot = MakeSingleTupleTableSlot(buildstate->sortdesc, &TTSOpsMinimalTuple);
	TqivfListCursor cur;
	int			list;
	bool		haveTuple;
	bool		isnull;
	TqEntry    *rdEntry;
	Size		rdEntrySize;

	cur.codeStage = palloc0(TQ_BLOCK_CODE_BYTES(dc));

	/*
	 * Aligned scratch entry for reading sorted bytea payloads (see memcpy
	 * below).
	 */
	rdEntrySize = TqEntrySize(buildstate->model.dimCodes, buildstate->model.bits, false);
	rdEntry = palloc(rdEntrySize);

	/* Prime the first sorted tuple */
	haveTuple = tuplesort_gettupleslot(buildstate->sortstate, true, false, sortSlot, NULL);
	if (haveTuple)
		list = DatumGetInt32(slot_getattr(sortSlot, 1, &isnull));
	else
		list = -1;

	for (int i = 0; i < buildstate->lists; i++)
	{
		CHECK_FOR_INTERRUPTS();

		/* Reset the per-list cursor + block staging */
		cur.codeStart = InvalidBlockNumber;
		cur.codeBuf = InvalidBuffer;
		cur.codePage = NULL;
		cur.codeState = NULL;
		cur.sideStart = InvalidBlockNumber;
		cur.sideBuf = InvalidBuffer;
		cur.sidePage = NULL;
		cur.sideState = NULL;
		cur.slot = 0;
		cur.blockCount = 0;
		cur.nvectors = 0;
		MemSet(&cur.sideStage, 0, sizeof(cur.sideStage));
		memset(cur.codeStage, 0, TQ_BLOCK_CODE_BYTES(dc));

		while (haveTuple && list == i)
		{
			bytea	   *payload = DatumGetByteaPP(slot_getattr(sortSlot, 2, &isnull));
			int			lane;

			/*
			 * Copy into an aligned scratch entry first: the tuplesort may
			 * return the bytea with a 1-byte short varlena header, so
			 * VARDATA_ANY can point at an unaligned address -- dereferencing
			 * the float/tid fields in place would be unaligned access (UB on
			 * strict-alignment targets).
			 */
			memcpy(rdEntry, VARDATA_ANY(payload), rdEntrySize);

			lane = cur.slot;
			TqScatterCodes(&buildstate->model, rdEntry->data, lane, cur.codeStage);
			cur.sideStage.side[lane].heaptid = rdEntry->heaptid;
			cur.sideStage.side[lane].norm = rdEntry->norm;
			cur.sideStage.side[lane].scale = rdEntry->scale;
			cur.slot++;
			cur.nvectors++;

			if (cur.slot == TQ_BLOCK_WIDTH)
				TqIvfFlushBlock(buildstate, &cur);

			haveTuple = tuplesort_gettupleslot(buildstate->sortstate, true, false, sortSlot, NULL);
			if (haveTuple)
				list = DatumGetInt32(slot_getattr(sortSlot, 1, &isnull));
			else
				list = -1;
		}

		/* Flush trailing partial block for this list */
		if (cur.slot > 0)
			TqIvfFlushBlock(buildstate, &cur);

		/* Close the per-list page cursors */
		if (cur.sidePage != NULL)
			TqCommitBuffer(cur.sideBuf, cur.sideState);
		if (cur.codePage != NULL)
			TqCommitBuffer(cur.codeBuf, cur.codeState);

		/* Back-patch the list directory record in place */
		{
			Buffer		buf;
			Page		page;
			GenericXLogState *state;
			TqivfList	dirlist;

			buf = ReadBufferExtended(index, forkNum, buildstate->listInfo[i].blkno, RBM_NORMAL, NULL);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buf, 0);
			dirlist = (TqivfList) PageGetItem(page, PageGetItemId(page, buildstate->listInfo[i].offno));

			dirlist->codeStart = cur.codeStart;
			dirlist->sideStart = cur.sideStart;
			dirlist->tailStart = InvalidBlockNumber;
			dirlist->tailInsertPage = InvalidBlockNumber;
			dirlist->blockCount = cur.blockCount;
			dirlist->nvectors = cur.nvectors;

			TqCommitBuffer(buf, state);
		}
	}

	pfree(cur.codeStage);
	pfree(rdEntry);
	ExecDropSingleTupleTableSlot(sortSlot);
}

/*
 * Write the meta page at block TQIVF_METAPAGE_BLKNO.  Variable block numbers /
 * nVectors are back-patched after the side pages and lists are written.
 */
static void
TqivfCreateMetaPage(TqivfBuildState *buildstate)
{
	Relation	index = buildstate->index;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	TqivfMetaPage metap;

	buf = TqNewBuffer(index, buildstate->forkNum);
	Assert(BufferGetBlockNumber(buf) == TQIVF_METAPAGE_BLKNO);
	TqInitRegisterPage(index, &buf, &page, &state, TQIVF_PAGE_ID);

	metap = TqivfPageGetMeta(page);
	metap->magicNumber = TQIVF_MAGIC_NUMBER;
	metap->version = TQIVF_VERSION;
	metap->dimensions = (uint16) buildstate->dim;
	metap->bits = (uint16) buildstate->bits;
	metap->metric = (uint16) buildstate->metric;
	metap->fastRotation = (uint16) (buildstate->fastRotation ? 1 : 0);
	metap->dimPadded = (uint16) buildstate->dimPadded;
	metap->lists = (uint16) buildstate->lists;
	metap->nLevels = (uint32) buildstate->nLevels;
	metap->nVectors = 0;
	metap->listStart = InvalidBlockNumber;
	metap->codebookStart = InvalidBlockNumber;
	metap->rotationStart = InvalidBlockNumber;
	metap->rotSeed = TQ_ROTATION_SEED;
	metap->qjlSeed = TQ_QJL_SEED;
	metap->qjlScale = 0.0f;

	((PageHeader) page)->pd_lower =
		((char *) metap + sizeof(TqivfMetaPageData)) - (char *) page;

	TqCommitBuffer(buf, state);
}

/*
 * Back-patch the variable fields of the meta page after the directory + side
 * pages are written.
 */
static void
TqivfUpdateMeta(TqivfBuildState *buildstate, BlockNumber listStart,
				BlockNumber codebookStart, BlockNumber rotationStart,
				uint32 nVectors)
{
	Relation	index = buildstate->index;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	TqivfMetaPage metap;

	buf = ReadBufferExtended(index, buildstate->forkNum, TQIVF_METAPAGE_BLKNO, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	metap = TqivfPageGetMeta(page);

	metap->listStart = listStart;
	metap->codebookStart = codebookStart;
	metap->rotationStart = rotationStart;
	metap->nVectors = nVectors;

	/*
	 * dimPadded is only known after TqivfBuildModelAndSidePages runs, which
	 * is after the meta page is first written; back-patch it here.
	 */
	metap->dimPadded = (uint16) buildstate->dimPadded;

	TqCommitBuffer(buf, state);
}

/*
 * Build the index (shared by tqivfbuild and tqivfbuildempty).
 */
static void
TqivfBuildIndex(Relation heap, Relation index, IndexInfo *indexInfo,
				TqivfBuildState *buildstate, ForkNumber forkNum)
{
	BlockNumber rotStart;
	BlockNumber cbStart;
	BlockNumber listStart;
	AttrNumber	attNums[] = {1};
	Oid			sortOperators[] = {Int4LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};

	TqivfInitBuildState(buildstate, heap, index, indexInfo, forkNum);

	/* Meta page first (block 0), then the global model side pages */
	TqivfCreateMetaPage(buildstate);
	TqivfBuildModelAndSidePages(buildstate, &rotStart, &cbStart);

	/* Sample + k-means → full-precision, un-rotated centers */
	TqivfComputeCenters(buildstate);

	/* List directory up front; record its head for the meta page */
	listStart = TqivfCreateListPages(buildstate);

	/*
	 * Assign + sort by list id, in parallel when the planner grants workers.
	 * The leader computes the model + centers serially (above); workers
	 * receive both via shared memory and encode + assign in parallel.
	 */
	{
		int			parallel_workers = 0;
		SortCoordinate coordinate = NULL;

		/*
		 * Only plan workers for a non-empty, multi-list build: a single list
		 * has no per-list parallelism to gain (the leader still emits
		 * serially), and tqivfbuildempty (heap == NULL) is always serial.
		 */
		if (heap != NULL && buildstate->lists > 1)
			parallel_workers = plan_create_index_workers(RelationGetRelid(heap),
														 RelationGetRelid(index));

		if (parallel_workers > 0)
			TqivfBeginParallel(buildstate, indexInfo->ii_Concurrent, parallel_workers);

		/* Set up coordination state if at least one worker launched. */
		if (buildstate->tqivfleader != NULL)
		{
			coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
			coordinate->isWorker = false;
			coordinate->nParticipants = buildstate->tqivfleader->nparticipanttuplesorts;
			coordinate->sharedsort = buildstate->tqivfleader->sharedsort;
		}

		buildstate->sortstate = tuplesort_begin_heap(buildstate->sortdesc, 1, attNums,
													 sortOperators, sortCollations,
													 nullsFirstFlags, maintenance_work_mem,
													 coordinate, false);

		if (heap != NULL)
		{
			if (buildstate->tqivfleader != NULL)
				buildstate->reltuples = ParallelHeapScan(buildstate);
			else
				buildstate->reltuples = table_index_build_scan(heap, index, indexInfo,
															   true, true, TqivfBuildCallback,
															   (void *) buildstate, NULL);
		}

		tuplesort_performsort(buildstate->sortstate);

		/* Per-list block emit + directory back-patch (leader, serial). */
		TqivfEmitLists(buildstate);

		tuplesort_end(buildstate->sortstate);

		if (buildstate->tqivfleader != NULL)
			TqivfEndParallel(buildstate->tqivfleader);
	}

	/* Finalize the meta page */
	TqivfUpdateMeta(buildstate, listStart, cbStart, rotStart,
					(uint32) buildstate->indtuples);

	/* Write WAL for the init fork since GenericXLog does not */
	if (forkNum == INIT_FORKNUM)
		log_newpage_range(index, forkNum, 0, RelationGetNumberOfBlocksInFork(index, forkNum), true);

	TqivfFreeBuildState(buildstate);
}

IndexBuildResult *
tqivfbuild(Relation heap, Relation index, struct IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	TqivfBuildState buildstate;

	MemSet(&buildstate, 0, sizeof(buildstate));
	TqivfBuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}

void
tqivfbuildempty(Relation index)
{
	IndexInfo  *indexInfo = BuildIndexInfo(index);
	TqivfBuildState buildstate;

	MemSet(&buildstate, 0, sizeof(buildstate));
	TqivfBuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}

/*
 * tqivfinsert -- insert one tuple into an existing tqivf index.
 *
 * Steps:
 *   1. Skip nulls; create a short-lived memory context (mirrors tqinsert).
 *   2. Detoast vector, load cached model, get metric/lists/listStart.
 *   3. Optionally normalize (FUNCTION 2 / TQIVF_NORM_PROC), same as build.
 *   4. Walk the list directory to find the nearest centroid (FUNCTION 1
 *      exact distance).  Record its blkno/offno plus tailStart/tailInsertPage.
 *   5. Encode the vector into a row-major TqEntry (same format the tail-scan
 *      path reads via TqScoreEntry).
 *   6. Append the entry to the list's tail chain at tailInsertPage (O(1)).
 *      If no tail chain yet: allocate first page, set both tailStart and
 *      tailInsertPage.  Otherwise walk/extend as needed (mirrors tqinsert's
 *      append_entry loop with ivfinsert's concurrency pattern).
 *   7. If tailStart/tailInsertPage changed, update the directory record in
 *      place under GenericXLog (mirrors IvfflatUpdateList).
 *   8. Clean up and return false (same as tqinsert/ivfflatinsert).
 *
 * Concurrency: mirrors tqinsert / ivfinsert.  We never hold two data-page
 * buffer locks simultaneously.  Directory-record update uses a separate
 * GenericXLog after all data-page locks are released.
 */
bool
tqivfinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
			Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
			,bool indexUnchanged
#endif
			,struct IndexInfo *indexInfo
)
{
	TqModel    *model;
	Datum		vecDatum;
	const TqTypeInfo *ti;
	float	   *vecScratch;
	TqEntry    *entry;
	Size		entrySize;
	MemoryContext insertCtx;
	MemoryContext oldCtx;
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	Oid			collation;
	TqMetric	metric;
	int			lists;
	BlockNumber listStart;

	/* The list directory location of the best list. */
	ListInfo	bestListInfo;
	BlockNumber bestTailStart;
	BlockNumber bestTailInsertPage;

	/* Skip nulls (mirror ivfflat) */
	if (isnull[0])
		return false;

	/*
	 * Short-lived memory context for detoast / encode scratch (mirrors
	 * tqinsert / ivfflatinsert).
	 */
	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Tqivf insert temporary context",
									  ALLOCSET_DEFAULT_SIZES);
	oldCtx = MemoryContextSwitchTo(insertCtx);

	/* Load (or reuse cached) model */
	model = TqivfGetCachedModel(index);

	/* Get meta info: metric, list count, directory head. */
	TqivfGetMetaInfo(index, NULL, &metric, &lists, &listStart);

	/* Detoast once for all calls */
	vecDatum = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

	/*
	 * Normalize if needed (FUNCTION 2 / TQIVF_NORM_PROC).  For inner product
	 * without a norm proc, skip — same as build.
	 */
	normprocinfo = IvfflatOptionalProcInfo(index, TQIVF_NORM_PROC);
	if (normprocinfo != NULL)
	{
		const IvfflatTypeInfo *typeInfo = IvfflatGetTypeInfo(index);

		collation = index->rd_indcollation[0];

		if (!IvfflatCheckNorm(normprocinfo, collation, vecDatum))
		{
			MemoryContextSwitchTo(oldCtx);
			MemoryContextDelete(insertCtx);
			return false;
		}

		vecDatum = IvfflatNormValue(typeInfo, collation, vecDatum);
	}

	/*
	 * Find the nearest list: walk the list-directory chain and compute the
	 * exact FUNCTION 1 distance from the vector to each centroid.  Record the
	 * directory location (blkno/offno) and the tail-chain pointers of the
	 * best list.  Mirrors ivfinsert's FindInsertPage but reading
	 * TqivfListData instead of IvfflatListData.
	 */
	{
		double		minDistance = DBL_MAX;
		BlockNumber nextblkno = listStart;
		bool		found = false;

		procinfo = index_getprocinfo(index, 1, TQIVF_DISTANCE_PROC);
		collation = index->rd_indcollation[0];

		/* Initialise to suppress compiler warnings */
		bestListInfo.blkno = listStart;
		bestListInfo.offno = FirstOffsetNumber;
		bestTailStart = InvalidBlockNumber;
		bestTailInsertPage = InvalidBlockNumber;

		while (BlockNumberIsValid(nextblkno))
		{
			Buffer		cbuf;
			Page		cpage;
			OffsetNumber maxoffno;
			OffsetNumber offno;
			BlockNumber nxt;

			cbuf = ReadBuffer(index, nextblkno);
			LockBuffer(cbuf, BUFFER_LOCK_SHARE);
			cpage = BufferGetPage(cbuf);
			maxoffno = PageGetMaxOffsetNumber(cpage);

			for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
			{
				TqivfList	list = (TqivfList) PageGetItem(cpage, PageGetItemId(cpage, offno));
				double		distance;

				distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation,
															vecDatum,
															PointerGetDatum(&list->center)));

				if (!found || distance < minDistance)
				{
					minDistance = distance;
					bestListInfo.blkno = nextblkno;
					bestListInfo.offno = offno;
					bestTailStart = list->tailStart;
					bestTailInsertPage = list->tailInsertPage;
					found = true;
				}
			}

			nxt = TqPageGetOpaque(cpage)->nextblkno;
			UnlockReleaseBuffer(cbuf);
			nextblkno = nxt;
		}

		if (!found)
		{
			MemoryContextSwitchTo(oldCtx);
			MemoryContextDelete(insertCtx);
			return false;
		}
	}

	/*
	 * Encode: allocate a row-major TqEntry (same format TqScoreEntry reads in
	 * the tail scan path).  bits is fixed at 4 and tqProd is off in the tqivf
	 * blocked layout; take them from the model so the size tracks the model
	 * if that ever changes.
	 */
	ti = TqGetTypeInfo(index, TQIVF_TYPE_INFO_PROC);
	vecScratch = palloc(sizeof(float) * model->dim);
	entrySize = MAXALIGN(TqEntrySize(model->dimCodes, model->bits, false));
	entry = palloc0(entrySize);
	entry->heaptid = *heap_tid;
	entry->deleted = 0;
	TqEncode(model, ti->toFloat(vecDatum, vecScratch, model->dim), entry);

	MemoryContextSwitchTo(oldCtx);

	/*
	 * ---- Append to the list's tail chain ----
	 *
	 * Two cases: A. No tail chain yet (tailStart == Invalid): allocate the
	 * first page, add the entry, then under the directory lock set tailStart
	 * (if it is still Invalid — another session may have won the race) and
	 * update tailInsertPage.  If we lose the race, the just-allocated page
	 * becomes a stranded page (reclaimed at REINDEX), and we fall through to
	 * Case B to insert our entry into the winner's chain so no row is lost.
	 * B. Tail chain exists: go to tailInsertPage; if there is room, add and
	 * we are done; if full, walk/extend the chain (mirrors tqinsert's
	 * append_entry loop), then update tailInsertPage in the directory.
	 *
	 * Chain invariant: every tail page that holds an entry MUST be reachable
	 * from the directory's tailStart by following nextblkno.  tailInsertPage
	 * is only a hint (the scan never uses it directly) and may be stale —
	 * it just saves walking the whole chain on the next insert.
	 *
	 * We track whether tailInsertPage actually changed so we only write the
	 * directory record when needed.
	 */
	{
		BlockNumber newTailInsertPage = bestTailInsertPage;
		bool		dirtyDir = false;

		if (!BlockNumberIsValid(bestTailStart))
		{
			/*
			 * Case A: first insert into this list's tail chain.
			 *
			 * Allocate and populate a new page before taking the directory
			 * lock (mirrors tqinsert's first-insert path).
			 */
			Buffer		newbuf;
			Page		newpage;
			GenericXLogState *newstate;
			BlockNumber newblk;
			OffsetNumber offno;

			LockRelationForExtension(index, ExclusiveLock);
			newbuf = TqNewBuffer(index, MAIN_FORKNUM);
			UnlockRelationForExtension(index, ExclusiveLock);

			newblk = BufferGetBlockNumber(newbuf);
			newstate = GenericXLogStart(index);
			newpage = GenericXLogRegisterBuffer(newstate, newbuf, GENERIC_XLOG_FULL_IMAGE);
			TqInitPage(newbuf, newpage, TQIVF_PAGE_ID);

			offno = PageAddItem(newpage, (Pointer) entry, entrySize,
								InvalidOffsetNumber, false, false);
			if (offno == InvalidOffsetNumber)
			{
				GenericXLogAbort(newstate);
				UnlockReleaseBuffer(newbuf);
				elog(ERROR, "failed to add tqivf tail entry to \"%s\"",
					 RelationGetRelationName(index));
			}

			GenericXLogFinish(newstate);
			UnlockReleaseBuffer(newbuf);

			/*
			 * Now take the directory lock and try to claim tailStart. Another
			 * session may have won the race and already set tailStart while
			 * we were allocating and writing newblk.
			 */
			{
				Buffer		dbuf;
				Page		dpage;
				GenericXLogState *dstate;
				TqivfList	dirlist;

				dbuf = ReadBufferExtended(index, MAIN_FORKNUM,
										  bestListInfo.blkno, RBM_NORMAL, NULL);
				LockBuffer(dbuf, BUFFER_LOCK_EXCLUSIVE);
				dstate = GenericXLogStart(index);
				dpage = GenericXLogRegisterBuffer(dstate, dbuf, 0);
				dirlist = (TqivfList) PageGetItem(dpage,
												  PageGetItemId(dpage, bestListInfo.offno));

				if (BlockNumberIsValid(dirlist->tailStart))
				{
					/*
					 * Another session won the first-insert race: it set
					 * tailStart before we acquired the directory lock.  Our
					 * newblk page is committed but not linked into the chain —
					 * it becomes a stranded page (reclaimed at REINDEX), the
					 * same trade-off as tqinsert / ivfinsert.  Abort our
					 * directory xlog, capture the winner's tailInsertPage,
					 * and fall through to Case B so our entry is inserted
					 * into the winner's chain and is not lost.
					 */
					newTailInsertPage = dirlist->tailInsertPage;
					GenericXLogAbort(dstate);
					UnlockReleaseBuffer(dbuf);

					goto append_to_existing_chain;
				}

				/* We won: claim tailStart and tailInsertPage. */
				dirlist->tailStart = newblk;
				dirlist->tailInsertPage = newblk;

				GenericXLogFinish(dstate);
				UnlockReleaseBuffer(dbuf);
			}

			/* Winner path: entry is already on newblk, chain is set. Done. */
			goto insert_done;
		}

		/*
		 * Case B: tail chain already exists (normal path or loser fallback).
		 * Append to tailInsertPage, extending when full.  Mirrors tqinsert's
		 * append_entry loop.
		 */
append_to_existing_chain:
		{
			BlockNumber insertPage = newTailInsertPage;
			Buffer		buf;
			Page		page;
			GenericXLogState *state;

			for (;;)
			{
				OffsetNumber offno;
				BlockNumber nextblkno;

				buf = ReadBuffer(index, insertPage);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

				state = GenericXLogStart(index);
				page = GenericXLogRegisterBuffer(state, buf, 0);

				if (PageGetFreeSpace(page) >= entrySize)
				{
					/* Room on this page. */
					offno = PageAddItem(page, (Pointer) entry, entrySize,
										InvalidOffsetNumber, false, false);
					if (offno == InvalidOffsetNumber)
					{
						GenericXLogAbort(state);
						UnlockReleaseBuffer(buf);
						elog(ERROR, "failed to add tqivf tail entry to \"%s\"",
							 RelationGetRelationName(index));
					}

					GenericXLogFinish(state);
					UnlockReleaseBuffer(buf);
					break;
				}

				nextblkno = TqPageGetOpaque(page)->nextblkno;

				if (BlockNumberIsValid(nextblkno))
				{
					/*
					 * Concurrent inserter already extended; follow the link
					 * rather than overwriting it (mirrors tqinsert).  Both
					 * pages remain linked via nextblkno and are reachable
					 * from tailStart, preserving the chain invariant.
					 */
					GenericXLogAbort(state);
					UnlockReleaseBuffer(buf);
					insertPage = nextblkno;
					continue;
				}

				/* No room and no concurrent extension: allocate a new page. */
				{
					Buffer		newbuf;
					Page		newpage;
					BlockNumber newblkno;

					LockRelationForExtension(index, ExclusiveLock);
					newbuf = TqNewBuffer(index, MAIN_FORKNUM);
					UnlockRelationForExtension(index, ExclusiveLock);

					/*
					 * Register the new page in the same xlog as the old page
					 * so that setting nextblkno on the old page and
					 * initializing the new page are atomic.  This ensures the
					 * new page is linked into the chain (reachable from
					 * tailStart) before any reader can see it, preserving the
					 * chain invariant.
					 */
					newpage = GenericXLogRegisterBuffer(state, newbuf,
														GENERIC_XLOG_FULL_IMAGE);
					TqInitPage(newbuf, newpage, TQIVF_PAGE_ID);

					newblkno = BufferGetBlockNumber(newbuf);
					TqPageGetOpaque(page)->nextblkno = newblkno;

					/* Commit link on old page + init of new page. */
					GenericXLogFinish(state);
					UnlockReleaseBuffer(buf);

					/* Now insert into the new page under a fresh xlog. */
					state = GenericXLogStart(index);
					buf = newbuf;
					page = GenericXLogRegisterBuffer(state, buf, 0);

					offno = PageAddItem(page, (Pointer) entry, entrySize,
										InvalidOffsetNumber, false, false);
					if (offno == InvalidOffsetNumber)
					{
						GenericXLogAbort(state);
						UnlockReleaseBuffer(buf);
						elog(ERROR, "failed to add tqivf tail entry to \"%s\"",
							 RelationGetRelationName(index));
					}

					GenericXLogFinish(state);
					UnlockReleaseBuffer(buf);

					newTailInsertPage = newblkno;
					dirtyDir = true;
					break;
				}
			}
		}

		/*
		 * Update the list directory record if tailInsertPage changed (i.e. we
		 * extended the chain).  tailStart is already valid at this point
		 * (Case B only runs when a chain exists), so we never overwrite it.
		 * This is a separate GenericXLog after all data-page locks have been
		 * released (mirrors IvfflatUpdateList's lock ordering).
		 */
		if (dirtyDir)
		{
			Buffer		dbuf;
			Page		dpage;
			GenericXLogState *dstate;
			TqivfList	dirlist;

			dbuf = ReadBufferExtended(index, MAIN_FORKNUM,
									  bestListInfo.blkno, RBM_NORMAL, NULL);
			LockBuffer(dbuf, BUFFER_LOCK_EXCLUSIVE);
			dstate = GenericXLogStart(index);
			dpage = GenericXLogRegisterBuffer(dstate, dbuf, 0);
			dirlist = (TqivfList) PageGetItem(dpage,
											  PageGetItemId(dpage, bestListInfo.offno));

			/*
			 * tailStart is guaranteed valid here (Case B path).  Only update
			 * tailInsertPage — it is a hint and may be stale, but must
			 * point to a page that is reachable from tailStart via nextblkno.
			 */
			Assert(BlockNumberIsValid(dirlist->tailStart));
			dirlist->tailInsertPage = newTailInsertPage;

			GenericXLogFinish(dstate);
			UnlockReleaseBuffer(dbuf);
		}
	}

insert_done:;

	MemoryContextDelete(insertCtx);

	return false;
}

/*
 * TqivfGetCachedModel -- load the TqModel from rd_amcache, or (re)load it.
 * Mirrors TqGetCachedModel.
 */
TqModel *
TqivfGetCachedModel(Relation index)
{
	if (index->rd_amcache != NULL)
		return (TqModel *) index->rd_amcache;

	index->rd_amcache = TqivfLoadModel(index, index->rd_indexcxt);
	return (TqModel *) index->rd_amcache;
}
