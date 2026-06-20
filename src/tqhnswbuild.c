#include "postgres.h"

#include <math.h>

#include "access/generic_xlog.h"
#include "access/parallel.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "commands/progress.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "tcop/tcopprot.h"
#include "tqhnsw.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#if PG_VERSION_NUM >= 140000
#include "utils/backend_progress.h"
#include "utils/backend_status.h"
#include "utils/wait_event.h"
#endif

#define PARALLEL_KEY_TQHNSW_SHARED		UINT64CONST(0xB000000000000001)
#define PARALLEL_KEY_TQHNSW_AREA		UINT64CONST(0xB000000000000002)
#define PARALLEL_KEY_QUERY_TEXT			UINT64CONST(0xB000000000000003)

/*
 * tqhnswbuild reuses TqInitRegisterPage / TqInitPage, which size the page
 * special area with sizeof(TqPageOpaqueData), while tqhnsw readers cast the
 * special area to TqhnswPageOpaqueData.  They are currently byte-identical; this
 * assertion catches a future divergence at compile time.
 */
StaticAssertDecl(sizeof(TqhnswPageOpaqueData) == sizeof(TqPageOpaqueData),
				 "tqhnsw page opaque must match tq page opaque");

/*
 * Serial in-memory build state.  Holds the model, the in-memory graph (a
 * singly-linked build list rooted at head), the entry point, and the build
 * parameters / scratch encode buffer.
 */
typedef struct TqhnswBuildState
{
	Relation	index;
	TqModel    *model;
	int			dim;
	TqMetric	metric;
	int			m;
	int			efConstruction;
	bool		fastRotation;
	int			dimCodes;
	int			codesBytes;
	double		ml;
	int			maxLevel;

	TqEntry    *scratch;		/* TqEntrySize(dimCodes, bits, false) bytes */
	Size		scratchSize;

	/* Type-info vtable (resolved from opclass support proc). */
	const TqTypeInfo *typeInfo;
	float	   *vecScratch;		/* dim floats, reused per tuple */
	float	   *rhatScratch;	/* dimCodes; float reconstruct buffer, packed
								 * to the element's fp16 rhat (per-worker) */

	TqhnswGraph graphData;		/* serial: graph lives here */
	TqhnswGraph *graph;			/* points at graphData (serial) or DSM
								 * (parallel) */
	BlockNumber firstElementPage;	/* first element page written during flush */

	MemoryContext graphCtx;		/* owns all in-memory graph nodes */
	MemoryContext tmpCtx;		/* per-tuple scratch */
	TqhnswAllocator allocator;	/* durable graph-node allocation */
	char	   *base;			/* relptr base: NULL serial, DSM area parallel */

	Relation	heap;
	int64		indtuples;		/* captured graph->nVectors before DSM
								 * teardown */
	/* Parallel build coordination (NULL/unused in serial build) */
	TqhnswLeader *tqhnswleader;
	TqhnswShared *tqhnswshared;
	char	   *tqhnswarea;
} TqhnswBuildState;

static void TqhnswFlushGraph(TqhnswBuildState *buildstate);

/*
 * TqhnswWriteModelAndMeta -- build the codebook (+ dense rotation), write the
 * meta page at block 0, and write the codebook/rotation side pages.
 *
 * Replicates TqBuildModelAndSidePages (static in tqbuild.c) + TqBuildIndex's
 * meta-first page ordering: the meta page placeholder is created at block 0
 * FIRST (so it is always the first block in the fork), THEN the side pages
 * append at blocks >= 1, THEN the meta page is back-patched with the side-chain
 * heads and header fields.  The entry point is written "empty" (entryLevel =
 * -1); the real build back-patches it.
 *
 * modelOut->boundaries / modelOut->centroids must be preallocated by the caller.
 */
static void
TqhnswWriteModelAndMeta(Relation index, ForkNumber forkNum, int dim, TqMetric metric,
						int m, int efConstruction, bool fastRotation,
						BlockNumber *codebookStart, BlockNumber *rotationStart,
						TqModel *modelOut)
{
	int			bits = TQ_DEFAULT_BITS;
	int			nLevels = 1 << bits;
	int			nBnd = nLevels - 1;
	int			dimPadded = fastRotation ? TqNextPow2(dim) : dim;
	int			dimCodes = fastRotation ? dimPadded : dim;
	Size		cbBytes = (Size) sizeof(float) * (nBnd + nLevels);
	char	   *cbBuf;
	Buffer		metabuf;
	Page		metapage;
	GenericXLogState *state;
	TqhnswMetaPage metap;

	/*
	 * Validate the element-tuple size against the page layout up front --
	 * otherwise the whole in-memory graph is built before the flush fails
	 * with a generic "failed to add index item".  fast_rotation pads dim to
	 * the next power of two, so dims in (8192, 16000] overflow only in fast
	 * mode.
	 */
	if (TQHNSW_ELEMENT_TUPLE_SIZE(TQ_CODES_BYTES(dimCodes, bits)) >
		TqPageCapacity() - sizeof(ItemIdData))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("tqhnsw element for %d dimensions (%d quantized) does not fit on an index page",
						dim, dimCodes),
				 errhint("Reduce the number of dimensions, or use fast_rotation = false to avoid power-of-two padding.")));

	/*
	 * Meta page is block 0; it must be the first block created in the fork.
	 * Create the placeholder (with Invalid side-chain heads) and commit it so
	 * the subsequent TqWriteBytes side pages land at blocks >= 1.  We
	 * back-patch the meta page below (mirror tqbuild.c's TqCreateMetaPage +
	 * TqUpdateMeta ordering).
	 */
	metabuf = TqNewBuffer(index, forkNum);
	Assert(BufferGetBlockNumber(metabuf) == TQHNSW_METAPAGE_BLKNO);
	TqInitRegisterPage(index, &metabuf, &metapage, &state, TQHNSW_PAGE_ID);
	metap = TqhnswPageGetMeta(metapage);
	memset(metap, 0, sizeof(TqhnswMetaPageData));
	metap->magicNumber = TQHNSW_MAGIC_NUMBER;
	metap->version = TQHNSW_VERSION;
	((PageHeader) metapage)->pd_lower =
		((char *) metap + sizeof(TqhnswMetaPageData)) - (char *) metapage;
	TqCommitBuffer(metabuf, state);

	/* Build the model (codebook + optional dense rotation) in caller memory. */
	TqBuildCodebook(dimCodes, bits, modelOut->boundaries, modelOut->centroids);
	modelOut->dim = dim;
	modelOut->bits = bits;
	modelOut->nLevels = nLevels;
	modelOut->metric = metric;
	modelOut->tqProd = false;
	modelOut->fastRotation = fastRotation;
	modelOut->dimPadded = dimPadded;
	modelOut->dimCodes = dimCodes;
	modelOut->rotation = NULL;
	modelOut->qjl = NULL;
	modelOut->rotSeed = TQ_ROTATION_SEED;
	modelOut->qjlSeed = TQ_QJL_SEED;
	modelOut->qjlScale = 0.0f;

	*rotationStart = InvalidBlockNumber;
	if (!fastRotation)
	{
		Size		rotBytes = (Size) sizeof(float) * dim * dim;

		modelOut->rotation = palloc(rotBytes);
		TqBuildRotation(dim, TQ_ROTATION_SEED, modelOut->rotation);
		*rotationStart = TqWriteBytes(index, forkNum, (const char *) modelOut->rotation,
									  rotBytes, TQHNSW_PAGE_ID);
	}

	cbBuf = palloc(cbBytes);
	memcpy(cbBuf, modelOut->boundaries, sizeof(float) * nBnd);
	memcpy(cbBuf + sizeof(float) * nBnd, modelOut->centroids, sizeof(float) * nLevels);
	*codebookStart = TqWriteBytes(index, forkNum, cbBuf, cbBytes, TQHNSW_PAGE_ID);
	pfree(cbBuf);

	/* Back-patch the meta page (block 0) with the header + side-chain heads. */
	metabuf = ReadBufferExtended(index, forkNum, TQHNSW_METAPAGE_BLKNO, RBM_NORMAL, NULL);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	metapage = GenericXLogRegisterBuffer(state, metabuf, 0);
	metap = TqhnswPageGetMeta(metapage);

	metap->magicNumber = TQHNSW_MAGIC_NUMBER;
	metap->version = TQHNSW_VERSION;
	metap->dimensions = (uint16) dim;
	metap->bits = (uint16) bits;
	metap->metric = (uint16) metric;
	metap->fastRotation = (uint16) (fastRotation ? 1 : 0);
	metap->dimPadded = (uint16) dimPadded;
	metap->m = (uint16) m;
	metap->efConstruction = (uint16) efConstruction;
	metap->nLevels = (uint32) nLevels;
	metap->nVectors = 0;
	metap->rotSeed = TQ_ROTATION_SEED;
	metap->codebookStart = *codebookStart;
	metap->rotationStart = *rotationStart;
	metap->entryBlkno = InvalidBlockNumber;
	metap->entryOffno = InvalidOffsetNumber;
	metap->entryLevel = -1;
	metap->insertPage = InvalidBlockNumber;
	metap->firstElementPage = InvalidBlockNumber;

	TqCommitBuffer(metabuf, state);
}

/*
 * Resolve build parameters (dim, metric, options) from the index relation.
 * Mirrors TqInitBuildState's dim/metric resolution.
 */
static void
TqhnswResolveBuildParams(Relation index, int *dim, TqMetric *metric,
						 int *m, int *efc, bool *fast)
{
	TqhnswOptions *opts = (TqhnswOptions *) index->rd_options;

	*m = opts ? opts->m : TQHNSW_DEFAULT_M;
	*efc = opts ? opts->efConstruction : TQHNSW_DEFAULT_EF_CONSTRUCTION;
	*fast = opts ? opts->fastRotation : TQ_DEFAULT_FAST_ROTATION;

	if (*efc < 2 * *m)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ef_construction must be greater than or equal to 2 * m")));

	*dim = TupleDescAttr(index->rd_att, 0)->atttypmod;	/* vector dim via typmod */
	if (*dim < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("column does not have dimensions")));

	/* Resolve metric + maxDimensions from the type-info support proc. */
	{
		const TqTypeInfo *ti = TqGetTypeInfo(index, TQHNSW_TYPE_INFO_PROC);

		*metric = ti->metric;
		if (*dim > ti->maxDimensions)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("column cannot have more than %d dimensions for tqhnsw index",
							ti->maxDimensions)));
	}
}

/*
 * Write a valid empty index (model + codebook side pages + meta with no graph)
 * to the given fork.  Shared by tqhnswbuildempty (INIT fork) and the empty-heap
 * fast path in tqhnswbuild (MAIN fork).
 */
static void
TqhnswWriteEmptyIndex(Relation index, ForkNumber forkNum)
{
	int			dim;
	TqMetric	metric;
	int			m;
	int			efc;
	bool		fast;
	TqModel		model;
	BlockNumber cbStart;
	BlockNumber rotStart;

	TqhnswResolveBuildParams(index, &dim, &metric, &m, &efc, &fast);

	model.boundaries = palloc(sizeof(float) * ((1 << TQ_DEFAULT_BITS) - 1));
	model.centroids = palloc(sizeof(float) * (1 << TQ_DEFAULT_BITS));
	TqhnswWriteModelAndMeta(index, forkNum, dim, metric, m, efc, fast,
							&cbStart, &rotStart, &model);

	/* Write WAL for the init fork (GenericXLog does not on INIT_FORKNUM). */
	if (forkNum == INIT_FORKNUM)
		log_newpage_range(index, forkNum, 0,
						  RelationGetNumberOfBlocksInFork(index, forkNum), true);
}

/*
 * Assign a random level (HnswInitElement: level = -log(U) * ml, capped at
 * maxLevel).  RandomDouble() is a macro in hnsw.h; replicate its body so levels
 * are drawn the same way (unseeded global PRNG -> nondeterministic across runs).
 */
int
TqhnswRandomLevel(double ml, int maxLevel)
{
	int			level = (int) (-log(TqhnswRandomDouble()) * ml);

	if (level > maxLevel)
		level = maxLevel;
	return level;
}

static void *
TqhnswMemoryContextAlloc(Size size, void *state)
{
	TqhnswBuildState *buildstate = (TqhnswBuildState *) state;

	/*
	 * memoryUsed is refreshed once per element by the caller (after all of an
	 * element's sub-allocations) rather than here --
	 * MemoryContextMemAllocated walks the context block list, so doing it per
	 * sub-allocation costs ~5-6x the walks per inserted tuple for no benefit.
	 */
	return MemoryContextAlloc(buildstate->graphCtx, size);
}

static void *
TqhnswSharedMemoryAlloc(Size size, void *state)
{
	TqhnswBuildState *buildstate = (TqhnswBuildState *) state;
	TqhnswGraph *graph = buildstate->graph;
	Size		alignedSize = MAXALIGN(size);
	void	   *chunk;

	if (alignedSize > 1024 * 1024)
		elog(ERROR, "tqhnsw allocation too large");
	if (graph->memoryUsed + alignedSize > graph->memoryTotal)
		elog(ERROR, "tqhnsw allocator out of memory");
	chunk = buildstate->tqhnswarea + graph->memoryUsed;
	graph->memoryUsed += alignedSize;
	return chunk;
}

/*
 * Allocate an in-memory element (level pre-assigned).  Neighbor arrays are sized
 * per layer (level 0 doubled) and zero-initialized.  Allocated in graphCtx.
 */
static TqhnswElement *
TqhnswAllocElement(TqhnswBuildState *buildstate, ItemPointer tid, int level)
{
	char	   *base = buildstate->base;
	TqhnswElement *element = TqhnswAlloc(&buildstate->allocator, sizeof(TqhnswElement));
	int			lc;

	MemSet(element, 0, sizeof(TqhnswElement));
	element->heaptid = *tid;
	element->level = (uint8) level;
	TqhnswPtrStore(base, element->rhat, (half *) TqhnswAlloc(&buildstate->allocator, sizeof(half) * buildstate->dimCodes));
	{
		char	   *codes = TqhnswAlloc(&buildstate->allocator, buildstate->codesBytes);

		MemSet(codes, 0, buildstate->codesBytes);
		TqhnswPtrStore(base, element->codes, codes);
	}
	{
		TqhnswNeighborArrayPtr *neighbors =
		TqhnswAlloc(&buildstate->allocator, sizeof(TqhnswNeighborArrayPtr) * (level + 1));

		TqhnswPtrStore(base, element->neighbors, neighbors);
		for (lc = 0; lc <= level; lc++)
		{
			int			lm = TqhnswGetLayerM(buildstate->m, lc);
			TqhnswNeighborArray *na = TqhnswAlloc(&buildstate->allocator, TQHNSW_NEIGHBOR_ARRAY_SIZE(lm));

			na->count = 0;
			TqhnswPtrStore(base, neighbors[lc], na);
		}
	}
	element->blkno = InvalidBlockNumber;
	element->offno = InvalidOffsetNumber;
	element->neighborPage = InvalidBlockNumber;
	element->neighborOffno = InvalidOffsetNumber;
	TqhnswPtrStore(base, element->next, (TqhnswElement *) NULL);
	LWLockInitialize(&element->lock, tqhnsw_lock_tranche_id);
	return element;
}

/*
 * Insert one already-formed (detoasted, cosine-normalized) value into the build
 * graph.  Mirrors hnswbuild.c InsertTuple: holds flushLock SHARED for the whole
 * in-memory insert; when the graph area fills, one worker flushes it to disk
 * under flushLock EXCLUSIVE and all workers fall through to the on-disk insert.
 */
static bool
TqhnswInsertTuple(TqhnswBuildState *buildstate, Datum value, ItemPointer tid)
{
	TqhnswGraph *graph = buildstate->graph;
	char	   *base = buildstate->base;
	LWLock	   *flushLock = &graph->flushLock;
	Size		memoryMargin = base == NULL ? 0 : 1024 * 1024;
	TqModel    *model = buildstate->model;
	int			level;
	TqhnswElement *element;

	/* Ensure the graph is not flushed out from under us mid-insert. */
	LWLockAcquire(flushLock, LW_SHARED);

	/* On-disk phase: route to the disk insert path. */
	if (graph->flushed)
	{
		LWLockRelease(flushLock);
		TqhnswInsertTupleOnDisk(buildstate->index, model, buildstate->metric,
								value, tid, CurrentMemoryContext);
		return true;
	}

	/* Reserve memory for the element under the allocator lock. */
	LWLockAcquire(&graph->allocatorLock, LW_EXCLUSIVE);
	if (graph->memoryUsed + memoryMargin >= graph->memoryTotal)
	{
		LWLockRelease(&graph->allocatorLock);
		LWLockRelease(flushLock);
		LWLockAcquire(flushLock, LW_EXCLUSIVE);
		if (!graph->flushed)
		{
			ereport(NOTICE,
					(errmsg("tqhnsw graph no longer fits into maintenance_work_mem after " INT64_FORMAT " tuples", (int64) graph->indtuples),
					 errdetail("Building will take significantly more time."),
					 errhint("Increase maintenance_work_mem to speed up builds.")));
			TqhnswFlushGraph(buildstate);
			graph->flushed = true;
		}
		LWLockRelease(flushLock);
		TqhnswInsertTupleOnDisk(buildstate->index, model, buildstate->metric,
								value, tid, CurrentMemoryContext);
		return true;
	}

	/*
	 * Allocate the element under the allocator lock (allocator no longer
	 * self-locks).
	 */
	level = TqhnswRandomLevel(buildstate->ml, buildstate->maxLevel);
	element = TqhnswAllocElement(buildstate, tid, level);

	/*
	 * Serial path: refresh the running memory total once, now that the
	 * element and all its sub-allocations are in graphCtx (the parallel bump
	 * allocator tracks memoryUsed exactly as it allocates, so skip it there).
	 */
	if (base == NULL)
		graph->memoryUsed = MemoryContextMemAllocated(buildstate->graphCtx, false);
	LWLockRelease(&graph->allocatorLock);

	/*
	 * Encode the value into the element (codes/norm/scale + reconstructed
	 * rhat).
	 */
	{
		const float *fv = TqExtractForEncode(buildstate->typeInfo, value,
											 buildstate->metric,
											 buildstate->vecScratch, buildstate->dim);

		char	   *codes;
		half	   *rhat;

		memset(buildstate->scratch, 0, buildstate->scratchSize);
		TqEncode(model, fv, buildstate->scratch);
		element->norm = buildstate->scratch->norm;
		element->scale = buildstate->scratch->scale;
		codes = TqhnswPtrAccess(base, element->codes);
		rhat = TqhnswPtrAccess(base, element->rhat);
		Assert(codes != NULL && rhat != NULL);
		memcpy(codes, buildstate->scratch->data, buildstate->codesBytes);
		TqhnswReconstructHalf(model, codes, element->norm, element->scale,
							  buildstate->metric == TQ_METRIC_COSINE,
							  buildstate->rhatScratch, rhat);
	}

	/*
	 * Insert into the in-memory graph.  Mirrors hnswbuild.c
	 * InsertTupleInMemory: a single unified path (no separate bootstrap
	 * branch), with the entry lock held across the whole search+connect so a
	 * concurrent entry-point raise blocks searchers mid-descent (entry ->
	 * element -> spinlock ordering).
	 */
	{
		TqhnswElement *entryPoint;

		/* Wait if another process needs the exclusive entry lock. */
		LWLockAcquire(&graph->entryWaitLock, LW_EXCLUSIVE);
		LWLockRelease(&graph->entryWaitLock);

		/* Get the entry point under the shared entry lock. */
		LWLockAcquire(&graph->entryLock, LW_SHARED);
		entryPoint = TqhnswPtrAccess(base, graph->entryPoint);

		/* Prevent concurrent inserts when likely updating the entry point. */
		if (entryPoint == NULL || element->level > entryPoint->level)
		{
			LWLockRelease(&graph->entryLock);

			/* Tell other processes to wait and take the exclusive lock. */
			LWLockAcquire(&graph->entryWaitLock, LW_EXCLUSIVE);
			LWLockAcquire(&graph->entryLock, LW_EXCLUSIVE);
			LWLockRelease(&graph->entryWaitLock);

			/* Re-read the entry point now that we hold the exclusive lock. */
			entryPoint = TqhnswPtrAccess(base, graph->entryPoint);
		}

		/*
		 * Add to the build list + count under the graph spinlock BEFORE
		 * publishing the element's reciprocal edges (below).  This mirrors
		 * the hnsw UpdateGraphInMemory order (AddElementInMemory then
		 * UpdateNeighborsInMemory): an element must be on graph->head before
		 * any neighbor edge can make it discoverable, so a flush walk over
		 * graph->head never misses an element another node already
		 * references. (A concurrent flush cannot interleave here regardless
		 * -- flushLock is held SHARED for the whole insert -- but preserving
		 * hnsw's invariant means correctness does not depend on that.)
		 */
		SpinLockAcquire(&graph->lock);
		TqhnswPtrStore(base, element->next, TqhnswPtrAccess(base, graph->head));
		TqhnswPtrStore(base, graph->head, element);
		graph->nVectors++;
		SpinLockRelease(&graph->lock);

		/*
		 * Search + connect against the current entry point.  entryPoint may
		 * be NULL for the very first element, in which case
		 * TqhnswInsertElement returns immediately and the element keeps empty
		 * neighbors -- it is already on the build list and is (below) made
		 * the entry point, so a lost entry-point race never orphans an
		 * element.
		 */
		TqhnswInsertElement(base, NULL, NULL, NULL, CurrentMemoryContext,
							element, entryPoint, buildstate->m,
							buildstate->efConstruction, buildstate->dimCodes,
							buildstate->metric, false);

		/*
		 * Update the entry point if needed.  We still hold the entry lock; it
		 * is exclusive whenever this condition can be true (a shared holder
		 * read an entryPoint that cannot have changed under it), so the store
		 * is safe.
		 */
		if (entryPoint == NULL || element->level > entryPoint->level)
			TqhnswPtrStore(base, graph->entryPoint, element);

		LWLockRelease(&graph->entryLock);
	}

	LWLockRelease(flushLock);
	return true;
}

/*
 * Build callback.  Extract -> encode -> insert (in-memory, or the on-disk
 * fallback once the graph has been flushed); the cosine float-normalize happens
 * inside TqExtractForEncode at the encode site.  Mirrors hnswbuild.c's
 * BuildCallback.
 */
static void
TqhnswBuildCallback(Relation index, ItemPointer tid, Datum *values,
					bool *isnull, bool tupleIsAlive, void *state)
{
	TqhnswBuildState *buildstate = (TqhnswBuildState *) state;
	MemoryContext oldCtx;
	Datum		value;

	if (isnull[0])
		return;

	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

	/*
	 * Skip zero-norm vectors under cosine (the operator returns NaN for
	 * them), mirroring hnsw/ivfflat.
	 */
	if (buildstate->metric == TQ_METRIC_COSINE)
	{
		const float *fv = buildstate->typeInfo->toFloat(value,
														buildstate->vecScratch,
														buildstate->dim);

		if (!TqCheckNorm(fv, buildstate->dim))
		{
			MemoryContextSwitchTo(oldCtx);
			MemoryContextReset(buildstate->tmpCtx);
			return;
		}
	}

	/*
	 * Detoast only -- normalization is done per encode path inside
	 * TqhnswInsertTuple (in-memory) / TqhnswQuantizeElement (on-disk
	 * fallback), so a value routed to disk on an OOM flush is normalized
	 * exactly once.
	 */
	if (TqhnswInsertTuple(buildstate, value, tid))
	{
		/* Report progress under the same spinlock (mirrors hnswbuild.c). */
		SpinLockAcquire(&buildstate->graph->lock);
		pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE,
									 ++buildstate->graph->indtuples);
		SpinLockRelease(&buildstate->graph->lock);
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Initialize a graph header.  Serial builds pass base==NULL and the graph lives
 * on the build-state stack; parallel builds (a later task) place it in DSM.
 * Mirrors hnswbuild.c's InitGraph.
 */
static void
TqhnswInitGraph(TqhnswGraph *graph, char *base, Size memoryTotal)
{
	/*
	 * Initialize the lock tranche if needed: when the library is loaded via
	 * shared_preload_libraries, _PG_init skips it (shared memory is not set
	 * up yet) and backends inherit an unset tranche id.  Mirrors hnsw
	 * InitGraph.
	 */
	TqhnswInitLockTranche();

	SpinLockInit(&graph->lock);
	TqhnswPtrStore(base, graph->head, (TqhnswElement *) NULL);
	graph->nVectors = 0;
	LWLockInitialize(&graph->entryLock, tqhnsw_lock_tranche_id);
	LWLockInitialize(&graph->entryWaitLock, tqhnsw_lock_tranche_id);
	TqhnswPtrStore(base, graph->entryPoint, (TqhnswElement *) NULL);
	LWLockInitialize(&graph->allocatorLock, tqhnsw_lock_tranche_id);
	graph->memoryUsed = 0;
	graph->memoryTotal = Min(memoryTotal, TQHNSW_MAX_GRAPH_MEMORY);
	LWLockInitialize(&graph->flushLock, tqhnsw_lock_tranche_id);
	graph->flushed = false;
	graph->indtuples = 0;
}

/*
 * Flush the in-memory graph to disk: element tuples + neighbor-tuple placeholders
 * (pass 1), then fill the neighbor tuples (pass 2), then back-patch the meta page
 * with the entry point + nVectors.  Mirrors hnswbuild.c CreateGraphPages +
 * WriteNeighborTuples + HnswUpdateMetaPage.
 */
static void
TqhnswFlushGraph(TqhnswBuildState *buildstate)
{
	Relation	index = buildstate->index;
	int			m = buildstate->m;
	Size		maxSize = TqPageCapacity();
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	TqhnswElement *head = TqhnswPtrAccess(buildstate->base, buildstate->graph->head);
	TqhnswElement *iter;
	char	   *etupBuf;
	char	   *ntupBuf;
	Size		etupAlloc;
	Size		ntupAlloc;
	BlockNumber lastElementPage;

	/* Worst-case tuple sizes (level <= maxLevel). */
	etupAlloc = TQHNSW_ELEMENT_TUPLE_SIZE(buildstate->codesBytes);
	ntupAlloc = TQHNSW_NEIGHBOR_TUPLE_SIZE(buildstate->maxLevel, m);
	etupBuf = palloc0(etupAlloc);
	ntupBuf = palloc0(ntupAlloc);

	/* Pass 1: write element tuples + neighbor-tuple placeholders. */
	buf = TqNewBuffer(index, MAIN_FORKNUM);
	TqInitRegisterPage(index, &buf, &page, &state, TQHNSW_PAGE_ID);
	buildstate->firstElementPage = BufferGetBlockNumber(buf);

	for (iter = head; iter != NULL; iter = TqhnswPtrAccess(buildstate->base, iter->next))
	{
		TqhnswElementTuple etup = (TqhnswElementTuple) etupBuf;
		Size		etupSize = TQHNSW_ELEMENT_TUPLE_SIZE(buildstate->codesBytes);
		Size		ntupSize = TQHNSW_NEIGHBOR_TUPLE_SIZE(iter->level, m);
		Size		combinedSize = etupSize + ntupSize + sizeof(ItemIdData);
		OffsetNumber offno;
		char	   *codes;

		MemSet(etupBuf, 0, etupAlloc);

		etup->type = TQHNSW_ELEMENT_TUPLE_TYPE;
		etup->level = iter->level;
		etup->deleted = 0;
		etup->version = 0;
		etup->heaptid = iter->heaptid;
		etup->norm = iter->norm;
		etup->scale = iter->scale;
		codes = TqhnswPtrAccess(buildstate->base, iter->codes);
		Assert(codes != NULL);
		memcpy(etup->codes, codes, buildstate->codesBytes);

		/* Keep element + its neighbor tuple on the same page when possible. */
		if (PageGetFreeSpace(page) < etupSize ||
			(combinedSize <= maxSize && PageGetFreeSpace(page) < combinedSize))
			TqAppendPage(index, &buf, &page, &state, MAIN_FORKNUM, TQHNSW_PAGE_ID);

		iter->blkno = BufferGetBlockNumber(buf);
		iter->offno = OffsetNumberNext(PageGetMaxOffsetNumber(page));
		if (combinedSize <= maxSize)
		{
			iter->neighborPage = iter->blkno;
			iter->neighborOffno = OffsetNumberNext(iter->offno);
		}
		else
		{
			iter->neighborPage = iter->blkno + 1;
			iter->neighborOffno = FirstOffsetNumber;
		}

		ItemPointerSet(&etup->neighbortid, iter->neighborPage, iter->neighborOffno);

		offno = PageAddItem(page, (Pointer) etup, etupSize, InvalidOffsetNumber,
							false, false);
		if (offno != iter->offno)
			elog(ERROR, "failed to add index item to \"%s\"",
				 RelationGetRelationName(index));

		/* Neighbor-tuple placeholder. */
		if (PageGetFreeSpace(page) < ntupSize)
			TqAppendPage(index, &buf, &page, &state, MAIN_FORKNUM, TQHNSW_PAGE_ID);

		offno = PageAddItem(page, (Pointer) ntupBuf, ntupSize, InvalidOffsetNumber,
							false, false);
		if (offno != iter->neighborOffno)
			elog(ERROR, "failed to add index item to \"%s\"",
				 RelationGetRelationName(index));
	}

	lastElementPage = BufferGetBlockNumber(buf);

	TqCommitBuffer(buf, state);

	/* Pass 2: fill neighbor tuples. */
	for (iter = head; iter != NULL; iter = TqhnswPtrAccess(buildstate->base, iter->next))
	{
		TqhnswNeighborTuple ntup = (TqhnswNeighborTuple) ntupBuf;
		Size		ntupSize = TQHNSW_NEIGHBOR_TUPLE_SIZE(iter->level, m);
		int			idx = 0;
		int			lc;

		CHECK_FOR_INTERRUPTS();

		MemSet(ntupBuf, 0, ntupAlloc);
		ntup->type = TQHNSW_NEIGHBOR_TUPLE_TYPE;
		ntup->version = 0;

		for (lc = iter->level; lc >= 0; lc--)
		{
			int			lm = TqhnswGetLayerM(m, lc);
			int			i;

			for (i = 0; i < lm; i++)
			{
				ItemPointer indextid = &ntup->indextids[idx++];

				{
					TqhnswNeighborArray *na = TqhnswGetNeighbors(buildstate->base, iter, lc);

					if (i < na->count)
					{
						TqhnswElement *ne = TqhnswPtrAccess(buildstate->base, na->items[i].element);

						ItemPointerSet(indextid, ne->blkno, ne->offno);
					}
					else
						ItemPointerSetInvalid(indextid);
				}
			}
		}
		ntup->count = (uint16) idx;

		buf = ReadBufferExtended(index, MAIN_FORKNUM, iter->neighborPage,
								 RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		if (!PageIndexTupleOverwrite(page, iter->neighborOffno, (Pointer) ntup, ntupSize))
			elog(ERROR, "failed to add index item to \"%s\"",
				 RelationGetRelationName(index));

		TqCommitBuffer(buf, state);
	}

	/* Back-patch the meta page entry point + nVectors. */
	buf = ReadBufferExtended(index, MAIN_FORKNUM, TQHNSW_METAPAGE_BLKNO,
							 RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	{
		TqhnswMetaPage metap = TqhnswPageGetMeta(page);
		TqhnswElement *entryPoint = TqhnswPtrAccess(buildstate->base, buildstate->graph->entryPoint);

		metap->nVectors = (uint32) buildstate->graph->nVectors;
		metap->firstElementPage = buildstate->firstElementPage;

		/*
		 * Insert hint: the LAST element page (mirrors hnswbuild) -- earlier
		 * pages are full, so hinting at the first would make the next insert
		 * lock and walk the whole element chain.
		 */
		metap->insertPage = lastElementPage;
		if (entryPoint != NULL)
		{
			metap->entryBlkno = entryPoint->blkno;
			metap->entryOffno = entryPoint->offno;
			metap->entryLevel = entryPoint->level;
		}
	}
	TqCommitBuffer(buf, state);

	pfree(etupBuf);
	pfree(ntupBuf);

	/*
	 * Serial path: free the in-memory graph now that it is on disk (mirrors
	 * the MemoryContextReset in hnsw's FlushPages).  On a mid-build OOM flush
	 * this is what actually reclaims maintenance_work_mem -- the caller sets
	 * graph->flushed so graph->head is never walked again.  The parallel
	 * build keeps its graph in the DSM area (not graphCtx) and tears it down
	 * via TqhnswEndParallel, so skip the reset there.
	 */
	if (buildstate->base == NULL)
	{
		MemoryContextReset(buildstate->graphCtx);
		buildstate->graph->memoryUsed = 0;
		TqhnswPtrStore(buildstate->base, buildstate->graph->head,
					   (TqhnswElement *) NULL);
	}
}

/*
 * Set up a worker's local build state: resolve params, LOAD the TQ model from
 * the index pages (the leader wrote it before launching), and prepare the encode
 * scratch + memory contexts.  The caller wires up the shared graph + DSM allocator.
 */
static void
TqhnswInitWorkerBuildState(TqhnswBuildState *buildstate, Relation index)
{
	int			bits = TQ_DEFAULT_BITS;

	memset(buildstate, 0, sizeof(*buildstate));
	buildstate->index = index;
	TqhnswResolveBuildParams(index, &buildstate->dim, &buildstate->metric,
							 &buildstate->m, &buildstate->efConstruction,
							 &buildstate->fastRotation);
	buildstate->model = TqhnswLoadModel(index, CurrentMemoryContext);
	buildstate->dimCodes = buildstate->model->dimCodes;
	buildstate->codesBytes = TQ_CODES_BYTES(buildstate->model->dimCodes, bits);
	buildstate->ml = TqhnswGetMl(buildstate->m);
	buildstate->maxLevel = TqhnswGetMaxLevel(buildstate->m);
	buildstate->scratchSize = TqEntrySize(buildstate->model->dimCodes, bits, false);
	buildstate->scratch = (TqEntry *) palloc(buildstate->scratchSize);
	buildstate->typeInfo = TqGetTypeInfo(index, TQHNSW_TYPE_INFO_PROC);
	buildstate->vecScratch = palloc(sizeof(float) * buildstate->dim);
	buildstate->rhatScratch = (float *) palloc(sizeof(float) * buildstate->dimCodes);
	buildstate->graphCtx = AllocSetContextCreate(CurrentMemoryContext,
												 "tqhnsw build graph",
												 ALLOCSET_DEFAULT_SIZES);
	buildstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											   "tqhnsw build temporary",
											   ALLOCSET_DEFAULT_SIZES);
}

/*
 * Perform a worker's portion of the parallel build: join the parallel heap scan
 * and insert into the shared DSM graph.  Mirrors HnswParallelScanAndInsert.
 */
static void
TqhnswParallelScanAndInsert(Relation heapRel, Relation indexRel,
							TqhnswShared *tqhnswshared, char *tqhnswarea, bool progress)
{
	TqhnswBuildState buildstate;
	TableScanDesc scan;
	double		reltuples;
	IndexInfo  *indexInfo;

	TqhnswInitWorkerBuildState(&buildstate, indexRel);
	buildstate.graph = &tqhnswshared->graphData;
	buildstate.tqhnswarea = tqhnswarea;
	buildstate.base = tqhnswarea;
	buildstate.allocator.alloc = TqhnswSharedMemoryAlloc;
	buildstate.allocator.state = &buildstate;

	indexInfo = BuildIndexInfo(indexRel);
	indexInfo->ii_Concurrent = tqhnswshared->isconcurrent;

	scan = table_beginscan_parallel(heapRel,
									ParallelTableScanFromTqhnswShared(tqhnswshared)
#if PG_VERSION_NUM >= 190000
									,SO_NONE
#endif
		);
	reltuples = table_index_build_scan(heapRel, indexRel, indexInfo,
									   true, progress, TqhnswBuildCallback,
									   (void *) &buildstate, scan);

	SpinLockAcquire(&tqhnswshared->mutex);
	tqhnswshared->nparticipantsdone++;
	tqhnswshared->reltuples += reltuples;
	SpinLockRelease(&tqhnswshared->mutex);

	ConditionVariableSignal(&tqhnswshared->workersdonecv);

	MemoryContextDelete(buildstate.graphCtx);
	MemoryContextDelete(buildstate.tmpCtx);
}

/*
 * Parallel worker entry point.  MUST be a non-static PGDLLEXPORT symbol named in
 * the parallel-context lookup (CreateParallelContext "TqhnswParallelBuildMain").
 */
PGDLLEXPORT void
TqhnswParallelBuildMain(dsm_segment *seg, shm_toc *toc)
{
	char	   *sharedquery;
	TqhnswShared *tqhnswshared;
	char	   *tqhnswarea;
	Relation	heapRel;
	Relation	indexRel;
	LOCKMODE	heapLockmode;
	LOCKMODE	indexLockmode;

	sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true);
	debug_query_string = sharedquery;
	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	tqhnswshared = shm_toc_lookup(toc, PARALLEL_KEY_TQHNSW_SHARED, false);

	if (!tqhnswshared->isconcurrent)
	{
		heapLockmode = ShareLock;
		indexLockmode = AccessExclusiveLock;
	}
	else
	{
		heapLockmode = ShareUpdateExclusiveLock;
		indexLockmode = RowExclusiveLock;
	}

	heapRel = table_open(tqhnswshared->heaprelid, heapLockmode);
	indexRel = index_open(tqhnswshared->indexrelid, indexLockmode);

	tqhnswarea = shm_toc_lookup(toc, PARALLEL_KEY_TQHNSW_AREA, false);

	TqhnswParallelScanAndInsert(heapRel, indexRel, tqhnswshared, tqhnswarea, false);

	index_close(indexRel, indexLockmode);
	table_close(heapRel, heapLockmode);
}

/*
 * Wait for parallel workers to finish their portion of the scan, then point the
 * leader's build state at the completed shared DSM graph.  Mirrors
 * ParallelHeapScan.  NOTE: also repoints buildstate->base at the DSM area so the
 * subsequent flush resolves relptrs correctly (tqhnsw-specific base field).
 */
static double
ParallelTqhnswHeapScan(TqhnswBuildState *buildstate)
{
	TqhnswShared *tqhnswshared = buildstate->tqhnswleader->tqhnswshared;
	int			nparticipanttuplesorts;
	double		reltuples;

	nparticipanttuplesorts = buildstate->tqhnswleader->nparticipanttuplesorts;
	for (;;)
	{
		SpinLockAcquire(&tqhnswshared->mutex);
		if (tqhnswshared->nparticipantsdone == nparticipanttuplesorts)
		{
			buildstate->graph = &tqhnswshared->graphData;
			buildstate->base = buildstate->tqhnswleader->tqhnswarea;
			reltuples = tqhnswshared->reltuples;
			SpinLockRelease(&tqhnswshared->mutex);
			break;
		}
		SpinLockRelease(&tqhnswshared->mutex);

		ConditionVariableSleep(&tqhnswshared->workersdonecv,
							   WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
	}

	ConditionVariableCancelSleep();

	return reltuples;
}

/*
 * End parallel build.  Mirrors HnswEndParallel.
 */
static void
TqhnswEndParallel(TqhnswLeader *tqhnswleader)
{
	/* Shutdown worker processes */
	WaitForParallelWorkersToFinish(tqhnswleader->pcxt);

	/* Free last reference to MVCC snapshot, if one was used */
	if (IsMVCCSnapshot(tqhnswleader->snapshot))
		UnregisterSnapshot(tqhnswleader->snapshot);
	DestroyParallelContext(tqhnswleader->pcxt);
	ExitParallelMode();
}

/*
 * Return size of shared memory required for parallel index build.  Mirrors
 * ParallelEstimateShared.
 */
static Size
ParallelEstimateShared(Relation heap, Snapshot snapshot)
{
	return add_size(BUFFERALIGN(sizeof(TqhnswShared)), table_parallelscan_estimate(heap, snapshot));
}

/*
 * Within leader, participate as a parallel worker.  Mirrors
 * HnswLeaderParticipateAsWorker.
 */
static void
TqhnswLeaderParticipateAsWorker(TqhnswBuildState *buildstate)
{
	TqhnswLeader *tqhnswleader = buildstate->tqhnswleader;

	/* Perform work common to all participants */
	TqhnswParallelScanAndInsert(buildstate->heap, buildstate->index,
								tqhnswleader->tqhnswshared, tqhnswleader->tqhnswarea, true);
}

/*
 * Begin parallel build.  Mirrors HnswBeginParallel.
 */
static void
TqhnswBeginParallel(TqhnswBuildState *buildstate, bool isconcurrent, int request)
{
	ParallelContext *pcxt;
	Snapshot	snapshot;
	Size		esttqhnswshared;
	Size		esttqhnswarea;
	Size		estother;
	TqhnswShared *tqhnswshared;
	char	   *tqhnswarea;
	TqhnswLeader *tqhnswleader = (TqhnswLeader *) palloc0(sizeof(TqhnswLeader));
	bool		leaderparticipates = true;
	int			querylen;

#ifdef DISABLE_LEADER_PARTICIPATION
	leaderparticipates = false;
#endif

	/* Enter parallel mode and create context */
	EnterParallelMode();
	Assert(request > 0);
	pcxt = CreateParallelContext("vector", "TqhnswParallelBuildMain", request);

	/* Get snapshot for table scan */
	if (!isconcurrent)
		snapshot = SnapshotAny;
	else
		snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/* Estimate size of workspaces */
	esttqhnswshared = ParallelEstimateShared(buildstate->heap, snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, esttqhnswshared);

	/* Leave space for other objects in shared memory */
	/* Docker has a default limit of 64 MB for shm_size */
	/* which happens to be the default value of maintenance_work_mem */
	esttqhnswarea = maintenance_work_mem * 1024L;
	estother = 3 * 1024 * 1024;
	if (esttqhnswarea > estother)
		esttqhnswarea -= estother;

	esttqhnswarea = Min(esttqhnswarea, TQHNSW_MAX_GRAPH_MEMORY);

	shm_toc_estimate_chunk(&pcxt->estimator, esttqhnswarea);
	shm_toc_estimate_keys(&pcxt->estimator, 2);

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
	tqhnswshared = (TqhnswShared *) shm_toc_allocate(pcxt->toc, esttqhnswshared);
	/* Initialize immutable state */
	tqhnswshared->heaprelid = RelationGetRelid(buildstate->heap);
	tqhnswshared->indexrelid = RelationGetRelid(buildstate->index);
	tqhnswshared->isconcurrent = isconcurrent;
	ConditionVariableInit(&tqhnswshared->workersdonecv);
	SpinLockInit(&tqhnswshared->mutex);
	/* Initialize mutable state */
	tqhnswshared->nparticipantsdone = 0;
	tqhnswshared->reltuples = 0;
	table_parallelscan_initialize(buildstate->heap,
								  ParallelTableScanFromTqhnswShared(tqhnswshared),
								  snapshot);

	tqhnswarea = (char *) shm_toc_allocate(pcxt->toc, esttqhnswarea);
	TqhnswInitGraph(&tqhnswshared->graphData, tqhnswarea, esttqhnswarea);

	/*
	 * Avoid base address for relptr for Postgres < 14.5
	 * https://github.com/postgres/postgres/commit/7201cd18627afc64850537806da7f22150d1a83b
	 */
#if PG_VERSION_NUM < 140005
	tqhnswshared->graphData.memoryUsed += MAXALIGN(1);
#endif

	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TQHNSW_SHARED, tqhnswshared);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TQHNSW_AREA, tqhnswarea);

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
	tqhnswleader->pcxt = pcxt;
	tqhnswleader->nparticipanttuplesorts = pcxt->nworkers_launched;
	if (leaderparticipates)
		tqhnswleader->nparticipanttuplesorts++;
	tqhnswleader->tqhnswshared = tqhnswshared;
	tqhnswleader->snapshot = snapshot;
	tqhnswleader->tqhnswarea = tqhnswarea;

	/* If no workers were successfully launched, back out (do serial build) */
	if (pcxt->nworkers_launched == 0)
	{
		TqhnswEndParallel(tqhnswleader);
		return;
	}

	/* Log participants */
	ereport(DEBUG1, (errmsg("using %d parallel workers", pcxt->nworkers_launched)));

	/* Save leader state now that it's clear build will be parallel */
	buildstate->tqhnswleader = tqhnswleader;

	/* Join heap scan ourselves */
	if (leaderparticipates)
		TqhnswLeaderParticipateAsWorker(buildstate);

	/* Wait for all launched workers */
	WaitForParallelWorkersToAttach(pcxt);
}

/*
 * Compute parallel workers.  Mirrors ComputeParallelWorkers.
 */
static int
ComputeParallelWorkers(Relation heap, Relation index)
{
	int			parallel_workers;

	/* Make sure it's safe to use parallel workers */
	parallel_workers = plan_create_index_workers(RelationGetRelid(heap), RelationGetRelid(index));
	if (parallel_workers == 0)
		return 0;

	/* Use parallel_workers storage parameter on table if set */
	parallel_workers = RelationGetParallelWorkers(heap, -1);
	if (parallel_workers != -1)
		return Min(parallel_workers, max_parallel_maintenance_workers);

	return max_parallel_maintenance_workers;
}

/*
 * Build the graph: launch parallel workers when warranted, otherwise scan
 * serially.  Flushes the graph to disk before any DSM teardown.
 */
static double
TqhnswBuildGraph(TqhnswBuildState *buildstate, Relation heap, Relation index,
				 IndexInfo *indexInfo)
{
	int			parallel_workers = 0;
	double		reltuples = 0;

	if (heap != NULL)
		parallel_workers = ComputeParallelWorkers(heap, index);

	if (parallel_workers > 0)
		TqhnswBeginParallel(buildstate, indexInfo->ii_Concurrent, parallel_workers);

	if (heap != NULL)
	{
		if (buildstate->tqhnswleader)
			reltuples = ParallelTqhnswHeapScan(buildstate);
		else
			reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
											   TqhnswBuildCallback, (void *) buildstate, NULL);
	}

	/* Capture totals before any DSM teardown. */
	buildstate->indtuples = buildstate->graph->indtuples;

	/* Flush the in-memory graph (unless a mid-build flush already did). */
	if (!buildstate->graph->flushed && buildstate->graph->nVectors > 0)
	{
		TqhnswFlushGraph(buildstate);
		buildstate->graph->flushed = true;
	}

	if (buildstate->tqhnswleader)
		TqhnswEndParallel(buildstate->tqhnswleader);

	return reltuples;
}

IndexBuildResult *
tqhnswbuild(Relation heap, Relation index, struct IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	double		reltuples;
	TqhnswBuildState buildstate;
	TqModel		model;
	BlockNumber cbStart;
	BlockNumber rotStart;
	int			bits = TQ_DEFAULT_BITS;

	memset(&buildstate, 0, sizeof(buildstate));
	buildstate.index = index;

	/*
	 * Rerank fetches the raw heap column (indkey.values[0]); an expression
	 * index has no backing attribute (attno 0) and cannot be reranked.
	 */
	if (indexInfo->ii_Expressions != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("tqhnsw indexes do not support expression index columns")));

	TqhnswResolveBuildParams(index, &buildstate.dim, &buildstate.metric,
							 &buildstate.m, &buildstate.efConstruction,
							 &buildstate.fastRotation);

	/* Build the model + write meta + codebook side pages (MAIN fork). */
	model.boundaries = palloc(sizeof(float) * ((1 << bits) - 1));
	model.centroids = palloc(sizeof(float) * (1 << bits));
	TqhnswWriteModelAndMeta(index, MAIN_FORKNUM, buildstate.dim, buildstate.metric,
							buildstate.m, buildstate.efConstruction,
							buildstate.fastRotation, &cbStart, &rotStart, &model);

	buildstate.model = &model;
	buildstate.dimCodes = model.dimCodes;
	buildstate.codesBytes = TQ_CODES_BYTES(model.dimCodes, bits);
	buildstate.ml = TqhnswGetMl(buildstate.m);
	buildstate.maxLevel = TqhnswGetMaxLevel(buildstate.m);

	/* Scratch encode entry + type-info vtable. */
	buildstate.scratchSize = TqEntrySize(model.dimCodes, bits, false);
	buildstate.scratch = (TqEntry *) palloc(buildstate.scratchSize);
	buildstate.typeInfo = TqGetTypeInfo(index, TQHNSW_TYPE_INFO_PROC);
	buildstate.vecScratch = palloc(sizeof(float) * buildstate.dim);
	buildstate.rhatScratch = (float *) palloc(sizeof(float) * buildstate.dimCodes);

	if (buildstate.typeInfo->toFloat == TqSparsevecToFloat)
		ereport(NOTICE,
				(errmsg("tq densifies sparsevec to %d dimensions; sparse storage advantage is not preserved",
						buildstate.dim)));

	buildstate.graphCtx = AllocSetContextCreate(CurrentMemoryContext,
												"tqhnsw build graph",
												ALLOCSET_DEFAULT_SIZES);
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "tqhnsw build temporary",
											  ALLOCSET_DEFAULT_SIZES);

	buildstate.base = NULL;
	buildstate.allocator.alloc = TqhnswMemoryContextAlloc;
	buildstate.allocator.state = &buildstate;

	buildstate.heap = heap;
	buildstate.tqhnswleader = NULL;

	TqhnswInitGraph(&buildstate.graphData, NULL, (Size) maintenance_work_mem * 1024L);
	buildstate.graph = &buildstate.graphData;

	/*
	 * The callback manages contexts itself: durable graph nodes are allocated
	 * into graphCtx, while the per-insert search/select scratch goes into
	 * tmpCtx and is reset after every tuple.  Run the scan in the build
	 * context.
	 */
	reltuples = TqhnswBuildGraph(&buildstate, heap, index, indexInfo);
	/* TqhnswBuildGraph flushes internally (before any DSM teardown). */

	MemoryContextDelete(buildstate.tmpCtx);
	MemoryContextDelete(buildstate.graphCtx);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;
	return result;
}

void
tqhnswbuildempty(Relation index)
{
	TqhnswWriteEmptyIndex(index, INIT_FORKNUM);
}
