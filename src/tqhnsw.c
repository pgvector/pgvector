#include "postgres.h"

#include <math.h>

#include "access/amapi.h"
#include "access/genam.h"
#include "access/reloptions.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tqhnsw.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"

#if PG_VERSION_NUM < 150000
#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)
#endif

int			tqhnsw_ef_search;
int			tqhnsw_rerank;
bool		tqhnsw_force_scalar;
static relopt_kind tqhnsw_relopt_kind;
int			tqhnsw_lock_tranche_id;

/*
 * Assign a tranche ID for our LWLocks (mirrors HnswInitLockTranche). Only one
 * backend needs to do this; the id is remembered in shared memory.
 *
 * This shared memory area is very small, so we just allocate it from the
 * "slop" that PostgreSQL reserves for small allocations like this. If this
 * grows bigger, we should use a shmem_request_hook and RequestAddinShmemSpace()
 * to pre-reserve space for this.
 */
void
TqhnswInitLockTranche(void)
{
	int		   *tranche_ids;
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	tranche_ids = ShmemInitStruct("tqhnsw LWLock ids",
								  sizeof(int) * 1,
								  &found);
	if (!found)
	{
#if PG_VERSION_NUM >= 190000
		tranche_ids[0] = LWLockNewTrancheId("TqhnswBuild");
#else
		tranche_ids[0] = LWLockNewTrancheId();
#endif
	}
	tqhnsw_lock_tranche_id = tranche_ids[0];
	LWLockRelease(AddinShmemInitLock);

#if PG_VERSION_NUM < 190000
	/* Per-backend registration of the tranche ID */
	LWLockRegisterTranche(tqhnsw_lock_tranche_id, "TqhnswBuild");
#endif
}

void
TqhnswInit(void)
{
	if (!process_shared_preload_libraries_in_progress)
		TqhnswInitLockTranche();

	tqhnsw_relopt_kind = add_reloption_kind();
	add_int_reloption(tqhnsw_relopt_kind, "m", "Max number of connections",
					  TQHNSW_DEFAULT_M, TQHNSW_MIN_M, TQHNSW_MAX_M, AccessExclusiveLock);
	add_int_reloption(tqhnsw_relopt_kind, "ef_construction", "Size of the dynamic candidate list for construction",
					  TQHNSW_DEFAULT_EF_CONSTRUCTION, TQHNSW_MIN_EF_CONSTRUCTION, TQHNSW_MAX_EF_CONSTRUCTION, AccessExclusiveLock);
	add_bool_reloption(tqhnsw_relopt_kind, "fast_rotation",
					   "Use structured randomized Hadamard rotation",
					   TQ_DEFAULT_FAST_ROTATION, AccessExclusiveLock);

	DefineCustomIntVariable("tqhnsw.ef_search", "Size of the dynamic candidate list for search", NULL,
							&tqhnsw_ef_search, TQHNSW_DEFAULT_EF_SEARCH, TQHNSW_MIN_EF_SEARCH, TQHNSW_MAX_EF_SEARCH,
							PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("tqhnsw.rerank", "Number of candidates to rerank with full precision", NULL,
							&tqhnsw_rerank, TQHNSW_DEFAULT_RERANK, 0, TQHNSW_MAX_RERANK,
							PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("tqhnsw.force_scalar", "Score with the scalar LUT path (debug)", NULL,
							 &tqhnsw_force_scalar, false, PGC_USERSET, 0, NULL, NULL, NULL);

	MarkGUCPrefixReserved("tqhnsw");
}

static bytea *
tqhnswoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"m", RELOPT_TYPE_INT, offsetof(TqhnswOptions, m)},
		{"ef_construction", RELOPT_TYPE_INT, offsetof(TqhnswOptions, efConstruction)},
		{"fast_rotation", RELOPT_TYPE_BOOL, offsetof(TqhnswOptions, fastRotation)},
	};

	return (bytea *) build_reloptions(reloptions, validate, tqhnsw_relopt_kind,
									  sizeof(TqhnswOptions), tab, lengthof(tab));
}

static bool
tqhnswvalidate(Oid opclassoid)
{
	return true;				/* permissive, mirrors tqivfvalidate */
}

static void
tqhnswcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				   Cost *indexStartupCost, Cost *indexTotalCost,
				   Selectivity *indexSelectivity, double *indexCorrelation,
				   double *indexPages)
{
	GenericCosts costs;

	if (path->indexorderbys == NIL)
	{
		*indexStartupCost = get_float8_infinity();
		*indexTotalCost = get_float8_infinity();
		*indexSelectivity = 0;
		*indexCorrelation = 0;
		*indexPages = 0;
#if PG_VERSION_NUM >= 180000
		path->path.disabled_nodes = 2;
#endif
		return;
	}

	MemSet(&costs, 0, sizeof(costs));
	genericcostestimate(root, path, loop_count, &costs);

	/*
	 * Mirror hnswcostestimate: estimate the fraction of tuples touched
	 * before the first row is returned (entry-layer descent plus the layer-0
	 * search bounded by ef_search) and charge that fraction of the total
	 * cost to startup.  See hnsw.c for the formula's derivation.
	 */
	{
		double		ratio;
		double		startupPages;
		double		spc_seq_page_cost;
		int			m;
		Relation	index;

		index = index_open(path->indexinfo->indexoid, NoLock);
		TqhnswGetMetaInfo(index, NULL, NULL, &m, NULL, NULL, NULL, NULL, NULL);
		index_close(index, NoLock);

		if (path->indexinfo->tuples > 0)
		{
			double		scalingFactor = 0.55;
			int			entryLevel = (int) (log(path->indexinfo->tuples) * TqhnswGetMl(m));
			int			layer0TuplesMax = TqhnswGetLayerM(m, 0) * tqhnsw_ef_search;
			double		layer0Selectivity = scalingFactor * log(path->indexinfo->tuples) /
				(log(m) * (1 + log(tqhnsw_ef_search)));

			ratio = (entryLevel * m + layer0TuplesMax * layer0Selectivity) /
				path->indexinfo->tuples;

			if (ratio > 1)
				ratio = 1;
		}
		else
			ratio = 1;

		get_tablespace_page_costs(path->indexinfo->reltablespace, NULL,
								  &spc_seq_page_cost);

		/* Startup cost is cost before returning the first row */
		costs.indexStartupCost = costs.indexTotalCost * ratio;

		/* Adjust cost if needed since TOAST not included in seq scan cost */
		startupPages = costs.numIndexPages * ratio;
		if (startupPages > path->indexinfo->rel->pages && ratio < 0.5)
		{
			/* Change all page cost from random to sequential */
			costs.indexStartupCost -= startupPages *
				(costs.spc_random_page_cost - spc_seq_page_cost);

			/* Remove cost of extra pages */
			costs.indexStartupCost -= (startupPages - path->indexinfo->rel->pages) * spc_seq_page_cost;
		}
	}

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnswhandler);
Datum
tqhnswhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 3;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
#if PG_VERSION_NUM >= 180000
	amroutine->amcanhash = false;
	amroutine->amconsistentequality = false;
	amroutine->amconsistentordering = false;
#endif
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
#if PG_VERSION_NUM >= 170000
	amroutine->amcanbuildparallel = true;
#endif
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false;
#if PG_VERSION_NUM >= 160000
	amroutine->amsummarizing = false;
#endif
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = tqhnswbuild;
	amroutine->ambuildempty = tqhnswbuildempty;
	amroutine->aminsert = tqhnswinsert;
#if PG_VERSION_NUM >= 170000
	amroutine->aminsertcleanup = NULL;
#endif
	amroutine->ambulkdelete = tqhnswbulkdelete;
	amroutine->amvacuumcleanup = tqhnswvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = tqhnswcostestimate;
#if PG_VERSION_NUM >= 180000
	amroutine->amgettreeheight = NULL;
#endif
	amroutine->amoptions = tqhnswoptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = tqhnswvalidate;
#if PG_VERSION_NUM >= 140000
	amroutine->amadjustmembers = NULL;
#endif
	amroutine->ambeginscan = tqhnswbeginscan;
	amroutine->amrescan = tqhnswrescan;
	amroutine->amgettuple = tqhnswgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = tqhnswendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;
#if PG_VERSION_NUM >= 180000
	amroutine->amtranslatestrategy = NULL;
	amroutine->amtranslatecmptype = NULL;
#endif

	PG_RETURN_POINTER(amroutine);
}

/* Type-specific l2_normalize, wired into cosine TqTypeInfo vtables for rerank. */
PGDLLEXPORT Datum l2_normalize(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum halfvec_l2_normalize(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum sparsevec_l2_normalize(PG_FUNCTION_ARGS);

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnsw_l2_support);
Datum
tqhnsw_l2_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_L2,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqVectorToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnsw_ip_support);
Datum
tqhnsw_ip_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_IP,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqVectorToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnsw_cosine_support);
Datum
tqhnsw_cosine_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_COSINE,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqVectorToFloat,
		.normalize = l2_normalize,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnsw_halfvec_l2_support);
Datum
tqhnsw_halfvec_l2_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_L2,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqHalfvecToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnsw_halfvec_ip_support);
Datum
tqhnsw_halfvec_ip_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_IP,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqHalfvecToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnsw_halfvec_cosine_support);
Datum
tqhnsw_halfvec_cosine_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_COSINE,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqHalfvecToFloat,
		.normalize = halfvec_l2_normalize,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnsw_sparsevec_l2_support);
Datum
tqhnsw_sparsevec_l2_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_L2,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqSparsevecToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnsw_sparsevec_ip_support);
Datum
tqhnsw_sparsevec_ip_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_IP,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqSparsevecToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnsw_sparsevec_cosine_support);
Datum
tqhnsw_sparsevec_cosine_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_COSINE,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqSparsevecToFloat,
		.normalize = sparsevec_l2_normalize,
	};

	PG_RETURN_POINTER(&ti);
}

/*
 * TqhnswLoadModel -- load the quantization model from the index meta/side pages.
 *
 * Mirrors TqivfLoadModel (src/tqivf.c) against the TqhnswMetaPageData layout.
 * TurboQuant is data-oblivious: ONE global rotation + ONE global codebook.
 * bits is fixed at 4 and tqProd/QJL are unused.
 */
TqModel *
TqhnswLoadModel(Relation index, MemoryContext ctx)
{
	Buffer		buf;
	Page		page;
	TqhnswMetaPage metap;
	MemoryContext oldCtx;
	TqModel    *model;
	int			dim;
	int			bits;
	int			nLevels;
	int			nBnd;
	BlockNumber rotationStart;
	BlockNumber codebookStart;
	TqMetric	metric;
	bool		fastRotation;
	int			dimPadded;
	uint64		rotSeed;
	Size		rotBytes;
	char	   *cbBuf;
	Size		cbBytes;

	buf = ReadBuffer(index, TQHNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = TqhnswPageGetMeta(page);

	if (unlikely(metap->magicNumber != TQHNSW_MAGIC_NUMBER))
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqhnsw index is not valid");
	}

	if (unlikely(metap->version != TQHNSW_VERSION))
	{
		uint32		v = metap->version;

		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqhnsw index version %u not supported (expected %u)", v, TQHNSW_VERSION);
	}

	dim = metap->dimensions;
	bits = metap->bits;
	nLevels = metap->nLevels;
	metric = (TqMetric) metap->metric;
	fastRotation = metap->fastRotation ? true : false;
	rotationStart = metap->rotationStart;
	codebookStart = metap->codebookStart;
	rotSeed = metap->rotSeed;
	Assert(metap->dimPadded > 0);
	dimPadded = metap->dimPadded ? (int) metap->dimPadded : dim;
	UnlockReleaseBuffer(buf);

	nBnd = nLevels - 1;
	rotBytes = (Size) sizeof(float) * dim * dim;
	cbBytes = (Size) sizeof(float) * (nBnd + nLevels);

	oldCtx = MemoryContextSwitchTo(ctx);

	/* Single rd_amcache chunk; rotation pre-pointed only in dense mode. */
	model = TqAllocModel(ctx, dim, nLevels, !fastRotation, false);
	model->dim = dim;
	model->bits = bits;
	model->nLevels = nLevels;
	model->metric = metric;
	model->tqProd = false;
	model->fastRotation = fastRotation;
	model->dimPadded = dimPadded;
	model->dimCodes = fastRotation ? dimPadded : dim;
	model->rotSeed = rotSeed;
	model->qjlSeed = TQ_QJL_SEED;
	model->qjlScale = 0.0f;

	MemoryContextSwitchTo(oldCtx);

	/*
	 * Read the rotation side page back (dense mode only; absent in fast
	 * mode).
	 */
	if (!fastRotation)
	{
		if (!BlockNumberIsValid(rotationStart))
			elog(ERROR, "tqhnsw index has no rotation matrix");
		TqReadBytes(index, rotationStart, (char *) model->rotation, rotBytes);
	}

	if (!BlockNumberIsValid(codebookStart))
		elog(ERROR, "tqhnsw index has no codebook");
	cbBuf = palloc(cbBytes);
	TqReadBytes(index, codebookStart, cbBuf, cbBytes);
	memcpy(model->boundaries, cbBuf, sizeof(float) * nBnd);
	memcpy(model->centroids, cbBuf + sizeof(float) * nBnd, sizeof(float) * nLevels);
	pfree(cbBuf);

	return model;
}

/*
 * TqhnswGetCachedModel -- load the TqModel from rd_amcache, or (re)load it.
 *
 * Mirrors TqGetCachedModel (tqbuild.c): allocate the model in
 * index->rd_indexcxt (the per-index relcache memory context) and store a
 * pointer in rd_amcache.  Postgres sets rd_amcache = NULL on relcache
 * invalidation, so the next call reloads automatically.
 */
TqModel *
TqhnswGetCachedModel(Relation index)
{
	if (index->rd_amcache != NULL)
		return (TqModel *) index->rd_amcache;

	index->rd_amcache = TqhnswLoadModel(index, index->rd_indexcxt);
	return (TqModel *) index->rd_amcache;
}

/*
 * TqhnswGetMetaInfo -- read fixed graph/header fields from the index meta page.
 *
 * Mirrors TqivfGetMetaInfo but returns the HNSW graph header (m + entry point).
 */
void
TqhnswGetMetaInfo(Relation index, int *dim, TqMetric *metric, int *m,
				  BlockNumber *entryBlkno, OffsetNumber *entryOffno,
				  int *entryLevel, int *efConstruction,
				  BlockNumber *firstElementPage)
{
	Buffer		buf;
	Page		page;
	TqhnswMetaPage metap;

	buf = ReadBuffer(index, TQHNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = TqhnswPageGetMeta(page);

	if (unlikely(metap->magicNumber != TQHNSW_MAGIC_NUMBER))
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqhnsw index is not valid");
	}

	if (unlikely(metap->version != TQHNSW_VERSION))
	{
		uint32		v = metap->version;

		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqhnsw index version %u not supported (expected %u)", v, TQHNSW_VERSION);
	}

	if (dim != NULL)
		*dim = metap->dimensions;
	if (metric != NULL)
		*metric = (TqMetric) metap->metric;
	if (m != NULL)
		*m = metap->m;
	if (entryBlkno != NULL)
		*entryBlkno = metap->entryBlkno;
	if (entryOffno != NULL)
		*entryOffno = metap->entryOffno;
	if (entryLevel != NULL)
		*entryLevel = metap->entryLevel;
	if (efConstruction != NULL)
		*efConstruction = metap->efConstruction;
	if (firstElementPage != NULL)
		*firstElementPage = metap->firstElementPage;

	UnlockReleaseBuffer(buf);
}

/*
 * tqhnsw_test_meta(regclass) RETURNS text
 *
 * Test-only helper: returns a formatted summary read from the meta page of a
 * tqhnsw index.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnsw_test_meta);
Datum
tqhnsw_test_meta(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	Relation	index;
	Buffer		buf;
	Page		page;
	TqhnswMetaPage metap;
	int			dim;
	int			m;
	int			efConstruction;
	int			bits;
	int			metric;
	uint32		nVectors;
	int			entryLevel;
	char		result[160];

	index = index_open(indexoid, AccessShareLock);

	buf = ReadBuffer(index, TQHNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = TqhnswPageGetMeta(page);

	if (unlikely(metap->magicNumber != TQHNSW_MAGIC_NUMBER))
	{
		UnlockReleaseBuffer(buf);
		index_close(index, AccessShareLock);
		elog(ERROR, "tqhnsw index is not valid");
	}

	if (unlikely(metap->version != TQHNSW_VERSION))
	{
		uint32		v = metap->version;

		UnlockReleaseBuffer(buf);
		index_close(index, AccessShareLock);
		elog(ERROR, "tqhnsw index version %u is unsupported; REINDEX required", v);
	}

	dim = metap->dimensions;
	m = metap->m;
	efConstruction = metap->efConstruction;
	bits = metap->bits;
	metric = (int) metap->metric;
	nVectors = metap->nVectors;
	entryLevel = metap->entryLevel;
	UnlockReleaseBuffer(buf);

	index_close(index, AccessShareLock);

	snprintf(result, sizeof(result),
			 "dim=%d m=%d ef_construction=%d bits=%d metric=%d nvectors=%u entry_level=%d",
			 dim, m, efConstruction, bits, metric, nVectors, entryLevel);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * tqhnsw_test_graph(regclass) RETURNS text
 *
 * Structural sanity check: walk the element pages to count nodes, then follow the
 * meta entry point -> its neighbor tuple and count valid level-0 neighbors.
 * Prints STABLE predicates (counts + booleans) so the expected output is
 * deterministic even though node levels are drawn from an unseeded PRNG.
 * Test-only helper.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhnsw_test_graph);
Datum
tqhnsw_test_graph(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	Relation	index;
	Buffer		buf;
	Page		page;
	TqhnswMetaPage metap;
	BlockNumber entryBlkno;
	OffsetNumber entryOffno;
	int			entryLevel;
	int			m;
	int			dimCodes;
	int			bits;
	int			codesBytes;
	BlockNumber nblocks;
	BlockNumber blkno;
	int64		nodes = 0;
	int			entryNeighbors0 = 0;
	char		result[160];

	index = index_open(indexoid, AccessShareLock);

	/* Read meta. */
	buf = ReadBuffer(index, TQHNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = TqhnswPageGetMeta(page);

	if (unlikely(metap->magicNumber != TQHNSW_MAGIC_NUMBER))
	{
		UnlockReleaseBuffer(buf);
		index_close(index, AccessShareLock);
		elog(ERROR, "tqhnsw index is not valid");
	}

	if (unlikely(metap->version != TQHNSW_VERSION))
	{
		uint32		v = metap->version;

		UnlockReleaseBuffer(buf);
		index_close(index, AccessShareLock);
		elog(ERROR, "tqhnsw index version %u is unsupported; REINDEX required", v);
	}

	entryBlkno = metap->entryBlkno;
	entryOffno = metap->entryOffno;
	entryLevel = metap->entryLevel;
	m = metap->m;
	bits = metap->bits;
	dimCodes = metap->fastRotation ? metap->dimPadded : metap->dimensions;
	codesBytes = TQ_CODES_BYTES(dimCodes, bits);
	UnlockReleaseBuffer(buf);

	/*
	 * Count element tuples across all blocks (skip the meta page at block 0).
	 * Codebook/rotation side pages share the page id, so additionally require
	 * the item length to equal an element tuple's size to avoid miscounting
	 * raw side-page chunks.
	 */
	nblocks = RelationGetNumberOfBlocks(index);
	for (blkno = 1; blkno < nblocks; blkno++)
	{
		OffsetNumber offno;
		OffsetNumber maxoff;

		buf = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		if (PageGetSpecialSize(page) == MAXALIGN(sizeof(TqhnswPageOpaqueData)) &&
			TqhnswPageGetOpaque(page)->page_id == TQHNSW_PAGE_ID)
		{
			maxoff = PageGetMaxOffsetNumber(page);
			for (offno = FirstOffsetNumber; offno <= maxoff; offno = OffsetNumberNext(offno))
			{
				ItemId		itemid = PageGetItemId(page, offno);
				TqhnswElementTuple etup;

				if (!ItemIdIsUsed(itemid))
					continue;
				etup = (TqhnswElementTuple) PageGetItem(page, itemid);
				if (etup->type == TQHNSW_ELEMENT_TUPLE_TYPE &&
					ItemIdGetLength(itemid) == (unsigned) TQHNSW_ELEMENT_TUPLE_SIZE(codesBytes))
					nodes++;
			}
		}
		UnlockReleaseBuffer(buf);
	}

	/* Count entry node's valid level-0 neighbors. */
	if (BlockNumberIsValid(entryBlkno))
	{
		ItemPointerData neighbortid;
		uint8		elevel;
		BlockNumber nbrPage;
		OffsetNumber nbrOffno;

		buf = ReadBuffer(index, entryBlkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		{
			TqhnswElementTuple etup =
			(TqhnswElementTuple) PageGetItem(page, PageGetItemId(page, entryOffno));

			elevel = etup->level;
			neighbortid = etup->neighbortid;
		}
		UnlockReleaseBuffer(buf);

		nbrPage = ItemPointerGetBlockNumber(&neighbortid);
		nbrOffno = ItemPointerGetOffsetNumber(&neighbortid);

		buf = ReadBuffer(index, nbrPage);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		{
			TqhnswNeighborTuple ntup =
			(TqhnswNeighborTuple) PageGetItem(page, PageGetItemId(page, nbrOffno));
			int			start = (elevel - 0) * m;	/* level-0 slice start */
			int			lm = TqhnswGetLayerM(m, 0);
			int			i;

			for (i = 0; i < lm; i++)
			{
				if (ItemPointerIsValid(&ntup->indextids[start + i]))
					entryNeighbors0++;
			}
		}
		UnlockReleaseBuffer(buf);
	}

	index_close(index, AccessShareLock);

	snprintf(result, sizeof(result),
			 "nodes=%lld entry_level_ge_0=%c entry_has_neighbors=%c",
			 (long long) nodes,
			 entryLevel >= 0 ? 't' : 'f',
			 entryNeighbors0 > 0 ? 't' : 'f');

	PG_RETURN_TEXT_P(cstring_to_text(result));
}
