#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/reloptions.h"
#include "catalog/index.h"
#include "catalog/pg_type_d.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "tqivf.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"

#if PG_VERSION_NUM < 150000
#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)
#endif

int			tqivf_probes;
int			tqivf_rerank;
int			tqivf_iterative_scan;
int			tqivf_max_probes;
bool		tqivf_force_scalar;
static relopt_kind tqivf_relopt_kind;

static const struct config_enum_entry tqivf_iterative_scan_options[] = {
	{"off", TQIVF_ITERATIVE_SCAN_OFF, false},
	{"relaxed_order", TQIVF_ITERATIVE_SCAN_RELAXED, false},
	{NULL, 0, false}
};

/*
 * Initialize index options and variables
 */
void
TqivfInit(void)
{
	tqivf_relopt_kind = add_reloption_kind();
	add_int_reloption(tqivf_relopt_kind, "lists", "Number of inverted lists",
					  TQIVF_DEFAULT_LISTS, TQIVF_MIN_LISTS, TQIVF_MAX_LISTS, AccessExclusiveLock);
	add_bool_reloption(tqivf_relopt_kind, "fast_rotation",
					   "Use structured randomized Hadamard rotation",
					   TQ_DEFAULT_FAST_ROTATION, AccessExclusiveLock);

	DefineCustomIntVariable("tqivf.probes", "Number of lists to probe", NULL,
							&tqivf_probes, TQIVF_DEFAULT_PROBES, 1, TQIVF_MAX_LISTS,
							PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("tqivf.rerank", "Number of candidates to rerank with full precision", NULL,
							&tqivf_rerank, TQIVF_DEFAULT_RERANK, 0, TQIVF_MAX_RERANK,
							PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomEnumVariable("tqivf.iterative_scan", "Scan additional lists on demand", NULL,
							 &tqivf_iterative_scan, TQIVF_ITERATIVE_SCAN_OFF,
							 tqivf_iterative_scan_options, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("tqivf.max_probes", "Max lists to probe in iterative scan", NULL,
							&tqivf_max_probes, TQIVF_MAX_LISTS, 1, TQIVF_MAX_LISTS,
							PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("tqivf.force_scalar", "Score blocks with float LUT (debug A/B)", NULL,
							 &tqivf_force_scalar, false, PGC_USERSET, 0, NULL, NULL, NULL);

	MarkGUCPrefixReserved("tqivf");
}

/*
 * Parse and validate the reloptions
 */
static bytea *
tqivfoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"lists", RELOPT_TYPE_INT, offsetof(TqivfOptions, lists)},
		{"fast_rotation", RELOPT_TYPE_BOOL, offsetof(TqivfOptions, fastRotation)},
	};

	return (bytea *) build_reloptions(reloptions, validate, tqivf_relopt_kind,
									  sizeof(TqivfOptions), tab, lengthof(tab));
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool
tqivfvalidate(Oid opclassoid)
{
	return true;
}

/*
 * Estimate the cost of an index scan
 */
static void
tqivfcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
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
	 * Mirror ivfflatcostestimate: the first row is returned only after the
	 * probed lists are read, scored, and sorted, so charge that fraction of
	 * the total cost to startup; treat half of the page reads as sequential
	 * (lists are read in chain order).
	 */
	{
		double		sequentialRatio = 0.5;
		double		ratio;
		double		startupPages;
		double		spc_seq_page_cost;
		int			lists;
		Relation	index;

		index = index_open(path->indexinfo->indexoid, NoLock);
		TqivfGetMetaInfo(index, NULL, NULL, &lists, NULL);
		index_close(index, NoLock);

		ratio = ((double) tqivf_probes) / lists;
		if (ratio > 1.0)
			ratio = 1.0;

		get_tablespace_page_costs(path->indexinfo->reltablespace, NULL,
								  &spc_seq_page_cost);

		/* Change some page cost from random to sequential */
		costs.indexTotalCost -= sequentialRatio * costs.numIndexPages *
			(costs.spc_random_page_cost - spc_seq_page_cost);

		/* Startup cost is cost before returning the first row */
		costs.indexStartupCost = costs.indexTotalCost * ratio;

		/* Adjust cost if needed since TOAST not included in seq scan cost */
		startupPages = costs.numIndexPages * ratio;
		if (startupPages > path->indexinfo->rel->pages && ratio < 0.5)
		{
			/* Change rest of page cost from random to sequential */
			costs.indexStartupCost -= (1 - sequentialRatio) * startupPages *
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

/*
 * Define index handler
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqivfhandler);
Datum
tqivfhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 6;
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

	amroutine->ambuild = tqivfbuild;
	amroutine->ambuildempty = tqivfbuildempty;
	amroutine->aminsert = tqivfinsert;
#if PG_VERSION_NUM >= 170000
	amroutine->aminsertcleanup = NULL;
#endif
	amroutine->ambulkdelete = tqivfbulkdelete;
	amroutine->amvacuumcleanup = tqivfvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = tqivfcostestimate;
#if PG_VERSION_NUM >= 180000
	amroutine->amgettreeheight = NULL;
#endif
	amroutine->amoptions = tqivfoptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = tqivfvalidate;
#if PG_VERSION_NUM >= 140000
	amroutine->amadjustmembers = NULL;
#endif
	amroutine->ambeginscan = tqivfbeginscan;
	amroutine->amrescan = tqivfrescan;
	amroutine->amgettuple = tqivfgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = tqivfendscan;
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

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqivf_l2_support);
Datum
tqivf_l2_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_L2,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqVectorToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqivf_ip_support);
Datum
tqivf_ip_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_IP,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqVectorToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqivf_cosine_support);
Datum
tqivf_cosine_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_COSINE,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqVectorToFloat,
		.normalize = l2_normalize,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqivf_halfvec_l2_support);
Datum
tqivf_halfvec_l2_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_L2,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqHalfvecToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqivf_halfvec_ip_support);
Datum
tqivf_halfvec_ip_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_IP,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqHalfvecToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqivf_halfvec_cosine_support);
Datum
tqivf_halfvec_cosine_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_COSINE,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqHalfvecToFloat,
		.normalize = halfvec_l2_normalize,
	};

	PG_RETURN_POINTER(&ti);
}

/*
 * TqivfLoadModel -- load the quantization model from the index meta/side pages.
 *
 * Adapted from tqbuild.c's TqLoadModel for the tqivf meta layout.  TurboQuant
 * is data-oblivious: ONE global rotation + ONE global codebook, shared by all
 * lists.  bits is fixed at 4 and tqProd/QJL are unused in the blocked layout.
 */
TqModel *
TqivfLoadModel(Relation index, MemoryContext ctx)
{
	Buffer		buf;
	Page		page;
	TqivfMetaPage metap;
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
	uint64		qjlSeed;
	Size		rotBytes;
	char	   *cbBuf;
	Size		cbBytes;

	buf = ReadBuffer(index, TQIVF_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = TqivfPageGetMeta(page);

	if (unlikely(metap->magicNumber != TQIVF_MAGIC_NUMBER))
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqivf index is not valid");
	}

	if (unlikely(metap->version != TQIVF_VERSION))
	{
		uint32		v = metap->version;

		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqivf index version %u not supported (expected %u)", v, TQIVF_VERSION);
	}

	dim = metap->dimensions;
	bits = metap->bits;
	nLevels = metap->nLevels;
	metric = (TqMetric) metap->metric;
	fastRotation = metap->fastRotation ? true : false;
	rotationStart = metap->rotationStart;
	codebookStart = metap->codebookStart;
	rotSeed = metap->rotSeed;
	qjlSeed = metap->qjlSeed;
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
	model->qjlSeed = qjlSeed;
	model->qjlScale = 0.0f;

	MemoryContextSwitchTo(oldCtx);

	/*
	 * Read the rotation side page back (dense mode only; absent in fast
	 * mode).
	 */
	if (!fastRotation)
	{
		if (!BlockNumberIsValid(rotationStart))
			elog(ERROR, "tqivf index has no rotation matrix");
		TqReadBytes(index, rotationStart, (char *) model->rotation, rotBytes);
	}

	if (!BlockNumberIsValid(codebookStart))
		elog(ERROR, "tqivf index has no codebook");
	cbBuf = palloc(cbBytes);
	TqReadBytes(index, codebookStart, cbBuf, cbBytes);
	memcpy(model->boundaries, cbBuf, sizeof(float) * nBnd);
	memcpy(model->centroids, cbBuf + sizeof(float) * nBnd, sizeof(float) * nLevels);
	pfree(cbBuf);

	return model;
}

/*
 * TqivfGetMetaInfo -- read fixed fields from the index meta page.
 */
void
TqivfGetMetaInfo(Relation index, int *dim, TqMetric *metric,
				 int *lists, BlockNumber *listStart)
{
	Buffer		buf;
	Page		page;
	TqivfMetaPage metap;

	buf = ReadBuffer(index, TQIVF_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = TqivfPageGetMeta(page);

	if (unlikely(metap->magicNumber != TQIVF_MAGIC_NUMBER))
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqivf index is not valid");
	}

	if (unlikely(metap->version != TQIVF_VERSION))
	{
		uint32		v = metap->version;

		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqivf index version %u not supported (expected %u)", v, TQIVF_VERSION);
	}

	if (dim != NULL)
		*dim = metap->dimensions;
	if (metric != NULL)
		*metric = (TqMetric) metap->metric;
	if (lists != NULL)
		*lists = metap->lists;
	if (listStart != NULL)
		*listStart = metap->listStart;

	UnlockReleaseBuffer(buf);
}

/*
 * tqivf_test_meta(regclass) RETURNS text
 *
 * Test-only helper: returns a formatted summary read from the meta page of a
 * tqivf index.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqivf_test_meta);
Datum
tqivf_test_meta(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	Relation	index;
	Buffer		buf;
	Page		page;
	TqivfMetaPage metap;
	int			dim;
	int			lists;
	int			bits;
	int			metric;
	uint32		nVectors;
	char		result[128];

	index = index_open(indexoid, AccessShareLock);

	buf = ReadBuffer(index, TQIVF_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = TqivfPageGetMeta(page);

	if (unlikely(metap->magicNumber != TQIVF_MAGIC_NUMBER))
	{
		UnlockReleaseBuffer(buf);
		index_close(index, AccessShareLock);
		elog(ERROR, "tqivf index is not valid");
	}

	dim = metap->dimensions;
	lists = metap->lists;
	bits = metap->bits;
	metric = (int) metap->metric;
	nVectors = metap->nVectors;
	UnlockReleaseBuffer(buf);

	index_close(index, AccessShareLock);

	snprintf(result, sizeof(result), "dim=%d lists=%d bits=%d metric=%d nvectors=%u",
			 dim, lists, bits, metric, nVectors);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * tqivf_test_list_counts(regclass) RETURNS int[]
 *
 * Test-only helper: walk the list directory and return each list's nvectors.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqivf_test_list_counts);
Datum
tqivf_test_list_counts(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	Relation	index;
	int			lists;
	BlockNumber listStart;
	Datum	   *elems;
	ArrayType  *result;
	int			n = 0;
	BlockNumber blkno;

	index = index_open(indexoid, AccessShareLock);

	TqivfGetMetaInfo(index, NULL, NULL, &lists, &listStart);

	elems = palloc(sizeof(Datum) * lists);

	blkno = listStart;
	while (BlockNumberIsValid(blkno) && n < lists)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber offno;

		buf = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		maxoff = PageGetMaxOffsetNumber(page);

		for (offno = FirstOffsetNumber; offno <= maxoff && n < lists; offno = OffsetNumberNext(offno))
		{
			TqivfList	list = (TqivfList) PageGetItem(page, PageGetItemId(page, offno));

			elems[n++] = Int32GetDatum((int) list->nvectors);
		}

		blkno = TqPageGetOpaque(page)->nextblkno;
		UnlockReleaseBuffer(buf);
	}

	index_close(index, AccessShareLock);

	result = construct_array(elems, n, INT4OID, sizeof(int32), true, TYPALIGN_INT);

	PG_RETURN_ARRAYTYPE_P(result);
}
