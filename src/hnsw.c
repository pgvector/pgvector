#include "postgres.h"

#include <float.h>
#include <math.h>

#include "access/amapi.h"
#include "access/reloptions.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "hnsw.h"
#include "miscadmin.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"

#if PG_VERSION_NUM < 150000
#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)
#endif

static const struct config_enum_entry hnsw_iterative_scan_options[] = {
	{"off", HNSW_ITERATIVE_SCAN_OFF, false},
	{"relaxed_order", HNSW_ITERATIVE_SCAN_RELAXED, false},
	{"strict_order", HNSW_ITERATIVE_SCAN_STRICT, false},
	{NULL, 0, false}
};

int			hnsw_ef_search;
int			hnsw_iterative_scan;
int			hnsw_max_scan_tuples;
double		hnsw_scan_mem_multiplier;
int			hnsw_lock_tranche_id;
static relopt_kind hnsw_relopt_kind;

/*
 * Assign a tranche ID for our LWLocks. This only needs to be done by one
 * backend, as the tranche ID is remembered in shared memory.
 *
 * This shared memory area is very small, so we just allocate it from the
 * "slop" that PostgreSQL reserves for small allocations like this. If
 * this grows bigger, we should use a shmem_request_hook and
 * RequestAddinShmemSpace() to pre-reserve space for this.
 */
void
HnswInitLockTranche(void)
{
	int		   *tranche_ids;
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	tranche_ids = ShmemInitStruct("hnsw LWLock ids",
								  sizeof(int) * 1,
								  &found);
	if (!found)
		tranche_ids[0] = LWLockNewTrancheId();
	hnsw_lock_tranche_id = tranche_ids[0];
	LWLockRelease(AddinShmemInitLock);

	/* Per-backend registration of the tranche ID */
	LWLockRegisterTranche(hnsw_lock_tranche_id, "HnswBuild");
}

/*
 * Initialize index options and variables
 */
void
HnswInit(void)
{
	if (!process_shared_preload_libraries_in_progress)
		HnswInitLockTranche();

	hnsw_relopt_kind = add_reloption_kind();
	add_int_reloption(hnsw_relopt_kind, "m", "Max number of connections",
					  HNSW_DEFAULT_M, HNSW_MIN_M, HNSW_MAX_M, AccessExclusiveLock);
	add_int_reloption(hnsw_relopt_kind, "ef_construction", "Size of the dynamic candidate list for construction",
					  HNSW_DEFAULT_EF_CONSTRUCTION, HNSW_MIN_EF_CONSTRUCTION, HNSW_MAX_EF_CONSTRUCTION, AccessExclusiveLock);

	DefineCustomIntVariable("hnsw.ef_search", "Sets the size of the dynamic candidate list for search",
							"Valid range is 1..1000.", &hnsw_ef_search,
							HNSW_DEFAULT_EF_SEARCH, HNSW_MIN_EF_SEARCH, HNSW_MAX_EF_SEARCH, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomEnumVariable("hnsw.iterative_scan", "Sets the mode for iterative scans",
							 NULL, &hnsw_iterative_scan,
							 HNSW_ITERATIVE_SCAN_OFF, hnsw_iterative_scan_options, PGC_USERSET, 0, NULL, NULL, NULL);

	/* This is approximate and does not affect the initial scan */
	DefineCustomIntVariable("hnsw.max_scan_tuples", "Sets the max number of tuples to visit for iterative scans",
							NULL, &hnsw_max_scan_tuples,
							20000, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);

	/* Same range as hash_mem_multiplier */
	DefineCustomRealVariable("hnsw.scan_mem_multiplier", "Sets the multiple of work_mem to use for iterative scans",
							 NULL, &hnsw_scan_mem_multiplier,
							 1, 1, 1000, PGC_USERSET, 0, NULL, NULL, NULL);

	MarkGUCPrefixReserved("hnsw");
}

/*
 * Get the name of index build phase
 */
static char *
hnswbuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
		case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
			return "initializing";
		case PROGRESS_HNSW_PHASE_LOAD:
			return "loading tuples";
		default:
			return NULL;
	}
}

/*
 * Estimate the cost of an index scan
 */
static void
hnswcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                 Cost *indexStartupCost, Cost *indexTotalCost,
                 Selectivity *indexSelectivity, double *indexCorrelation,
                 double *indexPages)
{
    GenericCosts costs;
    int     m;
    double  ratio;
    Relation index;

    /* Only meaningful for ORDER BY <=> queries */
    if (path->indexorderbys == NULL)
    {
        *indexStartupCost = get_float8_infinity();
        *indexTotalCost   = get_float8_infinity();
        *indexSelectivity = 0;
        *indexCorrelation = 0;
        *indexPages       = 0;
#if PG_VERSION_NUM >= 180000
        path->path.disabled_nodes = 2;
#endif
        return;
    }

    MemSet(&costs, 0, sizeof(costs));
    genericcostestimate(root, path, loop_count, &costs);   /* base costs */  /* docs describe these standard params */ /* [3](https://www.postgresql.org/docs/current/index-cost-estimation.html) */

    /* HNSW metadata */
    index = index_open(path->indexinfo->indexoid, NoLock);
    HnswGetMetaPageInfo(index, &m, NULL);
    index_close(index, NoLock);

    /* --- Visit fraction -------------------------------------------------- */
    double N  = (path->indexinfo->tuples > 1) ? path->indexinfo->tuples : 1.0;
    int    M  = m;
    int    ef = hnsw_ef_search;

	/* Defaults, should be exposed as parameters*/
	double HNSW_C1 = 1.0
	double HNSW_C2 = 1.0
	double hnsw_cpu_per_distance = 0.0005

    /* expected visited nodes ~ c1*M*log(N) + c2*ef  (HNSW theory) */       /* [1](https://arxiv.org/abs/1603.09320)[2](https://users.cs.utah.edu/~pandey/courses/cs6530/fall24/papers/vectordb/HNSW.pdf) */
    double visited  = HNSW_C1 * (double)M * log(N) + HNSW_C2 * (double)ef;
    ratio = visited / N;
    if (ratio > 1.0) ratio = 1.0;
    if (ratio < 0.0) ratio = 0.0;

    /* LIMIT awareness (optional) */
    double K = clamp_row_est(path->path.rows);
    if (K > 0 && K < N)
    {
        double limit_bias = 1.0 - exp(-K / (ef + 1.0));
        ratio *= limit_bias;
    }

    /* --- Scale base costs ------------------------------------------------ */
    double baseStartup = costs.indexStartupCost;
    double baseTotal   = costs.indexTotalCost;
    double runPart     = baseTotal - baseStartup;

    /* model “first row” cheaper (optional) */
    costs.indexStartupCost = baseStartup * ratio;

    /* run part scales with visited fraction */
    runPart *= ratio;

    /* --- Distance-evaluation CPU surcharge ------------------------------ */
    double dims          = hnsw_vector_dims;            /* GUC or detect from index key typmod */
    double op_cost       = Max(cpu_operator_cost, 0.0);
    double per_eval_cpu  = hnsw_cpu_per_distance * (dims / 128.0); /* tunable */

    double evals = (HNSW_C1 * (double)M * log(N)) + (HNSW_C2 * (double)ef);
    runPart += evals * (op_cost + per_eval_cpu);        /* add to run part */

    costs.indexTotalCost = baseStartup + runPart;

    /* --- Correlation & pages -------------------------------------------- */
    *indexCorrelation = 0.0;                            /* HNSW ~ no heap order correlation */ /* [3](https://www.postgresql.org/docs/current/index-cost-estimation.html) */
    costs.numIndexPages *= ratio;                       /* fewer leaf pages touched */
    *indexPages = costs.numIndexPages;

    /* --- Selectivity ----------------------------------------------------- */
    /* Optionally base on LIMIT K; otherwise keep generic selectivity */
    if (K > 0 && path->path.parent && path->path.parent->rows > 0)
        costs.indexSelectivity = Min(1.0, K / path->path.parent->rows);

    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost   = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
}


/*
 * Parse and validate the reloptions
 */
static bytea *
hnswoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"m", RELOPT_TYPE_INT, offsetof(HnswOptions, m)},
		{"ef_construction", RELOPT_TYPE_INT, offsetof(HnswOptions, efConstruction)},
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  hnsw_relopt_kind,
									  sizeof(HnswOptions),
									  tab, lengthof(tab));
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool
hnswvalidate(Oid opclassoid)
{
	return true;
}

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(hnswhandler);
Datum
hnswhandler(PG_FUNCTION_ARGS)
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
	amroutine->amcanbackward = false;	/* can change direction mid-scan */
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
	amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
#if PG_VERSION_NUM >= 160000
	amroutine->amsummarizing = false;
#endif
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
	amroutine->amkeytype = InvalidOid;

	/* Interface functions */
	amroutine->ambuild = hnswbuild;
	amroutine->ambuildempty = hnswbuildempty;
	amroutine->aminsert = hnswinsert;
#if PG_VERSION_NUM >= 170000
	amroutine->aminsertcleanup = NULL;
#endif
	amroutine->ambulkdelete = hnswbulkdelete;
	amroutine->amvacuumcleanup = hnswvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = hnswcostestimate;
#if PG_VERSION_NUM >= 180000
	amroutine->amgettreeheight = NULL;
#endif
	amroutine->amoptions = hnswoptions;
	amroutine->amproperty = NULL;	/* TODO AMPROP_DISTANCE_ORDERABLE */
	amroutine->ambuildphasename = hnswbuildphasename;
	amroutine->amvalidate = hnswvalidate;
#if PG_VERSION_NUM >= 140000
	amroutine->amadjustmembers = NULL;
#endif
	amroutine->ambeginscan = hnswbeginscan;
	amroutine->amrescan = hnswrescan;
	amroutine->amgettuple = hnswgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = hnswendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;

	/* Interface functions to support parallel index scans */
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

#if PG_VERSION_NUM >= 180000
	amroutine->amtranslatestrategy = NULL;
	amroutine->amtranslatecmptype = NULL;
#endif

	PG_RETURN_POINTER(amroutine);
}
