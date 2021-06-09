#include "postgres.h"

#include <float.h>

#include "access/amapi.h"
#include "commands/vacuum.h"
#include "ivfflat.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"

int			ivfflat_probes;
static relopt_kind ivfflat_relopt_kind;

/*
 * Initialize index options and variables
 */
void
_PG_init(void)
{
	ivfflat_relopt_kind = add_reloption_kind();
	add_int_reloption(ivfflat_relopt_kind, "lists", "Number of inverted lists",
					  IVFFLAT_DEFAULT_LISTS, 1, IVFFLAT_MAX_LISTS
#if PG_VERSION_NUM >= 130000
					  ,AccessExclusiveLock
#endif
		);

	DefineCustomIntVariable("ivfflat.probes", "Sets the number of probes",
							"Valid range is 1..lists.", &ivfflat_probes,
							1, 1, IVFFLAT_MAX_LISTS, PGC_USERSET, 0, NULL, NULL, NULL);
}

/*
 * Estimate the cost of an index scan
 */
static void
ivfflatcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation
#if PG_VERSION_NUM >= 100000
					,double *indexPages
#endif
)
{
	GenericCosts costs;
	int			lists;
	double		ratio;
	Relation	indexRel;

#if PG_VERSION_NUM < 120000
	List	   *qinfos;
#endif

	/* Never use index without order */
	if (path->indexorderbys == NULL)
	{
		*indexStartupCost = DBL_MAX;
		*indexTotalCost = DBL_MAX;
		*indexSelectivity = 0;
		*indexCorrelation = 0;
#if PG_VERSION_NUM >= 100000
		*indexPages = 0;
#endif
		return;
	}

	MemSet(&costs, 0, sizeof(costs));

#if PG_VERSION_NUM >= 120000
	genericcostestimate(root, path, loop_count, &costs);
#else
	qinfos = deconstruct_indexquals(path);
	genericcostestimate(root, path, loop_count, qinfos, &costs);
#endif

	indexRel = index_open(path->indexinfo->indexoid, NoLock);
	lists = IvfflatGetLists(indexRel);
	index_close(indexRel, NoLock);

	ratio = ((double) ivfflat_probes) / lists;
	if (ratio > 1)
		ratio = 1;

	costs.indexTotalCost *= ratio;

	/* Startup cost and total cost are same */
	*indexStartupCost = costs.indexTotalCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
#if PG_VERSION_NUM >= 100000
	*indexPages = costs.numIndexPages;
#endif
}

/*
 * Parse and validate the reloptions
 */
static bytea *
ivfflatoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"lists", RELOPT_TYPE_INT, offsetof(IvfflatOptions, lists)},
	};

#if PG_VERSION_NUM >= 130000
	return (bytea *) build_reloptions(reloptions, validate,
									  ivfflat_relopt_kind,
									  sizeof(IvfflatOptions),
									  tab, lengthof(tab));
#else
	relopt_value *options;
	int			numoptions;
	IvfflatOptions *rdopts;

	options = parseRelOptions(reloptions, validate, ivfflat_relopt_kind, &numoptions);
	rdopts = allocateReloptStruct(sizeof(IvfflatOptions), options, numoptions);
	fillRelOptions((void *) rdopts, sizeof(IvfflatOptions), options, numoptions,
				   validate, tab, lengthof(tab));

	return (bytea *) rdopts;
#endif
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool
ivfflatvalidate(Oid opclassoid)
{
	return true;
}

PG_FUNCTION_INFO_V1(ivfflathandler);
Datum
ivfflathandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 4;
#if PG_VERSION_NUM >= 130000
	amroutine->amoptsprocnum = 0;
#endif
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
	amroutine->amcanbackward = false;	/* can change direction mid-scan */
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
#if PG_VERSION_NUM >= 100000
	amroutine->amcanparallel = false;
#endif
#if PG_VERSION_NUM >= 110000
	amroutine->amcaninclude = false;
#endif
#if PG_VERSION_NUM >= 130000
	amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_NO_PARALLEL; /* TODO support parallel */
#endif
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = ivfflatbuild;
	amroutine->ambuildempty = ivfflatbuildempty;
	amroutine->aminsert = ivfflatinsert;
	amroutine->ambulkdelete = ivfflatbulkdelete;
	amroutine->amvacuumcleanup = ivfflatvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = ivfflatcostestimate;
	amroutine->amoptions = ivfflatoptions;
	amroutine->amproperty = NULL;	/* TODO AMPROP_DISTANCE_ORDERABLE */
#if PG_VERSION_NUM >= 120000
	amroutine->ambuildphasename = NULL;
#endif
	amroutine->amvalidate = ivfflatvalidate;
	amroutine->ambeginscan = ivfflatbeginscan;
	amroutine->amrescan = ivfflatrescan;
	amroutine->amgettuple = ivfflatgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = ivfflatendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
#if PG_VERSION_NUM >= 100000
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;
#endif

	PG_RETURN_POINTER(amroutine);
}
