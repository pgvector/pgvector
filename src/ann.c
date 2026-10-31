#include "postgres.h"

#include <math.h>

#include "access/amvalidate.h"
#include "ann.h"
#include "catalog/pg_type_d.h"
#include "commands/vacuum.h"
#include "optimizer/optimizer.h"
#include "utils/float.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"

/*
 * The CPU staging implementation reads and sorts the complete index.
 */
static void
anncostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				Cost *indexStartupCost, Cost *indexTotalCost,
				Selectivity *indexSelectivity, double *indexCorrelation,
				double *indexPages)
{
	GenericCosts costs = {0};
	double		tuples = path->indexinfo->tuples;

	if (path->indexorderbys == NIL)
	{
		*indexStartupCost = *indexTotalCost = get_float8_infinity();
		*indexSelectivity = *indexCorrelation = *indexPages = 0;
		return;
	}
	costs.numIndexTuples = tuples;
	genericcostestimate(root, path, loop_count, &costs);
	*indexStartupCost = costs.indexTotalCost +
		tuples * cpu_operator_cost * (1 + (tuples > 1 ? log(tuples) / log(2.0) : 0));
	*indexTotalCost = *indexStartupCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = 0;
	*indexPages = costs.numIndexPages;
}

static bool
annvalidate(Oid opclassoid)
{
	Oid			type = get_opclass_input_type(opclassoid);
	Oid			proc = get_opfamily_proc(get_opclass_family(opclassoid), type, type, ANN_DISTANCE_PROC);

	return type == AnnVectorTypeOid() && OidIsValid(proc) &&
		check_amproc_signature(proc, FLOAT8OID, true, 2, 2, type, type);
}

static bool
annproperty(Oid index_oid, int attno, IndexAMProperty prop, const char *propname,
			bool *res, bool *isnull)
{
	if (attno == 0 || prop != AMPROP_DISTANCE_ORDERABLE)
		return false;
	*res = true;
	*isnull = false;
	return true;
}

/*
 * Register only implemented access-method capabilities.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(annhandler);
Datum
annhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = ANN_NPROCS;
	amroutine->amcanorderbyop = true;
	amroutine->amoptionalkey = true;
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
	amroutine->amkeytype = InvalidOid;
	amroutine->ambuild = annbuild;
	amroutine->ambuildempty = annbuildempty;
	amroutine->aminsert = anninsert;
	amroutine->ambulkdelete = annbulkdelete;
	amroutine->amvacuumcleanup = annvacuumcleanup;
	amroutine->amcostestimate = anncostestimate;
	amroutine->amproperty = annproperty;
	amroutine->amvalidate = annvalidate;
	amroutine->ambeginscan = annbeginscan;
	amroutine->amrescan = annrescan;
	amroutine->amgettuple = anngettuple;
	amroutine->amendscan = annendscan;

	PG_RETURN_POINTER(amroutine);
}
