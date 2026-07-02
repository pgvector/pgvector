#include "postgres.h"

#include <float.h>
#include <math.h>

#include "access/amapi.h"
#include "access/reloptions.h"
#include "catalog/pg_type.h"
#include "commands/vacuum.h"
#include "tq.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"
#include "vector.h"

#if PG_VERSION_NUM < 150000
#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)
#endif

int			tqflat_rerank;
bool		tqflat_force_scalar = false;
static relopt_kind tq_relopt_kind;

/*
 * Initialize index options and variables
 */
void
TqInit(void)
{
	tq_relopt_kind = add_reloption_kind();
	add_int_reloption(tq_relopt_kind, "bits", "Bits per coordinate",
					  4, 4, 4, AccessExclusiveLock);
	add_bool_reloption(tq_relopt_kind, "tq_prod", "Enable 1-bit QJL residual stage",
					   TQ_DEFAULT_TQPROD, AccessExclusiveLock);
	add_bool_reloption(tq_relopt_kind, "fast_rotation",
					   "Use structured randomized Hadamard rotation",
					   TQ_DEFAULT_FAST_ROTATION, AccessExclusiveLock);

	DefineCustomIntVariable("tqflat.rerank", "Number of candidates to rerank with full precision",
							"0 disables rerank.", &tqflat_rerank,
							TQ_DEFAULT_RERANK, 0, TQ_MAX_RERANK, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("tqflat.force_scalar",
							 "Score blocked tqflat indexes with the float LUT instead of the 8-bit SIMD kernel (A/B diagnostic).",
							 NULL, &tqflat_force_scalar, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("tqflat");

	TqInitDispatch();			/* set SIMD function pointers (tqdistance.c) */
}

/*
 * Parse and validate the reloptions
 */
static bytea *
tqoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"bits", RELOPT_TYPE_INT, offsetof(TqOptions, bits)},
		{"tq_prod", RELOPT_TYPE_BOOL, offsetof(TqOptions, tqProd)},
		{"fast_rotation", RELOPT_TYPE_BOOL, offsetof(TqOptions, fastRotation)},
	};

	return (bytea *) build_reloptions(reloptions, validate, tq_relopt_kind,
									  sizeof(TqOptions), tab, lengthof(tab));
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool
tqvalidate(Oid opclassoid)
{
	return true;
}

/*
 * Estimate the cost of an index scan
 */
static void
tqcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
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
	 * The code/side/tail chains are read in order: treat the page reads as
	 * sequential rather than random (mirrors ivfflatcostestimate's
	 * adjustment, with the visit ratio fixed at 1.0).
	 */
	{
		double		spc_seq_page_cost;

		get_tablespace_page_costs(path->indexinfo->reltablespace, NULL,
								  &spc_seq_page_cost);
		costs.indexTotalCost -= costs.numIndexPages *
			(costs.spc_random_page_cost - spc_seq_page_cost);
	}

	/*
	 * A flat scan reads, scores, and sorts the WHOLE index before returning
	 * its first row, so all of the total cost is startup cost.  Without this
	 * the generic near-zero startup cost makes the index look like a cheap
	 * first-row plan for LIMIT queries.
	 */
	costs.indexStartupCost = costs.indexTotalCost;

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

/*
 * Define index handler
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqhandler);
Datum
tqhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 5;
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
	amroutine->amcanbuildparallel = false;
#endif
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false;
#if PG_VERSION_NUM >= 160000
	amroutine->amsummarizing = false;
#endif
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = tqbuild;
	amroutine->ambuildempty = tqbuildempty;
	amroutine->aminsert = tqinsert;
#if PG_VERSION_NUM >= 170000
	amroutine->aminsertcleanup = NULL;
#endif
	amroutine->ambulkdelete = tqbulkdelete;
	amroutine->amvacuumcleanup = tqvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = tqcostestimate;
#if PG_VERSION_NUM >= 180000
	amroutine->amgettreeheight = NULL;
#endif
	amroutine->amoptions = tqoptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = tqvalidate;
#if PG_VERSION_NUM >= 140000
	amroutine->amadjustmembers = NULL;
#endif
	amroutine->ambeginscan = tqbeginscan;
	amroutine->amrescan = tqrescan;
	amroutine->amgettuple = tqgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = tqendscan;
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

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_l2_support);
Datum
tqflat_l2_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_L2,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqVectorToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_ip_support);
Datum
tqflat_ip_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_IP,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqVectorToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_cosine_support);
Datum
tqflat_cosine_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_COSINE,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqVectorToFloat,
		.normalize = l2_normalize,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_halfvec_l2_support);
Datum
tqflat_halfvec_l2_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_L2,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqHalfvecToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_halfvec_ip_support);
Datum
tqflat_halfvec_ip_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_IP,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqHalfvecToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_halfvec_cosine_support);
Datum
tqflat_halfvec_cosine_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_COSINE,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqHalfvecToFloat,
		.normalize = halfvec_l2_normalize,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_sparsevec_l2_support);
Datum
tqflat_sparsevec_l2_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_L2,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqSparsevecToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_sparsevec_ip_support);
Datum
tqflat_sparsevec_ip_support(PG_FUNCTION_ARGS)
{
	static const TqTypeInfo ti = {
		.metric = TQ_METRIC_IP,
		.maxDimensions = TQ_MAX_DIM,
		.toFloat = TqSparsevecToFloat,
	};

	PG_RETURN_POINTER(&ti);
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_sparsevec_cosine_support);
Datum
tqflat_sparsevec_cosine_support(PG_FUNCTION_ARGS)
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
 * TqLoadModel and TqGetMetaInfo are implemented in tqbuild.c alongside the
 * page-writing helpers they share.
 */

/* ================================================================
 * Test-only SQL-callable wrappers (not part of the extension SQL; created on demand by the regression tests)
 * ================================================================ */

/*
 * tqflat_test_codebook(dim int, bits int) RETURNS float8[]
 *
 * Returns (nLevels-1) boundaries followed by nLevels centroids as a
 * flat float8 array, total length = (nLevels - 1) + nLevels = 2*nLevels - 1.
 * For bits=2 that is 3 + 4 = 7 elements.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_codebook);
Datum
tqflat_test_codebook(PG_FUNCTION_ARGS)
{
	int			dim = PG_GETARG_INT32(0);
	int			bits = PG_GETARG_INT32(1);
	int			nLevels = 1 << bits;
	int			nBnd = nLevels - 1;
	int			nTotal = nBnd + nLevels;
	float	   *boundaries;
	float	   *centroids;
	Datum	   *elems;
	ArrayType  *result;
	int			i;

	if (bits < TQ_MIN_BITS || bits > TQ_MAX_BITS)
		ereport(ERROR, (errmsg("tqflat_test_codebook: bits must be %d..%d", TQ_MIN_BITS, TQ_MAX_BITS)));
	if (dim < 3)
		ereport(ERROR, (errmsg("tqflat_test_codebook: dim must be >= 3")));

	boundaries = palloc(sizeof(float) * nBnd);
	centroids = palloc(sizeof(float) * nLevels);

	TqBuildCodebook(dim, bits, boundaries, centroids);

	elems = palloc(sizeof(Datum) * nTotal);
	for (i = 0; i < nBnd; i++)
		elems[i] = Float8GetDatum((float8) boundaries[i]);
	for (i = 0; i < nLevels; i++)
		elems[nBnd + i] = Float8GetDatum((float8) centroids[i]);

	result = construct_array(elems, nTotal,
							 FLOAT8OID,
							 sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

	pfree(boundaries);
	pfree(centroids);
	pfree(elems);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * tqflat_test_rotation_orthogonality(dim int) RETURNS float8
 *
 * Builds a dim x dim rotation matrix with the canonical seed and returns
 * max_{i,j} |(R R^T)_{ij} - delta_{ij}|, which should be ~1e-6 for a
 * properly orthonormal matrix.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_rotation_orthogonality);
Datum
tqflat_test_rotation_orthogonality(PG_FUNCTION_ARGS)
{
	int			dim = PG_GETARG_INT32(0);
	float	   *rotation;
	double		maxerr;
	int			i,
				j,
				k;

	if (dim < 1 || dim > TQ_MAX_DIM)
		ereport(ERROR, (errmsg("tqflat_test_rotation_orthogonality: dim out of range")));

	rotation = palloc(sizeof(float) * dim * dim);

	TqBuildRotation(dim, TQ_ROTATION_SEED, rotation);

	/*
	 * Compute max |( R * R^T )_{ij} - I_{ij}|.  R is row-major, so (R *
	 * R^T)_{ij} = sum_k R[i*dim+k] * R[j*dim+k].
	 */
	maxerr = 0.0;
	for (i = 0; i < dim; i++)
	{
		for (j = 0; j < dim; j++)
		{
			double		dot = 0.0;
			double		expected = (i == j) ? 1.0 : 0.0;
			double		err;

			for (k = 0; k < dim; k++)
				dot += (double) rotation[(Size) i * dim + k]
					* (double) rotation[(Size) j * dim + k];
			err = fabs(dot - expected);
			if (err > maxerr)
				maxerr = err;
		}
	}

	pfree(rotation);

	PG_RETURN_FLOAT8(maxerr);
}

/*
 * tqflat_test_roundtrip(v vector, bits int) RETURNS float8
 *
 * Builds rotation + codebook for the vector's dimension, encodes the
 * vector, reconstructs via scale * R^T * yhat, and returns the relative
 * error ||v - vhat|| / ||v||.
 *
 * Reconstruction: vhat = scale * R^T * yhat,  where yhat[i] = centroids[code_i].
 * Since R is orthonormal, R^T = R^{-1}.  (R is row-major so R^T matvec is:
 *   vhat[c] = scale * sum_r rotation[r*dim+c] * yhat[r]  )
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_roundtrip);
Datum
tqflat_test_roundtrip(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	int			bits = PG_GETARG_INT32(1);
	int			dim = vec->dim;
	int			nLevels = 1 << bits;
	int			nBnd = nLevels - 1;
	int			codesBytes = TQ_CODES_BYTES(dim, bits);
	TqModel		model;
	TqEntry    *entry;
	float	   *yhat;
	float	   *vhat;
	double		numerr;
	double		denom;
	int			i,
				r,
				c;

	if (bits < TQ_MIN_BITS || bits > TQ_MAX_BITS)
		ereport(ERROR, (errmsg("tqflat_test_roundtrip: bits must be %d..%d", TQ_MIN_BITS, TQ_MAX_BITS)));

	/* Build a transient model. */
	model.dim = dim;
	model.bits = bits;
	model.nLevels = nLevels;
	model.metric = TQ_METRIC_L2;
	model.tqProd = false;
	model.fastRotation = false;
	model.dimPadded = dim;
	model.dimCodes = dim;
	model.qjl = NULL;
	model.qjlScale = 0.0f;
	model.rotSeed = TQ_ROTATION_SEED;
	model.qjlSeed = TQ_QJL_SEED;

	model.rotation = palloc(sizeof(float) * dim * dim);
	model.boundaries = palloc(sizeof(float) * nBnd);
	model.centroids = palloc(sizeof(float) * nLevels);

	TqBuildRotation(dim, TQ_ROTATION_SEED, model.rotation);
	TqBuildCodebook(dim, bits, model.boundaries, model.centroids);

	/*
	 * Allocate a scratch TqEntry (full size: codes + no signs since
	 * tqProd=false).
	 */
	entry = palloc0(TqEntrySize(dim, bits, false));

	TqEncode(&model, vec->x, entry);

	/*
	 * Decode: yhat[i] = centroids[code_i]. Reconstruct: vhat[c] =
	 * entry->scale * sum_r rotation[r*dim+c] * yhat[r].
	 */
	yhat = palloc(sizeof(float) * dim);
	for (i = 0; i < dim; i++)
	{
		uint8		code = TqUnpackCode(entry->data, codesBytes, i, bits);

		yhat[i] = model.centroids[code];
	}

	vhat = palloc(sizeof(float) * dim);
	for (c = 0; c < dim; c++)
	{
		double		acc = 0.0;

		for (r = 0; r < dim; r++)
			acc += (double) model.rotation[(Size) r * dim + c] * yhat[r];
		vhat[c] = (float) (acc * entry->scale);
	}

	/* Relative error ||v - vhat|| / ||v||. */
	numerr = 0.0;
	denom = 0.0;
	for (i = 0; i < dim; i++)
	{
		double		diff = (double) vec->x[i] - (double) vhat[i];

		numerr += diff * diff;
		denom += (double) vec->x[i] * vec->x[i];
	}
	numerr = sqrt(numerr);
	denom = sqrt(denom);

	pfree(model.rotation);
	pfree(model.boundaries);
	pfree(model.centroids);
	pfree(entry);
	pfree(yhat);
	pfree(vhat);

	PG_RETURN_FLOAT8(denom > 1e-12 ? numerr / denom : 0.0);
}

/*
 * tqflat_test_ip_estimate(a vector, b vector, bits int, tq_prod bool) RETURNS float8
 *
 * Builds a transient model for dim = a's dimension, encodes b into an entry,
 * builds the query LUT from a, scores the entry, and returns the estimated
 * inner product <a, b>.  Used by the internals test to assert the asymmetric
 * estimator is close to the true <a,b>.  Test-only helper.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_ip_estimate);
Datum
tqflat_test_ip_estimate(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	int			bits = PG_GETARG_INT32(2);
	bool		tqProd = PG_GETARG_BOOL(3);
	int			dim = a->dim;
	int			nLevels = 1 << bits;
	int			nBnd = nLevels - 1;
	TqModel		model;
	TqEntry    *entry;
	float	   *lut;
	float	   *qjlQuery = NULL;
	float		est;

	if (bits < TQ_MIN_BITS || bits > TQ_MAX_BITS)
		ereport(ERROR, (errmsg("tqflat_test_ip_estimate: bits must be %d..%d", TQ_MIN_BITS, TQ_MAX_BITS)));
	if (b->dim != dim)
		ereport(ERROR, (errmsg("tqflat_test_ip_estimate: vectors must have the same dimension")));

	/* Build a transient model. */
	model.dim = dim;
	model.bits = bits;
	model.nLevels = nLevels;
	model.metric = TQ_METRIC_IP;
	model.tqProd = tqProd;
	model.fastRotation = false;
	model.dimPadded = dim;
	model.dimCodes = dim;
	model.rotation = palloc(sizeof(float) * dim * dim);
	model.boundaries = palloc(sizeof(float) * nBnd);
	model.centroids = palloc(sizeof(float) * nLevels);
	model.qjl = NULL;
	model.qjlScale = 0.0f;
	model.rotSeed = TQ_ROTATION_SEED;
	model.qjlSeed = TQ_QJL_SEED;

	TqBuildRotation(dim, TQ_ROTATION_SEED, model.rotation);
	TqBuildCodebook(dim, bits, model.boundaries, model.centroids);

	if (tqProd)
	{
		model.qjl = palloc(sizeof(float) * dim * dim);
		TqBuildQjl(dim, TQ_QJL_SEED, model.qjl);
		model.qjlScale = (float) (sqrt(M_PI / 2.0) / (double) dim);
	}

	/* Encode b into an entry. */
	entry = palloc0(TqEntrySize(dim, bits, tqProd));
	TqEncode(&model, b->x, entry);

	/* Build the LUT from a and score. */
	lut = palloc(sizeof(float) * dim * nLevels);
	if (tqProd)
		qjlQuery = palloc(sizeof(float) * dim);
	TqBuildQueryLut(&model, a->x, lut, qjlQuery);

	est = TqScoreEntry(&model, lut, qjlQuery, entry, entry->data);

	PG_RETURN_FLOAT8((double) est);
}

/*
 * tqflat_test_codebook_mse(dim int, bits int) RETURNS float8
 *
 * Returns the per-coordinate MSE of the Beta Lloyd-Max codebook: the integral
 * of (x - centroid(region(x)))^2 against the rotated-coordinate density
 *   f(x) propto (1 - x^2)^((d-3)/2)  on [-1, 1],
 * evaluated on a fine grid independent of the codebook's own build grid.
 * Multiplying by d recovers the paper's unit-vector distortion D_mse(b)
 * (Theorem 1: ~0.117, 0.03, 0.009 for b = 2, 3, 4).
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_codebook_mse);
Datum
tqflat_test_codebook_mse(PG_FUNCTION_ARGS)
{
	int			dim = PG_GETARG_INT32(0);
	int			bits = PG_GETARG_INT32(1);
	int			nLevels = 1 << bits;
	int			nBnd = nLevels - 1;
	float	   *boundaries;
	float	   *centroids;
	double		exponent;
	double		total = 0.0;
	double		mse = 0.0;
	const int	grid = 200000;
	int			i;

	if (bits < TQ_MIN_BITS || bits > TQ_MAX_BITS)
		ereport(ERROR, (errmsg("tqflat_test_codebook_mse: bits must be %d..%d", TQ_MIN_BITS, TQ_MAX_BITS)));
	if (dim < 3)
		ereport(ERROR, (errmsg("tqflat_test_codebook_mse: dim must be >= 3")));

	boundaries = palloc(sizeof(float) * nBnd);
	centroids = palloc(sizeof(float) * nLevels);
	TqBuildCodebook(dim, bits, boundaries, centroids);

	exponent = ((double) dim - 3.0) / 2.0;

	for (i = 0; i < grid; i++)
	{
		double		x = -1.0 + (2.0 * (i + 0.5)) / grid;
		double		base = 1.0 - x * x;
		double		dens = (base <= 0.0) ? 0.0 : pow(base, exponent);
		int			k = 0;
		double		diff;

		while (k < nBnd && x > boundaries[k])
			k++;
		diff = x - (double) centroids[k];
		mse += dens * diff * diff;
		total += dens;
	}

	pfree(boundaries);
	pfree(centroids);

	PG_RETURN_FLOAT8(total > 0.0 ? mse / total : 0.0);
}

/*
 * tqflat_test_rotation_coord_stats(dim int) RETURNS float8[]
 *
 * Returns {mean, variance, max_abs} over all dim*dim entries of the canonical
 * rotation matrix R.  Each column of R is a random unit vector, so its entries
 * are samples of a rotated coordinate (Lemma 1): mean 0, variance 1/dim.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_rotation_coord_stats);
Datum
tqflat_test_rotation_coord_stats(PG_FUNCTION_ARGS)
{
	int			dim = PG_GETARG_INT32(0);
	float	   *rotation;
	Size		n;
	Size		i;
	double		sum = 0.0;
	double		sumsq = 0.0;
	double		maxabs = 0.0;
	double		mean;
	double		var;
	Datum		elems[3];
	ArrayType  *result;

	if (dim < 1 || dim > TQ_MAX_DIM)
		ereport(ERROR, (errmsg("tqflat_test_rotation_coord_stats: dim out of range")));

	n = (Size) dim * dim;
	rotation = palloc(sizeof(float) * n);
	TqBuildRotation(dim, TQ_ROTATION_SEED, rotation);

	for (i = 0; i < n; i++)
	{
		double		v = (double) rotation[i];
		double		a = fabs(v);

		sum += v;
		sumsq += v * v;
		if (a > maxabs)
			maxabs = a;
	}
	mean = sum / (double) n;
	var = sumsq / (double) n - mean * mean;

	pfree(rotation);

	elems[0] = Float8GetDatum(mean);
	elems[1] = Float8GetDatum(var);
	elems[2] = Float8GetDatum(maxabs);
	result = construct_array(elems, 3, FLOAT8OID,
							 sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * tqflat_test_pack_roundtrip(dim int, bits int) RETURNS int
 *
 * Packs a distinct code per coordinate (code_i = i mod nLevels) via TqPackCode,
 * unpacks via TqUnpackCode, and returns the number of coordinates whose
 * unpacked code differs (0 == perfect roundtrip).  Stresses the 2-byte sliding
 * window across byte boundaries (bits=3 straddles bytes since 3 does not
 * divide 8).
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_pack_roundtrip);
Datum
tqflat_test_pack_roundtrip(PG_FUNCTION_ARGS)
{
	int			dim = PG_GETARG_INT32(0);
	int			bits = PG_GETARG_INT32(1);
	int			nLevels = 1 << bits;
	int			codesBytes;
	char	   *codes;
	int			mismatches = 0;
	int			i;

	if (bits < TQ_MIN_BITS || bits > TQ_MAX_BITS)
		ereport(ERROR, (errmsg("tqflat_test_pack_roundtrip: bits must be %d..%d", TQ_MIN_BITS, TQ_MAX_BITS)));
	if (dim < 1 || dim > TQ_MAX_DIM)
		ereport(ERROR, (errmsg("tqflat_test_pack_roundtrip: dim out of range")));

	codesBytes = TQ_CODES_BYTES(dim, bits);
	codes = palloc0(codesBytes);

	for (i = 0; i < dim; i++)
		TqPackCode(codes, codesBytes, i, bits, (uint8) (i % nLevels));

	for (i = 0; i < dim; i++)
	{
		uint8		got = TqUnpackCode(codes, codesBytes, i, bits);

		if (got != (uint8) (i % nLevels))
			mismatches++;
	}

	pfree(codes);

	PG_RETURN_INT32(mismatches);
}

/*
 * tqflat_test_qjl_estimate(a vector, b vector, bits int, qjl_seed int, fast bool) RETURNS float8
 *
 * Like tqflat_test_ip_estimate with tq_prod = true, but builds the QJL sketch
 * from the supplied seed (the rotation uses the canonical seed).  Averaging the
 * result over many seeds for a fixed (a,b) concentrates on <a,b> (Theorem 2,
 * unbiasedness over the QJL randomness).
 *
 * When fast = false: dense rotation + dense QJL matrices (original behavior).
 * When fast = true: structured RHT rotation + structured QJL (no dense matrices).
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_qjl_estimate);
Datum
tqflat_test_qjl_estimate(PG_FUNCTION_ARGS)
{
	Vector	   *a = PG_GETARG_VECTOR_P(0);
	Vector	   *b = PG_GETARG_VECTOR_P(1);
	int			bits = PG_GETARG_INT32(2);
	int			qjlSeed = PG_GETARG_INT32(3);
	bool		fast = PG_GETARG_BOOL(4);
	int			dim = a->dim;
	int			nLevels = 1 << bits;
	int			nBnd = nLevels - 1;
	TqModel		model;
	TqEntry    *entry;
	float	   *lut;
	float	   *qjlQuery;
	float		est;

	if (bits < TQ_MIN_BITS || bits > TQ_MAX_BITS)
		ereport(ERROR, (errmsg("tqflat_test_qjl_estimate: bits must be %d..%d", TQ_MIN_BITS, TQ_MAX_BITS)));
	if (b->dim != dim)
		ereport(ERROR, (errmsg("tqflat_test_qjl_estimate: vectors must have the same dimension")));

	model.dim = dim;
	model.bits = bits;
	model.nLevels = nLevels;
	model.metric = TQ_METRIC_IP;
	model.tqProd = true;
	model.boundaries = palloc(sizeof(float) * nBnd);
	model.centroids = palloc(sizeof(float) * nLevels);

	if (!fast)
	{
		/* Dense rotation + dense QJL (original behavior). */
		model.fastRotation = false;
		model.dimPadded = dim;
		model.dimCodes = dim;
		model.rotation = palloc(sizeof(float) * dim * dim);
		model.qjl = palloc(sizeof(float) * dim * dim);
		model.qjlScale = (float) (sqrt(M_PI / 2.0) / (double) dim);
		model.rotSeed = TQ_ROTATION_SEED;
		model.qjlSeed = TQ_QJL_SEED;

		TqBuildRotation(dim, TQ_ROTATION_SEED, model.rotation);
		TqBuildCodebook(dim, bits, model.boundaries, model.centroids);
		TqBuildQjl(dim, (uint64) qjlSeed, model.qjl);
	}
	else
	{
		/* Structured RHT rotation + structured QJL (no dense matrices). */
		model.fastRotation = true;
		model.dimPadded = TqNextPow2(dim);
		model.dimCodes = model.dimPadded;
		model.rotation = NULL;
		model.qjl = NULL;
		model.qjlScale = (float) (sqrt(M_PI / 2.0) / (double) model.dimPadded);
		model.rotSeed = TQ_ROTATION_SEED;
		model.qjlSeed = (uint64) qjlSeed;	/* vary QJL sketch per test seed */

		TqBuildCodebook(model.dimPadded, bits, model.boundaries, model.centroids);
	}

	/*
	 * Size buffers by dimCodes so fast mode (dimCodes > dim) doesn't
	 * under-allocate.
	 */
	entry = palloc0(TqEntrySize(model.dimCodes, bits, true));
	TqEncode(&model, b->x, entry);

	lut = palloc(sizeof(float) * model.dimCodes * nLevels);
	qjlQuery = palloc(sizeof(float) * model.dimCodes);
	TqBuildQueryLut(&model, a->x, lut, qjlQuery);

	est = TqScoreEntry(&model, lut, qjlQuery, entry, entry->data);

	PG_RETURN_FLOAT8((double) est);
}

/*
 * tqflat_test_fwht_involution(n int) RETURNS float8
 * Returns max_i |FWHT(FWHT(e))_i - n*e_i| for a deterministic test vector e,
 * which is ~0 for a correct involutive FWHT.  n must be a power of two.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_fwht_involution);
Datum
tqflat_test_fwht_involution(PG_FUNCTION_ARGS)
{
	int			n = PG_GETARG_INT32(0);
	double	   *x;
	double	   *orig;
	double		maxerr = 0.0;
	int			i;

	if (n < 1 || n > 65536 || (n & (n - 1)) != 0)
		ereport(ERROR, (errmsg("tqflat_test_fwht_involution: n must be a power of two, 1..65536")));

	x = palloc(sizeof(double) * n);
	orig = palloc(sizeof(double) * n);
	for (i = 0; i < n; i++)
		x[i] = orig[i] = sin(i * 0.7) + 0.3;

	TqFwht(x, n);
	TqFwht(x, n);

	for (i = 0; i < n; i++)
	{
		double		e = fabs(x[i] - (double) n * orig[i]);

		if (e > maxerr)
			maxerr = e;
	}
	pfree(x);
	pfree(orig);
	PG_RETURN_FLOAT8(maxerr);
}

/*
 * tqflat_test_rht_norm(d int) RETURNS float8
 * Returns |‖RHT(x)‖ - ‖x‖| / ‖x‖ for a deterministic d-dim vector padded to
 * next_pow2(d); ~0 for an orthonormal RHT.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_rht_norm);
Datum
tqflat_test_rht_norm(PG_FUNCTION_ARGS)
{
	int			d = PG_GETARG_INT32(0);
	int			dp = TqNextPow2(d);
	float	   *in;
	float	   *out;
	double		nin = 0.0,
				nout = 0.0;
	int			i;

	if (d < 1 || d > TQ_MAX_DIM)
		ereport(ERROR, (errmsg("tqflat_test_rht_norm: d out of range")));

	in = palloc(sizeof(float) * d);
	out = palloc(sizeof(float) * dp);
	for (i = 0; i < d; i++)
		in[i] = (float) (sin(i * 0.3) - 0.1);

	TqApplyRht(TQ_ROTATION_SEED, TQ_RHT_STAGES, dp, in, d, out);

	for (i = 0; i < d; i++)
		nin += (double) in[i] * in[i];
	for (i = 0; i < dp; i++)
		nout += (double) out[i] * out[i];
	nin = sqrt(nin);
	nout = sqrt(nout);
	pfree(in);
	pfree(out);
	PG_RETURN_FLOAT8(nin > 1e-12 ? fabs(nout - nin) / nin : 0.0);
}

/*
 * tqflat_test_rht_coord_stats(d int) RETURNS float8[]
 * Applies the RHT to a batch of deterministic unit vectors and returns
 * {mean, variance, max_abs} over all output coordinates.  By Lemma 1 (for the
 * structured rotation) mean~0 and variance~1/d' where d'=next_pow2(d).
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_rht_coord_stats);
Datum
tqflat_test_rht_coord_stats(PG_FUNCTION_ARGS)
{
	int			d = PG_GETARG_INT32(0);
	int			dp = TqNextPow2(d);
	int			nvec = 64;		/* enough unit vectors to average, still fast */
	float	   *in;
	float	   *out;
	double		sum = 0.0,
				sumsq = 0.0,
				maxabs = 0.0;
	double		count = 0.0;
	double		mean,
				var;
	Datum		elems[3];
	ArrayType  *result;
	int			g,
				i;

	if (d < 1 || d > TQ_MAX_DIM)
		ereport(ERROR, (errmsg("tqflat_test_rht_coord_stats: d out of range")));

	in = palloc(sizeof(float) * d);
	out = palloc(sizeof(float) * dp);
	for (g = 0; g < nvec; g++)
	{
		double		nrm = 0.0;

		for (i = 0; i < d; i++)
		{
			in[i] = (float) sin(i * 0.13 + g * 1.7);
			nrm += (double) in[i] * in[i];
		}
		nrm = sqrt(nrm);
		for (i = 0; i < d; i++)
			in[i] = (float) (in[i] / nrm);

		TqApplyRht(TQ_ROTATION_SEED, TQ_RHT_STAGES, dp, in, d, out);
		for (i = 0; i < dp; i++)
		{
			double		v = (double) out[i];
			double		a = fabs(v);

			sum += v;
			sumsq += v * v;
			if (a > maxabs)
				maxabs = a;
			count += 1.0;
		}
	}
	mean = sum / count;
	var = sumsq / count - mean * mean;
	pfree(in);
	pfree(out);

	elems[0] = Float8GetDatum(mean);
	elems[1] = Float8GetDatum(var);
	elems[2] = Float8GetDatum(maxabs);
	result = construct_array(elems, 3, FLOAT8OID,
							 sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);
	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * Build a transient fast-mode model for kernel unit tests (bits=4, no QJL).
 * Caller pfrees boundaries/centroids.
 */
static void
tq_test_init_model(TqModel *m, int dim)
{
	m->dim = dim;
	m->bits = 4;
	m->nLevels = 16;
	m->metric = TQ_METRIC_L2;
	m->tqProd = false;
	m->fastRotation = true;
	m->dimPadded = TqNextPow2(dim);
	m->dimCodes = m->dimPadded;
	m->rotSeed = TQ_ROTATION_SEED;
	m->qjlSeed = TQ_QJL_SEED;
	m->rotation = NULL;
	m->qjl = NULL;
	m->qjlScale = 0.0f;
	m->boundaries = palloc(sizeof(float) * (m->nLevels - 1));
	m->centroids = palloc(sizeof(float) * m->nLevels);
	TqBuildCodebook(m->dimCodes, m->bits, m->boundaries, m->centroids);
}

/*
 * tqflat_test_transpose_roundtrip(dim int, nvecs int) RETURNS int
 * Encode nvecs deterministic vectors, scatter their codes into a block plane,
 * unpack each lane back, count mismatches vs the original packed codes.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_transpose_roundtrip);
Datum
tqflat_test_transpose_roundtrip(PG_FUNCTION_ARGS)
{
	int			dim = PG_GETARG_INT32(0);
	int			nvecs = PG_GETARG_INT32(1);
	TqModel		m;
	int			dc;
	int			codesBytes;
	uint8	   *plane;
	int			mismatches = 0;
	int			v,
				i;
	TqEntry    *entry;
	Size		entrySize;
	char	  **saved;
	float	   *vec;

	if (nvecs < 1 || nvecs > TQ_BLOCK_WIDTH)
		ereport(ERROR, (errmsg("nvecs must be 1..%d", TQ_BLOCK_WIDTH)));

	tq_test_init_model(&m, dim);
	dc = m.dimCodes;
	codesBytes = TQ_CODES_BYTES(dc, m.bits);
	entrySize = TqEntrySize(dc, m.bits, false);
	plane = palloc0(TQ_BLOCK_CODE_BYTES(dc));

	saved = palloc(sizeof(char *) * nvecs);
	vec = palloc(sizeof(float) * dim);

	for (v = 0; v < nvecs; v++)
	{
		entry = palloc0(entrySize);
		for (i = 0; i < dim; i++)
			vec[i] = (float) sin(i * 0.17 + v * 1.1);
		TqEncode(&m, vec, entry);
		saved[v] = palloc(codesBytes);
		memcpy(saved[v], entry->data, codesBytes);
		TqScatterCodes(&m, entry->data, v, plane);
	}

	for (v = 0; v < nvecs; v++)
	{
		int			lane = v & 15;
		bool		high = v >= 16;

		for (i = 0; i < dc; i++)
		{
			uint8		cell = plane[(Size) i * 16 + lane];
			uint8		got = high ? (uint8) (cell >> 4) : (uint8) (cell & 0x0F);
			uint8		want = TqUnpackCode(saved[v], codesBytes, i, m.bits);

			if (got != want)
				mismatches++;
		}
	}

	PG_RETURN_INT32(mismatches);
}

/*
 * tqflat_test_lut8_recovery(dim int) RETURNS float8
 * Average relative error between the 8-bit-recovered mse and the float-LUT mse
 * over a block of deterministic vectors and a deterministic query.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_lut8_recovery);
Datum
tqflat_test_lut8_recovery(PG_FUNCTION_ARGS)
{
	int			dim = PG_GETARG_INT32(0);
	TqModel		m;
	int			dc;
	float	   *query;
	float	   *lut;
	uint8	   *lut8;
	float		lutBias,
				lutScale;
	uint8	   *plane;
	TqEntry    *entry;
	Size		entrySize;
	int			i,
				v;
	double		sumRel = 0.0;
	int			nv = TQ_BLOCK_WIDTH;
	TqBlockAccum acc;
	float	   *vec;

	tq_test_init_model(&m, dim);
	dc = m.dimCodes;
	entrySize = TqEntrySize(dc, m.bits, false);

	query = palloc(sizeof(float) * dim);
	for (i = 0; i < dim; i++)
		query[i] = (float) cos(i * 0.23);
	lut = palloc(sizeof(float) * dc * m.nLevels);
	TqBuildQueryLut(&m, query, lut, NULL);
	lut8 = palloc(dc * m.nLevels);
	TqBuildLut8(&m, lut, lut8, &lutBias, &lutScale);

	plane = palloc0(TQ_BLOCK_CODE_BYTES(dc));
	vec = palloc(sizeof(float) * dim);
	for (v = 0; v < nv; v++)
	{
		entry = palloc0(entrySize);
		for (i = 0; i < dim; i++)
			vec[i] = (float) sin(i * 0.13 + v * 0.7);
		TqEncode(&m, vec, entry);
		TqScatterCodes(&m, entry->data, v, plane);
	}

	TqBlockAccumInit(&acc);
	TqScoreBlockRange(lut8, plane, 0, dc, &acc);
	TqBlockAccumFinish(&acc);

	for (v = 0; v < nv; v++)
	{
		int			lane = v & 15;
		bool		high = v >= 16;
		double		fmse = 0.0;
		double		recovered;
		double		denom;

		for (i = 0; i < dc; i++)
		{
			uint8		cell = plane[(Size) i * 16 + lane];
			uint8		code = high ? (uint8) (cell >> 4) : (uint8) (cell & 0x0F);

			fmse += (double) lut[(Size) i * m.nLevels + code];
		}
		recovered = (double) lutScale * acc.acc32[v] + (double) dc * lutBias;
		denom = fabs(fmse) < 1e-9 ? 1e-9 : fabs(fmse);
		sumRel += fabs(recovered - fmse) / denom;
	}

	PG_RETURN_FLOAT8(sumRel / nv);
}

/*
 * tqflat_test_score_block_consistency(dim int) RETURNS int
 * Scores a 32-vector block with the scalar Default kernel and the dispatched
 * kernel (NEON/AVX-512 when present); returns the number of lanes whose uint32
 * sums differ.  0 == the SIMD variant is bit-identical to Default.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_score_block_consistency);
Datum
tqflat_test_score_block_consistency(PG_FUNCTION_ARGS)
{
	int			dim = PG_GETARG_INT32(0);
	TqModel		m;
	int			dc;
	float	   *query;
	float	   *lut;
	uint8	   *lut8;
	float		lutBias,
				lutScale;
	uint8	   *plane;
	Size		entrySize;
	int			i,
				v,
				mismatches = 0;
	TqBlockAccum a1,
				a2;
	float	   *vec;

	tq_test_init_model(&m, dim);
	dc = m.dimCodes;
	entrySize = TqEntrySize(dc, m.bits, false);

	query = palloc(sizeof(float) * dim);
	for (i = 0; i < dim; i++)
		query[i] = (float) cos(i * 0.19 + 0.5);
	lut = palloc(sizeof(float) * dc * m.nLevels);
	TqBuildQueryLut(&m, query, lut, NULL);
	lut8 = palloc(dc * m.nLevels);
	TqBuildLut8(&m, lut, lut8, &lutBias, &lutScale);

	plane = palloc0(TQ_BLOCK_CODE_BYTES(dc));
	vec = palloc(sizeof(float) * dim);
	for (v = 0; v < TQ_BLOCK_WIDTH; v++)
	{
		TqEntry    *entry = palloc0(entrySize);

		for (i = 0; i < dim; i++)
			vec[i] = (float) sin(i * 0.07 + v * 0.5);
		TqEncode(&m, vec, entry);
		TqScatterCodes(&m, entry->data, v, plane);
	}

	TqBlockAccumInit(&a1);
	TqScoreBlockRangeDefault(lut8, plane, 0, dc, &a1);
	TqBlockAccumFinish(&a1);

	TqBlockAccumInit(&a2);
	TqScoreBlockRange(lut8, plane, 0, dc, &a2);
	TqBlockAccumFinish(&a2);

	for (v = 0; v < TQ_BLOCK_WIDTH; v++)
		if (a1.acc32[v] != a2.acc32[v])
			mismatches++;

	PG_RETURN_INT32(mismatches);
}

/*
 * tqflat_test_score_block(dim int) RETURNS int
 * Assert the kernel's uint32 lane sums equal a uint64 reference over the 8-bit
 * LUT (exactness of accumulation incl. the >128-coord flush). Returns mismatches.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_score_block);
Datum
tqflat_test_score_block(PG_FUNCTION_ARGS)
{
	int			dim = PG_GETARG_INT32(0);
	TqModel		m;
	int			dc;
	float	   *query;
	float	   *lut;
	uint8	   *lut8;
	float		lutBias,
				lutScale;
	uint8	   *plane;
	TqEntry    *entry;
	Size		entrySize;
	int			i,
				v;
	int			mismatches = 0;
	TqBlockAccum acc;
	float	   *vec;

	tq_test_init_model(&m, dim);
	dc = m.dimCodes;
	entrySize = TqEntrySize(dc, m.bits, false);

	query = palloc(sizeof(float) * dim);
	for (i = 0; i < dim; i++)
		query[i] = (float) cos(i * 0.31 + 0.2);
	lut = palloc(sizeof(float) * dc * m.nLevels);
	TqBuildQueryLut(&m, query, lut, NULL);
	lut8 = palloc(dc * m.nLevels);
	TqBuildLut8(&m, lut, lut8, &lutBias, &lutScale);

	plane = palloc0(TQ_BLOCK_CODE_BYTES(dc));
	vec = palloc(sizeof(float) * dim);
	for (v = 0; v < TQ_BLOCK_WIDTH; v++)
	{
		entry = palloc0(entrySize);
		for (i = 0; i < dim; i++)
			vec[i] = (float) sin(i * 0.11 + v * 0.9);
		TqEncode(&m, vec, entry);
		TqScatterCodes(&m, entry->data, v, plane);
	}

	TqBlockAccumInit(&acc);
	TqScoreBlockRange(lut8, plane, 0, dc, &acc);
	TqBlockAccumFinish(&acc);

	for (v = 0; v < TQ_BLOCK_WIDTH; v++)
	{
		int			lane = v & 15;
		bool		high = v >= 16;
		uint64		ref = 0;

		for (i = 0; i < dc; i++)
		{
			uint8		cell = plane[(Size) i * 16 + lane];
			uint8		code = high ? (uint8) (cell >> 4) : (uint8) (cell & 0x0F);

			ref += lut8[(Size) i * m.nLevels + code];
		}
		if ((uint64) acc.acc32[v] != ref)
			mismatches++;
	}

	PG_RETURN_INT32(mismatches);
}
