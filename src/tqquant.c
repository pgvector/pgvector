#include "postgres.h"

#include <float.h>
#include <math.h>

#include "miscadmin.h"
#include "tq.h"

/*
 * tqquant.c -- TurboQuant quantizer core: rotation, Beta Lloyd-Max codebook,
 *              bit-packing, and encode.
 *
 * TqBuildRotation  -- deterministic Gaussian + modified Gram-Schmidt
 * TqBuildCodebook  -- grid-based Lloyd-Max on Beta coordinate density
 * TqEncode + pack/unpack + scale computation
 */

/* ----------------------------------------------------------------
 * xorshift128+ PRNG + Box-Muller Gaussian sampling (deterministic)
 * ---------------------------------------------------------------- */
typedef struct
{
	uint64		s0,
				s1;
} TqRng;

static void
tq_rng_init(TqRng *r, uint64 seed)
{
	r->s0 = seed ^ UINT64CONST(0x9E3779B97F4A7C15);
	r->s1 = (seed * UINT64CONST(6364136223846793005)) + UINT64CONST(1442695040888963407);
	if (r->s0 == 0 && r->s1 == 0)
		r->s0 = 1;
}

static uint64
tq_rng_next(TqRng *r)
{
	uint64		x = r->s0;
	uint64		y = r->s1;

	r->s0 = y;
	x ^= x << 23;
	r->s1 = x ^ y ^ (x >> 17) ^ (y >> 26);
	return r->s1 + y;
}

/* Returns a uniform double in (0, 1) using 53-bit mantissa. */
static double
tq_uniform(TqRng *r)
{
	return ((tq_rng_next(r) >> 11) + 0.5) * (1.0 / 9007199254740992.0);
}

/* Returns a standard-normal double via Box-Muller. */
static double
tq_gauss(TqRng *r)
{
	double		u1 = tq_uniform(r);
	double		u2 = tq_uniform(r);

	return sqrt(-2.0 * log(u1)) * cos(2.0 * M_PI * u2);
}

/* ----------------------------------------------------------------
 * TqBuildRotation
 *
 * Builds a dim x dim orthonormal matrix (row-major) by generating a
 * dim x dim Gaussian matrix and orthonormalizing its columns via
 * modified Gram-Schmidt (MGS).
 *
 * Storage convention:
 *   The working buffer `a` is column-major: a[col*dim + row].
 *   Output `rotation` is row-major:  rotation[row*dim + col].
 * ---------------------------------------------------------------- */
void
TqBuildRotation(int dim, uint64 seed, float *rotation)
{
	TqRng		rng;
	double	   *a;				/* column-major working buffer, dim*dim */
	int			i,
				c,
				p,
				r;

	/*
	 * O(dim²) build-time scratch (~2 GB at TQ_MAX_DIM -- past MaxAllocSize
	 * from dim ~11586, hence the huge variant).
	 */
	a = (double *) MemoryContextAllocHuge(CurrentMemoryContext,
										  sizeof(double) * (Size) dim * dim);

	tq_rng_init(&rng, seed);
	for (i = 0; i < dim * dim; i++)
		a[i] = tq_gauss(&rng);

	/* Orthonormalize columns via MGS. */
	for (c = 0; c < dim; c++)
	{
		double	   *vc = a + (Size) c * dim;
		double		nrm;

		/* O(dim³) total: keep the build cancellable. */
		CHECK_FOR_INTERRUPTS();

		/* Subtract projection onto each already-orthonormal column. */
		for (p = 0; p < c; p++)
		{
			double	   *vp = a + (Size) p * dim;
			double		dot = 0.0;

			for (r = 0; r < dim; r++)
				dot += vc[r] * vp[r];
			for (r = 0; r < dim; r++)
				vc[r] -= dot * vp[r];
		}

		/* Normalize the column. */
		nrm = 0.0;
		for (r = 0; r < dim; r++)
			nrm += vc[r] * vc[r];
		nrm = sqrt(nrm);
		if (nrm < 1e-12)
		{
			elog(WARNING, "tqflat: degenerate rotation column %d (near-zero norm)", c);
			nrm = 1.0;			/* degenerate: leave as-is rather than NaN */
		}
		for (r = 0; r < dim; r++)
			vc[r] /= nrm;
	}

	/*
	 * Transpose into row-major output: rotation[row*dim+col] =
	 * a[col*dim+row].
	 */
	for (r = 0; r < dim; r++)
		for (c = 0; c < dim; c++)
			rotation[(Size) r * dim + c] = (float) a[(Size) c * dim + r];

	pfree(a);
}

/* ----------------------------------------------------------------
 * TqBuildQjl
 *
 * Builds the QJL random-projection matrix S (dim x dim, row-major) with
 * i.i.d. N(0,1) entries.  Unlike the rotation matrix, S is NOT
 * orthonormalized: the QJL sign estimator
 *   <q,r> ~= qjlScale * ||r|| * <S q, sign(S r)>
 * is only unbiased for a raw Gaussian sketch (each row independent N(0,I)).
 * Uses a seed distinct from the rotation so the two matrices are independent.
 * ---------------------------------------------------------------- */
void
TqBuildQjl(int dim, uint64 seed, float *qjl)
{
	TqRng		rng;
	Size		n = (Size) dim * dim;
	Size		i;

	tq_rng_init(&rng, seed);
	for (i = 0; i < n; i++)
		qjl[i] = (float) tq_gauss(&rng);
}

/* ----------------------------------------------------------------
 * TqL2NormalizeFloat
 *
 * In-place L2 normalization of a float array, matching l2_normalize semantics:
 * norm accumulated in double; zero norm leaves the vector unchanged (zeros).
 * Unlike l2_normalize we skip the isinf overflow guard: since norm >= |v[i]|
 * for every i, each output |v[i] / norm| <= 1, so the result is bounded and
 * cannot overflow a float.
 * ---------------------------------------------------------------- */
void
TqL2NormalizeFloat(float *v, int dim)
{
	double		norm = 0;

	for (int i = 0; i < dim; i++)
		norm += (double) v[i] * (double) v[i];
	norm = sqrt(norm);

	if (norm > 0)
	{
		for (int i = 0; i < dim; i++)
			v[i] = (float) (v[i] / norm);
	}
}

/* ----------------------------------------------------------------
 * TqCheckNorm
 *
 * Check if non-zero norm.  Mirrors HnswCheckNorm / IvfflatCheckNorm: cosine
 * distance is undefined for zero vectors (the operator returns NaN), so
 * build/insert skip tuples that fail this check under the cosine metric.
 * ---------------------------------------------------------------- */
bool
TqCheckNorm(const float *v, int dim)
{
	double		norm = 0;

	for (int i = 0; i < dim; i++)
		norm += (double) v[i] * (double) v[i];

	return norm > 0;
}

/* ----------------------------------------------------------------
 * TqBuildCodebook
 *
 * Grid-based Lloyd-Max quantizer for the marginal coordinate density
 *   f(y) propto (1 - y^2)^((d-3)/2)  on [-1, 1]
 * of a uniform-on-sphere vector after random rotation.
 *
 * Algorithm: discretize [-1,1] into TQ_GRID equally-spaced points;
 * iterate TQ_LLOYD_ITERS times (a fixed count, no convergence test)
 * alternating
 *   boundaries[k] = (cen[k] + cen[k+1]) / 2
 *   cen[k]        = density-weighted mean of points in region k
 *
 * boundaries[0..nLevels-2] and centroids[0..nLevels-1] are written
 * in ascending order.
 * ---------------------------------------------------------------- */
/* 8192-point grid: trades finer resolution against build compute; chosen for
 * smooth convergence of the Beta Lloyd-Max solve across bits in [2,4]. */
#define TQ_GRID			8192
/* 200 iterations: sufficient for convergence of the Beta Lloyd-Max solve at
 * all supported bit widths; increasing beyond ~150 yields negligible change. */
#define TQ_LLOYD_ITERS	200

void
TqBuildCodebook(int dim, int bits, float *boundaries, float *centroids)
{
	int			nLevels = 1 << bits;
	double	   *x;				/* grid points */
	double	   *w;				/* normalized density weights */
	double		exponent;
	double		total;
	double	   *cen;			/* running centroids */
	double	   *bnd;			/* running boundaries */
	int			i,
				it,
				k,
				j;

	x = palloc(sizeof(double) * TQ_GRID);
	w = palloc(sizeof(double) * TQ_GRID);

	/* Exponent (d-3)/2; may be negative for d < 3. */
	exponent = ((double) dim - 3.0) / 2.0;

	/* Build the density PMF on the grid. */
	total = 0.0;
	for (i = 0; i < TQ_GRID; i++)
	{
		double		xi = -1.0 + (2.0 * (i + 0.5)) / TQ_GRID;
		double		base = 1.0 - xi * xi;
		double		dens = (base <= 0.0) ? 0.0 : pow(base, exponent);

		x[i] = xi;
		w[i] = dens;
		total += dens;
	}

	/* Normalize to a probability mass function. */
	if (total > 0.0)
		for (i = 0; i < TQ_GRID; i++)
			w[i] /= total;

	/* Initialize centroids spread uniformly across [-0.9, 0.9]. */
	cen = palloc(sizeof(double) * nLevels);
	for (k = 0; k < nLevels; k++)
		cen[k] = -0.9 + 1.8 * k / (double) (nLevels - 1);

	bnd = palloc(sizeof(double) * (nLevels - 1));

	/* Lloyd-Max iterations. */
	for (it = 0; it < TQ_LLOYD_ITERS; it++)
	{
		/* Step 1: boundaries = midpoints of adjacent centroids. */
		for (k = 0; k < nLevels - 1; k++)
			bnd[k] = 0.5 * (cen[k] + cen[k + 1]);

		/* Step 2: recompute centroids as weighted mean of each cell. */
		{
			double		num[1 << TQ_MAX_BITS] = {0};
			double		den[1 << TQ_MAX_BITS] = {0};

			k = 0;
			for (i = 0; i < TQ_GRID; i++)
			{
				while (k < nLevels - 1 && x[i] > bnd[k])
					k++;
				num[k] += w[i] * x[i];
				den[k] += w[i];
			}

			for (j = 0; j < nLevels; j++)
			{
				/*
				 * Rescue zero-mass cells: at high dim the Beta marginal's
				 * support collapses (~1/sqrt(dim)) and the far-tail PMF
				 * underflows to 0, so an outer cell can receive no mass and
				 * freeze at its init value.  Pull such a cell halfway toward
				 * its neighbor(s) each iteration so code levels stay inside the
				 * support.  Cells with mass are byte-identical to before, so
				 * low-dim (dim<=4096) codebooks are unchanged.
				 */
				if (den[j] > 0.0)
					cen[j] = num[j] / den[j];
				else if (j == 0)
					cen[j] = 0.5 * (cen[j] + cen[j + 1]);
				else if (j == nLevels - 1)
					cen[j] = 0.5 * (cen[j - 1] + cen[j]);
				else
					cen[j] = 0.5 * (cen[j - 1] + cen[j + 1]);
			}
		}
	}

	/* Write outputs. */
	for (k = 0; k < nLevels - 1; k++)
		boundaries[k] = (float) bnd[k];
	for (k = 0; k < nLevels; k++)
		centroids[k] = (float) cen[k];

	pfree(x);
	pfree(w);
	pfree(cen);
	pfree(bnd);
}

/* ----------------------------------------------------------------
 * TqAllocModel
 *
 * Allocate a TqModel as ONE contiguous chunk (header + boundaries + centroids
 * + optional rotation + optional qjl) so it satisfies the rd_amcache contract:
 * the relcache stores a single palloc'd chunk that is freed wholesale on
 * invalidation.  The array fields point into the trailing bytes; callers fill
 * the scalar header fields and copy the array data into the pre-pointed slots.
 * ---------------------------------------------------------------- */
TqModel *
TqAllocModel(MemoryContext ctx, int dim, int nLevels, bool dense, bool withQjl)
{
	Size		headSize = MAXALIGN(sizeof(TqModel));
	Size		bndSize = MAXALIGN(sizeof(float) * (nLevels - 1));
	Size		cenSize = MAXALIGN(sizeof(float) * nLevels);
	Size		rotSize = dense ? MAXALIGN((Size) sizeof(float) * dim * dim) : 0;
	Size		qjlSize = withQjl ? rotSize : 0;
	char	   *chunk;
	TqModel    *model;

	Assert(dense || !withQjl);

	chunk = MemoryContextAllocHuge(ctx, headSize + bndSize + cenSize + rotSize + qjlSize);
	MemSet(chunk, 0, headSize);
	model = (TqModel *) chunk;
	model->boundaries = (float *) (chunk + headSize);
	model->centroids = (float *) (chunk + headSize + bndSize);
	if (dense)
		model->rotation = (float *) (chunk + headSize + bndSize + cenSize);
	if (withQjl)
		model->qjl = (float *) (chunk + headSize + bndSize + cenSize + rotSize);
	return model;
}

/* ----------------------------------------------------------------
 * Pack helper
 *
 * Codes are packed/unpacked via TqPackCode / TqUnpackCode (tq.h), which share
 * the 2-byte sliding-window layout: code i occupies bits [i*bits, i*bits+bits).
 * ---------------------------------------------------------------- */

/* ----------------------------------------------------------------
 * TqEncode
 *
 * Steps:
 *   1. Compute ||vec|| and normalize to unit sphere: u = vec / ||vec||.
 *   2. Rotate: y = R * u  (row-major matvec).
 *   3. Quantize each coordinate by scanning boundaries.
 *   4. Pack codes directly into entry->data[0..codesBytes-1].
 *   5. Compute scale = ||vec|| / <y, yhat>  where yhat[i] = centroids[code_i].
 *      This is the renormalization factor so that scale * yhat ~= y, and
 *      scale * ||vec|| * R^T * yhat ~= vec.
 *   6. If tqProd, compute the rotated-space residual ry[i] = y[i] - yhat[i],
 *      its norm residualNorm = ||ry||, project p = S * (ry/residualNorm) with
 *      the QJL matrix, and store sign(p[j]) bits (bit set iff p[j] >= 0) into
 *      entry->data + TQ_CODES_BYTES(dim,bits) (the signs region).
 *      Otherwise residualNorm = 0 and no signs are written.
 *
 * Sets entry->norm, entry->scale, and entry->residualNorm.
 * The caller must memset the entry to zero before calling so that unused
 * code/sign bits are clean.
 *
 * Note on residual space: turboquant projects the residual in the *original*
 * space (r = x - x_hat) with its own Gaussian S.  Here we project the residual
 * in the *rotated* space (ry = y - yhat).  Because the rotation R is
 * orthonormal, ||ry|| = ||r|| and the QJL sign estimator is invariant to the
 * choice of orthonormal basis (S being rotation-invariant in distribution), so
 * projecting the rotated residual with its own Gaussian S is equivalent for the
 * estimator and matches our pipeline (ry is already available here).
 * ---------------------------------------------------------------- */
void
TqEncode(const TqModel *model, const float *vec, TqEntry *entry)
{
	int			dim = model->dim;
	int			dc = model->dimCodes;
	int			bits = model->bits;
	int			nLevels = model->nLevels;
	int			codesBytes = TQ_CODES_BYTES(dc, bits);
	double		norm;
	float	   *u;
	float	   *y;
	uint8	   *codeOf = NULL;	/* per-coordinate code (kept for tqProd
								 * residual) */
	double		ip;
	int			i,
				r,
				c;
	double		inv;

	/* 1. Compute L2 norm. */
	norm = 0.0;
	for (i = 0; i < dim; i++)
		norm += (double) vec[i] * vec[i];
	norm = sqrt(norm);
	/* Clamp the double->float cast: an over-FLT_MAX norm would store +inf and
	 * poison the estimator's inf - inf cancellation. */
	entry->norm = (norm > FLT_MAX) ? FLT_MAX : (float) norm;

	/* 2. Normalize to unit sphere. */
	u = palloc(sizeof(float) * dim);
	inv = (norm > 1e-12) ? (1.0 / norm) : 0.0;
	for (i = 0; i < dim; i++)
		u[i] = (float) (vec[i] * inv);

	/*
	 * 3. Rotate to y (length dc).  Fast mode: structured RHT producing
	 * dimPadded (== dc) coords from the dim-length unit vector (zero-padded
	 * internally). Dense mode: y = R * u over dim (dc == dim).
	 */
	y = palloc(sizeof(float) * dc);
	if (model->fastRotation)
	{
		TqApplyRht(model->rotSeed, TQ_RHT_STAGES, model->dimPadded, u, dim, y);
	}
	else
	{
		for (r = 0; r < dim; r++)
		{
			const float *row = model->rotation + (Size) r * dim;
			double		acc = 0.0;

			for (c = 0; c < dim; c++)
				acc += (double) row[c] * u[c];
			y[r] = (float) acc;
		}
	}

	/*
	 * 4. Quantize and pack codes into entry->data; accumulate <y, yhat> for
	 * scale.
	 */
	if (model->tqProd)
		codeOf = palloc(sizeof(uint8) * dc);

	ip = 0.0;
	for (i = 0; i < dc; i++)
	{
		uint8		code = 0;

		while (code < (uint8) (nLevels - 1) && y[i] > model->boundaries[code])
			code++;
		TqPackCode(entry->data, codesBytes, i, bits, code);
		ip += (double) y[i] * model->centroids[code];	/* <y, yhat> */
		if (codeOf != NULL)
			codeOf[i] = code;
	}

	/* 5. Scale = norm / <y, yhat>.  If inner product is tiny, fall back.  Clamp
	 * the double->float cast to +/-FLT_MAX so a tiny ip cannot store +/-inf. */
	{
		double		scale = (fabs(ip) > 1e-12) ? (norm / ip) : norm;

		if (scale > FLT_MAX)
			scale = FLT_MAX;
		else if (scale < -FLT_MAX)
			scale = -FLT_MAX;
		entry->scale = (float) scale;
	}

	/*
	 * 6. QJL residual stage (tqProd only).  Build the rotated-space residual
	 * ry[i] = y[i] - centroids[code_i], project the unit residual onto the
	 * QJL matrix, and store the sign bits after the codes in entry->data.
	 */
	if (model->tqProd)
	{
		Size		signsBytes = TQ_SIGNS_BYTES((Size) dc);
		char	   *signs = entry->data + codesBytes;
		float	   *ry = palloc(sizeof(float) * dc);
		double		rnorm;
		double		rinv;

		Assert(codeOf != NULL);

		rnorm = 0.0;
		for (i = 0; i < dc; i++)
		{
			ry[i] = y[i] - model->centroids[codeOf[i]];
			rnorm += (double) ry[i] * ry[i];
		}
		rnorm = sqrt(rnorm);
		entry->residualNorm = (rnorm > FLT_MAX) ? FLT_MAX : (float) rnorm;

		memset(signs, 0, signsBytes);

		/*
		 * Guard a (near-)zero residual: the unit direction is undefined, so
		 * residualNorm is ~0 and the residual term contributes nothing in the
		 * estimator regardless of the stored signs.  Leave signs all-zero.
		 */
		rinv = (rnorm > 1e-12) ? (1.0 / rnorm) : 0.0;
		if (rinv != 0.0)
		{
			if (model->fastRotation)
			{
				/*
				 * Structured QJL over the padded residual.  ry is already in
				 * the padded rotated space (length dc == dimPadded), so no
				 * padding occurs; project the UNIT residual and store sign
				 * bits.
				 *
				 * KNOWN LIMITATION: the QJL sign estimator +
				 * qjlScale=sqrt(pi/2)/d assume i.i.d. Gaussian sketch rows;
				 * the orthonormal RHT here is NOT Gaussian, so this estimate
				 * is biased (bias grows with dim: ~20% at d=128).  A
				 * structured-QJL sketch over a non-orthonormal
				 * Hadamard/Rademacher transform would remove the bias if the
				 * QJL second stage proves worthwhile in fast mode.
				 */
				float	   *ru = palloc(sizeof(float) * dc);
				float	   *p = palloc(sizeof(float) * dc);

				for (i = 0; i < dc; i++)
					ru[i] = (float) (ry[i] * rinv);
				TqApplyRht(model->qjlSeed, TQ_RHT_STAGES, model->dimPadded, ru, dc, p);
				for (i = 0; i < dc; i++)
					if (p[i] >= 0.0f)
						signs[i >> 3] |= (char) (1 << (i & 7));
				pfree(ru);
				pfree(p);
			}
			else
			{
				/* p = S * (ry / rnorm); store bit j iff p[j] >= 0. */
				for (r = 0; r < dim; r++)
				{
					const float *row = model->qjl + (Size) r * dim;
					double		acc = 0.0;

					for (c = 0; c < dim; c++)
						acc += (double) row[c] * (ry[c] * rinv);
					if (acc >= 0.0)
						signs[r >> 3] |= (char) (1 << (r & 7));
				}
			}
		}

		pfree(ry);
		pfree(codeOf);
	}
	else
	{
		entry->residualNorm = 0.0f;
	}

	pfree(u);
	pfree(y);
}

/* ----------------------------------------------------------------
 * TqBuildQueryLut
 *
 * Builds the per-query asymmetric lookup table.
 *
 *   1. Rotate the full-precision query once: qr = R * query  (row-major matvec).
 *   2. Fill lut[i*nLevels + c] = qr[i] * centroids[c] for all coordinates i and
 *      codes c.  Then the LUT-accumulated score for a stored entry is
 *        mse = sum_i lut[i*nLevels + code_i] = sum_i qr[i] * centroids[code_i]
 *            = <qr, yhat>
 *      i.e. the inner product of the rotated query with the entry's
 *      reconstruction yhat in rotated unit space.
 *   3. If model->tqProd and qjlQuery != NULL, also project the rotated query
 *      through the SAME QJL matrix used at encode time:
 *        qjlQuery[j] = sum_c QJL[j*dim + c] * qr[c]
 *      (full precision, length dim).  This is used by TqScoreEntry to add the
 *      residual term  qjlScale * residualNorm * <qjlQuery, signs>.
 *
 * Note: the query is NOT normalized here (the scan normalizes for cosine before
 * calling); the rotation is applied to whatever query vector is passed in.
 * ---------------------------------------------------------------- */
void
TqBuildQueryLut(const TqModel *model, const float *query,
				float *lut, float *qjlQuery)
{
	int			dim = model->dim;
	int			dc = model->dimCodes;
	int			nLevels = model->nLevels;
	float	   *qr;
	int			i,
				r,
				c;

	qr = palloc(sizeof(float) * dc);

	/*
	 * 1. Rotate the query to qr (length dc).  Fast mode: structured RHT
	 * (zero-padded from dim to dimPadded == dc).  Dense mode: qr = R * query.
	 */
	if (model->fastRotation)
	{
		TqApplyRht(model->rotSeed, TQ_RHT_STAGES, model->dimPadded, query, dim, qr);
	}
	else
	{
		for (r = 0; r < dim; r++)
		{
			const float *row = model->rotation + (Size) r * dim;
			double		acc = 0.0;

			for (c = 0; c < dim; c++)
				acc += (double) row[c] * query[c];
			qr[r] = (float) acc;
		}
	}

	/* 2. lut[i*nLevels + c] = qr[i] * centroids[c]. */
	for (i = 0; i < dc; i++)
	{
		float		qri = qr[i];
		float	   *lrow = lut + (Size) i * nLevels;

		for (c = 0; c < nLevels; c++)
			lrow[c] = qri * model->centroids[c];
	}

	/* 3. qjlQuery = QJL * qr (full precision), tqProd only. */
	if (model->tqProd && qjlQuery != NULL)
	{
		if (model->fastRotation)
		{
			/*
			 * Structured QJL over the already-rotated query (length dc ==
			 * dimPadded); input length equals dimPadded, so no padding.
			 */
			TqApplyRht(model->qjlSeed, TQ_RHT_STAGES, model->dimPadded, qr, dc, qjlQuery);
		}
		else
		{
			for (r = 0; r < dim; r++)
			{
				const float *row = model->qjl + (Size) r * dim;
				double		acc = 0.0;

				for (c = 0; c < dim; c++)
					acc += (double) row[c] * qr[c];
				qjlQuery[r] = (float) acc;
			}
		}
	}

	pfree(qr);
}
