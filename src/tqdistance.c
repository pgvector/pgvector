#include "postgres.h"

#include "halfvec.h"
#include "tq.h"

/*
 * tqdistance.c -- TqScoreEntry scoring kernel + load-time dispatch.
 *
 * Scoring is done through a function pointer (TqScoreEntry) set at extension
 * load by TqInitDispatch.  Currently only the scalar Default variant exists;
 * the indirection is kept so SIMD variants can be added later (mirroring
 * halfutils.c / bitutils.c), without changing callers.
 */

/*
 * TqScoreEntryDefault -- scalar asymmetric scorer.
 *
 * Returns an estimate of the raw inner product <q, x> between the
 * full-precision query q (whose LUT/qjlQuery were built by TqBuildQueryLut)
 * and the original stored vector x that produced `entry`.
 *
 * Let mse = sum_i lut[i*nLevels + code_i] = <qr, yhat>, where yhat is the
 * entry's reconstruction in rotated unit space.
 *
 * There are TWO debiasing schemes, selected by model->tqProd; exactly ONE is
 * applied (never both -- applying both would double-correct the magnitude):
 *
 *   tqProd == false (RaBitQ-style scale):
 *       est<q,x> = entry->scale * mse
 *     The stored scale = ||x|| / <y, yhat> already corrects both magnitude and
 *     the projection loss of quantization.
 *
 *   tqProd == true (QJL residual):
 *       est<q,x> = entry->norm * ( mse
 *                                  + qjlScale * entry->residualNorm * Q )
 *     where Q = sum_j qjlQuery[j] * s_j, and s_j in {+1,-1} is decoded from the
 *     stored QJL sign bit j (bit set => +1, clear => -1).  Here entry->norm*mse
 *     estimates the reconstructed part <q, norm*yhat> and the QJL term estimates
 *     entry->norm*<qr, ry> (the residual projection), summing to
 *     entry->norm*<qr, y> = <q, x>.  entry->scale is NOT used in this path.
 *
 * codes points at entry->data: the packed codes occupy the first codesBytes
 * bytes, and (when tqProd) the QJL sign bits follow at codes + codesBytes.
 */
static float
TqScoreEntryDefault(const TqModel *model, const float *lut,
					const float *qjlQuery, const TqEntry *entry,
					const char *codes)
{
	int			dc = model->dimCodes;
	int			bits = model->bits;
	int			nLevels = model->nLevels;
	int			codesBytes = TQ_CODES_BYTES(dc, bits);
	double		mse = 0.0;
	int			i;

	/* mse = sum_i lut[i*nLevels + code_i] = <qr, yhat>. */
	for (i = 0; i < dc; i++)
	{
		uint8		code = TqUnpackCode(codes, codesBytes, i, bits);

		mse += (double) lut[(Size) i * nLevels + code];
	}

	if (model->tqProd)
	{
		const char *signs = codes + codesBytes;
		double		qsum = 0.0;

		/* Q = sum_j qjlQuery[j] * sign_j  (bit set => +1, clear => -1). */
		for (i = 0; i < dc; i++)
		{
			int			s = (signs[i >> 3] >> (i & 7)) & 1;

			if (s)
				qsum += (double) qjlQuery[i];
			else
				qsum -= (double) qjlQuery[i];
		}

		return (float) ((double) entry->norm *
						(mse + (double) model->qjlScale *
						 (double) entry->residualNorm * qsum));
	}
	else
	{
		return (float) ((double) entry->scale * mse);
	}
}

float		(*TqScoreEntry) (const TqModel *model, const float *lut,
							 const float *qjlQuery, const TqEntry *entry,
							 const char *codes);

/*
 * TqInitDispatch -- set the scoring function pointers at load time.
 *
 * Dispatch order: Default (baseline) -> NEON (arm64) -> AVX-512F+BW (x86-64,
 * runtime-detected).  All SIMD variants are bit-identical to Default by
 * construction; TqScoreBlockRange is the only pointer currently upgraded.
 */
void
TqInitDispatch(void)
{
	TqScoreEntry = TqScoreEntryDefault;
	TqScoreBlockRange = TqScoreBlockRangeDefault;
#if defined(__aarch64__)
	TqScoreBlockRange = TqScoreBlockRangeNeon;
#endif
#if defined(USE_DISPATCH) && defined(__x86_64__)
	if (TqSupportsAvx512())
		TqScoreBlockRange = TqScoreBlockRangeAvx512;
#endif
}
