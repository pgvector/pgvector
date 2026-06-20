#include "postgres.h"

#include <math.h>

#include "tq.h"

/*
 * tqfwht.c -- Fast Walsh-Hadamard Transform + randomized Hadamard transform.
 *
 * The RHT is a structured, O(d*log d) orthonormal "random rotation" used in
 * fast_rotation mode in place of the dense d*d matrix.  Same routine drives the
 * QJL sketch with an independent seed.
 */

/* Smallest power of two >= n (n >= 1). */
int
TqNextPow2(int n)
{
	int			p = 1;

	while (p < n)
		p <<= 1;
	return p;
}

/* splitmix64 finalizer over seed ^ (stage,index) -> deterministic +1/-1 sign. */
static inline double
tq_sign(uint64 seed, int stage, int idx)
{
	uint64		z = seed ^ ((uint64) (uint32) stage * UINT64CONST(0x100000001B3))
	^ ((uint64) (uint32) idx * UINT64CONST(0x9E3779B97F4A7C15));

	z += UINT64CONST(0x9E3779B97F4A7C15);
	z = (z ^ (z >> 30)) * UINT64CONST(0xBF58476D1CE4E5B9);
	z = (z ^ (z >> 27)) * UINT64CONST(0x94D049BB133111EB);
	z = z ^ (z >> 31);
	return (z & 1) ? 1.0 : -1.0;
}

/*
 * In-place unnormalized Fast Walsh-Hadamard Transform.  n MUST be a power of
 * two.  Satisfies H*H = n*I, so applying twice multiplies by n.
 */
void
TqFwht(double *x, int n)
{
	int			len,
				i,
				j;

	for (len = 1; len < n; len <<= 1)
	{
		for (i = 0; i < n; i += (len << 1))
		{
			for (j = i; j < i + len; j++)
			{
				double		a = x[j];
				double		b = x[j + len];

				x[j] = a + b;
				x[j + len] = a - b;
			}
		}
	}
}

/*
 * TqApplyRht -- y = (D_k H ... D_1 H) x / d'^(k/2), an orthonormal RHT.
 *
 * in[0..d-1] is zero-padded to dPadded (a power of two); out[0..dPadded-1]
 * receives the transform.  When d == dPadded no padding occurs (used for the
 * QJL stage, whose input is already in the padded rotated space).  nstages is
 * the number of (sign-flip, Hadamard) rounds (3 = near-Haar mixing).
 */
void
TqApplyRht(uint64 seed, int nstages, int dPadded, const float *in, int d, float *out)
{
	double	   *buf = palloc(sizeof(double) * dPadded);
	double		norm = 1.0;
	int			i,
				s;

	for (i = 0; i < d; i++)
		buf[i] = (double) in[i];
	for (i = d; i < dPadded; i++)
		buf[i] = 0.0;

	for (s = 0; s < nstages; s++)
	{
		for (i = 0; i < dPadded; i++)
			buf[i] *= tq_sign(seed, s, i);
		TqFwht(buf, dPadded);
	}

	for (s = 0; s < nstages; s++)
		norm /= sqrt((double) dPadded);
	for (i = 0; i < dPadded; i++)
		out[i] = (float) (buf[i] * norm);

	pfree(buf);
}
