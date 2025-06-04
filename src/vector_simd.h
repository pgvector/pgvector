#ifndef VECTOR_SIMD_H
#define VECTOR_SIMD_H

#include "postgres.h"
#include "vector.h"

#if defined(__ARM_NEON)
#include <arm_neon.h>

static inline float
VectorL2SquaredDistanceNEON(int dim, float *ax, float *bx)
{
    float32x4_t sum1 = vdupq_n_f32(0.0f);
    float32x4_t sum2 = vdupq_n_f32(0.0f);
    float32x4_t sum3 = vdupq_n_f32(0.0f);
    float32x4_t sum4 = vdupq_n_f32(0.0f);
    float32x4_t a1, a2, a3, a4;
    float32x4_t b1, b2, b3, b4;
    float32x4_t diff1, diff2, diff3, diff4;
    float32x2_t sum_lo, sum_hi, sum_half;
    float neon_sum, remaining_sum;
    int i = 0;

    for (; i < dim - 15; i += 16) {
        a1 = vld1q_f32(&ax[i]);
        b1 = vld1q_f32(&bx[i]);
        diff1 = vsubq_f32(a1, b1);
        sum1 = vaddq_f32(sum1, vmulq_f32(diff1, diff1));

        a2 = vld1q_f32(&ax[i + 4]);
        b2 = vld1q_f32(&bx[i + 4]);
        diff2 = vsubq_f32(a2, b2);
        sum2 = vaddq_f32(sum2, vmulq_f32(diff2, diff2));

        a3 = vld1q_f32(&ax[i + 8]);
        b3 = vld1q_f32(&bx[i + 8]);
        diff3 = vsubq_f32(a3, b3);
        sum3 = vaddq_f32(sum3, vmulq_f32(diff3, diff3));

        a4 = vld1q_f32(&ax[i + 12]);
        b4 = vld1q_f32(&bx[i + 12]);
        diff4 = vsubq_f32(a4, b4);
        sum4 = vaddq_f32(sum4, vmulq_f32(diff4, diff4));
    }

    sum1 = vaddq_f32(sum1, sum2);
    sum1 = vaddq_f32(sum1, sum3);
    sum1 = vaddq_f32(sum1, sum4);

    remaining_sum = 0.0f;
    for (; i < dim; i++) {
        float diff = ax[i] - bx[i];
        remaining_sum += diff * diff;
    }

    sum_lo = vget_low_f32(sum1);
    sum_hi = vget_high_f32(sum1);
    sum_half = vadd_f32(sum_lo, sum_hi);
    neon_sum = vget_lane_f32(vpadd_f32(sum_half, sum_half), 0);

    return neon_sum + remaining_sum;
}

static inline float
VectorInnerProductNEON(int dim, float *ax, float *bx)
{
    float32x4_t sum = vdupq_n_f32(0.0f);
    float32x4_t a, b;
    float32x2_t sum2;
    float neon_sum, remaining_sum;
    int i = 0;

    for (; i < dim - 3; i += 4) {
        a = vld1q_f32(&ax[i]);
        b = vld1q_f32(&bx[i]);
        sum = vaddq_f32(sum, vmulq_f32(a, b));
    }

    remaining_sum = 0.0f;
    for (; i < dim; i++) {
        remaining_sum += ax[i] * bx[i];
    }

    sum2 = vadd_f32(vget_low_f32(sum), vget_high_f32(sum));
    neon_sum = vget_lane_f32(vpadd_f32(sum2, sum2), 0);
    return neon_sum + remaining_sum;
}
#endif /* __ARM_NEON */
#endif /* VECTOR_SIMD_H */
