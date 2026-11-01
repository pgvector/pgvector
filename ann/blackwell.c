/*
 * Blackwell scan module: quantized gather scans for the cuVS worker.
 *
 * Eight threads score one listed row, each taking 16-dimension chunks with
 * one 8-byte (NVFP4) or 16-byte (FP8) load and, for NVFP4, one block-scale
 * decode, so a warp scores four rows. That reads 4 or 8 bits per dimension
 * where the cuVS backend's gather kernel reads 16 or 32. Decoding uses Blackwell's conversion instructions, which
 * need the architecture-specific target sm_100a; the module is compiled
 * once per device with NVRTC and only on compute capability 10.x.
 */
#include "blackwell.h"
#include "nvrtc.h"

/* The driver API header is not ISO C pedantic-clean. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cuda.h>
#pragma GCC diagnostic pop
#include <cuda_runtime_api.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ANN_BLACKWELL_MAX_DEVICES 64
#define ANN_BLACKWELL_BLOCK 256
#define ANN_BLACKWELL_GROUP 8	/* threads per scored row, as GROUP below */

/* Pieces stay under ISO C's string literal limit; joined before compiling. */
static const char *const BlackwellSource[] = {
	"__device__ __forceinline__ float half_bits(unsigned short h)\n"
	"{ float f; asm(\"cvt.f32.f16 %0, %1;\" : \"=f\"(f) : \"h\"(h)); return f; }\n"
	"/* Two E4M3 values (low byte first) to floats. */\n"
	"__device__ __forceinline__ void e4m3x2(unsigned short v, float &lo, float &hi)\n"
	"{\n"
	"  unsigned int h2;\n"
	"  asm(\"cvt.rn.f16x2.e4m3x2 %0, %1;\" : \"=r\"(h2) : \"h\"(v));\n"
	"  lo = half_bits((unsigned short) (h2 & 0xffffu)); hi = half_bits((unsigned short) (h2 >> 16));\n"
	"}\n"
	"/* Two E2M1 values (low nibble first) to floats. */\n"
	"__device__ __forceinline__ void e2m1x2(unsigned int v, float &lo, float &hi)\n"
	"{\n"
	"  unsigned int h2; unsigned short w = (unsigned short) (v & 0xffu);\n"
	"  asm(\"{ .reg .b8 b; cvt.u8.u16 b, %1; cvt.rn.f16x2.e2m1x2 %0, b; }\" : \"=r\"(h2) : \"h\"(w));\n"
	"  lo = half_bits((unsigned short) (h2 & 0xffffu)); hi = half_bits((unsigned short) (h2 >> 16));\n"
	"}\n"
	"#define GROUP 8 /* threads per row; each takes 16-dimension chunks */\n"
	"__device__ __forceinline__ float group_sum(float s)\n"
	"{\n"
	"  for (int o = GROUP / 2; o; o >>= 1) s += __shfl_xor_sync(0xffffffffu, s, o, GROUP);\n"
	"  return s;\n"
	"}\n"
	"/* Accumulate (x * scale - q)^2 over 2 decoded values at dimension d. */\n"
	"#define ACC(x0, x1, d) { float t0 = (x0) * scale - query[d], t1 = (x1) * scale - query[(d) + 1]; s += t0 * t0 + t1 * t1; }\n"
	"",
	"/*\n"
	" * Squared distance of one row, computed by a group of GROUP threads (t is\n"
	" * the thread's place in it) and returned to all of them; +inf for no row.\n"
	" * Every thread of the warp must call it: the group sum shuffles.\n"
	" */\n"
	"__device__ __forceinline__ float score4(const unsigned char *data, const unsigned char *scales,\n"
	"    const float *rowScales, unsigned rowBytes, unsigned blocks, unsigned row, const float *query,\n"
	"    int dims, int t)\n"
	"{\n"
	"  float s = 0.0f;\n"
	"  if (row != 0xffffffffu)\n"
	"  {\n"
	"    const uint2 *v = (const uint2 *) (data + (unsigned long long) row * rowBytes);\n"
	"    const unsigned char *sc = scales + (unsigned long long) row * blocks;\n"
	"    float rs = rowScales[row];\n"
	"    for (unsigned c = t; c < blocks; c += GROUP)\n"
	"    {\n"
	"      uint2 w = v[c];\n"
	"      float scale, unused, x0, x1;\n"
	"      e4m3x2((unsigned short) sc[c], scale, unused);\n"
	"      scale *= rs;\n"
	"      int d = c * 16;\n"
	"      if (d + 16 <= dims)\n"
	"      {\n"
	"        for (int b = 0; b < 4; b++) { e2m1x2(w.x >> (8 * b), x0, x1); ACC(x0, x1, d + 2 * b); }\n"
	"        for (int b = 0; b < 4; b++) { e2m1x2(w.y >> (8 * b), x0, x1); ACC(x0, x1, d + 8 + 2 * b); }\n"
	"      }\n"
	"      else\n"
	"        for (int b = 0; b < 8; b++)\n"
	"        {\n"
	"          e2m1x2((b < 4 ? w.x : w.y) >> (8 * (b & 3)), x0, x1);\n"
	"          int e = d + 2 * b;\n"
	"          if (e < dims) { float t0 = x0 * scale - query[e]; s += t0 * t0; }\n"
	"          if (e + 1 < dims) { float t1 = x1 * scale - query[e + 1]; s += t1 * t1; }\n"
	"        }\n"
	"    }\n"
	"  }\n"
	"  s = group_sum(s);\n"
	"  return row == 0xffffffffu ? __int_as_float(0x7f800000) : s;\n"
	"}\n",
	"__device__ __forceinline__ float score8(const unsigned char *data, const float *rowScales, unsigned rowBytes,\n"
	"    unsigned row, const float *query, int dims, int t)\n"
	"{\n"
	"  float s = 0.0f;\n"
	"  if (row != 0xffffffffu)\n"
	"  {\n"
	"    const uint4 *v = (const uint4 *) (data + (unsigned long long) row * rowBytes);\n"
	"    float scale = rowScales[row];\n"
	"    for (unsigned c = t; c < rowBytes / 16; c += GROUP)\n"
	"    {\n"
	"      uint4 w = v[c];\n"
	"      unsigned int words[4] = {w.x, w.y, w.z, w.w};\n"
	"      int d = c * 16;\n"
	"      float x0, x1;\n"
	"      if (d + 16 <= dims)\n"
	"        for (int b = 0; b < 8; b++) { e4m3x2((unsigned short) (words[b >> 1] >> (16 * (b & 1))), x0, x1); ACC(x0, x1, d + 2 * b); }\n"
	"      else\n"
	"        for (int b = 0; b < 8; b++)\n"
	"        {\n"
	"          e4m3x2((unsigned short) (words[b >> 1] >> (16 * (b & 1))), x0, x1);\n"
	"          int e = d + 2 * b;\n"
	"          if (e < dims) { float t0 = x0 * scale - query[e]; s += t0 * t0; }\n"
	"          if (e + 1 < dims) { float t1 = x1 * scale - query[e + 1]; s += t1 * t1; }\n"
	"        }\n"
	"    }\n"
	"  }\n"
	"  s = group_sum(s);\n"
	"  return row == 0xffffffffu ? __int_as_float(0x7f800000) : s;\n"
	"}\n"
	"/* Score every listed row (UINT32_MAX scores +inf); GROUP threads a row, no early exit. */\n"
	"extern \"C\" __global__ void score_nvfp4(const unsigned char *data, const unsigned char *scales,\n"
	"    const float *rowScales, unsigned rowBytes, unsigned blocks, const unsigned *rows, unsigned n,\n"
	"    const float *query, int dims, float *out)\n"
	"{\n"
	"  unsigned i = (blockIdx.x * blockDim.x + threadIdx.x) / GROUP;\n"
	"  float s = score4(data, scales, rowScales, rowBytes, blocks, i < n ? rows[i] : 0xffffffffu, query, dims, threadIdx.x % GROUP);\n"
	"  if (threadIdx.x % GROUP == 0 && i < n) out[i] = s;\n"
	"}\n"
	"extern \"C\" __global__ void score_fp8(const unsigned char *data, const float *rowScales,\n"
	"    unsigned rowBytes, const unsigned *rows, unsigned n, const float *query, int dims, float *out)\n"
	"{\n"
	"  unsigned i = (blockIdx.x * blockDim.x + threadIdx.x) / GROUP;\n"
	"  float s = score8(data, rowScales, rowBytes, i < n ? rows[i] : 0xffffffffu, query, dims, threadIdx.x % GROUP);\n"
	"  if (threadIdx.x % GROUP == 0 && i < n) out[i] = s;\n"
	"}\n",
	"",
	"/* Sorted (ascending) 32 smallest of a warp's best list and new candidates. */\n"
	"__device__ __forceinline__ void bitonic_sort(float &d, unsigned &r, int lane)\n"
	"{\n"
	"  for (int k = 2; k <= 32; k <<= 1)\n"
	"    for (int j = k >> 1; j > 0; j >>= 1)\n"
	"    {\n"
	"      float od = __shfl_xor_sync(0xffffffffu, d, j); unsigned orr = __shfl_xor_sync(0xffffffffu, r, j);\n"
	"      bool keepMin = ((lane & j) == 0) == ((lane & k) == 0);\n"
	"      if (keepMin ? od < d : od > d) { d = od; r = orr; }\n"
	"    }\n"
	"}\n"
	"/* best and fresh are ascending across lanes; best becomes the 32 smallest of both. */\n"
	"__device__ __forceinline__ void merge_best(float &bd, unsigned &br, float fd, unsigned fr, int lane)\n"
	"{\n"
	"  float rd = __shfl_sync(0xffffffffu, fd, 31 - lane); unsigned rr = __shfl_sync(0xffffffffu, fr, 31 - lane);\n"
	"  if (rd < bd) { bd = rd; br = rr; }\n"
	"  for (int j = 16; j > 0; j >>= 1)\n"
	"  {\n"
	"    float od = __shfl_xor_sync(0xffffffffu, bd, j); unsigned orr = __shfl_xor_sync(0xffffffffu, br, j);\n"
	"    if ((lane & j) == 0 ? od < bd : od > bd) { bd = od; br = orr; }\n"
	"  }\n"
	"}\n"
	"",
	"/*\n"
	" * Fused scoring and top-32 selection of one query over a list of rows.\n"
	" * Each warp scores four rows a step (8 threads a row) and keeps its own\n"
	" * best 32 in registers, merging new rows only when one beats both its own\n"
	" * 32nd best and the smallest 32nd best any warp of the block has published\n"
	" * (float bits in shared memory; non-negative floats order as integers), so\n"
	" * the scan has no block-wide barrier. Warps then merge per block;\n"
	" * the last block to finish merges every block's list, its warps each\n"
	" * taking a share before warp 0 merges theirs. Output holds positions in\n"
	" * the row list, ascending by distance.\n"
	" */\n"
	"template <int QUANT>\n"
	"__device__ void search(const unsigned char *data, const unsigned char *scales, const float *rowScales,\n"
	"    unsigned rowBytes, unsigned blocks, const unsigned *rows, unsigned n, unsigned perBlock,\n"
	"    const float *query, int dims, float *partD, unsigned *partR, unsigned *counter,\n"
	"    float *outD, unsigned *outR)\n"
	"{\n"
	"  __shared__ float sd[32 * 32]; __shared__ unsigned sr[32 * 32]; __shared__ bool last; __shared__ unsigned bound;\n"
	"  int lane = threadIdx.x & 31, warp = threadIdx.x >> 5, warps = blockDim.x >> 5;\n"
	"  int group = lane / GROUP, t = lane % GROUP, groups = 32 / GROUP;\n"
	"  const float inf = __int_as_float(0x7f800000);\n"
	"  float bd = inf, threshold = inf; unsigned br = 0xffffffffu;\n"
	"  unsigned begin = blockIdx.x * perBlock, end = min(n, begin + perBlock);\n"
	"  if (threadIdx.x == 0) bound = 0x7f800000u; /* block-wide bound, float bits */\n"
	"  __syncthreads();\n"
	"  volatile unsigned *shared = &bound;\n"
	"  for (unsigned base = begin + warp * groups; base < end; base += warps * groups)\n"
	"  {\n"
	"    /* Any warp's 32nd best bounds the block's 32nd best: rows past it can go. */\n"
	"    threshold = fminf(threshold, __uint_as_float(*shared));\n"
	"    unsigned i = base + group;\n"
	"    unsigned row = i < end ? rows[i] : 0xffffffffu;\n"
	"    float s = QUANT == 1 ? score4(data, scales, rowScales, rowBytes, blocks, row, query, dims, t)\n"
	"                         : score8(data, rowScales, rowBytes, row, query, dims, t);\n"
	"    bool pass = t == 0 && row != 0xffffffffu && s < threshold;\n"
	"    if (__ballot_sync(0xffffffffu, pass))\n"
	"    {\n"
	"      float fd = pass ? s : inf; unsigned fr = pass ? i : 0xffffffffu;\n"
	"      bitonic_sort(fd, fr, lane);\n"
	"      merge_best(bd, br, fd, fr, lane);\n"
	"      threshold = __shfl_sync(0xffffffffu, bd, 31);\n"
	"      if (lane == 0 && threshold < inf) atomicMin(&bound, __float_as_uint(threshold));\n"
	"    }\n"
	"  }\n"
	"  sd[warp * 32 + lane] = bd; sr[warp * 32 + lane] = br;\n"
	"  __syncthreads();\n"
	"  if (warp == 0)\n"
	"  {\n"
	"    for (int w = 1; w < warps; w++) merge_best(bd, br, sd[w * 32 + lane], sr[w * 32 + lane], lane);\n"
	"    partD[blockIdx.x * 32 + lane] = bd; partR[blockIdx.x * 32 + lane] = br;\n"
	"    __threadfence();\n"
	"    if (lane == 0) last = atomicAdd(counter, 1) == gridDim.x - 1;\n"
	"  }\n"
	"  __syncthreads();\n"
	"  if (!last) return;\n"
	"  __threadfence();\n"
	"  bd = inf; br = 0xffffffffu;\n"
	"  for (unsigned b = warp; b < gridDim.x; b += warps)\n"
	"    merge_best(bd, br, ((volatile float *) partD)[b * 32 + lane], ((volatile unsigned *) partR)[b * 32 + lane], lane);\n"
	"  sd[warp * 32 + lane] = bd; sr[warp * 32 + lane] = br;\n"
	"  __syncthreads();\n"
	"  if (warp != 0) return;\n"
	"  for (int w = 1; w < warps; w++) merge_best(bd, br, sd[w * 32 + lane], sr[w * 32 + lane], lane);\n"
	"  outD[lane] = bd; outR[lane] = br;\n"
	"  if (lane == 0) { counter[0] = 0; counter[1] = 0x7f800000u; }\n"
	"}\n",
	"extern \"C\" __global__ void search_nvfp4(const unsigned char *data, const unsigned char *scales,\n"
	"    const float *rowScales, unsigned rowBytes, unsigned blocks, const unsigned *rows, unsigned n,\n"
	"    unsigned perBlock, const float *query, int dims, float *partD, unsigned *partR, unsigned *counter,\n"
	"    float *outD, unsigned *outR)\n"
	"{ search<1>(data, scales, rowScales, rowBytes, blocks, rows, n, perBlock, query, dims, partD, partR, counter, outD, outR); }\n"
	"extern \"C\" __global__ void search_fp8(const unsigned char *data, const float *rowScales, unsigned rowBytes,\n"
	"    const unsigned *rows, unsigned n, unsigned perBlock, const float *query, int dims, float *partD,\n"
	"    unsigned *partR, unsigned *counter, float *outD, unsigned *outR)\n"
	"{ search<2>(data, 0, rowScales, rowBytes, 0, rows, n, perBlock, query, dims, partD, partR, counter, outD, outR); }\n",
	"",
	"/* Packed half2 helpers through PTX, so no CUDA headers are needed. */\n"
	"__device__ __forceinline__ unsigned hmul2(unsigned a, unsigned b)\n"
	"{ unsigned r; asm(\"mul.rn.f16x2 %0, %1, %2;\" : \"=r\"(r) : \"r\"(a), \"r\"(b)); return r; }\n"
	"__device__ __forceinline__ unsigned e2m1pair(unsigned byte)\n"
	"{\n"
	"  unsigned h2; unsigned short w = (unsigned short) (byte & 0xffu);\n"
	"  asm(\"{ .reg .b8 b; cvt.u8.u16 b, %1; cvt.rn.f16x2.e2m1x2 %0, b; }\" : \"=r\"(h2) : \"h\"(w));\n"
	"  return h2;\n"
	"}\n"
	"__device__ __forceinline__ unsigned e4m3pair(unsigned word)\n"
	"{ unsigned h2; asm(\"cvt.rn.f16x2.e4m3x2 %0, %1;\" : \"=r\"(h2) : \"h\"((unsigned short) (word & 0xffffu))); return h2; }\n"
	"/* An E4M3 block scale as a half2 holding it twice. */\n"
	"__device__ __forceinline__ unsigned scale2(unsigned byte)\n"
	"{ unsigned h2 = e4m3pair(byte & 0xffu); return (h2 & 0xffffu) | (h2 << 16); }\n"
	"__device__ __forceinline__ unsigned f32x2(float lo, float hi)\n"
	"{ unsigned r; asm(\"cvt.rn.f16x2.f32 %0, %1, %2;\" : \"=r\"(r) : \"f\"(hi), \"f\"(lo)); return r; }\n"
	"__device__ __forceinline__ float sumsq2(unsigned h2)\n"
	"{ float lo = half_bits((unsigned short) (h2 & 0xffffu)), hi = half_bits((unsigned short) (h2 >> 16)); return lo * lo + hi * hi; }\n"
	"/* D += A (16 rows x 16 k, FP16) * B (16 k x 8 queries, FP16), FP32 accumulate. */\n"
	"__device__ __forceinline__ void mma16816(float *c, unsigned a0, unsigned a1, unsigned a2, unsigned a3,\n"
	"    unsigned b0, unsigned b1)\n"
	"{\n"
	"  asm volatile(\"mma.sync.aligned.m16n8k16.row.col.f32.f16.f16.f32 {%0,%1,%2,%3}, {%4,%5,%6,%7}, {%8,%9}, {%0,%1,%2,%3};\"\n"
	"      : \"+f\"(c[0]), \"+f\"(c[1]), \"+f\"(c[2]), \"+f\"(c[3]) : \"r\"(a0), \"r\"(a1), \"r\"(a2), \"r\"(a3), \"r\"(b0), \"r\"(b1));\n"
	"}\n",
	"/*\n"
	" * Distances of up to 16 queries to every listed row on the tensor cores,\n"
	" * written as a [queries][rows] matrix for a batched top-k selection. Each\n"
	" * warp takes 16 rows at a time, loads their quantized bytes and block\n"
	" * scales up front, and multiplies them against all 16 queries with\n"
	" * mma.sync (FP16 in, FP32 out): the FP4 and FP8 conversion instructions\n"
	" * yield the A fragments directly (a half2 is two neighbouring dimensions\n"
	" * of one row), block scales are folded into them, and the B fragments\n"
	" * are two 32-bit loads of the half queries in shared memory. Distances\n"
	" * follow from |x|^2 + |q|^2 - 2 x.q with |x|^2 summed from the same half\n"
	" * values. Rows absent from the index (UINT32_MAX) score +inf.\n"
	" */\n"
	"#define KSEG 8 /* k-steps loaded per round: 128 dimensions */\n"
	"template <int QUANT>\n"
	"__device__ void score_mma(const unsigned char *data, const unsigned char *scales,\n"
	"    const float *rowScales, unsigned rowBytes, unsigned blocks, const unsigned *rows, unsigned n,\n"
	"    const float *queries, int nq, int dims, float *out)\n"
	"{\n"
	"  extern __shared__ unsigned short qh[];\n"
	"  __shared__ float qn[16];\n"
	"  int lane = threadIdx.x & 31, warp = threadIdx.x >> 5, warps = blockDim.x >> 5;\n"
	"  int g = lane >> 2, t = lane & 3;\n"
	"  int qstride = (dims + 15) & ~15;\n"
	"  const float inf = __int_as_float(0x7f800000);\n"
	"  for (int i = threadIdx.x; i < 16 * qstride; i += blockDim.x)\n"
	"  {\n"
	"    int q = i / qstride, d = i - q * qstride;\n"
	"    float v = (q < nq && d < dims) ? queries[q * dims + d] : 0.0f;\n"
	"    unsigned short h; asm(\"cvt.rn.f16.f32 %0, %1;\" : \"=h\"(h) : \"f\"(v)); qh[i] = h;\n"
	"  }\n"
	"  __syncthreads();\n"
	"  if (threadIdx.x < 16)\n"
	"  {\n"
	"    float s = 0.0f;\n"
	"    for (int d = 0; d < dims; d++) { float v = half_bits(qh[threadIdx.x * qstride + d]); s += v * v; }\n"
	"    qn[threadIdx.x] = s;\n"
	"  }\n"
	"  __syncthreads();\n"
	"  for (unsigned base = (blockIdx.x * warps + warp) * 16; base < n; base += gridDim.x * warps * 16)\n"
	"  {\n"
	"    unsigned i0 = base + g, i1 = base + g + 8;\n"
	"    unsigned row0 = i0 < n ? rows[i0] : 0xffffffffu, row1 = i1 < n ? rows[i1] : 0xffffffffu;\n"
	"    bool v0 = row0 != 0xffffffffu, v1 = row1 != 0xffffffffu;\n"
	"    unsigned r0 = v0 ? row0 : 0u, r1 = v1 ? row1 : 0u;\n"
	"    float acc[8]; float nx0 = 0.0f, nx1 = 0.0f;\n"
	"#pragma unroll\n"
	"    for (int j = 0; j < 8; j++) acc[j] = 0.0f;\n"
	"    for (unsigned seg = 0; seg < blocks; seg += KSEG)\n",
	"    {\n"
	"      uint2 w0[KSEG], w1[KSEG]; uint2 e0 = {0, 0}, e1 = {0, 0};\n"
	"      if (QUANT == 3)\n"
	"      {\n"
	"        /* FP16 dataset: this lane's two pairs of a k-step are 32-bit words at k = 2t and 2t + 8. */\n"
	"        const unsigned short *p0 = (const unsigned short *) (data + (unsigned long long) r0 * rowBytes) + seg * 16 + 2 * t;\n"
	"        const unsigned short *p1 = (const unsigned short *) (data + (unsigned long long) r1 * rowBytes) + seg * 16 + 2 * t;\n"
	"#pragma unroll\n"
	"        for (int k = 0; k < KSEG; k++)\n"
	"          if (seg + k < blocks)\n"
	"          {\n"
	"            w0[k].x = *(const unsigned *) (p0 + k * 16); w0[k].y = *(const unsigned *) (p0 + k * 16 + 8);\n"
	"            w1[k].x = *(const unsigned *) (p1 + k * 16); w1[k].y = *(const unsigned *) (p1 + k * 16 + 8);\n"
	"          }\n"
	"      }\n"
	"      else if (QUANT == 4)\n"
	"      {\n"
	"        /* FP32 dataset: float pairs, converted to half2 at use. */\n"
	"        const float2 *p0 = (const float2 *) (data + (unsigned long long) r0 * rowBytes) + seg * 8 + t;\n"
	"        const float2 *p1 = (const float2 *) (data + (unsigned long long) r1 * rowBytes) + seg * 8 + t;\n"
	"#pragma unroll\n"
	"        for (int k = 0; k < KSEG; k++)\n"
	"          if (seg + k < blocks)\n"
	"          {\n"
	"            float2 a = p0[k * 8], b = p0[k * 8 + 4], c = p1[k * 8], d = p1[k * 8 + 4];\n"
	"            w0[k].x = f32x2(a.x, a.y); w0[k].y = f32x2(b.x, b.y); w1[k].x = f32x2(c.x, c.y); w1[k].y = f32x2(d.x, d.y);\n"
	"          }\n"
	"      }\n"
	"      else if (QUANT == 1)\n",
	"      {\n"
	"        const uint2 *p0 = (const uint2 *) (data + (unsigned long long) r0 * rowBytes) + seg;\n"
	"        const uint2 *p1 = (const uint2 *) (data + (unsigned long long) r1 * rowBytes) + seg;\n"
	"#pragma unroll\n"
	"        for (int k = 0; k < KSEG; k++) if (seg + k < blocks) { w0[k] = p0[k]; w1[k] = p1[k]; }\n"
	"        /* 8 block scales at once when rows hold whole multiples of 8 (aligned, in bounds); else bytes. */\n"
	"        if ((blocks & 7) == 0)\n"
	"        {\n"
	"          e0 = *(const uint2 *) (scales + (unsigned long long) r0 * blocks + seg);\n"
	"          e1 = *(const uint2 *) (scales + (unsigned long long) r1 * blocks + seg);\n"
	"        }\n"
	"        else\n"
	"        {\n"
	"          const unsigned char *s0 = scales + (unsigned long long) r0 * blocks + seg;\n"
	"          const unsigned char *s1 = scales + (unsigned long long) r1 * blocks + seg;\n"
	"#pragma unroll\n"
	"          for (int k = 0; k < KSEG; k++)\n"
	"            if (seg + k < blocks)\n"
	"            {\n"
	"              unsigned b0 = s0[k], b1 = s1[k];\n"
	"              if (k < 4) { e0.x |= b0 << (8 * k); e1.x |= b1 << (8 * k); } else { e0.y |= b0 << (8 * (k - 4)); e1.y |= b1 << (8 * (k - 4)); }\n"
	"            }\n"
	"        }\n"
	"      }\n"
	"      else\n"
	"      {\n"
	"        const uint4 *p0 = (const uint4 *) (data + (unsigned long long) r0 * rowBytes) + seg;\n"
	"        const uint4 *p1 = (const uint4 *) (data + (unsigned long long) r1 * rowBytes) + seg;\n"
	"#pragma unroll\n"
	"        for (int k = 0; k < KSEG; k++)\n"
	"          if (seg + k < blocks)\n"
	"          {\n"
	"            uint4 x0 = p0[k], x1 = p1[k];\n"
	"            w0[k].x = t < 2 ? x0.x : x0.y; w0[k].y = t < 2 ? x0.z : x0.w;\n"
	"            w1[k].x = t < 2 ? x1.x : x1.y; w1[k].y = t < 2 ? x1.z : x1.w;\n"
	"          }\n"
	"      }\n"
	"#pragma unroll\n"
	"      for (int k = 0; k < KSEG; k++)\n"
	"      {\n"
	"        if (seg + k >= blocks) break;\n"
	"        unsigned a0, a1, a2, a3;\n"
	"        if (QUANT == 1)\n"
	"        {\n"
	"          unsigned sc0 = scale2((k < 4 ? e0.x : e0.y) >> (8 * (k & 3))), sc1 = scale2((k < 4 ? e1.x : e1.y) >> (8 * (k & 3)));\n"
	"          a0 = hmul2(e2m1pair(w0[k].x >> (8 * t)), sc0); a2 = hmul2(e2m1pair(w0[k].y >> (8 * t)), sc0);\n"
	"          a1 = hmul2(e2m1pair(w1[k].x >> (8 * t)), sc1); a3 = hmul2(e2m1pair(w1[k].y >> (8 * t)), sc1);\n"
	"        }\n"
	"        else if (QUANT == 2)\n"
	"        {\n"
	"          a0 = e4m3pair(w0[k].x >> (16 * (t & 1))); a2 = e4m3pair(w0[k].y >> (16 * (t & 1)));\n"
	"          a1 = e4m3pair(w1[k].x >> (16 * (t & 1))); a3 = e4m3pair(w1[k].y >> (16 * (t & 1)));\n"
	"        }\n"
	"        else\n"
	"        { a0 = w0[k].x; a2 = w0[k].y; a1 = w1[k].x; a3 = w1[k].y; }\n"
	"        /* Dimensions past dims (a dataset's padding) count for nothing. */\n"
	"        if ((seg + k) * 16 + 16 > dims)\n"
	"        {\n"
	"          int d0 = (seg + k) * 16 + 2 * t, d2 = d0 + 8;\n"
	"          unsigned m0 = d0 + 1 < dims ? 0xffffffffu : d0 < dims ? 0x0000ffffu : 0u;\n"
	"          unsigned m2 = d2 + 1 < dims ? 0xffffffffu : d2 < dims ? 0x0000ffffu : 0u;\n"
	"          a0 &= m0; a1 &= m0; a2 &= m2; a3 &= m2;\n"
	"        }\n"
	"        nx0 += sumsq2(a0) + sumsq2(a2); nx1 += sumsq2(a1) + sumsq2(a3);\n"
	"        const unsigned short *qk = qh + (seg + k) * 16 + 2 * t;\n"
	"        unsigned b0 = *(const unsigned *) (qk + g * qstride), b1 = *(const unsigned *) (qk + g * qstride + 8);\n"
	"        unsigned b2 = *(const unsigned *) (qk + (8 + g) * qstride), b3 = *(const unsigned *) (qk + (8 + g) * qstride + 8);\n"
	"        mma16816(acc, a0, a1, a2, a3, b0, b1);\n"
	"        mma16816(acc + 4, a0, a1, a2, a3, b2, b3);\n"
	"      }\n"
	"    }\n",
	"    nx0 += __shfl_xor_sync(0xffffffffu, nx0, 1); nx0 += __shfl_xor_sync(0xffffffffu, nx0, 2);\n"
	"    nx1 += __shfl_xor_sync(0xffffffffu, nx1, 1); nx1 += __shfl_xor_sync(0xffffffffu, nx1, 2);\n"
	"    float rs0 = rowScales ? rowScales[r0] : 1.0f, rs1 = rowScales ? rowScales[r1] : 1.0f;\n"
	"    /* acc[nt * 4 + c]: c0 (row g, query nt*8 + 2t), c1 (g, +1), c2 (row g + 8, 2t), c3 (g + 8, +1) */\n"
	"#pragma unroll\n"
	"    for (int j = 0; j < 8; j++)\n"
	"    {\n"
	"      int nn = (j >> 2) * 8 + 2 * t + (j & 1);\n"
	"      unsigned i = (j & 2) ? i1 : i0; bool vr = (j & 2) ? v1 : v0;\n"
	"      float nx = (j & 2) ? nx1 : nx0; float rs = (j & 2) ? rs1 : rs0;\n"
	"      float dist = fmaxf(rs * rs * nx + qn[nn] - 2.0f * rs * acc[j], 0.0f);\n"
	"      if (nn < nq && i < n) out[(unsigned long long) nn * n + i] = vr ? dist : inf;\n"
	"    }\n"
	"  }\n"
	"}\n",
	"#define MMA(NAME, QUANT) extern \"C\" __global__ void NAME(const unsigned char *data, \\\n"
	"    const unsigned char *scales, const float *rowScales, unsigned rowBytes, unsigned blocks, \\\n"
	"    const unsigned *rows, unsigned n, const float *queries, int nq, int dims, float *out) \\\n"
	"{ score_mma<QUANT>(data, scales, rowScales, rowBytes, blocks, rows, n, queries, nq, dims, out); }\n"
	"MMA(score_mma_nvfp4, 1)\n"
	"MMA(score_mma_fp8, 2)\n"
	"MMA(score_mma_half, 3)\n"
	"MMA(score_mma_float, 4)\n",
	"/*\n"
	" * Batched top-32 over a [queries][rows] score matrix. Block (slice, q)\n"
	" * scans its slice of query q's scores: each warp takes 32 scores a step,\n"
	" * one per lane, and keeps a sorted best-32 list in registers, merging a\n"
	" * step's scores only when one beats both its 32nd best and the block's\n"
	" * bound (so, with long slices, almost never). Warps merge per block into\n"
	" * one partial list per (q, slice); merge32 then folds a query's slices.\n"
	" */\n"
	"extern \"C\" __global__ void select32(const float *scores, unsigned n, unsigned perBlock, float *partD,\n"
	"    unsigned *partR)\n"
	"{\n"
	"  __shared__ float sd[8 * 32]; __shared__ unsigned sr[8 * 32]; __shared__ unsigned bound;\n"
	"  int lane = threadIdx.x & 31, warp = __shfl_sync(0xffffffffu, (int) (threadIdx.x >> 5), 0);\n"
	"  int warps = blockDim.x >> 5;\n"
	"  const float inf = __int_as_float(0x7f800000);\n"
	"  const float *row = scores + (unsigned long long) blockIdx.y * n;\n"
	"  unsigned begin = blockIdx.x * perBlock, end = min(n, begin + perBlock);\n"
	"  float bd = inf; unsigned br = 0xffffffffu;\n"
	"  if (threadIdx.x == 0) bound = 0x7f800000u;\n"
	"  __syncthreads();\n"
	"  for (unsigned base = begin + warp * 32; base < end; base += warps * 32)\n"
	"  {\n"
	"    unsigned i = base + lane;\n"
	"    float v = i < end ? row[i] : inf;\n"
	"    float thr = fminf(__shfl_sync(0xffffffffu, bd, 31), __uint_as_float(*(volatile unsigned *) &bound));\n"
	"    if (__ballot_sync(0xffffffffu, v < thr) != 0u)\n"
	"    {\n"
	"      float fd = v < thr ? v : inf; unsigned fr = v < thr ? i : 0xffffffffu;\n"
	"      bitonic_sort(fd, fr, lane);\n"
	"      merge_best(bd, br, fd, fr, lane);\n"
	"      float worst = __shfl_sync(0xffffffffu, bd, 31);\n"
	"      if (lane == 0 && worst < inf) atomicMin(&bound, __float_as_uint(worst));\n"
	"    }\n"
	"  }\n"
	"  sd[warp * 32 + lane] = bd; sr[warp * 32 + lane] = br;\n"
	"  __syncthreads();\n"
	"  if (warp == 0)\n"
	"  {\n"
	"    for (int w = 1; w < warps; w++) merge_best(bd, br, sd[w * 32 + lane], sr[w * 32 + lane], lane);\n"
	"    partD[((unsigned long long) blockIdx.y * gridDim.x + blockIdx.x) * 32 + lane] = bd;\n"
	"    partR[((unsigned long long) blockIdx.y * gridDim.x + blockIdx.x) * 32 + lane] = br;\n"
	"  }\n"
	"}\n"
	"/* One warp per query merges its slices' lists into the final 32, ascending. */\n"
	"extern \"C\" __global__ void merge32(const float *partD, const unsigned *partR, unsigned slices, float *outD,\n"
	"    unsigned *outR)\n"
	"{\n"
	"  int lane = threadIdx.x & 31;\n"
	"  unsigned q = blockIdx.x;\n"
	"  float bd = partD[((unsigned long long) q * slices) * 32 + lane]; unsigned br = partR[((unsigned long long) q * slices) * 32 + lane];\n"
	"  for (unsigned b = 1; b < slices; b++)\n"
	"    merge_best(bd, br, partD[((unsigned long long) q * slices + b) * 32 + lane], partR[((unsigned long long) q * slices + b) * 32 + lane], lane);\n"
	"  outD[q * 32 + lane] = bd; outR[q * 32 + lane] = br;\n"
	"}\n",
NULL};

typedef struct BlackwellModule
{
	int			state;			/* 0 untried, 1 ready, -1 unavailable */
	CUcontext	context;
	CUmodule	module;
	CUfunction	nvfp4;
	CUfunction	fp8;
	CUfunction	searchNvfp4;
	CUfunction	searchFp8;
	CUfunction	mma[4];			/* [format - 1]: score matrix of up to 16
								 * queries from NVFP4, FP8, FP16, FP32 rows */
	CUfunction	select32;		/* batched top-32 over a score matrix */
	CUfunction	merge32;
	int			sms;			/* streaming multiprocessors */
} BlackwellModule;

/*
 * Per caller thread, device and slot (one per shard a thread searches on
 * that device): block partial lists for up to ANN_BLACKWELL_GROUP_MAX
 * queries, a finish counter, the final lists, and pinned host copies.
 */
typedef struct SearchSlot
{
	unsigned	maxBlocks;
	float	   *scores;			/* [queries][rows] for grouped searches */
	size_t		scoreCapacity;
	float	   *partD;
	unsigned   *partR;
	unsigned   *counter;
	float	   *outD;
	unsigned   *outR;
	float	   *hostD;
	unsigned   *hostR;
} SearchSlot;

static _Thread_local SearchSlot *slots[ANN_BLACKWELL_MAX_DEVICES][ANN_INDEX_MAX_SHARDS];

static BlackwellModule modules[ANN_BLACKWELL_MAX_DEVICES];
static pthread_mutex_t moduleMutex = PTHREAD_MUTEX_INITIALIZER;

static char *
JoinSource(void)
{
	size_t		length = 1;
	char	   *source;

	for (int i = 0; BlackwellSource[i]; i++)
		length += strlen(BlackwellSource[i]);
	if ((source = malloc(length)) == NULL)
		return NULL;
	source[0] = '\0';
	for (int i = 0; BlackwellSource[i]; i++)
		strcat(source, BlackwellSource[i]);
	return source;
}

int
AnnBlackwellReady(int device)
{
	BlackwellModule *module;
	CUdevice	handle;
	int			major = 0;
	char	   *image = NULL;
	char	   *source = NULL;
	size_t		size = 0;

	if (device < 0 || device >= ANN_BLACKWELL_MAX_DEVICES)
		return 0;
	module = &modules[device];
	if (module->state != 0)
		return module->state > 0;
	(void) pthread_mutex_lock(&moduleMutex);
	if (module->state != 0)
		goto unlock;
	module->state = -1;
	if (cudaDeviceGetAttribute(&major, cudaDevAttrComputeCapabilityMajor, device) != cudaSuccess || major != 10)
	{
		fprintf(stderr, "Blackwell module: device %d has compute capability %d.x, not 10.x; "
				"quantized scans are off there\n", device, major);
		goto unlock;
	}
	if (cuInit(0) != CUDA_SUCCESS || cuDeviceGet(&handle, device) != CUDA_SUCCESS ||
		cuDevicePrimaryCtxRetain(&module->context, handle) != CUDA_SUCCESS ||
		cuCtxSetCurrent(module->context) != CUDA_SUCCESS ||
		(source = JoinSource()) == NULL ||
		(image = AnnNvrtcCompile(source, "Blackwell module", "sm_100a", &size)) == NULL ||
		cuModuleLoadData(&module->module, image) != CUDA_SUCCESS ||
		cuModuleGetFunction(&module->nvfp4, module->module, "score_nvfp4") != CUDA_SUCCESS ||
		cuModuleGetFunction(&module->fp8, module->module, "score_fp8") != CUDA_SUCCESS ||
		cuModuleGetFunction(&module->searchNvfp4, module->module, "search_nvfp4") != CUDA_SUCCESS ||
		cuModuleGetFunction(&module->searchFp8, module->module, "search_fp8") != CUDA_SUCCESS ||
		cuModuleGetFunction(&module->mma[0], module->module, "score_mma_nvfp4") != CUDA_SUCCESS ||
		cuModuleGetFunction(&module->mma[1], module->module, "score_mma_fp8") != CUDA_SUCCESS ||
		cuModuleGetFunction(&module->mma[2], module->module, "score_mma_half") != CUDA_SUCCESS ||
		cuModuleGetFunction(&module->mma[3], module->module, "score_mma_float") != CUDA_SUCCESS ||
		cuModuleGetFunction(&module->select32, module->module, "select32") != CUDA_SUCCESS ||
		cuModuleGetFunction(&module->merge32, module->module, "merge32") != CUDA_SUCCESS ||
		cudaDeviceGetAttribute(&module->sms, cudaDevAttrMultiProcessorCount, device) != cudaSuccess)
	{
		fprintf(stderr, "Blackwell module: unavailable on device %d (NVRTC or sm_100a load failed)\n", device);
		goto unlock;
	}
	module->state = 1;
	{
		/* Register arrays indexed by an unrolled loop must not spill. */
		const char *names[] = {"search_nvfp4", "score_mma_nvfp4", "score_mma_fp8"};
		CUfunction	functions[] = {module->searchNvfp4, module->mma[0], module->mma[1]};

		for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++)
		{
			int			registers = 0,
						local = 0;

			if (cuFuncGetAttribute(&registers, CU_FUNC_ATTRIBUTE_NUM_REGS, functions[i]) == CUDA_SUCCESS &&
				cuFuncGetAttribute(&local, CU_FUNC_ATTRIBUTE_LOCAL_SIZE_BYTES, functions[i]) == CUDA_SUCCESS &&
				(local > 0 || getenv("ANN_BLACKWELL_VERBOSE")))
				fprintf(stderr, "Blackwell module: %s uses %d registers and %d bytes of local memory%s\n",
						names[i], registers, local, local > 0 ? " (spilling)" : "");
		}
	}
unlock:
	free(source);
	free(image);
	(void) pthread_mutex_unlock(&moduleMutex);
	return module->state > 0;
}

void
AnnBlackwellFree(AnnBlackwellShard *shard)
{
	if (shard->owned)
	{
		(void) cudaFree(shard->data);
		(void) cudaFree(shard->scales);
		(void) cudaFree(shard->rowScales);
	}
	memset(shard, 0, sizeof(*shard));
}

int
AnnBlackwellView(const void *dataset, int64_t stride, int half, uint32_t dims, int device, AnnBlackwellShard *out)
{
	memset(out, 0, sizeof(*out));
	if (dataset == NULL || stride < (int64_t) dims || !AnnBlackwellReady(device))
		return 0;
	out->quant = half ? ANN_BLACKWELL_HALF : ANN_BLACKWELL_FLOAT;
	out->data = (uint8_t *) (uintptr_t) dataset;
	out->rowBytes = (uint32_t) (stride * (half ? 2 : 4));
	out->blocks = (dims + 15) / 16;
	return 1;
}

int
AnnBlackwellUpload(const AnnIndexFile *file, uint32_t s, int device, AnnBlackwellShard *out)
{
	uint64_t	first = file->shards[s].first;
	size_t		rows = file->shards[s].rows;
	size_t		dataBytes = rows * file->quantRowBytes;
	size_t		scaleBytes = file->quant == ANN_QUANT_NVFP4 ? rows * file->quantBlocks : 0;

	memset(out, 0, sizeof(*out));
	if (file->quant == ANN_QUANT_NONE || !AnnBlackwellReady(device) || cudaSetDevice(device) != cudaSuccess)
		return 0;
	if (cudaMalloc((void **) &out->data, dataBytes) != cudaSuccess ||
		cudaMemcpy(out->data, file->quantData + first * file->quantRowBytes, dataBytes,
				   cudaMemcpyHostToDevice) != cudaSuccess ||
		(scaleBytes &&
		 (cudaMalloc((void **) &out->scales, scaleBytes) != cudaSuccess ||
		  cudaMemcpy(out->scales, file->quantScales + first * file->quantBlocks, scaleBytes,
					 cudaMemcpyHostToDevice) != cudaSuccess)) ||
		cudaMalloc((void **) &out->rowScales, rows * sizeof(float)) != cudaSuccess ||
		cudaMemcpy(out->rowScales, file->rowScales + first, rows * sizeof(float), cudaMemcpyHostToDevice) != cudaSuccess)
	{
		fprintf(stderr, "Blackwell module: cannot copy shard %u's %s rows to device %d\n", s,
				AnnIndexQuantName(file->quant), device);
		AnnBlackwellFree(out);
		return 0;
	}
	out->quant = file->quant;
	out->owned = 1;
	out->rowBytes = file->quantRowBytes;
	out->blocks = file->quantBlocks;
	out->bytes = dataBytes + scaleBytes + rows * sizeof(float);
	return 1;
}

int
AnnBlackwellScore(const AnnBlackwellShard *shard, int device, const uint32_t *rows, unsigned n,
				  const float *query, int dims, float *out, void *stream)
{
	BlackwellModule *module = &modules[device];
	unsigned	grid = (unsigned) (((uint64_t) n * ANN_BLACKWELL_GROUP + ANN_BLACKWELL_BLOCK - 1) / ANN_BLACKWELL_BLOCK);
	unsigned	rowBytes = shard->rowBytes;
	unsigned	blocks = shard->blocks;

	if (n == 0)
		return 1;
	if (shard->quant == ANN_QUANT_NONE || shard->quant >= ANN_BLACKWELL_HALF || module->state <= 0 ||
		cuCtxSetCurrent(module->context) != CUDA_SUCCESS)
		return 0;
	if (shard->quant == ANN_QUANT_NVFP4)
	{
		void	   *args[] = {(void *) &shard->data, (void *) &shard->scales, (void *) &shard->rowScales,
		&rowBytes, &blocks, (void *) &rows, &n, (void *) &query, &dims, &out};

		return cuLaunchKernel(module->nvfp4, grid, 1, 1, ANN_BLACKWELL_BLOCK, 1, 1, 0,
							  (CUstream) stream, args, NULL) == CUDA_SUCCESS;
	}
	{
		void	   *args[] = {(void *) &shard->data, (void *) &shard->rowScales, &rowBytes,
		(void *) &rows, &n, (void *) &query, &dims, &out};

		return cuLaunchKernel(module->fp8, grid, 1, 1, ANN_BLACKWELL_BLOCK, 1, 1, 0,
							  (CUstream) stream, args, NULL) == CUDA_SUCCESS;
	}
}

/* A thread's slot on the current device, sized for maxBlocks; allocated on first use. */
static SearchSlot *
GetSlot(int device, unsigned slot, unsigned maxBlocks)
{
	SearchSlot *sc = slots[device][slot];
	size_t		lists = (size_t) maxBlocks * ANN_BLACKWELL_GROUP_MAX * ANN_BLACKWELL_TOP;
	unsigned	zero[2] = {0, 0x7f800000u};

	if (sc && sc->maxBlocks >= maxBlocks)
		return sc;
	if (sc)
	{
		(void) cudaFree(sc->scores);
		(void) cudaFree(sc->partD);
		(void) cudaFree(sc->partR);
		(void) cudaFree(sc->counter);
		(void) cudaFree(sc->outD);
		(void) cudaFree(sc->outR);
		(void) cudaFreeHost(sc->hostD);
		(void) cudaFreeHost(sc->hostR);
		memset(sc, 0, sizeof(*sc));
	}
	else if ((sc = slots[device][slot] = calloc(1, sizeof(*sc))) == NULL)
		return NULL;
	if (cudaMalloc((void **) &sc->partD, lists * sizeof(float)) != cudaSuccess ||
		cudaMalloc((void **) &sc->partR, lists * sizeof(unsigned)) != cudaSuccess ||
		cudaMalloc((void **) &sc->counter, sizeof(zero)) != cudaSuccess ||
		cudaMemcpy(sc->counter, zero, sizeof(zero), cudaMemcpyHostToDevice) != cudaSuccess ||
		cudaMalloc((void **) &sc->outD, ANN_BLACKWELL_GROUP_MAX * ANN_BLACKWELL_TOP * sizeof(float)) != cudaSuccess ||
		cudaMalloc((void **) &sc->outR, ANN_BLACKWELL_GROUP_MAX * ANN_BLACKWELL_TOP * sizeof(unsigned)) != cudaSuccess ||
		cudaMallocHost((void **) &sc->hostD, ANN_BLACKWELL_GROUP_MAX * ANN_BLACKWELL_TOP * sizeof(float)) != cudaSuccess ||
		cudaMallocHost((void **) &sc->hostR, ANN_BLACKWELL_GROUP_MAX * ANN_BLACKWELL_TOP * sizeof(unsigned)) != cudaSuccess)
	{
		fprintf(stderr, "Blackwell module: cannot allocate search scratch on device %d\n", device);
		return NULL;
	}
	sc->maxBlocks = maxBlocks;
	return sc;
}

int
AnnBlackwellGroupLimit(int dims)
{
	/* 16 half queries live in dynamic shared memory next to 3 KB of lists. */
	int			qstride = (dims + 15) & ~15;

	return (size_t) 16 * qstride * 2 <= 45 * 1024 ? ANN_BLACKWELL_GROUP_MAX : 1;
}

int
AnnBlackwellSearch(const AnnBlackwellShard *shard, int device, unsigned slot, const uint32_t *rows, unsigned n,
				   const float *queries, unsigned nq, int dims, void *stream)
{
	BlackwellModule *module = &modules[device];
	unsigned	maxBlocks,
				grid,
				perBlock;
	unsigned	rowBytes = shard->rowBytes,
				blocks = shard->blocks;
	int			dimensions = dims,
				count = (int) nq;
	SearchSlot *sc;
	CUfunction	function;

	if (shard->quant == ANN_QUANT_NONE || module->state <= 0 || n == 0 || nq == 0 ||
		nq > (unsigned) AnnBlackwellGroupLimit(dims) || slot >= ANN_INDEX_MAX_SHARDS)
		return 0;

	/*
	 * Small lists get 128-row blocks, so enough warps are in flight to hide
	 * scattered row loads; larger ones 512-row blocks, up to four per SM, so
	 * the last block's merge of every block's list stays short. Measured on
	 * B200 (10M x 128, NVFP4): 10k rows 26.7 -> 15.4 us with 128-row blocks;
	 * 100k and 1M rows fastest with 512 (40.8 and 156 us).
	 */
	maxBlocks = (unsigned) module->sms * 4;
	grid = n <= 32768 ? (n + 127) / 128 : (n + 511) / 512;
	if (grid > maxBlocks)
		grid = maxBlocks;
	perBlock = (n + grid - 1) / grid;
	if ((sc = GetSlot(device, slot, maxBlocks)) == NULL || cuCtxSetCurrent(module->context) != CUDA_SUCCESS)
		return 0;
	if (nq == 1 && shard->quant < ANN_BLACKWELL_HALF)
	{
		function = shard->quant == ANN_QUANT_NVFP4 ? module->searchNvfp4 : module->searchFp8;
		if (shard->quant == ANN_QUANT_NVFP4)
		{
			void	   *args[] = {(void *) &shard->data, (void *) &shard->scales, (void *) &shard->rowScales,
				&rowBytes, &blocks, (void *) &rows, &n, &perBlock, (void *) &queries, &dimensions, &sc->partD,
			&sc->partR, &sc->counter, &sc->outD, &sc->outR};

			if (cuLaunchKernel(function, grid, 1, 1, ANN_BLACKWELL_BLOCK, 1, 1, 0, (CUstream) stream, args, NULL) != CUDA_SUCCESS)
				return 0;
		}
		else
		{
			void	   *args[] = {(void *) &shard->data, (void *) &shard->rowScales, &rowBytes, (void *) &rows,
				&n, &perBlock, (void *) &queries, &dimensions, &sc->partD, &sc->partR, &sc->counter, &sc->outD,
			&sc->outR};

			if (cuLaunchKernel(function, grid, 1, 1, ANN_BLACKWELL_BLOCK, 1, 1, 0, (CUstream) stream, args, NULL) != CUDA_SUCCESS)
				return 0;
		}
	}
	else
	{
		/*
		 * Several queries (or a lone one over a dataset read in place): the
		 * tensor-core kernel writes every distance to a [queries][rows]
		 * matrix, select32 keeps each query's best 32 per slice of rows, and
		 * merge32 folds the slices. About a quarter of the SMs' worth of
		 * slices was fastest on B200 (1M rows x 16 queries: 113 us against
		 * 165 with one slice per SM): long slices let a warp's list fill
		 * early and then rarely change, and the merge stays short.
		 */
		size_t		needed = (size_t) nq * n;
		unsigned	sgrid = (n + 127) / 128 < maxBlocks ? (n + 127) / 128 : maxBlocks;
		unsigned	cap = (unsigned) module->sms / 4 > 1 ? (unsigned) module->sms / 4 : 1;
		unsigned	slices = (n + 8191) / 8192 < cap ? (n + 8191) / 8192 : cap;
		unsigned	perSlice;
		unsigned	shared = (unsigned) (16 * (((unsigned) dims + 15) & ~15u) * sizeof(uint16_t));

		if (slices < 1)
			slices = 1;
		perSlice = (n + slices - 1) / slices;
		if (shard->quant == ANN_QUANT_FP8)
			blocks = rowBytes / 16;
		else if (shard->quant >= ANN_BLACKWELL_HALF)
			blocks = ((unsigned) dims + 15) / 16;
		if (needed > sc->scoreCapacity)
		{
			(void) cudaFree(sc->scores);
			sc->scores = NULL;
			sc->scoreCapacity = 0;
			if (cudaMalloc((void **) &sc->scores, needed * 2 * sizeof(float)) != cudaSuccess)
				return 0;
			sc->scoreCapacity = needed * 2;
		}
		{
			void	   *args[] = {(void *) &shard->data, (void *) &shard->scales, (void *) &shard->rowScales,
			&rowBytes, &blocks, (void *) &rows, &n, (void *) &queries, &count, &dimensions, &sc->scores};
			void	   *selectArgs[] = {&sc->scores, &n, &perSlice, &sc->partD, &sc->partR};
			void	   *mergeArgs[] = {&sc->partD, &sc->partR, &slices, &sc->outD, &sc->outR};

			if (cuLaunchKernel(module->mma[shard->quant - 1], sgrid, 1, 1, ANN_BLACKWELL_BLOCK, 1, 1, shared,
							   (CUstream) stream, args, NULL) != CUDA_SUCCESS ||
				cuLaunchKernel(module->select32, slices, nq, 1, ANN_BLACKWELL_BLOCK, 1, 1, 0, (CUstream) stream,
							   selectArgs, NULL) != CUDA_SUCCESS ||
				cuLaunchKernel(module->merge32, nq, 1, 1, 32, 1, 1, 0, (CUstream) stream, mergeArgs, NULL) != CUDA_SUCCESS)
				return 0;
		}
	}
	return cudaMemcpyAsync(sc->hostD, sc->outD, (size_t) nq * ANN_BLACKWELL_TOP * sizeof(float),
						   cudaMemcpyDeviceToHost, (cudaStream_t) stream) == cudaSuccess &&
		cudaMemcpyAsync(sc->hostR, sc->outR, (size_t) nq * ANN_BLACKWELL_TOP * sizeof(unsigned),
						cudaMemcpyDeviceToHost, (cudaStream_t) stream) == cudaSuccess;
}

void
AnnBlackwellResults(int device, unsigned slot, unsigned q, const uint32_t **positions, const float **distances)
{
	SearchSlot *sc = slots[device][slot];

	*positions = sc->hostR + (size_t) q * ANN_BLACKWELL_TOP;
	*distances = sc->hostD + (size_t) q * ANN_BLACKWELL_TOP;
}
