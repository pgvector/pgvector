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
	int			sms;			/* streaming multiprocessors */
} BlackwellModule;

/*
 * Per caller thread and device: block partial lists, a finish counter and
 * the final list per slot, plus pinned host copies of the final lists.
 */
typedef struct Scratch
{
	unsigned	slots;
	unsigned	maxBlocks;
	float	   *partD;
	unsigned   *partR;
	unsigned   *counters;
	float	   *outD;
	unsigned   *outR;
	float	   *hostD;
	unsigned   *hostR;
} Scratch;

static _Thread_local Scratch scratch[ANN_BLACKWELL_MAX_DEVICES];

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
		cudaDeviceGetAttribute(&module->sms, cudaDevAttrMultiProcessorCount, device) != cudaSuccess)
	{
		fprintf(stderr, "Blackwell module: unavailable on device %d (NVRTC or sm_100a load failed)\n", device);
		goto unlock;
	}
	module->state = 1;
unlock:
	free(source);
	free(image);
	(void) pthread_mutex_unlock(&moduleMutex);
	return module->state > 0;
}

void
AnnBlackwellFree(AnnBlackwellShard *shard)
{
	(void) cudaFree(shard->data);
	(void) cudaFree(shard->scales);
	(void) cudaFree(shard->rowScales);
	memset(shard, 0, sizeof(*shard));
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
	if (shard->quant == ANN_QUANT_NONE || module->state <= 0 || cuCtxSetCurrent(module->context) != CUDA_SUCCESS)
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

/* Each slot's finish counter and grid-wide bound start at {0, +inf}. */
static int
ResetCounters(unsigned *counters, unsigned slots)
{
	unsigned   *host = malloc(slots * 2 * sizeof(unsigned));
	int			ok;

	if (host == NULL)
		return 0;
	for (unsigned i = 0; i < slots; i++)
	{
		host[2 * i] = 0;
		host[2 * i + 1] = 0x7f800000u;
	}
	ok = cudaMemcpy(counters, host, slots * 2 * sizeof(unsigned), cudaMemcpyHostToDevice) == cudaSuccess;
	free(host);
	return ok;
}

/* Size this thread's scratch on the current device for slots searches. */
static Scratch *
GetScratch(int device, unsigned slots, unsigned maxBlocks)
{
	Scratch    *sc = &scratch[device];

	if (sc->slots >= slots && sc->maxBlocks >= maxBlocks)
		return sc;
	(void) cudaFree(sc->partD);
	(void) cudaFree(sc->partR);
	(void) cudaFree(sc->counters);
	(void) cudaFree(sc->outD);
	(void) cudaFree(sc->outR);
	(void) cudaFreeHost(sc->hostD);
	(void) cudaFreeHost(sc->hostR);
	memset(sc, 0, sizeof(*sc));
	slots = slots > 8 ? slots : 8;
	if (cudaMalloc((void **) &sc->partD, (size_t) slots * maxBlocks * ANN_BLACKWELL_TOP * sizeof(float)) != cudaSuccess ||
		cudaMalloc((void **) &sc->partR, (size_t) slots * maxBlocks * ANN_BLACKWELL_TOP * sizeof(unsigned)) != cudaSuccess ||
		cudaMalloc((void **) &sc->counters, slots * 2 * sizeof(unsigned)) != cudaSuccess ||
		!ResetCounters(sc->counters, slots) ||
		cudaMalloc((void **) &sc->outD, (size_t) slots * ANN_BLACKWELL_TOP * sizeof(float)) != cudaSuccess ||
		cudaMalloc((void **) &sc->outR, (size_t) slots * ANN_BLACKWELL_TOP * sizeof(unsigned)) != cudaSuccess ||
		cudaMallocHost((void **) &sc->hostD, (size_t) slots * ANN_BLACKWELL_TOP * sizeof(float)) != cudaSuccess ||
		cudaMallocHost((void **) &sc->hostR, (size_t) slots * ANN_BLACKWELL_TOP * sizeof(unsigned)) != cudaSuccess)
	{
		memset(sc, 0, sizeof(*sc));
		return NULL;
	}
	sc->slots = slots;
	sc->maxBlocks = maxBlocks;
	return sc;
}

int
AnnBlackwellSearch(const AnnBlackwellShard *shard, int device, unsigned slot, unsigned slots,
				   const uint32_t *rows, unsigned n, const float *query, int dims, void *stream)
{
	BlackwellModule *module = &modules[device];
	unsigned	maxBlocks,
				grid,
				perBlock;
	Scratch    *sc;
	float	   *partD,
			   *outD;
	unsigned   *partR,
			   *counter,
			   *outR;

	if (shard->quant == ANN_QUANT_NONE || module->state <= 0 || n == 0 || slot >= slots)
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
	if ((sc = GetScratch(device, slots, maxBlocks)) == NULL || cuCtxSetCurrent(module->context) != CUDA_SUCCESS)
		return 0;
	partD = sc->partD + (size_t) slot * sc->maxBlocks * ANN_BLACKWELL_TOP;
	partR = sc->partR + (size_t) slot * sc->maxBlocks * ANN_BLACKWELL_TOP;
	counter = sc->counters + (size_t) slot * 2;
	outD = sc->outD + (size_t) slot * ANN_BLACKWELL_TOP;
	outR = sc->outR + (size_t) slot * ANN_BLACKWELL_TOP;
	if (shard->quant == ANN_QUANT_NVFP4)
	{
		unsigned	rowBytes = shard->rowBytes,
					blocks = shard->blocks;
		void	   *args[] = {(void *) &shard->data, (void *) &shard->scales, (void *) &shard->rowScales,
			&rowBytes, &blocks, (void *) &rows, &n, &perBlock, (void *) &query, &dims, &partD, &partR,
		&counter, &outD, &outR};

		if (cuLaunchKernel(module->searchNvfp4, grid, 1, 1, ANN_BLACKWELL_BLOCK, 1, 1, 0,
						   (CUstream) stream, args, NULL) != CUDA_SUCCESS)
			return 0;
	}
	else
	{
		unsigned	rowBytes = shard->rowBytes;
		void	   *args[] = {(void *) &shard->data, (void *) &shard->rowScales, &rowBytes, (void *) &rows,
		&n, &perBlock, (void *) &query, &dims, &partD, &partR, &counter, &outD, &outR};

		if (cuLaunchKernel(module->searchFp8, grid, 1, 1, ANN_BLACKWELL_BLOCK, 1, 1, 0,
						   (CUstream) stream, args, NULL) != CUDA_SUCCESS)
			return 0;
	}
	return cudaMemcpyAsync(sc->hostD + (size_t) slot * ANN_BLACKWELL_TOP, outD, ANN_BLACKWELL_TOP * sizeof(float),
						   cudaMemcpyDeviceToHost, (cudaStream_t) stream) == cudaSuccess &&
		cudaMemcpyAsync(sc->hostR + (size_t) slot * ANN_BLACKWELL_TOP, outR, ANN_BLACKWELL_TOP * sizeof(unsigned),
						cudaMemcpyDeviceToHost, (cudaStream_t) stream) == cudaSuccess;
}

void
AnnBlackwellResults(int device, unsigned slot, const uint32_t **positions, const float **distances)
{
	*positions = scratch[device].hostR + (size_t) slot * ANN_BLACKWELL_TOP;
	*distances = scratch[device].hostD + (size_t) slot * ANN_BLACKWELL_TOP;
}
