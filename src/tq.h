#ifndef TQ_H
#define TQ_H

#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/generic_xlog.h"
#include "fmgr.h"
#include "lib/pairingheap.h"
#include "nodes/execnodes.h"
#include "utils/relcache.h"
#include "vector.h"

/* MSVC's math.h does not define M_PI without _USE_MATH_DEFINES */
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* PG 19 added a ScanOptions flags argument to table_index_fetch_begin */
#if PG_VERSION_NUM >= 190000
#define TqTableIndexFetchBegin(rel) table_index_fetch_begin((rel), 0)
#else
#define TqTableIndexFetchBegin(rel) table_index_fetch_begin(rel)
#endif

/* Limits / defaults */
#define TQ_MAX_DIM 16000
#define TQ_MIN_BITS 2
#define TQ_MAX_BITS 4
#define TQ_DEFAULT_BITS 4
#define TQ_DEFAULT_TQPROD false /* QJL stage off by default (mse estimator
								 * wins on recall + IP accuracy on real data) */
#define TQ_DEFAULT_FAST_ROTATION true	/* structured RHT rotation by default */
#define TQ_DEFAULT_RERANK 100
#define TQ_MAX_RERANK 1000000
#define TQ_ROTATION_SEED 42
#define TQ_QJL_SEED 1042		/* QJL projection seed (distinct from
								 * rotation) */

/* Page layout */
#define TQ_METAPAGE_BLKNO 0
#define TQ_MAGIC_NUMBER 0x71665451	/* magic for tqflat meta page */
#define TQ_VERSION 1
#define TQ_PAGE_ID 0xFF92		/* for identification of tqflat indexes */

/*
 * Standard page opaque (special space) for every tqflat page.  Mirrors
 * IvfflatPageOpaqueData: a singly linked list of pages via nextblkno, with a
 * page_id for sanity checks.
 */
typedef struct TqPageOpaqueData
{
	BlockNumber nextblkno;
	uint16		unused;
	uint16		page_id;		/* for identification of tqflat indexes */
} TqPageOpaqueData;

typedef TqPageOpaqueData *TqPageOpaque;

#define TqPageGetOpaque(page)	((TqPageOpaque) PageGetSpecialPointer(page))

/* Metric enum (matches opclass) */
typedef enum
{
	TQ_METRIC_L2 = 0,
	TQ_METRIC_IP = 1,
	TQ_METRIC_COSINE = 2
} TqMetric;

/* Support function numbers (opclass FUNCTION slots) */
#define TQ_DISTANCE_PROC   1	/* exact distance, used by rerank */
#define TQ_NORM_PROC       2	/* norm (l2_normalize); declared in the cosine
								 * opclass for parity with hnsw but unused at
								 * runtime -- the cosine path uses the stored
								 * entry->norm for the estimate and the
								 * typeInfo->normalize vtable entry (not this
								 * proc slot) for exact rerank. */
#define TQ_TYPE_INFO_PROC  5	/* tqflat_support */

/* GUC */
extern int	tqflat_rerank;
extern bool tqflat_force_scalar;

/* reloptions */
typedef struct TqOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			bits;			/* bits per coordinate (2..4) */
	bool		tqProd;			/* enable 1-bit QJL residual stage */
	bool		fastRotation;	/* use structured randomized Hadamard rotation */
} TqOptions;

/*
 * Meta page contents. The codebook (and, in dense mode only, the rotation and
 * QJL matrices) follow on subsequent pages (see tqbuild.c); only the fixed-size
 * header lives here.  In fast_rotation mode the rotation/QJL are regenerated
 * from seeds, so no matrix side pages are written.
 */
typedef struct TqMetaPageData
{
	uint32		magicNumber;
	uint32		version;
	uint16		dimensions;
	uint16		bits;
	uint16		metric;			/* TqMetric */
	uint16		tqProd;			/* bool */
	uint16		fastRotation;	/* bool: structured RHT rotation/QJL */
	uint16		dimPadded;		/* next_pow2(dim); working dim in fast mode */
	uint32		nLevels;		/* 1 << bits */
	uint32		nVectors;		/* live + tombstoned entries written; this
								 * uint32 caps a tqflat index at 2^32-1 (~4.29
								 * billion) vectors */
	BlockNumber rotationStart;	/* first page of rotation matrix (Invalid in
								 * fast mode) */
	BlockNumber codebookStart;	/* first page of codebook */
	BlockNumber qjlStart;		/* first page of QJL matrix (Invalid if
								 * !tqProd or fast mode) */
	BlockNumber dataStart;		/* first data page */
	BlockNumber codeStart;		/* first page of the code-plane chain */
	BlockNumber sideStart;		/* first page of the side-data chain */
	BlockNumber tailStart;		/* first page of the row-major insert tail
								 * (Invalid until first insert) */
	uint16		blockWidth;		/* TQ_BLOCK_WIDTH at build time */
	uint32		blockCount;		/* number of full+partial blocks written */
	float		qjlScale;		/* QJL estimator constant (0 if !tqProd) */
} TqMetaPageData;

typedef TqMetaPageData *TqMetaPage;

#define TqPageGetMeta(page) ((TqMetaPage) PageGetContents(page))

/*
 * In-memory descriptor loaded once per build/scan from the meta + side pages.
 * Owned by the caller's memory context.
 */
typedef struct TqModel
{
	int			dim;
	int			bits;
	int			nLevels;		/* 1 << bits */
	TqMetric	metric;
	bool		tqProd;
	bool		fastRotation;
	int			dimPadded;		/* next_pow2(dim) in fast mode, else dim */
	int			dimCodes;		/* coords quantized: dimPadded (fast) or dim */
	float	   *rotation;		/* dim*dim, row-major */
	float	   *boundaries;		/* nLevels-1 decision boundaries */
	float	   *centroids;		/* nLevels centroid values */
	float	   *qjl;			/* dim*dim QJL matrix (NULL if !tqProd) */
	float		qjlScale;
	uint64		rotSeed;		/* rotation RHT seed (TQ_ROTATION_SEED by
								 * default) */
	uint64		qjlSeed;		/* QJL sketch seed (TQ_QJL_SEED by default) */
} TqModel;

/*
 * One packed entry per heap tuple. Variable-length tail laid out as:
 *   [codes: dimCodes*bits/8 bytes][qjlSigns: dimCodes/8 bytes if tqProd]
 * where dimCodes = dim in dense mode and next_pow2(dim) in fast_rotation mode.
 */
typedef struct TqEntry
{
	ItemPointerData heaptid;
	uint8		deleted;		/* tombstone flag */
	uint8		unused;
	float		norm;			/* stripped L2 length */
	float		scale;			/* renormalization correction */
	float		residualNorm;	/* QJL residual norm (0 if !tqProd) */
	/* followed by packed codes (+ qjl signs) */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} TqEntry;

/* byte sizes */
#define TQ_CODES_BYTES(dim, bits) (((dim) * (bits) + 7) / 8)
#define TQ_SIGNS_BYTES(dim) (((dim) + 7) / 8)
static inline Size
TqEntrySize(int dim, int bits, bool tqProd)
{
	Size		s = offsetof(TqEntry, data) + TQ_CODES_BYTES(dim, bits);

	if (tqProd)
		s += TQ_SIGNS_BYTES(dim);
	return MAXALIGN(s);
}

/*
 * TqPackCode -- store the bits-wide code at index idx into the codes buffer.
 * Codes are packed sequentially: code idx occupies bits [idx*bits, idx*bits+bits).
 * The high byte of the 2-byte window is only written when it falls within the
 * buffer, so no guard byte beyond codesBytes is needed or stored.
 */
static inline void
TqPackCode(char *codes, int codesBytes, int idx, int bits, uint8 code)
{
	int			bitpos = idx * bits;
	int			byte = bitpos >> 3;
	int			off = bitpos & 7;
	uint16		cur;

	cur = (uint16) ((uint8) codes[byte]);
	if (byte + 1 < codesBytes)
		cur |= ((uint16) ((uint8) codes[byte + 1])) << 8;
	cur &= ~(((uint16) ((1 << bits) - 1)) << off);
	cur |= ((uint16) code) << off;
	codes[byte] = (char) (cur & 0xFF);
	if (byte + 1 < codesBytes)
		codes[byte + 1] = (char) (cur >> 8);
}

/*
 * TqUnpackCode -- extract the bits-wide code stored at index idx.
 * codesBytes is TQ_CODES_BYTES(dim, bits); the high byte of the 2-byte window
 * is only read when it falls within the buffer, making the read safe for
 * on-disk entries where no guard byte is stored beyond codesBytes.
 */
static inline uint8
TqUnpackCode(const char *codes, int codesBytes, int idx, int bits)
{
	int			bitpos = idx * bits;
	int			byte = bitpos >> 3;
	int			off = bitpos & 7;
	uint16		cur;

	cur = (uint16) ((uint8) codes[byte]);
	if (byte + 1 < codesBytes)
		cur |= ((uint16) ((uint8) codes[byte + 1])) << 8;
	return (uint8) ((cur >> off) & ((1 << bits) - 1));
}

/* Blocked fast-scan layout. */
#define TQ_BLOCK_WIDTH 32		/* vectors per block (fixed SIMD width) */
#define TQ_LUT_FLUSH 128		/* coords between uint16->uint32 accumulator
								 * flushes */

/* Per-lane side data, parallel to a block's code-plane (never read by the kernel). */
typedef struct TqBlockSide
{
	ItemPointerData heaptid;
	float		norm;			/* stripped L2 length */
	float		scale;			/* renormalization correction */
} TqBlockSide;

/* One side-chain record per block: tombstone mask + live count + 32 side records. */
typedef struct TqBlockSideRec
{
	uint32		deletedMask;	/* bit j set => lane j tombstoned */
	uint16		nvecs;			/* live+tombstoned lanes populated (1..32) */
	uint16		pad;
	TqBlockSide side[TQ_BLOCK_WIDTH];
} TqBlockSideRec;

/* Code-plane byte size for one block at dimCodes dc (4-bit nibble layout). */
#define TQ_BLOCK_CODE_BYTES(dc) ((Size) (dc) * 16)

/* Kernel accumulator: 32 lanes, with a uint16 stage flushed to uint32. */
typedef struct TqBlockAccum
{
	uint16		acc16[TQ_BLOCK_WIDTH];
	uint32		acc32[TQ_BLOCK_WIDTH];
	int			sinceFlush;		/* coords accumulated since last flush */
} TqBlockAccum;

/* Type-info vtable returned by each opclass's type-info support proc. */
typedef struct TqTypeInfo
{
	TqMetric	metric;
	int			maxDimensions;	/* reject declared dim above this */

	/*
	 * Produce a dense float array of length `dim` from a column Datum.
	 *   vector:    returns ->x directly (scratch unused -- zero-copy)
	 *   halfvec:   HalfToFloat4 per coord into scratch, returns scratch
	 *   sparsevec: zeroes scratch, scatters nonzeros, returns scratch
	 */
	const float *(*toFloat) (Datum value, float *scratch, int dim);

	/*
	 * Type-specific l2_normalize (returns a normalized Datum of the same
	 * type). Used for cosine on the native-Datum exact-rerank path and the
	 * rescan query-normalize. NULL for non-cosine opclasses (only the cosine
	 * vtables set it). Mirrors HnswTypeInfo.normalize.
	 */
	PGFunction	normalize;
} TqTypeInfo;

/* ---- tqtypeinfo.c ---- */
extern const float *TqVectorToFloat(Datum value, float *scratch, int dim);
extern const float *TqHalfvecToFloat(Datum value, float *scratch, int dim);
extern const float *TqSparsevecToFloat(Datum value, float *scratch, int dim);
extern const TqTypeInfo *TqGetTypeInfo(Relation index, int procnum);
extern const float *TqExtractForEncode(const TqTypeInfo *ti, Datum value,
									   TqMetric metric, float *scratch, int dim);

/* ---- tqfwht.c ---- */
extern int	TqNextPow2(int n);
extern void TqFwht(double *x, int n);
extern void TqApplyRht(uint64 seed, int nstages, int dPadded,
					   const float *in, int d, float *out);

/* Number of (sign-flip, Hadamard) rounds in the RHT (near-Haar mixing). */
#define TQ_RHT_STAGES 3

/* ---- tqquant.c ---- */
extern void TqL2NormalizeFloat(float *v, int dim);
extern bool TqCheckNorm(const float *v, int dim);
extern TqModel *TqAllocModel(MemoryContext ctx, int dim, int nLevels, bool dense, bool withQjl);
extern void TqBuildCodebook(int dim, int bits, float *boundaries, float *centroids);
extern void TqBuildRotation(int dim, uint64 seed, float *rotation);
extern void TqBuildQjl(int dim, uint64 seed, float *qjl);
extern void TqEncode(const TqModel *model, const float *vec, TqEntry *entry);
extern void TqBuildQueryLut(const TqModel *model, const float *query,
							float *lut /* dim*nLevels */ , float *qjlQuery /* dim or NULL */ );

/* ---- tqfastscan.c ---- */
/* Reset an accumulator to zero before scoring a block. */
extern void TqBlockAccumInit(TqBlockAccum *acc);

/* Fold coords [c0,c1) of one block's code-plane into acc (function pointer;
 * Default/Neon/Avx512 set by TqInitDispatch). codeRun points at the byte for
 * coord c0 (i.e. codePlane + c0*16). lut8 is the full dc*nLevels 8-bit LUT. */
extern void (*TqScoreBlockRange) (const uint8 *lut8, const uint8 *codeRun,
								  int c0, int c1, TqBlockAccum *acc);

/* Finalize: flush the uint16 stage into acc32 (call after the last range). */
extern void TqBlockAccumFinish(TqBlockAccum *acc);

/* Build the 8-bit per-query LUT + affine recovery constants from the float LUT. */
extern void TqBuildLut8(const TqModel *model, const float *lut,
						uint8 *lut8, float *lutBias, float *lutScale);

/* Scatter one vector's row-major packed codes into a block code-plane at `slot`. */
extern void TqScatterCodes(const TqModel *model, const char *codes,
						   int slot, uint8 *codePlane);

/* Scalar reference range kernel (also the Default pointer target). */
extern void TqScoreBlockRangeDefault(const uint8 *lut8, const uint8 *codeRun,
									 int c0, int c1, TqBlockAccum *acc);

/* NEON range kernel (arm64 only; selected by TqInitDispatch when available). */
extern void TqScoreBlockRangeNeon(const uint8 *lut8, const uint8 *codeRun,
								  int c0, int c1, TqBlockAccum *acc);

/* AVX-512F+BW range kernel (x86-64 only; selected by TqInitDispatch when detected). */
#if defined(__x86_64__)
extern bool TqSupportsAvx512(void);
extern void TqScoreBlockRangeAvx512(const uint8 *lut8, const uint8 *codeRun,
									int c0, int c1, TqBlockAccum *acc);
#endif

/* ---- tqdistance.c ---- */
extern void TqInitDispatch(void);

/*
 * Scoring kernel selected at load time (TqInitDispatch).  Returns an estimate
 * of the raw inner product <q, x>; metric->distance conversion happens in the
 * scan, not here.  Function-pointer indirection mirrors halfutils.c so SIMD
 * variants can be added later.
 */
extern float (*TqScoreEntry) (const TqModel *model, const float *lut,
							  const float *qjlQuery, const TqEntry *entry,
							  const char *codes);

/* ---- tq.c ---- */
extern void TqInit(void);
extern TqModel *TqLoadModel(Relation index, MemoryContext ctx);
extern void TqGetMetaInfo(Relation index, int *dim, int *bits, TqMetric *metric, bool *tqProd);

/* ---- tqbuild.c ---- */
extern IndexBuildResult *tqbuild(Relation heap, Relation index, struct IndexInfo *indexInfo);
extern void tqbuildempty(Relation index);
extern bool tqinsert(Relation index, Datum *values, bool *isnull,
					 ItemPointer heap_tid, Relation heap,
					 IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
					 ,bool indexUnchanged
#endif
					 ,struct IndexInfo *indexInfo
);
extern TqModel *TqGetCachedModel(Relation index);
extern void TqReadBytes(Relation index, BlockNumber startBlock, char *dest, Size nbytes);

/*
 * Forward-only cursor over a linked page chain of raw bytes; lets scans read
 * the code plane block-by-block instead of buffering the whole chain.
 */
typedef struct TqByteStream
{
	Relation	index;
	BlockNumber blkno;
	OffsetNumber offno;
	Size		itemOff;		/* byte offset within the current item */
} TqByteStream;

extern void TqByteStreamInit(TqByteStream *bs, Relation index, BlockNumber startBlock);
extern void TqByteStreamRead(TqByteStream *bs, char *dest, Size len);

/*
 * Build-time page helpers, exported for reuse by the tqivf access method.
 * These operate on a (Relation, ForkNumber) page chain and are independent of
 * any AM-specific build state.
 */
extern Buffer TqNewBuffer(Relation index, ForkNumber forkNum);
extern void TqInitPage(Buffer buf, Page page, uint16 pageId);
extern void TqInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, uint16 pageId);
extern void TqCommitBuffer(Buffer buf, GenericXLogState *state);
extern void TqAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum, uint16 pageId);
extern Size TqPageCapacity(void);
extern BlockNumber TqWriteBytes(Relation index, ForkNumber forkNum, const char *bytes, Size nbytes, uint16 pageId);

/* ---- tqscan.c ---- */
extern IndexScanDesc tqbeginscan(Relation index, int nkeys, int norderbys);
extern void tqrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
extern bool tqgettuple(IndexScanDesc scan, ScanDirection dir);
extern void tqendscan(IndexScanDesc scan);

/* ---- tqvacuum.c ---- */
extern IndexBulkDeleteResult *tqbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
										   IndexBulkDeleteCallback callback, void *callback_state);
extern IndexBulkDeleteResult *tqvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);

#endif							/* TQ_H */
