#ifndef TQHNSW_H
#define TQHNSW_H

#include "postgres.h"

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/parallel.h"
#include "access/reloptions.h"
#include "fmgr.h"
#include "lib/pairingheap.h"
#include "nodes/execnodes.h"
#include "utils/hsearch.h"
#include "storage/condition_variable.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
#include "storage/s_lock.h"
#include "storage/shm_toc.h"
#include "utils/relcache.h"
#include "utils/relptr.h"
#include "tq.h"					/* TqModel, TqEntry, TqMetric, TqTypeInfo,
								 * TqPackCode */
#include "vector.h"
#include "halfvec.h"			/* half type for fp16 rhat */

#if PG_VERSION_NUM >= 150000
#include "common/pg_prng.h"
#define TqhnswRandomDouble() pg_prng_double(&pg_global_prng_state)
#else
#define TqhnswRandomDouble() (((double) random()) / MAX_RANDOM_VALUE)
#endif

/*
 * base==NULL -> absolute pointer (serial build, on-disk insert, vacuum);
 * base!=NULL -> relptr into the shared parallel-build area.  Differs from
 * hnsw.h's HnswPtr by casting base to (char *) so relptr's byte-offset
 * arithmetic is correct regardless of the caller's base type.
 */
#define TqhnswPtrDeclare(type, relptrtype, ptrtype) \
	relptr_declare(type, relptrtype); \
	typedef union { type *ptr; relptrtype relptr; } ptrtype

typedef struct TqhnswElementData TqhnswElementData;
typedef struct TqhnswNeighborArray TqhnswNeighborArray;

TqhnswPtrDeclare(TqhnswElementData, TqhnswElementRelptr, TqhnswElementPtr);
TqhnswPtrDeclare(TqhnswNeighborArray, TqhnswNeighborArrayRelptr, TqhnswNeighborArrayPtr);
/* neighbors: relptr to an array of per-layer TqhnswNeighborArrayPtr (relptr-capable
 * so the in-memory graph is DSM-relocatable for parallel build). */
TqhnswPtrDeclare(TqhnswNeighborArrayPtr, TqhnswNeighborsRelptr, TqhnswNeighborsPtr);
/* rhat/codes: relptr-capable element payloads (DSM-relocatable for parallel build). */
TqhnswPtrDeclare(half, TqhnswRhatRelptr, TqhnswRhatPtr);
TqhnswPtrDeclare(char, TqhnswCodesRelptr, TqhnswCodesPtr);

#define TqhnswPtrAccess(base, hp) ((base) == NULL ? (hp).ptr : relptr_access((char *)(base), (hp).relptr))
#define TqhnswPtrStore(base, hp, v) ((base) == NULL ? (void) ((hp).ptr = (v)) : (void) relptr_store((char *)(base), (hp).relptr, v))
#define TqhnswPtrIsNull(base, hp) ((base) == NULL ? (hp).ptr == NULL : relptr_is_null((hp).relptr))

typedef struct TqhnswCandidate
{
	TqhnswElementPtr element;
	ItemPointerData tid;		/* disk-path: TID before element is
								 * materialized */
	double		distance;
} TqhnswCandidate;

struct TqhnswNeighborArray
{
	int			count;
	TqhnswCandidate items[FLEXIBLE_ARRAY_MEMBER];
};

#define TQHNSW_NEIGHBOR_ARRAY_SIZE(lm) \
	(offsetof(TqhnswNeighborArray, items) + sizeof(TqhnswCandidate) * (lm))

/* Limits / defaults */
#define TQHNSW_DEFAULT_M 16
#define TQHNSW_MIN_M 2
#define TQHNSW_MAX_M 100
#define TQHNSW_DEFAULT_EF_CONSTRUCTION 64
#define TQHNSW_MIN_EF_CONSTRUCTION 4
#define TQHNSW_MAX_EF_CONSTRUCTION 1000
#define TQHNSW_DEFAULT_EF_SEARCH 40
#define TQHNSW_MIN_EF_SEARCH 1
#define TQHNSW_MAX_EF_SEARCH 1000
#define TQHNSW_DEFAULT_RERANK 100
#define TQHNSW_MAX_RERANK 1000000

/*
 * Page-lock id used to serialize inserts against the graph-mutating vacuum,
 * mirroring HNSW's HNSW_UPDATE_LOCK.  Inserts hold it ShareLock for the
 * duration (ExclusiveLock when they may change the entry point) so vacuum can
 * drain in-flight inserts before repairing the graph.
 */
#define TQHNSW_UPDATE_LOCK 0

/*
 * Page-lock id used to drain in-flight scans before vacuum's MarkDeleted pass
 * reclaims tuple slots, mirroring HNSW's HNSW_SCAN_LOCK.  Scans hold it ShareLock
 * while loading graph candidates; MarkDeleted takes it ExclusiveLock to drain.
 */
#define TQHNSW_SCAN_LOCK 1

/* Page layout */
#define TQHNSW_METAPAGE_BLKNO 0
#define TQHNSW_MAGIC_NUMBER 0x71685451	/* distinct from tqflat 0x71665451 /
										 * tqivf 0x71715451 */
#define TQHNSW_VERSION 1
#define TQHNSW_PAGE_ID 0xFF94	/* distinct from tqflat 0xFF92 / tqivf 0xFF93 */

/* Tuple type tags */
#define TQHNSW_ELEMENT_TUPLE_TYPE 1
#define TQHNSW_NEIGHBOR_TUPLE_TYPE 2

/* Support function numbers (opclass FUNCTION slots) */
#define TQHNSW_DISTANCE_PROC 1	/* exact distance, used by rerank */
#define TQHNSW_NORM_PROC 2		/* l2 norm (cosine/ip parity; mirrors hnsw) */
#define TQHNSW_TYPE_INFO_PROC 3 /* tqhnsw_*_support -> TqTypeInfo.metric */

/* Per-level fanout: level 0 gets 2*m, upper levels m (HNSW convention). */
#define TqhnswGetLayerM(m, lc) ((lc) == 0 ? (m) * 2 : (m))
/* Level multiplier (HNSW paper); same as HnswGetMl. */
#define TqhnswGetMl(m) (1 / log(m))
/* Max level bounded by neighbor-tuple page capacity (mirror HnswGetMaxLevel). */
#define TqhnswGetMaxLevel(m) \
	Min(((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(TqhnswPageOpaqueData)) \
		  - offsetof(TqhnswNeighborTupleData, indextids) - sizeof(ItemIdData)) \
		 / (sizeof(ItemPointerData)) / (m)) - 2, 255)

/* GUCs */
extern int	tqhnsw_ef_search;
extern int	tqhnsw_rerank;
extern bool tqhnsw_force_scalar;

/* reloptions */
typedef struct TqhnswOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			m;				/* graph connectivity */
	int			efConstruction; /* build-time candidate list size */
	bool		fastRotation;	/* structured randomized Hadamard rotation */
} TqhnswOptions;

/* Standard page opaque (mirrors HnswPageOpaqueData / TqPageOpaqueData). */
typedef struct TqhnswPageOpaqueData
{
	BlockNumber nextblkno;
	uint16		unused;
	uint16		page_id;		/* TQHNSW_PAGE_ID */
} TqhnswPageOpaqueData;

typedef TqhnswPageOpaqueData *TqhnswPageOpaque;

#define TqhnswPageGetOpaque(page) ((TqhnswPageOpaque) PageGetSpecialPointer(page))

/*
 * Meta page. Fuses HNSW's graph header (m, efConstruction, entry point) with the
 * TQ model header (so the TqLoadModel logic is reused). Codebook (and, dense mode
 * only, rotation matrix) follow on side pages.
 */
typedef struct TqhnswMetaPageData
{
	uint32		magicNumber;
	uint32		version;
	uint16		dimensions;
	uint16		bits;			/* always 4 */
	uint16		metric;			/* TqMetric */
	uint16		fastRotation;	/* bool */
	uint16		dimPadded;		/* next_pow2(dim) in fast mode, else dim */
	uint16		m;
	uint16		efConstruction;
	uint32		nLevels;		/* 1 << bits */
	uint32		nVectors;		/* live nodes written */
	uint64		rotSeed;
	BlockNumber codebookStart;	/* codebook side-page chain */
	BlockNumber rotationStart;	/* dense mode only (Invalid in fast mode) */
	BlockNumber entryBlkno;		/* graph entry point element tuple */
	OffsetNumber entryOffno;
	int16		entryLevel;		/* -1 when empty */
	BlockNumber insertPage;		/* element-page hint for inserts */
	BlockNumber firstElementPage;	/* head of the element-page nextblkno
									 * chain (after meta + codebook/rotation
									 * side pages); vacuum walks this chain */
} TqhnswMetaPageData;

typedef TqhnswMetaPageData *TqhnswMetaPage;

#define TqhnswPageGetMeta(page) ((TqhnswMetaPage) PageGetContents(page))

/*
 * On-disk element tuple. Mirrors HnswElementTupleData but replaces the inline
 * Vector with a TQ tail (norm + scale + packed codes). neighbortid points at the
 * element's neighbor tuple. Codes length = TQ_CODES_BYTES(dimCodes, 4).
 */
typedef struct TqhnswElementTupleData
{
	uint8		type;			/* TQHNSW_ELEMENT_TUPLE_TYPE */
	uint8		level;
	uint8		deleted;		/* tombstone: set by vacuum MarkDeleted; slot
								 * reusable by insert */
	uint8		version;		/* bumped on tombstone (1..15 wrap); carried
								 * on slot reuse */
	ItemPointerData heaptid;	/* the indexed heap row (single; duplicates
								 * not merged, unlike hnsw) */
	ItemPointerData neighbortid;
	float		norm;			/* stripped L2 length */
	float		scale;			/* renormalization correction */
	char		codes[FLEXIBLE_ARRAY_MEMBER];
} TqhnswElementTupleData;

typedef TqhnswElementTupleData *TqhnswElementTuple;

#define TQHNSW_ELEMENT_TUPLE_SIZE(_codesBytes) \
	MAXALIGN(offsetof(TqhnswElementTupleData, codes) + (_codesBytes))

/*
 * On-disk neighbor tuple. Byte-identical scheme to HnswNeighborTupleData:
 * (level+2)*m item pointers, layer lc's slice starting at (level-lc)*m, level 0
 * doubled. Quantization does not touch graph edges, so this is reused verbatim.
 */
typedef struct TqhnswNeighborTupleData
{
	uint8		type;			/* TQHNSW_NEIGHBOR_TUPLE_TYPE */
	uint8		version;
	uint16		count;			/* (level+2)*m */
	ItemPointerData indextids[FLEXIBLE_ARRAY_MEMBER];
} TqhnswNeighborTupleData;

typedef TqhnswNeighborTupleData *TqhnswNeighborTuple;

#define TQHNSW_NEIGHBOR_TUPLE_SIZE(level, m) \
	MAXALIGN(offsetof(TqhnswNeighborTupleData, indextids) + ((level) + 2) * (m) * sizeof(ItemPointerData))

/* ---- tqhnsw.c ---- */
/* LWLock tranche for per-element / graph locks (parallel build). */
extern int	tqhnsw_lock_tranche_id;
extern void TqhnswInitLockTranche(void);
extern void TqhnswInit(void);
extern TqModel *TqhnswLoadModel(Relation index, MemoryContext ctx);
extern TqModel *TqhnswGetCachedModel(Relation index);
extern void TqhnswGetMetaInfo(Relation index, int *dim, TqMetric *metric, int *m,
							  BlockNumber *entryBlkno, OffsetNumber *entryOffno,
							  int *entryLevel, int *efConstruction,
							  BlockNumber *firstElementPage);

/*
 * In-memory graph node used during build.  Pointer fields are relptr-capable
 * (resolved against base: NULL serial, DSM area parallel) and lock guards the
 * neighbor arrays under parallel build.  rhat is the reconstructed rotated
 * vector used for the build-time distance (unit-normalized for cosine).
 * blkno/offno/neighborPage/neighborOffno are assigned during the flush-to-disk
 * pass.
 */
typedef struct TqhnswElementData TqhnswElement; /* keep the existing name */

struct TqhnswElementData
{
	ItemPointerData heaptid;
	uint8		level;
	uint8		version;		/* on-disk tuple version (0 fresh; carried from
								 * etup->version on load).  Written into both the
								 * element and neighbor tuples so a vacuum-repaired
								 * slot-reused element keeps the two tuple versions
								 * in sync (mirrors hnsw element->version) */
	TqhnswRhatPtr rhat;			/* reconstructed rotated vector, dimCodes
								 * halfs (fp16) */
	TqhnswCodesPtr codes;		/* packed codes (flushed to disk) */
	float		norm;
	float		scale;
	TqhnswNeighborsPtr neighbors;	/* [level+1] array of per-layer relptrs */
	/* assigned during flush */
	BlockNumber blkno;
	OffsetNumber offno;
	BlockNumber neighborPage;
	OffsetNumber neighborOffno;
	TqhnswElementPtr next;		/* build-list link (relptr-capable for DSM) */
	LWLock		lock;			/* protects this element's neighbor arrays */
};

static inline TqhnswNeighborArray *
TqhnswGetNeighbors(char *base, TqhnswElement *element, int lc)
{
	TqhnswNeighborArrayPtr *neighborList = TqhnswPtrAccess(base, element->neighbors);

	Assert(lc <= element->level);	/* neighbors[] is sized [0..level] */
	return TqhnswPtrAccess(base, neighborList[lc]);
}

typedef struct TqhnswAllocator
{
	void	   *(*alloc) (Size size, void *state);
	void	   *state;
} TqhnswAllocator;

#define TqhnswAlloc(allocator, size) ((allocator)->alloc((size), (allocator)->state))

/*
 * Relocatable graph header: owns the build-list head, the entry point, and the
 * vector counter.  Lives on the build-state stack for serial builds and in a
 * DSM segment (embedded in TqhnswShared) for parallel builds.  Mirrors hnsw's
 * HnswGraph.
 */
typedef struct TqhnswGraph
{
	slock_t		lock;			/* protects head + nVectors */
	TqhnswElementPtr head;
	int64		nVectors;

	LWLock		entryLock;
	LWLock		entryWaitLock;
	TqhnswElementPtr entryPoint;

	LWLock		allocatorLock;
	Size		memoryUsed;
	Size		memoryTotal;
	LWLock		flushLock;		/* serialize the in-memory -> on-disk flush */
	bool		flushed;		/* true once the graph has been flushed to
								 * disk */
	int64		indtuples;		/* total tuples inserted (in-memory + on-disk) */
} TqhnswGraph;

#define TQHNSW_MAX_GRAPH_MEMORY (SIZE_MAX / 2)

typedef struct TqhnswShared
{
	/* Immutable state */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isconcurrent;

	/* Worker progress */
	ConditionVariable workersdonecv;
	slock_t		mutex;
	int			nparticipantsdone;
	double		reltuples;

	/*
	 * Shared graph (the ParallelTableScanDesc is BUFFERALIGN'd after this
	 * struct)
	 */
	TqhnswGraph graphData;
} TqhnswShared;

#define ParallelTableScanFromTqhnswShared(shared) \
	(ParallelTableScanDesc) ((char *) (shared) + BUFFERALIGN(sizeof(TqhnswShared)))

typedef struct TqhnswLeader
{
	ParallelContext *pcxt;
	int			nparticipanttuplesorts;
	TqhnswShared *tqhnswshared;
	char	   *tqhnswarea;
	Snapshot	snapshot;
} TqhnswLeader;

/* ---- tqhnswutils.c ---- */
/*
 * Reconstruct rhat from codes into a float scratch, optionally cosine
 * unit-normalize, and pack to fp16 `out`.  scratch and out are both dimCodes
 * long and caller-owned.
 */
extern void TqhnswReconstructHalf(const TqModel *model, const char *codes,
								  float norm, float scale, bool normalize,
								  float *scratch /* dimCodes */ ,
								  half *out /* dimCodes */ );

/* Build-time distance on reconstructed rotated vectors (smaller = nearer). */
extern double TqhnswBuildDistance(half *a, half *b, int dc,
								  TqMetric metric);

/* Add element as a neighbor of target at layer lc, pruning target's neighbor
 * list back to lm via SelectNeighbors if it overflows (HnswUpdateConnection).
 * Replaces the pruned-out item IN PLACE; updateIdx (optional) reports -2 for
 * append or the original-order index of the replaced item. */
extern void TqhnswUpdateConnection(char *base, TqhnswElement *target,
								   TqhnswElement *element, double distance,
								   int lm, int lc, int dc, TqMetric metric,
								   int *updateIdx);

/* Insert element into the graph (paper Alg 1).  build path: base=NULL, index=NULL.
 * existing=true is used by vacuum repair: element is already in the graph and needs
 * its neighbors re-selected (skip self, widen beam, no inline reciprocal connect). */
extern void TqhnswInsertElement(char *base, Relation index, const TqModel *model,
								HTAB *cache, MemoryContext ctx,
								TqhnswElement *element, TqhnswElement *entryPoint,
								int m, int efConstruction, int dc, TqMetric metric,
								bool existing);

/*
 * Per-insert element cache entry: TID -> materialized graph node. The key
 * (tid) MUST be the first field (dynahash stores the key at offset 0).
 */
typedef struct TqhnswElementCacheEntry
{
	ItemPointerData tid;		/* hash key */
	TqhnswElement *element;
} TqhnswElementCacheEntry;

/* Create the TID->element cache used during on-disk insert search. */
extern HTAB *TqhnswCreateElementCache(MemoryContext ctx);

/* Lazily materialize a disk element (codes->rhat) into ctx, cached by TID. */
extern TqhnswElement *TqhnswLoadElement(Relation index, const TqModel *model,
										TqMetric metric, ItemPointer tid,
										MemoryContext ctx, HTAB *cache);

/* Load element's layer-lc neighbor TIDs into a TqhnswNeighborArray.  The
 * items[].element pointers start NULL and are resolved lazily in
 * TqhnswSearchLayer via TqhnswLoadElement (not here). */
extern TqhnswNeighborArray *TqhnswLoadNeighbors(Relation index, const TqModel *model,
												TqMetric metric, TqhnswElement *element,
												int lc, int m, MemoryContext ctx);

/* ---- tqhnswbuild.c ---- */
/* ---- tqhnswbuild.c (parallel worker entry point) ---- */
extern PGDLLEXPORT void TqhnswParallelBuildMain(dsm_segment *seg, shm_toc *toc);
extern int	TqhnswRandomLevel(double ml, int maxLevel);
extern IndexBuildResult *tqhnswbuild(Relation heap, Relation index, struct IndexInfo *indexInfo);
extern void tqhnswbuildempty(Relation index);
extern bool tqhnswinsert(Relation index, Datum *values, bool *isnull,
						 ItemPointer heap_tid, Relation heap,
						 IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
						 ,bool indexUnchanged
#endif
						 ,struct IndexInfo *indexInfo
);

/* ---- tqhnswscan.c ---- */
extern IndexScanDesc tqhnswbeginscan(Relation index, int nkeys, int norderbys);
extern void tqhnswrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
extern bool tqhnswgettuple(IndexScanDesc scan, ScanDirection dir);
extern void tqhnswendscan(IndexScanDesc scan);

/* ---- tqhnswvacuum.c ---- */

/*
 * Shared state threaded through the vacuum passes (RemoveHeapTids ->
 * RepairGraph -> ConfirmRepaired -> MarkDeleted).  Mirrors HnswVacuumState; the
 * single-heaptid element layout means there is no heaptids[] array to compact,
 * and highestPoint is tracked as a (blkno, offno, level) triple rather than an
 * HnswElement.
 */
typedef struct TqhnswVacuumState
{
	Relation	index;
	IndexBulkDeleteResult *stats;
	IndexBulkDeleteCallback callback;
	void	   *callback_state;
	const TqModel *model;
	TqMetric	metric;
	int			m;
	int			efConstruction;
	int			dimCodes;
	BlockNumber firstElementPage;
	HTAB	   *deleted;		/* set of dead element TIDs (key =
								 * ItemPointerData) */
	BlockNumber highestBlkno;	/* highest live element; Invalid when none */
	OffsetNumber highestOffno;
	int			highestLevel;
	BlockNumber fallbackBlkno;	/* second-highest live element; used as the
								 * replacement entry point when the highest
								 * turns out to be the current entry point */
	OffsetNumber fallbackOffno;
	int			fallbackLevel;
	MemoryContext tmpCtx;
	BufferAccessStrategy bas;
} TqhnswVacuumState;

extern IndexBulkDeleteResult *tqhnswbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
											   IndexBulkDeleteCallback callback, void *callback_state);
extern IndexBulkDeleteResult *tqhnswvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);

/* ---- tqhnswinsert.c (exposed for vacuum repair) ---- */

/* on-disk single-tuple insert; the build's out-of-memory fallback routes here */
extern void TqhnswInsertTupleOnDisk(Relation index, const TqModel *model, TqMetric metric, Datum value, ItemPointer heaptid, MemoryContext insertCtx);

typedef enum TqhnswEntryUpdate
{
	TQHNSW_ENTRY_NO_UPDATE = 0, /* leave entry point unchanged */
	TQHNSW_ENTRY_GREATER,		/* set only if newElement->level > current
								 * entry level */
	TQHNSW_ENTRY_ALWAYS			/* force entry point to newElement (may be
								 * NULL -> empty) */
} TqhnswEntryUpdate;

/* exposed for vacuum repair (full neighbor-tuple overwrite) */
extern void TqhnswSetNeighborTuple(TqhnswNeighborTuple ntup, TqhnswElement *e, int m);

/* exposed for vacuum repair (writes reciprocal edges back to the element) */
extern void TqhnswUpdateNeighborsOnDisk(Relation index, const TqModel *model,
										TqMetric metric, HTAB *cache, MemoryContext ctx,
										TqhnswElement *newElement, int m, int dc);

/* extended meta-update used by insert + vacuum */
extern void TqhnswUpdateMetaPage(Relation index, TqhnswElement *newElement,
								 TqhnswEntryUpdate entryUpdate, BlockNumber newInsertPage);

#endif							/* TQHNSW_H */
