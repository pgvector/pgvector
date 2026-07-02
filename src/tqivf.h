#ifndef TQIVF_H
#define TQIVF_H

#include "postgres.h"

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/parallel.h"	/* dsm_segment, shm_toc */
#include "access/reloptions.h"
#include "fmgr.h"
#include "lib/pairingheap.h"
#include "nodes/execnodes.h"
#include "utils/relcache.h"
#include "ivfflat.h"			/* VectorArray, IvfflatTypeInfo, ListInfo */
#include "tq.h"					/* TqModel, TqEntry, TqBlockSideRec, TqMetric,
								 * kernel */
#include "vector.h"

/* Limits / defaults */
#define TQIVF_DEFAULT_LISTS 100
#define TQIVF_MIN_LISTS 1
#define TQIVF_MAX_LISTS 32768
#define TQIVF_DEFAULT_PROBES 1
#define TQIVF_DEFAULT_RERANK 100
#define TQIVF_MAX_RERANK 1000000

/* Page layout */
#define TQIVF_METAPAGE_BLKNO 0
#define TQIVF_MAGIC_NUMBER 0x71715451	/* distinct from tqflat 0x71665451 */
#define TQIVF_VERSION 1
#define TQIVF_PAGE_ID 0xFF93	/* distinct from tqflat 0xFF92 */

/* Support function numbers (opclass FUNCTION slots) */
#define TQIVF_DISTANCE_PROC       1 /* exact distance: assign/probe + rerank */
#define TQIVF_NORM_PROC           2 /* l2_norm (ip/cosine) */
#define TQIVF_KMEANS_DISTANCE_PROC 3	/* IvfflatKmeans */
#define TQIVF_KMEANS_NORM_PROC    4 /* spherical k-means (ip/cosine) */
/* NOTE: slot 5 deliberately UNUSED so IvfflatGetTypeInfo() returns the default
 * Vector typeInfo; tqivf's own metric lookup is at slot 6. */
#define TQIVF_TYPE_INFO_PROC      6 /* tqivf_*_support → metric */

/* Iterative scan modes (mirror ivfflat) */
typedef enum
{
	TQIVF_ITERATIVE_SCAN_OFF = 0,
	TQIVF_ITERATIVE_SCAN_RELAXED
} TqivfIterativeScanMode;

/* GUCs */
extern int	tqivf_probes;
extern int	tqivf_rerank;
extern int	tqivf_iterative_scan;
extern int	tqivf_max_probes;
extern bool tqivf_force_scalar;

/* reloptions */
typedef struct TqivfOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			lists;			/* number of k-means lists */
	bool		fastRotation;	/* structured randomized Hadamard rotation */
} TqivfOptions;

/*
 * Meta page. Mirrors the model fields of TqMetaPageData (so TqLoadModel logic is
 * reused) plus IVF's `lists` and the list-directory head. The codebook (and, in
 * dense mode only, the rotation matrix) follow on side pages.
 */
typedef struct TqivfMetaPageData
{
	uint32		magicNumber;
	uint32		version;
	uint16		dimensions;
	uint16		bits;			/* always 4 */
	uint16		metric;			/* TqMetric */
	uint16		fastRotation;	/* bool */
	uint16		dimPadded;		/* next_pow2(dim) in fast mode, else dim */
	uint16		lists;			/* number of k-means lists */
	uint32		nLevels;		/* 1 << bits */
	uint32		nVectors;		/* live+tombstoned across all lists; ~4.29B
								 * cap */
	BlockNumber listStart;		/* first list-directory page */
	BlockNumber codebookStart;	/* global codebook chain */
	BlockNumber rotationStart;	/* dense mode only (Invalid in fast mode) */
	uint64		rotSeed;
	uint64		qjlSeed;		/* carried for TqModel parity; QJL unused in
								 * tqivf */
	float		qjlScale;		/* 0 (no QJL) */
} TqivfMetaPageData;

typedef TqivfMetaPageData *TqivfMetaPage;

#define TqivfPageGetMeta(page) ((TqivfMetaPage) PageGetContents(page))

/*
 * List directory record (one per list). Mirrors IvfflatListData: variable-length
 * with `center` last, so record size = offsetof(...center) + the centroid
 * type's itemSize(dim) (VECTOR_SIZE for vector, smaller for halfvec).
 * The per-list payload is the TQ chain heads instead of a single startPage.
 */
typedef struct TqivfListData
{
	BlockNumber codeStart;		/* head of this list's code-plane chain */
	BlockNumber sideStart;		/* head of this list's side chain */
	BlockNumber tailStart;		/* head of row-major insert tail (Invalid
								 * until first insert) */
	BlockNumber tailInsertPage; /* current tail append page → O(1) insert */
	uint32		blockCount;		/* full+partial blocks in this list */
	uint32		nvectors;		/* live+tombstoned members (build-time hint) */
	Vector		center;			/* full-precision, un-rotated centroid;
								 * FLEXIBLE tail */
} TqivfListData;

typedef TqivfListData *TqivfList;


/* Scan-time list candidate (ordered by centroid distance). */
typedef struct TqivfScanList
{
	pairingheap_node ph_node;
	BlockNumber codeStart;
	BlockNumber sideStart;
	BlockNumber tailStart;
	uint32		blockCount;
	uint32		nvectors;
	double		distance;		/* query→centroid distance */
} TqivfScanList;

/* ---- tqivf.c ---- */
extern void TqivfInit(void);
extern TqModel *TqivfLoadModel(Relation index, MemoryContext ctx);
extern void TqivfGetMetaInfo(Relation index, int *dim, TqMetric *metric,
							 int *lists, BlockNumber *listStart);

/* ---- tqivfbuild.c ---- */
extern IndexBuildResult *tqivfbuild(Relation heap, Relation index, struct IndexInfo *indexInfo);
extern void tqivfbuildempty(Relation index);

/* Parallel build worker entry point (looked up by name via CreateParallelContext) */
PGDLLEXPORT void TqivfParallelBuildMain(dsm_segment *seg, shm_toc *toc);
extern bool tqivfinsert(Relation index, Datum *values, bool *isnull,
						ItemPointer heap_tid, Relation heap,
						IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
						,bool indexUnchanged
#endif
						,struct IndexInfo *indexInfo
);
extern TqModel *TqivfGetCachedModel(Relation index);

/* ---- tqivfscan.c ---- */
extern IndexScanDesc tqivfbeginscan(Relation index, int nkeys, int norderbys);
extern void tqivfrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
extern bool tqivfgettuple(IndexScanDesc scan, ScanDirection dir);
extern void tqivfendscan(IndexScanDesc scan);

/* ---- tqivfvacuum.c ---- */
extern IndexBulkDeleteResult *tqivfbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
											  IndexBulkDeleteCallback callback, void *callback_state);
extern IndexBulkDeleteResult *tqivfvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);

#endif							/* TQIVF_H */
