#ifndef HNSW_H
#define HNSW_H

#include "postgres.h"

#include "access/genam.h"
#include "access/parallel.h"
#include "lib/pairingheap.h"
#include "nodes/execnodes.h"
#include "port.h"				/* for random() */
#include "utils/relptr.h"
#include "utils/sampling.h"
#include "vector.h"

#define HNSW_MAX_DIM 2000
#define HNSW_MAX_NNZ 1000

/* Support functions */
#define HNSW_DISTANCE_PROC 1
#define HNSW_NORM_PROC 2
#define HNSW_TYPE_INFO_PROC 3

#define HNSW_VERSION	1
#define HNSW_MAGIC_NUMBER 0xA953A953
#define HNSW_PAGE_ID	0xFF90

/* Preserved page numbers */
#define HNSW_METAPAGE_BLKNO	0
#define HNSW_HEAD_BLKNO		1	/* first element page */

/* Must correspond to page numbers since page lock is used */
#define HNSW_UPDATE_LOCK 	0
#define HNSW_SCAN_LOCK		1

/* HNSW parameters */
#define HNSW_DEFAULT_M	16
#define HNSW_MIN_M	2
#define HNSW_MAX_M		100
#define HNSW_DEFAULT_EF_CONSTRUCTION	64
#define HNSW_MIN_EF_CONSTRUCTION	4
#define HNSW_MAX_EF_CONSTRUCTION		1000
#define HNSW_DEFAULT_EF_SEARCH	40
#define HNSW_MIN_EF_SEARCH		1
#define HNSW_MAX_EF_SEARCH		1000

/* Tuple types */
#define HNSW_ELEMENT_TUPLE_TYPE  1
#define HNSW_NEIGHBOR_TUPLE_TYPE 2

/* Make graph robust against non-HOT updates */
#define HNSW_HEAPTIDS 10

#define HNSW_UPDATE_ENTRY_GREATER 1
#define HNSW_UPDATE_ENTRY_ALWAYS 2

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 */
#define PROGRESS_HNSW_PHASE_LOAD		2

#define HNSW_MAX_SIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(HnswPageOpaqueData)) - sizeof(ItemIdData))
#define HNSW_TUPLE_ALLOC_SIZE BLCKSZ

#define HNSW_ELEMENT_TUPLE_SIZE(size)	MAXALIGN(offsetof(HnswElementTupleData, data) + (size))
#define HNSW_NEIGHBOR_TUPLE_SIZE(level, m)	MAXALIGN(offsetof(HnswNeighborTupleData, indextids) + ((level) + 2) * (m) * sizeof(ItemPointerData))

#define HNSW_NEIGHBOR_ARRAY_SIZE(lm)	(offsetof(HnswNeighborArray, items) + sizeof(HnswCandidate) * (lm))

#define HnswPageGetOpaque(page)	((HnswPageOpaque) PageGetSpecialPointer(page))
#define HnswPageGetMeta(page)	((HnswMetaPageData *) PageGetContents(page))

#if PG_VERSION_NUM >= 150000
#define RandomDouble() pg_prng_double(&pg_global_prng_state)
#define SeedRandom(seed) pg_prng_seed(&pg_global_prng_state, seed)
#else
#define RandomDouble() (((double) random()) / MAX_RANDOM_VALUE)
#define SeedRandom(seed) srandom(seed)
#endif

#if PG_VERSION_NUM < 130000
#define list_delete_last(list) list_truncate(list, list_length(list) - 1)
#define list_sort(list, cmp) ((list) = list_qsort(list, cmp))
#endif

#define HnswIsElementTuple(tup) ((tup)->type == HNSW_ELEMENT_TUPLE_TYPE)
#define HnswIsNeighborTuple(tup) ((tup)->type == HNSW_NEIGHBOR_TUPLE_TYPE)

/* 2 * M connections for ground layer */
#define HnswGetLayerM(m, layer) (layer == 0 ? (m) * 2 : (m))

/* Optimal ML from paper */
#define HnswGetMl(m) (1 / log(m))

/* Ensure fits on page and in uint8 */
#define HnswGetMaxLevel(m) Min(((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(HnswPageOpaqueData)) - offsetof(HnswNeighborTupleData, indextids) - sizeof(ItemIdData)) / (sizeof(ItemPointerData)) / (m)) - 2, 255)

#define HnswGetValue(base, element) PointerGetDatum(HnswPtrAccess(base, (element)->value))

#if PG_VERSION_NUM < 140005
#define relptr_offset(rp) ((rp).relptr_off - 1)
#endif

/* Pointer macros */
#define HnswPtrAccess(base, hp) ((base) == NULL ? (hp).ptr : relptr_access(base, (hp).relptr))
#define HnswPtrStore(base, hp, value) ((base) == NULL ? (void) ((hp).ptr = (value)) : (void) relptr_store(base, (hp).relptr, value))
#define HnswPtrIsNull(base, hp) ((base) == NULL ? (hp).ptr == NULL : relptr_is_null((hp).relptr))
#define HnswPtrEqual(base, hp1, hp2) ((base) == NULL ? (hp1).ptr == (hp2).ptr : relptr_offset((hp1).relptr) == relptr_offset((hp2).relptr))

/* For code paths dedicated to each type */
#define HnswPtrPointer(hp) (hp).ptr
#define HnswPtrOffset(hp) relptr_offset((hp).relptr)

/* Variables */
extern int	hnsw_ef_search;
extern int	hnsw_lock_tranche_id;

typedef struct HnswElementData HnswElementData;
typedef struct HnswNeighborArray HnswNeighborArray;

#define HnswPtrDeclare(type, relptrtype, ptrtype) \
	relptr_declare(type, relptrtype); \
	typedef union { type *ptr; relptrtype relptr; } ptrtype;

/* Pointers that can be absolute or relative */
/* Use char for DatumPtr so works with Pointer */
HnswPtrDeclare(HnswElementData, HnswElementRelptr, HnswElementPtr);
HnswPtrDeclare(HnswNeighborArray, HnswNeighborArrayRelptr, HnswNeighborArrayPtr);
HnswPtrDeclare(HnswNeighborArrayPtr, HnswNeighborsRelptr, HnswNeighborsPtr);
HnswPtrDeclare(char, DatumRelptr, DatumPtr);

struct HnswElementData
{
	HnswElementPtr next;
	ItemPointerData heaptids[HNSW_HEAPTIDS];
	uint8		heaptidsLength;
	uint8		level;
	uint8		deleted;
	uint32		hash;
	HnswNeighborsPtr neighbors;
	BlockNumber blkno;
	OffsetNumber offno;
	OffsetNumber neighborOffno;
	BlockNumber neighborPage;
	DatumPtr	value;
	LWLock		lock;
};

typedef HnswElementData * HnswElement;

typedef struct HnswCandidate
{
	HnswElementPtr element;
	float		distance;
	bool		closer;
}			HnswCandidate;

struct HnswNeighborArray
{
	int			length;
	bool		closerSet;
	HnswCandidate items[FLEXIBLE_ARRAY_MEMBER];
};

typedef struct HnswPairingHeapNode
{
	pairingheap_node ph_node;
	HnswCandidate *inner;
}			HnswPairingHeapNode;

/* HNSW index options */
typedef struct HnswOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			m;				/* number of connections */
	int			efConstruction; /* size of dynamic candidate list */
}			HnswOptions;

typedef struct HnswGraph
{
	/* Graph state */
	slock_t		lock;
	HnswElementPtr head;
	double		indtuples;

	/* Entry state */
	LWLock		entryLock;
	LWLock		entryWaitLock;
	HnswElementPtr entryPoint;

	/* Allocations state */
	LWLock		allocatorLock;
	long		memoryUsed;
	long		memoryTotal;

	/* Flushed state */
	LWLock		flushLock;
	bool		flushed;
}			HnswGraph;

typedef struct HnswShared
{
	/* Immutable state */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isconcurrent;

	/* Worker progress */
	ConditionVariable workersdonecv;

	/* Mutex for mutable state */
	slock_t		mutex;

	/* Mutable state */
	int			nparticipantsdone;
	double		reltuples;
	HnswGraph	graphData;
}			HnswShared;

#define ParallelTableScanFromHnswShared(shared) \
	(ParallelTableScanDesc) ((char *) (shared) + BUFFERALIGN(sizeof(HnswShared)))

typedef struct HnswLeader
{
	ParallelContext *pcxt;
	int			nparticipanttuplesorts;
	HnswShared *hnswshared;
	Snapshot	snapshot;
	char	   *hnswarea;
}			HnswLeader;

typedef struct HnswAllocator
{
	void	   *(*alloc) (Size size, void *state);
	void	   *state;
}			HnswAllocator;

typedef struct HnswTypeInfo
{
	int			maxDimensions;
	Datum		(*normalize) (PG_FUNCTION_ARGS);
	void		(*checkValue) (Pointer v);
}			HnswTypeInfo;

typedef struct HnswBuildState
{
	/* Info */
	Relation	heap;
	Relation	index;
	IndexInfo  *indexInfo;
	ForkNumber	forkNum;
	const		HnswTypeInfo *typeInfo;

	/* Settings */
	int			dimensions;
	int			m;
	int			efConstruction;

	/* Statistics */
	double		indtuples;
	double		reltuples;

	/* Support functions */
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	Oid			collation;

	/* Variables */
	HnswGraph	graphData;
	HnswGraph  *graph;
	double		ml;
	int			maxLevel;

	/* Memory */
	MemoryContext graphCtx;
	MemoryContext tmpCtx;
	HnswAllocator allocator;

	/* Parallel builds */
	HnswLeader *hnswleader;
	HnswShared *hnswshared;
	char	   *hnswarea;
}			HnswBuildState;

typedef struct HnswMetaPageData
{
	uint32		magicNumber;
	uint32		version;
	uint32		dimensions;
	uint16		m;
	uint16		efConstruction;
	BlockNumber entryBlkno;
	OffsetNumber entryOffno;
	int16		entryLevel;
	BlockNumber insertPage;
}			HnswMetaPageData;

typedef HnswMetaPageData * HnswMetaPage;

typedef struct HnswPageOpaqueData
{
	BlockNumber nextblkno;
	uint16		unused;
	uint16		page_id;		/* for identification of HNSW indexes */
}			HnswPageOpaqueData;

typedef HnswPageOpaqueData * HnswPageOpaque;

typedef struct HnswElementTupleData
{
	uint8		type;
	uint8		level;
	uint8		deleted;
	uint8		unused;
	ItemPointerData heaptids[HNSW_HEAPTIDS];
	ItemPointerData neighbortid;
	uint16		unused2;
	Vector		data;
}			HnswElementTupleData;

typedef HnswElementTupleData * HnswElementTuple;

typedef struct HnswNeighborTupleData
{
	uint8		type;
	uint8		unused;
	uint16		count;
	ItemPointerData indextids[FLEXIBLE_ARRAY_MEMBER];
}			HnswNeighborTupleData;

typedef HnswNeighborTupleData * HnswNeighborTuple;

typedef struct HnswScanOpaqueData
{
	const		HnswTypeInfo *typeInfo;
	bool		first;
	List	   *w;
	MemoryContext tmpCtx;

	/* Support functions */
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	Oid			collation;
}			HnswScanOpaqueData;

typedef HnswScanOpaqueData * HnswScanOpaque;

typedef struct HnswVacuumState
{
	/* Info */
	Relation	index;
	IndexBulkDeleteResult *stats;
	IndexBulkDeleteCallback callback;
	void	   *callback_state;

	/* Settings */
	int			m;
	int			efConstruction;

	/* Support functions */
	FmgrInfo   *procinfo;
	Oid			collation;

	/* Variables */
	struct tidhash_hash *deleted;
	BufferAccessStrategy bas;
	HnswNeighborTuple ntup;
	HnswElementData highestPoint;

	/* Memory */
	MemoryContext tmpCtx;
}			HnswVacuumState;

/* Methods */
int			HnswGetM(Relation index);
int			HnswGetEfConstruction(Relation index);
FmgrInfo   *HnswOptionalProcInfo(Relation index, uint16 procnum);
Datum		HnswNormValue(const HnswTypeInfo * typeInfo, Oid collation, Datum value);
bool		HnswCheckNorm(FmgrInfo *procinfo, Oid collation, Datum value);
Buffer		HnswNewBuffer(Relation index, ForkNumber forkNum);
void		HnswInitPage(Buffer buf, Page page);
void		HnswInit(void);
List	   *HnswSearchLayer(char *base, Datum q, List *ep, int ef, int lc, Relation index, FmgrInfo *procinfo, Oid collation, int m, bool inserting, HnswElement skipElement);
HnswElement HnswGetEntryPoint(Relation index);
void		HnswGetMetaPageInfo(Relation index, int *m, HnswElement * entryPoint);
void	   *HnswAlloc(HnswAllocator * allocator, Size size);
HnswElement HnswInitElement(char *base, ItemPointer tid, int m, double ml, int maxLevel, HnswAllocator * alloc);
HnswElement HnswInitElementFromBlock(BlockNumber blkno, OffsetNumber offno);
void		HnswFindElementNeighbors(char *base, HnswElement element, HnswElement entryPoint, Relation index, FmgrInfo *procinfo, Oid collation, int m, int efConstruction, bool existing);
HnswCandidate *HnswEntryCandidate(char *base, HnswElement em, Datum q, Relation rel, FmgrInfo *procinfo, Oid collation, bool loadVec);
void		HnswUpdateMetaPage(Relation index, int updateEntry, HnswElement entryPoint, BlockNumber insertPage, ForkNumber forkNum, bool building);
void		HnswSetNeighborTuple(char *base, HnswNeighborTuple ntup, HnswElement e, int m);
void		HnswAddHeapTid(HnswElement element, ItemPointer heaptid);
void		HnswInitNeighbors(char *base, HnswElement element, int m, HnswAllocator * alloc);
bool		HnswInsertTupleOnDisk(Relation index, Datum value, Datum *values, bool *isnull, ItemPointer heap_tid, bool building);
void		HnswUpdateNeighborsOnDisk(Relation index, FmgrInfo *procinfo, Oid collation, HnswElement e, int m, bool checkExisting, bool building);
void		HnswLoadElementFromTuple(HnswElement element, HnswElementTuple etup, bool loadHeaptids, bool loadVec);
void		HnswLoadElement(HnswElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadVec, float *maxDistance);
void		HnswSetElementTuple(char *base, HnswElementTuple etup, HnswElement element);
void		HnswUpdateConnection(char *base, HnswElement element, HnswCandidate * hc, int lm, int lc, int *updateIdx, Relation index, FmgrInfo *procinfo, Oid collation);
void		HnswLoadNeighbors(HnswElement element, Relation index, int m);
void		HnswInitLockTranche(void);
const		HnswTypeInfo *HnswGetTypeInfo(Relation index);
PGDLLEXPORT void HnswParallelBuildMain(dsm_segment *seg, shm_toc *toc);

/* Index access methods */
IndexBuildResult *hnswbuild(Relation heap, Relation index, IndexInfo *indexInfo);
void		hnswbuildempty(Relation index);
bool		hnswinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
					   ,bool indexUnchanged
#endif
					   ,IndexInfo *indexInfo
);
IndexBulkDeleteResult *hnswbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state);
IndexBulkDeleteResult *hnswvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
IndexScanDesc hnswbeginscan(Relation index, int nkeys, int norderbys);
void		hnswrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool		hnswgettuple(IndexScanDesc scan, ScanDirection dir);
void		hnswendscan(IndexScanDesc scan);

static inline HnswNeighborArray *
HnswGetNeighbors(char *base, HnswElement element, int lc)
{
	HnswNeighborArrayPtr *neighborList = HnswPtrAccess(base, element->neighbors);

	Assert(element->level >= lc);

	return HnswPtrAccess(base, neighborList[lc]);
}

/* Hash tables */
typedef struct TidHashEntry
{
	ItemPointerData tid;
	char		status;
}			TidHashEntry;

#define SH_PREFIX tidhash
#define SH_ELEMENT_TYPE TidHashEntry
#define SH_KEY_TYPE ItemPointerData
#define SH_SCOPE extern
#define SH_DECLARE
#include "lib/simplehash.h"

typedef struct PointerHashEntry
{
	uintptr_t	ptr;
	char		status;
}			PointerHashEntry;

#define SH_PREFIX pointerhash
#define SH_ELEMENT_TYPE PointerHashEntry
#define SH_KEY_TYPE uintptr_t
#define SH_SCOPE extern
#define SH_DECLARE
#include "lib/simplehash.h"

typedef struct OffsetHashEntry
{
	Size		offset;
	char		status;
}			OffsetHashEntry;

#define SH_PREFIX offsethash
#define SH_ELEMENT_TYPE OffsetHashEntry
#define SH_KEY_TYPE Size
#define SH_SCOPE extern
#define SH_DECLARE
#include "lib/simplehash.h"

#endif
