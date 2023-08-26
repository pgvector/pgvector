#ifndef HNSW_H
#define HNSW_H

#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/reloptions.h"
#include "nodes/execnodes.h"
#include "port.h"				/* for random() */
#include "utils/sampling.h"
#include "vector.h"

#if PG_VERSION_NUM < 110000
#error "Requires PostgreSQL 11+"
#endif

#define HNSW_MAX_DIM 2000

/* Support functions */
#define HNSW_DISTANCE_PROC 1
#define HNSW_NORM_PROC 2

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

#define HNSW_ELEMENT_TUPLE_SIZE(_dim)	MAXALIGN(offsetof(HnswElementTupleData, vec) + VECTOR_SIZE(_dim))
#define HNSW_NEIGHBOR_TUPLE_SIZE(level, m)	MAXALIGN(offsetof(HnswNeighborTupleData, indextids) + ((level) + 2) * (m) * sizeof(ItemPointerData))

#define HnswPageGetOpaque(page)	((HnswPageOpaque) PageGetSpecialPointer(page))
#define HnswPageGetMeta(page)	((HnswMetaPageData *) PageGetContents(page))

#if PG_VERSION_NUM >= 150000
#define RandomDouble() pg_prng_double(&pg_global_prng_state)
#else
#define RandomDouble() (((double) random()) / MAX_RANDOM_VALUE)
#endif

#if PG_VERSION_NUM < 130000
#define list_delete_last(list) list_truncate(list, list_length(list) - 1)
#define list_sort(list, cmp) list_qsort(list, cmp)
#endif

#define HnswIsElementTuple(tup) ((tup)->type == HNSW_ELEMENT_TUPLE_TYPE)
#define HnswIsNeighborTuple(tup) ((tup)->type == HNSW_NEIGHBOR_TUPLE_TYPE)

/* 2 * M connections for ground layer */
#define HnswGetLayerM(m, layer) (layer == 0 ? (m) * 2 : (m))

/* Optimal ML from paper */
#define HnswGetMl(m) (1 / log(m))

/* Ensure fits on page and in uint8 */
#define HnswGetMaxLevel(m) Min(((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(HnswPageOpaqueData)) - offsetof(HnswNeighborTupleData, indextids) - sizeof(ItemIdData)) / (sizeof(ItemPointerData)) / m) - 2, 255)

/* Variables */
extern int	hnsw_ef_search;

typedef struct HnswNeighborArray HnswNeighborArray;

typedef struct HnswElementData
{
	List	   *heaptids;
	uint8		level;
	uint8		deleted;
	HnswNeighborArray *neighbors;
	BlockNumber blkno;
	OffsetNumber offno;
	OffsetNumber neighborOffno;
	BlockNumber neighborPage;
	Vector	   *vec;
}			HnswElementData;

typedef HnswElementData * HnswElement;

typedef struct HnswCandidate
{
	HnswElement element;
	float		distance;
}			HnswCandidate;

typedef struct HnswNeighborArray
{
	int			length;
	HnswCandidate *items;
}			HnswNeighborArray;

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

typedef struct HnswBuildState
{
	/* Info */
	Relation	heap;
	Relation	index;
	IndexInfo  *indexInfo;
	ForkNumber	forkNum;

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
	List	   *elements;
	HnswElement entryPoint;
	double		ml;
	int			maxLevel;
	double		maxInMemoryElements;
	bool		flushed;
	Vector	   *normvec;

	/* Memory */
	MemoryContext tmpCtx;
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
	Vector		vec;
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
	bool		first;
	Buffer		buf;
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
	HTAB	   *deleted;
	BufferAccessStrategy bas;
	HnswNeighborTuple ntup;
	HnswElementData highestPoint;

	/* Memory */
	MemoryContext tmpCtx;
}			HnswVacuumState;

/* Methods */
int			HnswGetM(Relation index);
int			HnswGetEfConstruction(Relation index);
FmgrInfo   *HnswOptionalProcInfo(Relation rel, uint16 procnum);
bool		HnswNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, Vector * result);
void		HnswCommitBuffer(Buffer buf, GenericXLogState *state);
Buffer		HnswNewBuffer(Relation index, ForkNumber forkNum);
void		HnswInitPage(Buffer buf, Page page);
void		HnswInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state);
void		HnswInit(void);
List	   *HnswSearchLayer(Datum q, List *ep, int ef, int lc, Relation index, FmgrInfo *procinfo, Oid collation, bool inserting, HnswElement skipElement);
HnswElement HnswGetEntryPoint(Relation index);
HnswElement HnswInitElement(ItemPointer tid, int m, double ml, int maxLevel);
void		HnswFreeElement(HnswElement element);
HnswElement HnswInitElementFromBlock(BlockNumber blkno, OffsetNumber offno);
void		HnswInsertElement(HnswElement element, HnswElement entryPoint, Relation index, FmgrInfo *procinfo, Oid collation, int m, int efConstruction, bool existing);
HnswElement HnswFindDuplicate(HnswElement e);
HnswCandidate *HnswEntryCandidate(HnswElement em, Datum q, Relation rel, FmgrInfo *procinfo, Oid collation, bool loadVec);
void		HnswUpdateMetaPage(Relation index, int updateEntry, HnswElement entryPoint, BlockNumber insertPage, ForkNumber forkNum);
void		HnswSetNeighborTuple(HnswNeighborTuple ntup, HnswElement e, int m);
void		HnswAddHeapTid(HnswElement element, ItemPointer heaptid);
void		HnswInitNeighbors(HnswElement element, int m);
bool		HnswInsertTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel);
void		HnswUpdateNeighborPages(Relation index, FmgrInfo *procinfo, Oid collation, HnswElement e, int m, bool checkExisting);
void		HnswLoadElementFromTuple(HnswElement element, HnswElementTuple etup, bool loadHeaptids, bool loadVec);
void		HnswLoadElement(HnswElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadVec);
void		HnswSetElementTuple(HnswElementTuple etup, HnswElement element);
void		HnswUpdateConnection(HnswElement element, HnswCandidate * hc, int m, int lc, int *updateIdx, Relation index, FmgrInfo *procinfo, Oid collation);
void		HnswLoadNeighbors(HnswElement element, Relation index);

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

#endif
