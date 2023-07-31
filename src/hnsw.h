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
#define HNSW_PAGE_ID	0xFF85

/* Preserved page numbers */
#define HNSW_METAPAGE_BLKNO	0
#define HNSW_HEAD_BLKNO		1	/* first element page */

#define HNSW_DEFAULT_M	16
#define HNSW_MIN_M	4
#define HNSW_MAX_M		100
#define HNSW_DEFAULT_EF_CONSTRUCTION	40
#define HNSW_MIN_EF_CONSTRUCTION	10
#define HNSW_MAX_EF_CONSTRUCTION		1000
#define HNSW_DEFAULT_EF_SEARCH	40
#define HNSW_MIN_EF_SEARCH		10
#define HNSW_MAX_EF_SEARCH		1000

#define HNSW_HEAPTIDS 10

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 */
#define PROGRESS_HNSW_PHASE_LOAD		2

#define HNSW_ELEMENT_TUPLE_SIZE(_dim)	(offsetof(HnswElementTupleData, vec) + VECTOR_SIZE(_dim))

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

#define GetLayerM(m, layer) (layer == 0 ? m * 2 : m)
#define HnswGetMl(m) (1 / log(m))

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

typedef struct HnswUpdate
{
	HnswCandidate hc;
	int			level;
	int			index;
}			HnswUpdate;

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
	uint32		m;
	uint32		efConstruction;
	BlockNumber entryBlkno;
	OffsetNumber entryOffno;
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
	ItemPointerData heaptids[HNSW_HEAPTIDS];
	uint8		level;
	uint8		deleted;
	uint16		unused;
	BlockNumber neighborPage;
	Vector		vec;
}			HnswElementTupleData;

typedef HnswElementTupleData * HnswElementTuple;

typedef struct HnswNeighborTupleData
{
	ItemPointerData indextid;
	uint16		unused;
	float		distance;
}			HnswNeighborTupleData;

typedef HnswNeighborTupleData * HnswNeighborTuple;

typedef struct HnswScanOpaqueData
{
	bool		first;
	Buffer		buf;
	List	   *w;

	/* Support functions */
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	Oid			collation;
}			HnswScanOpaqueData;

typedef HnswScanOpaqueData * HnswScanOpaque;

typedef struct HnswVacuumState
{
	Relation	index;
	IndexBulkDeleteResult *stats;
	IndexBulkDeleteCallback callback;
	void	   *callback_state;
	int			m;
	int			efConstruction;
	HTAB	   *deleted;
	BufferAccessStrategy bas;
	FmgrInfo   *procinfo;
	Oid			collation;
	HnswNeighborTuple ntup;
	Size		nsize;
	HnswElementData highestPoint;
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
List	   *SearchLayer(Datum q, List *ep, int ef, int lc, Relation index, FmgrInfo *procinfo, Oid collation, bool inserting, BlockNumber *skipPage);
HnswElement GetEntryPoint(Relation index);
HnswElement HnswInitElement(ItemPointer tid, int m, double ml, int maxLevel);
void		HnswFreeElement(HnswElement element);
HnswElement HnswInsertElement(HnswElement element, HnswElement entryPoint, Relation index, FmgrInfo *procinfo, Oid collation, int m, int efConstruction, List **updates, bool vacuuming);
HnswCandidate *EntryCandidate(HnswElement em, Datum q, Relation rel, FmgrInfo *procinfo, Oid collation, bool loadvec);
void		UpdateMetaPage(Relation index, bool updateEntry, HnswElement entryPoint, BlockNumber insertPage, ForkNumber forkNum);
void		AddNeighborsToPage(Relation index, Page page, HnswElement e, HnswNeighborTuple neighbor, Size neighborsz, int m);
void		HnswAddHeapTid(HnswElement element, ItemPointer heaptid);
void		HnswInitNeighbors(HnswElement element, int m);
bool		HnswInsertTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel);
void		HnswLoadElement(HnswElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadvec);
void		HnswSetElementTuple(HnswElementTuple etup, HnswElement element);

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

/* Ensure fits in uint8 */
#define HnswGetMaxLevel(m) Min(((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(HnswPageOpaqueData))) / (MAXALIGN(sizeof(HnswNeighborTupleData)) + sizeof(ItemIdData)) / m) - 2, 255)

#endif
