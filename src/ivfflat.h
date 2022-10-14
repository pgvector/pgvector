#ifndef IVFFLAT_H
#define IVFFLAT_H

#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/reloptions.h"
#include "nodes/execnodes.h"
#include "utils/sampling.h"
#include "utils/tuplesort.h"
#include "vector.h"

#ifdef IVFFLAT_BENCH
#include "portability/instr_time.h"
#endif

#if PG_VERSION_NUM < 100000
#error "Requires PostgreSQL 10+"
#endif

/* Support functions */
#define IVFFLAT_DISTANCE_PROC 1
#define IVFFLAT_NORM_PROC 2
#define IVFFLAT_KMEANS_DISTANCE_PROC 3
#define IVFFLAT_KMEANS_NORM_PROC 4

#define IVFFLAT_VERSION	1
#define IVFFLAT_MAGIC_NUMBER 0x14FF1A7
#define IVFFLAT_PAGE_ID	0xFF84

/* Preserved page numbers */
#define IVFFLAT_METAPAGE_BLKNO	0
#define IVFFLAT_HEAD_BLKNO		1	/* first list page */

#define IVFFLAT_DEFAULT_LISTS	100
#define IVFFLAT_MAX_LISTS		32768

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 */
#define PROGRESS_IVFFLAT_PHASE_SAMPLE	2
#define PROGRESS_IVFFLAT_PHASE_KMEANS	3
#define PROGRESS_IVFFLAT_PHASE_SORT		4
#define PROGRESS_IVFFLAT_PHASE_LOAD		5

#define IVFFLAT_LIST_SIZE(_dim)	(offsetof(IvfflatListData, center) + VECTOR_SIZE(_dim))

#define IvfflatPageGetOpaque(page)	((IvfflatPageOpaque) PageGetSpecialPointer(page))
#define IvfflatPageGetMeta(page)	((IvfflatMetaPageData *) PageGetContents(page))

#ifdef IVFFLAT_BENCH
#define IvfflatBench(name, code) \
	do { \
		instr_time	start; \
		instr_time	duration; \
		INSTR_TIME_SET_CURRENT(start); \
		(code); \
		INSTR_TIME_SET_CURRENT(duration); \
		INSTR_TIME_SUBTRACT(duration, start); \
		elog(INFO, "%s: %.3f ms", name, INSTR_TIME_GET_MILLISEC(duration)); \
	} while (0)
#else
#define IvfflatBench(name, code) (code)
#endif

/* Variables */
extern int	ivfflat_probes;

typedef struct VectorArrayData
{
	int			length;
	int			maxlen;
	int			dim;
	Vector		items[FLEXIBLE_ARRAY_MEMBER];
}			VectorArrayData;

typedef VectorArrayData * VectorArray;

typedef struct ListInfo
{
	BlockNumber blkno;
	OffsetNumber offno;
}			ListInfo;

/* IVFFlat index options */
typedef struct IvfflatOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			lists;			/* number of lists */
}			IvfflatOptions;

typedef struct IvfflatBuildState
{
	/* Info */
	Relation	heap;
	Relation	index;
	IndexInfo  *indexInfo;

	/* Settings */
	int			dimensions;
	int			lists;

	/* Statistics */
	double		indtuples;
	double		reltuples;

	/* Support functions */
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	FmgrInfo   *kmeansnormprocinfo;
	Oid			collation;

	/* Variables */
	VectorArray samples;
	VectorArray centers;
	ListInfo   *listInfo;
	Vector	   *normvec;

#ifdef IVFFLAT_KMEANS_DEBUG
	double		inertia;
	double	   *listSums;
	int		   *listCounts;
#endif

	/* Sampling */
	BlockSamplerData bs;
	ReservoirStateData rstate;
	int			rowstoskip;

	/* Sorting */
	Tuplesortstate *sortstate;
	TupleDesc	tupdesc;
	TupleTableSlot *slot;
}			IvfflatBuildState;

typedef struct IvfflatMetaPageData
{
	uint32		magicNumber;
	uint32		version;
	uint16		dimensions;
	uint16		lists;
}			IvfflatMetaPageData;

typedef IvfflatMetaPageData * IvfflatMetaPage;

typedef struct IvfflatPageOpaqueData
{
	BlockNumber nextblkno;
	uint16		unused;
	uint16		page_id;		/* for identification of IVFFlat indexes */
}			IvfflatPageOpaqueData;

typedef IvfflatPageOpaqueData * IvfflatPageOpaque;

typedef struct IvfflatListData
{
	BlockNumber startPage;
	BlockNumber insertPage;
	Vector		center;
}			IvfflatListData;

typedef IvfflatListData * IvfflatList;

typedef struct IvfflatScanList
{
	pairingheap_node ph_node;
	BlockNumber startPage;
	double		distance;
}			IvfflatScanList;

typedef struct IvfflatScanOpaqueData
{
	int			probes;
	bool		first;
	Buffer		buf;

	/* Sorting */
	Tuplesortstate *sortstate;
	TupleDesc	tupdesc;
	TupleTableSlot *slot;
	bool		isnull;

	/* Support functions */
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	Oid			collation;

	/* Lists */
	pairingheap *listQueue;
	IvfflatScanList lists[FLEXIBLE_ARRAY_MEMBER];	/* must come last */
}			IvfflatScanOpaqueData;

typedef IvfflatScanOpaqueData * IvfflatScanOpaque;

#define VECTOR_ARRAY_SIZE(_length, _dim) (offsetof(VectorArrayData, items) + _length * VECTOR_SIZE(_dim))
#define VECTOR_ARRAY_OFFSET(_arr, _offset) ((char*) _arr + offsetof(VectorArrayData, items) + (_offset) * VECTOR_SIZE(_arr->dim))
#define VectorArrayGet(_arr, _offset) ((Vector *) VECTOR_ARRAY_OFFSET(_arr, _offset))
#define VectorArraySet(_arr, _offset, _val) (memcpy(VECTOR_ARRAY_OFFSET(_arr, _offset), _val, VECTOR_SIZE(_arr->dim)))

/* Methods */
void		_PG_init(void);
VectorArray VectorArrayInit(int maxlen, int dimensions);
void		PrintVectorArray(char *msg, VectorArray arr);
void		IvfflatKmeans(Relation index, VectorArray samples, VectorArray centers);
FmgrInfo   *IvfflatOptionalProcInfo(Relation rel, uint16 procnum);
bool		IvfflatNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, Vector * result);
int			IvfflatGetLists(Relation index);
void		IvfflatUpdateList(Relation index, GenericXLogState *state, ListInfo listInfo, BlockNumber insertPage, BlockNumber originalInsertPage, BlockNumber startPage, ForkNumber forkNum);
void		IvfflatCommitBuffer(Buffer buf, GenericXLogState *state);
void		IvfflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum);
Buffer		IvfflatNewBuffer(Relation index, ForkNumber forkNum);
void		IvfflatInitPage(Buffer buf, Page page);
void		IvfflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state);

/* Index access methods */
IndexBuildResult *ivfflatbuild(Relation heap, Relation index, IndexInfo *indexInfo);
void		ivfflatbuildempty(Relation index);
bool		ivfflatinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
						  ,bool indexUnchanged
#endif
#if PG_VERSION_NUM >= 100000
						  ,IndexInfo *indexInfo
#endif
);
IndexBulkDeleteResult *ivfflatbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state);
IndexBulkDeleteResult *ivfflatvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
IndexScanDesc ivfflatbeginscan(Relation index, int nkeys, int norderbys);
void		ivfflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool		ivfflatgettuple(IndexScanDesc scan, ScanDirection dir);
void		ivfflatendscan(IndexScanDesc scan);

#endif
