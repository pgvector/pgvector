#ifndef IVFFLAT_H
#define IVFFLAT_H

#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/parallel.h"
#include "access/reloptions.h"
#include "nodes/execnodes.h"
#include "port.h"				/* for random() */
#include "utils/sampling.h"
#include "utils/tuplesort.h"
#include "vector.h"

#if PG_VERSION_NUM >= 150000
#include "common/pg_prng.h"
#endif

#if PG_VERSION_NUM < 120000
#include "access/relscan.h"
#endif

#ifdef IVFFLAT_BENCH
#include "portability/instr_time.h"
#endif

#define IVFFLAT_MAX_DIM 2000

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

/* IVFFlat parameters */
#define IVFFLAT_DEFAULT_LISTS	100
#define IVFFLAT_MIN_LISTS		1
#define IVFFLAT_MAX_LISTS		32768
#define IVFFLAT_DEFAULT_PROBES	1

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 */
#define PROGRESS_IVFFLAT_PHASE_KMEANS	2
#define PROGRESS_IVFFLAT_PHASE_ASSIGN	3
#define PROGRESS_IVFFLAT_PHASE_LOAD		4

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

#if PG_VERSION_NUM >= 150000
#define RandomDouble() pg_prng_double(&pg_global_prng_state)
#define RandomInt() pg_prng_uint32(&pg_global_prng_state)
#else
#define RandomDouble() (((double) random()) / MAX_RANDOM_VALUE)
#define RandomInt() random()
#endif

/* Variables */
extern int	ivfflat_probes;

typedef struct VectorArrayData
{
	int			length;
	int			maxlen;
	int			dim;
	Vector	   *items;
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

typedef struct IvfflatSpool
{
	Tuplesortstate *sortstate;
	Relation	heap;
	Relation	index;
}			IvfflatSpool;

typedef struct IvfflatShared
{
	/* Immutable state */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isconcurrent;
	int			scantuplesortstates;

	/* Worker progress */
	ConditionVariable workersdonecv;

	/* Mutex for mutable state */
	slock_t		mutex;

	/* Mutable state */
	int			nparticipantsdone;
	double		reltuples;
	double		indtuples;

#ifdef IVFFLAT_KMEANS_DEBUG
	double		inertia;
#endif

#if PG_VERSION_NUM < 120000
	ParallelHeapScanDescData heapdesc;	/* must come last */
#endif
}			IvfflatShared;

#if PG_VERSION_NUM >= 120000
#define ParallelTableScanFromIvfflatShared(shared) \
	(ParallelTableScanDesc) ((char *) (shared) + BUFFERALIGN(sizeof(IvfflatShared)))
#endif

typedef struct IvfflatLeader
{
	ParallelContext *pcxt;
	int			nparticipanttuplesorts;
	IvfflatShared *ivfshared;
	Sharedsort *sharedsort;
	Snapshot	snapshot;
	Vector	   *ivfcenters;
}			IvfflatLeader;

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

	/* Memory */
	MemoryContext tmpCtx;

	/* Parallel builds */
	IvfflatLeader *ivfleader;
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

#define VECTOR_ARRAY_SIZE(_length, _dim) (sizeof(VectorArrayData) + (_length) * VECTOR_SIZE(_dim))
#define VECTOR_ARRAY_OFFSET(_arr, _offset) ((char*) (_arr)->items + (_offset) * VECTOR_SIZE((_arr)->dim))
#define VectorArrayGet(_arr, _offset) ((Vector *) VECTOR_ARRAY_OFFSET(_arr, _offset))
#define VectorArraySet(_arr, _offset, _val) memcpy(VECTOR_ARRAY_OFFSET(_arr, _offset), _val, VECTOR_SIZE((_arr)->dim))

/* Methods */
VectorArray VectorArrayInit(int maxlen, int dimensions);
void		VectorArrayFree(VectorArray arr);
void		PrintVectorArray(char *msg, VectorArray arr);
void		IvfflatKmeans(Relation index, VectorArray samples, VectorArray centers);
FmgrInfo   *IvfflatOptionalProcInfo(Relation rel, uint16 procnum);
bool		IvfflatNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, Vector * result);
int			IvfflatGetLists(Relation index);
void		IvfflatUpdateList(Relation index, ListInfo listInfo, BlockNumber insertPage, BlockNumber originalInsertPage, BlockNumber startPage, ForkNumber forkNum);
void		IvfflatCommitBuffer(Buffer buf, GenericXLogState *state);
void		IvfflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum);
Buffer		IvfflatNewBuffer(Relation index, ForkNumber forkNum);
void		IvfflatInitPage(Buffer buf, Page page);
void		IvfflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state);
void		IvfflatInit(void);
PGDLLEXPORT void IvfflatParallelBuildMain(dsm_segment *seg, shm_toc *toc);

/* Index access methods */
IndexBuildResult *ivfflatbuild(Relation heap, Relation index, IndexInfo *indexInfo);
void		ivfflatbuildempty(Relation index);
bool		ivfflatinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
						  ,bool indexUnchanged
#endif
						  ,IndexInfo *indexInfo
);
IndexBulkDeleteResult *ivfflatbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state);
IndexBulkDeleteResult *ivfflatvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
IndexScanDesc ivfflatbeginscan(Relation index, int nkeys, int norderbys);
void		ivfflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool		ivfflatgettuple(IndexScanDesc scan, ScanDirection dir);
void		ivfflatendscan(IndexScanDesc scan);

#endif
