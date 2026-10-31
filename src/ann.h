#ifndef ANN_H
#define ANN_H

#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/itup.h"
#include "access/relscan.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "vector.h"

#define ANN_MAGIC_NUMBER 0xA991A991
#define ANN_VERSION 1
#define ANN_PAGE_ID 0xFF92
#define ANN_METAPAGE_BLKNO 0
#define ANN_MAX_DIM 2000
#define ANN_DISTANCE_PROC 1
#define ANN_NPROCS 1

typedef struct AnnPageOpaqueData
{
	uint16		page_id;
} AnnPageOpaqueData;

typedef struct AnnMetaPageData
{
	uint32		magicNumber;
	uint32		version;
	uint32		dimensions;
	BlockNumber insertPage;
} AnnMetaPageData;

#define AnnPageGetMeta(page) ((AnnMetaPageData *) PageGetContents(page))
#define AnnPageGetOpaque(page) ((AnnPageOpaqueData *) PageGetSpecialPointer(page))

extern const CustomPathMethods AnnCustomPathMethods;
extern const CustomScanMethods AnnCustomScanMethods;
void		AnnInit(void);

Oid			AnnVectorTypeOid(void);
void		AnnCheckPage(Page page);
void		AnnCheckIndex(Relation index);
void		AnnInitMetaPage(Relation index, ForkNumber forkNum);
void		AnnCheckMetaPage(Page page);
void		AnnInsertTuple(Relation index, Datum *values, bool *isnull,
						   ItemPointer tid, bool building);

IndexBuildResult *annbuild(Relation heap, Relation index, IndexInfo *indexInfo);
void		annbuildempty(Relation index);
bool		anninsert(Relation index, Datum *values, bool *isnull, ItemPointer tid,
					  Relation heap, IndexUniqueCheck checkUnique, bool indexUnchanged,
					  IndexInfo *indexInfo);
IndexScanDesc annbeginscan(Relation index, int nkeys, int norderbys);
void		annrescan(IndexScanDesc scan, ScanKey keys, int nkeys,
					  ScanKey orderbys, int norderbys);
bool		anngettuple(IndexScanDesc scan, ScanDirection dir);
void		annendscan(IndexScanDesc scan);
IndexBulkDeleteResult *annbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
									 IndexBulkDeleteCallback callback, void *callback_state);
IndexBulkDeleteResult *annvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);

#endif
