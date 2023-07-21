#include "postgres.h"

#include <math.h>

#include "catalog/index.h"
#include "hnsw.h"
#include "miscadmin.h"
#include "lib/pairingheap.h"
#include "nodes/pg_list.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"

#if PG_VERSION_NUM >= 140000
#include "utils/backend_progress.h"
#elif PG_VERSION_NUM >= 120000
#include "pgstat.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "access/tableam.h"
#include "commands/progress.h"
#else
#define PROGRESS_CREATEIDX_TUPLES_DONE 0
#endif

#if PG_VERSION_NUM >= 130000
#define CALLBACK_ITEM_POINTER ItemPointer tid
#else
#define CALLBACK_ITEM_POINTER HeapTuple hup
#endif

#if PG_VERSION_NUM >= 120000
#define UpdateProgress(index, val) pgstat_progress_update_param(index, val)
#else
#define UpdateProgress(index, val) ((void)val)
#endif

/*
 * Create the metapage
 */
static void
CreateMetaPage(HnswBuildState * buildstate)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	HnswMetaPage metap;

	buf = HnswNewBuffer(index, forkNum);
	HnswInitRegisterPage(index, &buf, &page, &state);

	/* Set metapage data */
	metap = HnswPageGetMeta(page);
	metap->magicNumber = HNSW_MAGIC_NUMBER;
	metap->version = HNSW_VERSION;
	metap->dimensions = buildstate->dimensions;
	metap->m = buildstate->m;
	metap->efConstruction = buildstate->efConstruction;
	metap->entryBlkno = InvalidBlockNumber;
	metap->entryOffno = InvalidOffsetNumber;
	metap->insertPage = InvalidBlockNumber;
	((PageHeader) page)->pd_lower =
		((char *) metap + sizeof(HnswMetaPageData)) - (char *) page;

	HnswCommitBuffer(buf, state);
}

/*
 * Create element pages
 */
static void
CreateElementPages(HnswBuildState * buildstate)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	int			dimensions = buildstate->dimensions;
	Size		elementsz;
	HnswElementTuple element;
	int			elementsPerPage;
	BlockNumber neighborPage;
	BlockNumber insertPage;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	ListCell   *lc;

	/* Allocate once */
	elementsz = MAXALIGN(HNSW_ELEMENT_TUPLE_SIZE(dimensions));
	element = palloc0(elementsz);

	/* Calculate starting neighbor page */
	elementsPerPage = (BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(HnswPageOpaqueData))) / (elementsz + sizeof(ItemIdData));
	neighborPage = HNSW_HEAD_BLKNO + (int) ceil(list_length(buildstate->elements) / (double) elementsPerPage);

	/* Prepare first page */
	buf = HnswNewBuffer(index, forkNum);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
	HnswInitPage(buf, page);

	foreach(lc, buildstate->elements)
	{
		HnswElement e = lfirst(lc);

		/* Calculate neighbor page */
		/* Will be rechecked later */
		e->neighborPage = neighborPage++;

		/* Set item data */
		for (int i = 0; i < HNSW_HEAPTIDS; i++)
		{
			if (i < list_length(e->heaptids))
				element->heaptids[i] = *((ItemPointer) list_nth(e->heaptids, i));
			else
				ItemPointerSetInvalid(&element->heaptids[i]);
		}
		element->level = e->level;
		element->deleted = 0;
		element->neighborPage = e->neighborPage;
		memcpy(&element->vec, e->vec, VECTOR_SIZE(dimensions));

		/* Ensure free space */
		if (PageGetFreeSpace(page) < elementsz)
		{
			/* Add a new page */
			Buffer		newbuf = HnswNewBuffer(index, forkNum);

			/* Update previous page */
			HnswPageGetOpaque(page)->nextblkno = BufferGetBlockNumber(newbuf);

			/* Commit */
			MarkBufferDirty(buf);
			GenericXLogFinish(state);
			UnlockReleaseBuffer(buf);

			/* Can take a while, so ensure we can interrupt */
			/* Needs to be called when no buffer locks are held */
			CHECK_FOR_INTERRUPTS();

			/* Prepare new page */
			buf = newbuf;
			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
			HnswInitPage(buf, page);
		}

		/* Add the item */
		e->blkno = BufferGetBlockNumber(buf);
		e->offno = PageAddItem(page, (Item) element, elementsz, InvalidOffsetNumber, false, false);
		if (e->offno == InvalidOffsetNumber)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		if (e == buildstate->entryPoint)
			UpdateMetaPage(index, true, e, InvalidBlockNumber, forkNum);
	}

	insertPage = BufferGetBlockNumber(buf);

	/* Commit */
	MarkBufferDirty(buf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	UpdateMetaPage(index, false, NULL, insertPage, forkNum);
}

/*
 * Create neighbor pages
 */
static void
CreateNeighborPages(HnswBuildState * buildstate)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	Size		neighborsz;
	HnswNeighborTuple neighbor;
	ListCell   *lc;

	/* Allocate once */
	neighborsz = MAXALIGN(sizeof(HnswNeighborTupleData));
	neighbor = palloc0(neighborsz);

	foreach(lc, buildstate->elements)
	{
		HnswElement e = lfirst(lc);
		Buffer		buf;
		Page		page;
		GenericXLogState *state;

		/* Can take a while, so ensure we can interrupt */
		/* Needs to be called when no buffer locks are held */
		CHECK_FOR_INTERRUPTS();

		buf = HnswNewBuffer(index, forkNum);

		/* Check block number */
		if (BufferGetBlockNumber(buf) != e->neighborPage)
			elog(ERROR, "expected neighbor page %d, got %d", e->neighborPage, BufferGetBlockNumber(buf));

		/* Prepare page */
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
		HnswInitPage(buf, page);

		AddNeighborsToPage(index, page, e, neighbor, neighborsz, buildstate->m);

		/* Commit */
		MarkBufferDirty(buf);
		GenericXLogFinish(state);
		UnlockReleaseBuffer(buf);
	}
}

/*
 * Free elements
 */
static void
FreeElements(HnswBuildState * buildstate)
{
	ListCell   *lc;

	foreach(lc, buildstate->elements)
		HnswFreeElement(lfirst(lc));

	list_free(buildstate->elements);
}

/*
 * Flush pages
 */
static void
FlushPages(HnswBuildState * buildstate)
{
	CreateMetaPage(buildstate);
	CreateElementPages(buildstate);
	CreateNeighborPages(buildstate);

	buildstate->flushed = true;
	FreeElements(buildstate);
}

/*
 * Insert tuple
 */
static bool
InsertTuple(Relation index, Datum *values, HnswElement element, HnswBuildState * buildstate, HnswElement * dup)
{
	FmgrInfo   *procinfo = buildstate->procinfo;
	Oid			collation = buildstate->collation;
	HnswElement entryPoint = buildstate->entryPoint;
	int			efConstruction = buildstate->efConstruction;
	int			m = buildstate->m;

	/* Detoast once for all calls */
	Datum		value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

	/* Normalize if needed */
	if (buildstate->normprocinfo != NULL)
	{
		if (!HnswNormValue(buildstate->normprocinfo, collation, &value, buildstate->normvec))
			return false;
	}

	/* Copy value to element so accessible outside of memory context */
	memcpy(element->vec, DatumGetVector(value), VECTOR_SIZE(buildstate->dimensions));

	/* Insert element in graph */
	*dup = HnswInsertElement(element, entryPoint, NULL, procinfo, collation, m, efConstruction, NULL, false);

	/* Update entry point if needed */
	if (*dup == NULL && (entryPoint == NULL || element->level > entryPoint->level))
		buildstate->entryPoint = element;

	UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++buildstate->indtuples);

	return *dup == NULL;
}

/*
 * Callback for table_index_build_scan
 */
static void
BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values,
			  bool *isnull, bool tupleIsAlive, void *state)
{
	HnswBuildState *buildstate = (HnswBuildState *) state;
	MemoryContext oldCtx;
	HnswElement element;
	HnswElement dup = NULL;
	bool		inserted;

#if PG_VERSION_NUM < 130000
	ItemPointer tid = &hup->t_self;
#endif

	/* Skip nulls */
	if (isnull[0])
		return;

	if (buildstate->indtuples >= buildstate->maxInMemoryElements)
	{
		if (!buildstate->flushed)
		{
			/* TODO Improve message */
			ereport(NOTICE,
					(errmsg("hnsw graph no longer fits into maintenance_work_mem"),
					 errdetail("Building will take significantly more time."),
					 errhint("Increase maintenance_work_mem to speed up future builds.")));

			FlushPages(buildstate);
		}

		oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

		if (HnswInsertTuple(buildstate->index, values, isnull, tid, buildstate->heap))
			UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++buildstate->indtuples);

		/* Reset memory context */
		MemoryContextSwitchTo(oldCtx);
		MemoryContextReset(buildstate->tmpCtx);

		return;
	}

	/* Allocate necessary memory outside of memory context */
	element = HnswInitElement(tid, buildstate->m, buildstate->ml, buildstate->maxLevel);
	element->vec = palloc(VECTOR_SIZE(buildstate->dimensions));

	/* Use memory context since detoast can allocate */
	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	/* Insert tuple */
	inserted = InsertTuple(index, values, element, buildstate, &dup);

	/* Reset memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);

	/* Add outside memory context */
	if (dup != NULL)
		HnswAddHeapTid(dup, tid);

	/* Add to buildstate or free */
	if (inserted)
		buildstate->elements = lappend(buildstate->elements, element);
	else
		HnswFreeElement(element);
}

/*
 * Get the max number of elements that fit into maintenance_work_mem
 */
static double
HnswGetMaxInMemoryElements(int m, double ml, int dimensions)
{
	Size		elementSize = sizeof(HnswElementData);
	double		avgLevel = -log(0.5) * ml;

	elementSize += sizeof(HnswCandidate) * (m * (avgLevel + 2));
	elementSize += sizeof(ItemPointerData);
	elementSize += VECTOR_SIZE(dimensions);
	return (maintenance_work_mem * 1024L) / elementSize;
}

/*
 * Initialize the build state
 */
static void
InitBuildState(HnswBuildState * buildstate, Relation heap, Relation index, IndexInfo *indexInfo, ForkNumber forkNum)
{
	buildstate->heap = heap;
	buildstate->index = index;
	buildstate->indexInfo = indexInfo;
	buildstate->forkNum = forkNum;

	buildstate->m = HnswGetM(index);
	buildstate->efConstruction = HnswGetEfConstruction(index);
	buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

	/* Require column to have dimensions to be indexed */
	if (buildstate->dimensions < 0)
		elog(ERROR, "column does not have dimensions");

	if (buildstate->dimensions > HNSW_MAX_DIM)
		elog(ERROR, "column cannot have more than %d dimensions for hnsw index", HNSW_MAX_DIM);

	buildstate->reltuples = 0;
	buildstate->indtuples = 0;

	/* Get support functions */
	buildstate->procinfo = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
	buildstate->normprocinfo = HnswOptionalProcInfo(index, HNSW_NORM_PROC);
	buildstate->collation = index->rd_indcollation[0];

	buildstate->elements = NIL;
	buildstate->entryPoint = NULL;
	buildstate->ml = HnswGetMl(buildstate->m);
	buildstate->maxLevel = HnswGetMaxLevel(buildstate->m);
	buildstate->maxInMemoryElements = HnswGetMaxInMemoryElements(buildstate->m, buildstate->ml, buildstate->dimensions);
	buildstate->flushed = false;

	/* Reuse for each tuple */
	buildstate->normvec = InitVector(buildstate->dimensions);

	buildstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											   "Hnsw build temporary context",
											   ALLOCSET_DEFAULT_SIZES);
}

/*
 * Free resources
 */
static void
FreeBuildState(HnswBuildState * buildstate)
{
	MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Build graph
 */
static void
BuildGraph(HnswBuildState * buildstate, ForkNumber forkNum)
{
	UpdateProgress(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_HNSW_PHASE_LOAD);

#if PG_VERSION_NUM >= 120000
	buildstate->reltuples = table_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
												   true, true, BuildCallback, (void *) buildstate, NULL);
#else
	buildstate->reltuples = IndexBuildHeapScan(buildstate->heap, buildstate->index, buildstate->indexInfo,
											   true, BuildCallback, (void *) buildstate, NULL);
#endif
}

/*
 * Build the index
 */
static void
BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo,
		   HnswBuildState * buildstate, ForkNumber forkNum)
{
	InitBuildState(buildstate, heap, index, indexInfo, forkNum);

	if (buildstate->heap != NULL)
		BuildGraph(buildstate, forkNum);

	if (!buildstate->flushed)
		FlushPages(buildstate);

	FreeBuildState(buildstate);
}

/*
 * Build the index for a logged table
 */
IndexBuildResult *
hnswbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	HnswBuildState buildstate;

	BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}

/*
 * Build the index for an unlogged table
 */
void
hnswbuildempty(Relation index)
{
	IndexInfo  *indexInfo = BuildIndexInfo(index);
	HnswBuildState buildstate;

	BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}
