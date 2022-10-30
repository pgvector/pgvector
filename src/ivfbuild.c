#include "postgres.h"

#include <float.h>

#include "catalog/index.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"

#if PG_VERSION_NUM >= 140000
#include "utils/backend_progress.h"
#elif PG_VERSION_NUM >= 120000
#include "pgstat.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "access/tableam.h"
#include "commands/progress.h"
#else
#define PROGRESS_CREATEIDX_SUBPHASE 0
#define PROGRESS_CREATEIDX_TUPLES_TOTAL 0
#define PROGRESS_CREATEIDX_TUPLES_DONE 0
#endif

#if PG_VERSION_NUM >= 110000
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#else
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
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
 * Callback for sampling
 */
static void
SampleCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values,
			   bool *isnull, bool tupleIsAlive, void *state)
{
	IvfflatBuildState *buildstate = (IvfflatBuildState *) state;
	VectorArray samples = buildstate->samples;
	int			targsamples = samples->maxlen;
	Datum		value = values[0];

	/* Skip nulls */
	if (isnull[0])
		return;

	/*
	 * Normalize with KMEANS_NORM_PROC since spherical distance function
	 * expects unit vectors
	 */
	if (buildstate->kmeansnormprocinfo != NULL)
	{
		if (!IvfflatNormValue(buildstate->kmeansnormprocinfo, buildstate->collation, &value, buildstate->normvec))
			return;
	}

	if (samples->length < targsamples)
	{
		VectorArraySet(samples, samples->length, DatumGetVector(value));
		samples->length++;
	}
	else
	{
		if (buildstate->rowstoskip < 0)
			buildstate->rowstoskip = reservoir_get_next_S(&buildstate->rstate, samples->length, targsamples);

		if (buildstate->rowstoskip <= 0)
		{
#if PG_VERSION_NUM >= 150000
			int			k = (int) (targsamples * sampler_random_fract(&buildstate->rstate.randstate));
#else
			int			k = (int) (targsamples * sampler_random_fract(buildstate->rstate.randstate));
#endif

			Assert(k >= 0 && k < targsamples);
			VectorArraySet(samples, k, DatumGetVector(value));
		}

		buildstate->rowstoskip -= 1;
	}
}

/*
 * Sample rows with same logic as ANALYZE
 */
static void
SampleRows(IvfflatBuildState * buildstate)
{
	int			targsamples = buildstate->samples->maxlen;
	BlockNumber totalblocks = RelationGetNumberOfBlocks(buildstate->heap);

	UpdateProgress(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_IVFFLAT_PHASE_SAMPLE);

	buildstate->rowstoskip = -1;

	BlockSampler_Init(&buildstate->bs, totalblocks, targsamples, random());

	reservoir_init_selection_state(&buildstate->rstate, targsamples);
	while (BlockSampler_HasMore(&buildstate->bs))
	{
		BlockNumber targblock = BlockSampler_Next(&buildstate->bs);

#if PG_VERSION_NUM >= 120000
		table_index_build_range_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
									 false, true, false, targblock, 1, SampleCallback, (void *) buildstate, NULL);
#elif PG_VERSION_NUM >= 110000
		IndexBuildHeapRangeScan(buildstate->heap, buildstate->index, buildstate->indexInfo,
								false, true, targblock, 1, SampleCallback, (void *) buildstate, NULL);
#else
		IndexBuildHeapRangeScan(buildstate->heap, buildstate->index, buildstate->indexInfo,
								false, true, targblock, 1, SampleCallback, (void *) buildstate);
#endif
	}
}

/*
 * Callback for table_index_build_scan
 */
static void
BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values,
			  bool *isnull, bool tupleIsAlive, void *state)
{
	IvfflatBuildState *buildstate = (IvfflatBuildState *) state;
	double		distance;
	double		minDistance = DBL_MAX;
	int			closestCenter = -1;
	VectorArray centers = buildstate->centers;
	TupleTableSlot *slot = buildstate->slot;
	Datum		value = values[0];
	int			i;

#if PG_VERSION_NUM < 130000
	ItemPointer tid = &hup->t_self;
#endif

	if (isnull[0])
		return;

	/* Normalize if needed */
	if (buildstate->normprocinfo != NULL)
	{
		if (!IvfflatNormValue(buildstate->normprocinfo, buildstate->collation, &value, buildstate->normvec))
			return;
	}

	/* Find the list that minimizes the distance */
	for (i = 0; i < centers->length; i++)
	{
		distance = DatumGetFloat8(FunctionCall2Coll(buildstate->procinfo, buildstate->collation, value, PointerGetDatum(VectorArrayGet(centers, i))));

		if (distance < minDistance)
		{
			minDistance = distance;
			closestCenter = i;
		}
	}

#ifdef IVFFLAT_KMEANS_DEBUG
	buildstate->inertia += minDistance;
	buildstate->listSums[closestCenter] += minDistance;
	buildstate->listCounts[closestCenter]++;
#endif

	/* Create a virtual tuple */
	ExecClearTuple(slot);
	slot->tts_values[0] = Int32GetDatum(closestCenter);
	slot->tts_isnull[0] = false;
	slot->tts_values[1] = PointerGetDatum(tid);
	slot->tts_isnull[1] = false;
	slot->tts_values[2] = value;
	slot->tts_isnull[2] = false;
	ExecStoreVirtualTuple(slot);

	/*
	 * Add tuple to sort
	 *
	 * tuplesort_puttupleslot comment: Input data is always copied; the caller
	 * need not save it.
	 */
	tuplesort_puttupleslot(buildstate->sortstate, slot);

	buildstate->indtuples++;
}

/*
 * Get index tuple from sort state
 */
static inline void
GetNextTuple(Tuplesortstate *sortstate, TupleDesc tupdesc, TupleTableSlot *slot, IndexTuple *itup, int *list)
{
	Datum		value;
	bool		isnull;

#if PG_VERSION_NUM >= 100000
	if (tuplesort_gettupleslot(sortstate, true, false, slot, NULL))
#else
	if (tuplesort_gettupleslot(sortstate, true, slot, NULL))
#endif
	{
		*list = DatumGetInt32(slot_getattr(slot, 1, &isnull));
		value = slot_getattr(slot, 3, &isnull);

		/* Form the index tuple */
		*itup = index_form_tuple(tupdesc, &value, &isnull);
		(*itup)->t_tid = *((ItemPointer) DatumGetPointer(slot_getattr(slot, 2, &isnull)));
	}
	else
		*list = -1;
}

/*
 * Create initial entry pages
 */
static void
InsertTuples(Relation index, IvfflatBuildState * buildstate, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	int			list;
	IndexTuple	itup = NULL;	/* silence compiler warning */
	BlockNumber startPage;
	BlockNumber insertPage;
	Size		itemsz;
	int			i;
	int64		inserted = 0;

#if PG_VERSION_NUM >= 120000
	TupleTableSlot *slot = MakeSingleTupleTableSlot(buildstate->tupdesc, &TTSOpsMinimalTuple);
#else
	TupleTableSlot *slot = MakeSingleTupleTableSlot(buildstate->tupdesc);
#endif
	TupleDesc	tupdesc = RelationGetDescr(index);

	UpdateProgress(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_IVFFLAT_PHASE_LOAD);

	UpdateProgress(PROGRESS_CREATEIDX_TUPLES_TOTAL, buildstate->indtuples);

	GetNextTuple(buildstate->sortstate, tupdesc, slot, &itup, &list);

	for (i = 0; i < buildstate->centers->length; i++)
	{
		/* Can take a while, so ensure we can interrupt */
		/* Needs to be called when no buffer locks are held */
		CHECK_FOR_INTERRUPTS();

		buf = IvfflatNewBuffer(index, forkNum);
		IvfflatInitRegisterPage(index, &buf, &page, &state);

		startPage = BufferGetBlockNumber(buf);

		/* Get all tuples for list */
		while (list == i)
		{
			/* Check for free space */
			itemsz = MAXALIGN(IndexTupleSize(itup));
			if (PageGetFreeSpace(page) < itemsz)
				IvfflatAppendPage(index, &buf, &page, &state, forkNum);

			/* Add the item */
			if (PageAddItem(page, (Item) itup, itemsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
				elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

			pfree(itup);

			UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++inserted);

			GetNextTuple(buildstate->sortstate, tupdesc, slot, &itup, &list);
		}

		insertPage = BufferGetBlockNumber(buf);

		IvfflatCommitBuffer(buf, state);

		/* Set the start and insert pages */
		IvfflatUpdateList(index, state, buildstate->listInfo[i], insertPage, InvalidBlockNumber, startPage, forkNum);
	}
}

/*
 * Initialize the build state
 */
static void
InitBuildState(IvfflatBuildState * buildstate, Relation heap, Relation index, IndexInfo *indexInfo)
{
	buildstate->heap = heap;
	buildstate->index = index;
	buildstate->indexInfo = indexInfo;

	buildstate->lists = IvfflatGetLists(index);
	buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

	/* Require column to have dimensions to be indexed */
	if (buildstate->dimensions < 0)
		elog(ERROR, "column does not have dimensions");

	buildstate->reltuples = 0;
	buildstate->indtuples = 0;

	/* Get support functions */
	buildstate->procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
	buildstate->normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	buildstate->kmeansnormprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
	buildstate->collation = index->rd_indcollation[0];

	/* Require more than one dimension for spherical k-means */
	/* Lists check for backwards compatibility */
	/* TODO Remove lists check in 0.3.0 */
	if (buildstate->kmeansnormprocinfo != NULL && buildstate->dimensions == 1 && buildstate->lists > 1)
		elog(ERROR, "dimensions must be greater than one for this opclass");

	/* Create tuple description for sorting */
#if PG_VERSION_NUM >= 120000
	buildstate->tupdesc = CreateTemplateTupleDesc(3);
#else
	buildstate->tupdesc = CreateTemplateTupleDesc(3, false);
#endif
	TupleDescInitEntry(buildstate->tupdesc, (AttrNumber) 1, "list", INT4OID, -1, 0);
	TupleDescInitEntry(buildstate->tupdesc, (AttrNumber) 2, "tid", TIDOID, -1, 0);
#if PG_VERSION_NUM >= 110000
	TupleDescInitEntry(buildstate->tupdesc, (AttrNumber) 3, "vector", RelationGetDescr(index)->attrs[0].atttypid, -1, 0);
#else
	TupleDescInitEntry(buildstate->tupdesc, (AttrNumber) 3, "vector", RelationGetDescr(index)->attrs[0]->atttypid, -1, 0);
#endif

#if PG_VERSION_NUM >= 120000
	buildstate->slot = MakeSingleTupleTableSlot(buildstate->tupdesc, &TTSOpsVirtual);
#else
	buildstate->slot = MakeSingleTupleTableSlot(buildstate->tupdesc);
#endif

	buildstate->centers = VectorArrayInit(buildstate->lists, buildstate->dimensions);
	buildstate->listInfo = palloc(sizeof(ListInfo) * buildstate->lists);

	/* Reuse for each tuple */
	buildstate->normvec = InitVector(buildstate->dimensions);

#ifdef IVFFLAT_KMEANS_DEBUG
	buildstate->inertia = 0;
	buildstate->listSums = palloc0(sizeof(double) * buildstate->lists);
	buildstate->listCounts = palloc0(sizeof(int) * buildstate->lists);
#endif
}

/*
 * Free resources
 */
static void
FreeBuildState(IvfflatBuildState * buildstate)
{
	pfree(buildstate->centers);
	pfree(buildstate->listInfo);
	pfree(buildstate->normvec);

#ifdef IVFFLAT_KMEANS_DEBUG
	pfree(buildstate->listSums);
	pfree(buildstate->listCounts);
#endif
}

/*
 * Compute centers
 */
static void
ComputeCenters(IvfflatBuildState * buildstate)
{
	int			numSamples;

	/* Target 50 samples per list, with at least 10000 samples */
	/* The number of samples has a large effect on index build time */
	numSamples = buildstate->lists * 50;
	if (numSamples < 10000)
		numSamples = 10000;

	/* Skip samples for unlogged table */
	if (buildstate->heap == NULL)
		numSamples = 1;

	/* Sample rows */
	/* TODO Ensure within maintenance_work_mem */
	buildstate->samples = VectorArrayInit(numSamples, buildstate->dimensions);
	if (buildstate->heap != NULL)
		SampleRows(buildstate);

	/* Calculate centers */
	UpdateProgress(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_IVFFLAT_PHASE_KMEANS);
	IvfflatBench("k-means", IvfflatKmeans(buildstate->index, buildstate->samples, buildstate->centers));

	/* Free samples before we allocate more memory */
	pfree(buildstate->samples);
}

/*
 * Create the metapage
 */
static void
CreateMetaPage(Relation index, int dimensions, int lists, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	IvfflatMetaPage metap;

	buf = IvfflatNewBuffer(index, forkNum);
	IvfflatInitRegisterPage(index, &buf, &page, &state);

	/* Set metapage data */
	metap = IvfflatPageGetMeta(page);
	metap->magicNumber = IVFFLAT_MAGIC_NUMBER;
	metap->version = IVFFLAT_VERSION;
	metap->dimensions = dimensions;
	metap->lists = lists;
	((PageHeader) page)->pd_lower =
		((char *) metap + sizeof(IvfflatMetaPageData)) - (char *) page;

	IvfflatCommitBuffer(buf, state);
}

/*
 * Create list pages
 */
static void
CreateListPages(Relation index, VectorArray centers, int dimensions,
				int lists, ForkNumber forkNum, ListInfo * *listInfo)
{
	int			i;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	OffsetNumber offno;
	Size		itemsz;
	IvfflatList list;

	itemsz = MAXALIGN(IVFFLAT_LIST_SIZE(dimensions));
	list = palloc(itemsz);

	buf = IvfflatNewBuffer(index, forkNum);
	IvfflatInitRegisterPage(index, &buf, &page, &state);

	for (i = 0; i < lists; i++)
	{
		/* Load list */
		list->startPage = InvalidBlockNumber;
		list->insertPage = InvalidBlockNumber;
		memcpy(&list->center, VectorArrayGet(centers, i), VECTOR_SIZE(dimensions));

		/* Ensure free space */
		if (PageGetFreeSpace(page) < itemsz)
			IvfflatAppendPage(index, &buf, &page, &state, forkNum);

		/* Add the item */
		offno = PageAddItem(page, (Item) list, itemsz, InvalidOffsetNumber, false, false);
		if (offno == InvalidOffsetNumber)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		/* Save location info */
		(*listInfo)[i].blkno = BufferGetBlockNumber(buf);
		(*listInfo)[i].offno = offno;
	}

	IvfflatCommitBuffer(buf, state);

	pfree(list);
}

/*
 * Print k-means metrics
 */
#ifdef IVFFLAT_KMEANS_DEBUG
static void
PrintKmeansMetrics(IvfflatBuildState * buildstate)
{
	elog(INFO, "inertia: %.3e", buildstate->inertia);

	/* Calculate Davies-Bouldin index */
	if (buildstate->lists > 1)
	{
		double		db = 0.0;

		/* Calculate average distance */
		for (int i = 0; i < buildstate->lists; i++)
		{
			if (buildstate->listCounts[i] > 0)
				buildstate->listSums[i] /= buildstate->listCounts[i];
		}

		for (int i = 0; i < buildstate->lists; i++)
		{
			double		max = 0.0;
			double		distance;

			for (int j = 0; j < buildstate->lists; j++)
			{
				if (j == i)
					continue;

				distance = DatumGetFloat8(FunctionCall2Coll(buildstate->procinfo, buildstate->collation, PointerGetDatum(VectorArrayGet(buildstate->centers, i)), PointerGetDatum(VectorArrayGet(buildstate->centers, j))));
				distance = (buildstate->listSums[i] + buildstate->listSums[j]) / distance;

				if (distance > max)
					max = distance;
			}
			db += max;
		}
		db /= buildstate->lists;
		elog(INFO, "davies-bouldin: %.3f", db);
	}
}
#endif

/*
 * Create entry pages
 */
static void
CreateEntryPages(IvfflatBuildState * buildstate, ForkNumber forkNum)
{
	AttrNumber	attNums[] = {1};
	Oid			sortOperators[] = {Float8LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};

	UpdateProgress(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_IVFFLAT_PHASE_SORT);

#if PG_VERSION_NUM >= 110000
	buildstate->sortstate = tuplesort_begin_heap(buildstate->tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, maintenance_work_mem, NULL, false);
#else
	buildstate->sortstate = tuplesort_begin_heap(buildstate->tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, maintenance_work_mem, false);
#endif

	/* Add tuples to sort */
	if (buildstate->heap != NULL)
	{
#if PG_VERSION_NUM >= 120000
		buildstate->reltuples = table_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
													   true, true, BuildCallback, (void *) buildstate, NULL);
#elif PG_VERSION_NUM >= 110000
		buildstate->reltuples = IndexBuildHeapScan(buildstate->heap, buildstate->index, buildstate->indexInfo,
												   true, BuildCallback, (void *) buildstate, NULL);
#else
		buildstate->reltuples = IndexBuildHeapScan(buildstate->heap, buildstate->index, buildstate->indexInfo,
												   true, BuildCallback, (void *) buildstate);
#endif
	}

	/* Sort */
	tuplesort_performsort(buildstate->sortstate);

#ifdef IVFFLAT_KMEANS_DEBUG
	PrintKmeansMetrics(buildstate);
#endif

	/* Insert */
	InsertTuples(buildstate->index, buildstate, forkNum);
	tuplesort_end(buildstate->sortstate);
}

/*
 * Build the index
 */
static void
BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo,
		   IvfflatBuildState * buildstate, ForkNumber forkNum)
{
	InitBuildState(buildstate, heap, index, indexInfo);

	ComputeCenters(buildstate);

	/* Create pages */
	CreateMetaPage(index, buildstate->dimensions, buildstate->lists, forkNum);
	CreateListPages(index, buildstate->centers, buildstate->dimensions, buildstate->lists, forkNum, &buildstate->listInfo);
	IvfflatBench("CreateEntryPages", CreateEntryPages(buildstate, forkNum));

	FreeBuildState(buildstate);
}

/*
 * Build the index for a logged table
 */
IndexBuildResult *
ivfflatbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	IvfflatBuildState buildstate;

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
ivfflatbuildempty(Relation index)
{
	IndexInfo  *indexInfo = BuildIndexInfo(index);
	IvfflatBuildState buildstate;

	BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}
