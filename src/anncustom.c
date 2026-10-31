#include "postgres.h"

#include "access/gin.h"
#include "access/htup_details.h"
#include "ann.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_proc.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplesort.h"

/* Eligible rows awaiting worker candidates; tuples keep the full child row. */
typedef struct AnnChunk
{
	uint64		request;
	MemoryContext context;
	MinimalTuple *tuples;
	float	   *vectors;
	float	   *shared;			/* this chunk's slot in worker shared memory */
	int			count;
} AnnChunk;

/* custom_private: sort columns, operators, collations, null order, distance. */
typedef struct AnnCustomScanState
{
	CustomScanState css;
	PlanState  *child;
	TupleTableSlot *input;
	TupleTableSlot *output;
	Tuplesortstate *sortstate;
	ExprState  *query;
	ExprState  *limit;
	ExprState  *offset;
	FmgrInfo	distance;
	AttrNumber	distanceAttno;
	int			nkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;
	bool		withTies;
	uint64		eligibleRows;
	uint64		distanceEvaluations;

	/* Worker route state, valid while rows are collected. */
	bool		useWorker;
	bool		workerUsed;
	Datum		queryDatum;
	bool		queryIsNull;
	int			dimensions;
	int			chunkRows;
	int64		bound;
	TupleTableSlot *chunkSlot;
	bool	   *nulls;
	AnnChunk   *chunks;
	int			nchunks;		/* chunks array length: depth + 1 */
	int			filling;		/* chunk being filled, or -1 */
	int			inflight;
	uint64		workerChunks;
	uint64		localChunks;
	uint64		workerCandidates;
} AnnCustomScanState;

static Plan *PlanCustomPath(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
							List *tlist, List *clauses, List *custom_plans);
static Node *CreateCustomScanState(CustomScan *scan);
static void BeginCustomScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *ExecCustomScan(CustomScanState *node);
static void EndCustomScan(CustomScanState *node);
static void ReScanCustomScan(CustomScanState *node);
static void ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es);

const		CustomPathMethods AnnCustomPathMethods = {
	.CustomName = "AnnFilteredSearch",
	.PlanCustomPath = PlanCustomPath
};

const CustomScanMethods AnnCustomScanMethods = {
	.CustomName = "AnnFilteredSearch",
	.CreateCustomScanState = CreateCustomScanState
};

static const CustomExecMethods AnnCustomExecMethods = {
	.CustomName = "AnnFilteredSearch",
	.BeginCustomScan = BeginCustomScan,
	.ExecCustomScan = ExecCustomScan,
	.EndCustomScan = EndCustomScan,
	.ReScanCustomScan = ReScanCustomScan,
	.ExplainCustomScan = ExplainCustomScan
};

static Plan *
PlanCustomPath(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
			   List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *scan = makeNode(CustomScan);
	Plan	   *child = linitial(custom_plans);
	OpExpr	   *distance = lsecond(path->custom_private);
	List	   *sorts = linitial(path->custom_private);
	List	   *columns = NIL;
	List	   *operators = NIL;
	List	   *collations = NIL;
	List	   *nulls = NIL;
	TargetEntry *entry;
	ListCell   *lc;
	int			distanceAttno = 0;

	scan->scan.plan.targetlist = tlist;
	scan->flags = path->flags;
	scan->methods = &AnnCustomScanMethods;
	scan->custom_scan_tlist = copyObject(child->targetlist);
	foreach(lc, sorts)
	{
		SortGroupClause *sort = lfirst(lc);

		entry = get_sortgroupclause_tle(sort, child->targetlist);
		columns = lappend_int(columns, entry->resno);
		operators = lappend_oid(operators, sort->sortop);
		collations = lappend_oid(collations, exprCollation((Node *) entry->expr));
		nulls = lappend_int(nulls, sort->nulls_first);
	}

	/*
	 * Replace the distance projection with its vector input in the predicate
	 * child. The custom executor fills that output column only after quals.
	 */
	foreach(lc, child->targetlist)
	{
		entry = lfirst(lc);
		if (equal(entry->expr, distance))
		{
			distanceAttno = entry->resno;
			entry->expr = copyObject(linitial(distance->args));
			break;
		}
	}
	if (!distanceAttno)
		elog(ERROR, "missing distance column in ann predicate plan");
	scan->custom_plans = custom_plans;
	scan->custom_exprs = list_make3(copyObject(lsecond(distance->args)),
									copyObject(list_nth(path->custom_private, 2)),
									copyObject(list_nth(path->custom_private, 3)));
	scan->custom_private = list_make5(columns, operators, collations, nulls,
									  list_make3(makeInteger(distanceAttno),
												 makeInteger((int) get_opcode(distance->opno)),
												 copyObject(list_nth(path->custom_private, 4))));
	return &scan->scan.plan;
}

static Node *
CreateCustomScanState(CustomScan *scan)
{
	AnnCustomScanState *state = palloc0_object(AnnCustomScanState);

	NodeSetTag(&state->css, T_CustomScanState);
	state->css.methods = &AnnCustomExecMethods;
	return (Node *) state;
}

static void
BeginCustomScan(CustomScanState *node, EState *estate, int eflags)
{
	AnnCustomScanState *state = (AnnCustomScanState *) node;
	CustomScan *scan = (CustomScan *) node->ss.ps.plan;
	List	   *data = scan->custom_private;
	List	   *distance = list_nth(data, 4);
	Oid			proc = (Oid) intVal(lsecond(distance));
	AclResult	aclresult;

	/* Cached plans must preserve complete eligibility after GUC changes. */
	if (GinFuzzySearchLimit != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ann filtered search requires gin_fuzzy_search_limit = 0")));
	state->child = ExecInitNode(linitial(scan->custom_plans), estate, eflags);
	node->custom_ps = list_make1(state->child);
	state->input = MakeSingleTupleTableSlot(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor, &TTSOpsVirtual);
	state->output = MakeSingleTupleTableSlot(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor, &TTSOpsMinimalTuple);
	state->query = ExecInitExpr(linitial(scan->custom_exprs), &node->ss.ps);
	state->limit = ExecInitExpr(lsecond(scan->custom_exprs), &node->ss.ps);
	state->offset = ExecInitExpr(lthird(scan->custom_exprs), &node->ss.ps);
	state->distanceAttno = intVal(linitial(distance));
	state->withTies = intVal(lthird(distance));
	/* The moved operator must retain ordinary executor function checks. */
#if PG_VERSION_NUM >= 160000
	aclresult = object_aclcheck(ProcedureRelationId, proc, GetUserId(), ACL_EXECUTE);
#else
	aclresult = pg_proc_aclcheck(proc, GetUserId(), ACL_EXECUTE);
#endif
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(proc));
	InvokeFunctionExecuteHook(proc);
	fmgr_info(proc, &state->distance);
	fmgr_info_set_expr((Node *) get_tle_by_resno(scan->custom_scan_tlist, state->distanceAttno)->expr,
					   &state->distance);
	state->nkeys = list_length(linitial(data));
	state->sortColIdx = palloc(mul_size(sizeof(AttrNumber), state->nkeys));
	state->sortOperators = palloc(mul_size(sizeof(Oid), state->nkeys));
	state->collations = palloc(mul_size(sizeof(Oid), state->nkeys));
	state->nullsFirst = palloc(mul_size(sizeof(bool), state->nkeys));
	for (int i = 0; i < state->nkeys; i++)
	{
		state->sortColIdx[i] = list_nth_int(linitial(data), i);
		state->sortOperators[i] = list_nth_oid(lsecond(data), i);
		state->collations[i] = list_nth_oid(lthird(data), i);
		state->nullsFirst[i] = list_nth_int(lfourth(data), i);
	}
}

/* Bounds come from current execution parameters, never planner estimates. */
static int64
GetSortBound(AnnCustomScanState *state)
{
	ExprContext *context = state->css.ss.ps.ps_ExprContext;
	bool		isnull;
	int64		limit;
	int64		offset = 0;
	Datum		value;

	if (state->withTies || !state->limit)
		return -1;
	value = ExecEvalExprSwitchContext(state->limit, context, &isnull);
	if (isnull)
		return -1;
	limit = DatumGetInt64(value);
	if (state->offset)
	{
		value = ExecEvalExprSwitchContext(state->offset, context, &isnull);
		if (!isnull)
			offset = DatumGetInt64(value);
	}
	if (limit <= 0 || offset < 0 || limit > PG_INT64_MAX - offset)
		return -1;
	return limit + offset;
}

/* Score one child row with the PostgreSQL operator and add it to the sort. */
static void
PutRow(AnnCustomScanState *state, TupleTableSlot *slot)
{
	ExprContext *context = state->css.ss.ps.ps_ExprContext;
	TupleTableSlot *input = state->input;
	int			attno = state->distanceAttno - 1;
	MemoryContext oldCtx;

	slot_getallattrs(slot);
	ExecClearTuple(input);
	memcpy(input->tts_values, slot->tts_values, sizeof(Datum) * slot->tts_tupleDescriptor->natts);
	memcpy(input->tts_isnull, slot->tts_isnull, sizeof(bool) * slot->tts_tupleDescriptor->natts);
	oldCtx = MemoryContextSwitchTo(context->ecxt_per_tuple_memory);
	if (state->queryIsNull || input->tts_isnull[attno])
	{
		input->tts_isnull[attno] = true;
		input->tts_values[attno] = (Datum) 0;
	}
	else
	{
		input->tts_values[attno] = FunctionCall2Coll(&state->distance, InvalidOid,
													 PointerGetDatum(DatumGetVector(input->tts_values[attno])),
													 state->queryDatum);
		state->distanceEvaluations++;
	}
	ExecStoreVirtualTuple(input);
	MemoryContextSwitchTo(oldCtx);
	tuplesort_puttupleslot(state->sortstate, input);
	ExecClearTuple(input);
	ResetExprContext(context);
}

/* Chunk tuples omit the vector; rebuild it from the chunk's floats. */
static void
PutChunkRow(AnnCustomScanState *state, AnnChunk *chunk, int i)
{
	TupleTableSlot *slot = state->chunkSlot;
	int			attno = state->distanceAttno - 1;
	MemoryContext oldCtx = MemoryContextSwitchTo(chunk->context);
	Vector	   *vector = InitVector(state->dimensions);

	MemoryContextSwitchTo(oldCtx);
	memcpy(vector->x, chunk->vectors + (size_t) i * state->dimensions,
		   sizeof(float) * state->dimensions);
	ExecStoreMinimalTuple(chunk->tuples[i], slot, false);
	slot_getallattrs(slot);
	slot->tts_values[attno] = PointerGetDatum(vector);
	slot->tts_isnull[attno] = false;
	PutRow(state, slot);
	ExecClearTuple(slot);
}

static void
ResetChunk(AnnChunk *chunk)
{
	MemoryContextReset(chunk->context);
	chunk->request = 0;
	chunk->count = 0;
	chunk->tuples = NULL;
	chunk->vectors = NULL;
}

static void
ScoreChunkLocally(AnnCustomScanState *state, AnnChunk *chunk)
{
	for (int i = 0; i < chunk->count; i++)
		PutChunkRow(state, chunk, i);
	state->localChunks++;
	ResetChunk(chunk);
}

/* Stop using the worker; rows already sent are scored here instead. */
static void
AbandonWorker(AnnCustomScanState *state)
{
	state->useWorker = false;
	if (state->filling >= 0)
	{
		ScoreChunkLocally(state, &state->chunks[state->filling]);
		state->filling = -1;
	}
	for (int i = 0; i < state->nchunks; i++)
		if (state->chunks[i].request != 0)
			ScoreChunkLocally(state, &state->chunks[i]);
	state->inflight = 0;
}

/*
 * The worker returns every row of a chunk near or within that chunk's top
 * bound. Each global top-bound row is in its chunk's top bound, so the sort
 * only needs these candidates, re-scored with the SQL operator.
 */
static void
ReceiveChunk(AnnCustomScanState *state)
{
	uint64		request;
	uint32		status;
	uint32	   *ids;
	uint32		count;
	AnnChunk   *chunk = NULL;

	if (!AnnWorkerReceive(&request, &status, &ids, &count))
	{
		AbandonWorker(state);
		return;
	}
	for (int i = 0; i < state->nchunks; i++)
		if (state->chunks[i].request == request)
			chunk = &state->chunks[i];
	if (chunk == NULL)
	{
		AbandonWorker(state);
		return;
	}
	state->inflight--;
	if (status != 0)
	{
		/* Overload and other request errors fall back for this chunk only. */
		ScoreChunkLocally(state, chunk);
		return;
	}
	for (uint32 i = 0; i < count; i++)
		if (ids[i] >= (uint32) chunk->count || (i && ids[i] == ids[i - 1]))
		{
			pfree(ids);
			ScoreChunkLocally(state, chunk);
			return;
		}
	for (uint32 i = 0; i < count; i++)
		PutChunkRow(state, chunk, ids[i]);
	pfree(ids);
	state->workerChunks++;
	state->workerCandidates += count;
	ResetChunk(chunk);
}

static void
SendChunk(AnnCustomScanState *state)
{
	AnnChunk   *chunk = &state->chunks[state->filling];

	state->filling = -1;
	while (state->useWorker && state->inflight > 0 && AnnWorkerReady())
		ReceiveChunk(state);

	/*
	 * With the pipeline full, score here instead of waiting. A busy worker
	 * then sheds work to its clients rather than adding queueing delay.
	 */
	if (!state->useWorker || state->inflight >= state->nchunks - 1)
	{
		ScoreChunkLocally(state, chunk);
		return;
	}
	chunk->request = AnnWorkerSubmit(chunk->count, state->dimensions, (uint32) state->bound,
									 DatumGetVector(state->queryDatum)->x, chunk->vectors);
	if (chunk->request == 0)
	{
		ScoreChunkLocally(state, chunk);
		AbandonWorker(state);
		return;
	}
	state->inflight++;
}

static void
AddChunkRow(AnnCustomScanState *state, TupleTableSlot *slot, Vector *vector)
{
	AnnChunk   *chunk;
	MemoryContext oldCtx;

	if (state->filling < 0)
	{
		for (int i = 0; i < state->nchunks; i++)
			if (state->chunks[i].request == 0)
			{
				state->filling = i;
				break;
			}
		Assert(state->filling >= 0);
		chunk = &state->chunks[state->filling];
		oldCtx = MemoryContextSwitchTo(chunk->context);
		chunk->tuples = palloc(sizeof(MinimalTuple) * state->chunkRows);
		chunk->vectors = chunk->shared ? chunk->shared :
			palloc(mul_size(sizeof(float) * state->dimensions, state->chunkRows));
		MemoryContextSwitchTo(oldCtx);
	}
	chunk = &state->chunks[state->filling];
	memcpy(state->nulls, slot->tts_isnull, sizeof(bool) * slot->tts_tupleDescriptor->natts);
	state->nulls[state->distanceAttno - 1] = true;
	oldCtx = MemoryContextSwitchTo(chunk->context);
#if PG_VERSION_NUM >= 180000
	chunk->tuples[chunk->count] = heap_form_minimal_tuple(slot->tts_tupleDescriptor,
														  slot->tts_values, state->nulls, 0);
#else
	chunk->tuples[chunk->count] = heap_form_minimal_tuple(slot->tts_tupleDescriptor,
														  slot->tts_values, state->nulls);
#endif
	MemoryContextSwitchTo(oldCtx);
	memcpy(chunk->vectors + (size_t) chunk->count * state->dimensions, vector->x,
		   sizeof(float) * state->dimensions);
	if (++chunk->count == state->chunkRows)
		SendChunk(state);
}

/*
 * Use the worker only when a bound makes per-chunk candidates much smaller
 * than the chunk. The chunk size and depth are fixed for the whole scan.
 */
static void
BeginWorkerRoute(AnnCustomScanState *state)
{
	MemoryContext queryCtx = state->css.ss.ps.state->es_query_cxt;
	int			depth = ann_worker_pipeline_depth;
	size_t		sharedSize;
	float	   *shared;
	size_t		slotBytes;

	state->useWorker = false;
	if (state->queryIsNull || state->bound <= 0 ||
		state->bound > ann_worker_chunk_rows / 2 || !AnnWorkerBegin())
		return;
	state->dimensions = DatumGetVector(state->queryDatum)->dim;
	state->chunkRows = ann_worker_chunk_rows;
	if (state->chunks == NULL)
	{
		state->nchunks = depth + 1;
		state->chunks = MemoryContextAllocZero(queryCtx, sizeof(AnnChunk) * state->nchunks);
		for (int i = 0; i < state->nchunks; i++)
			state->chunks[i].context = AllocSetContextCreate(queryCtx, "ann worker chunk",
															 ALLOCSET_DEFAULT_SIZES);
		state->chunkSlot = MakeSingleTupleTableSlot(ExecGetResultType(state->child),
													&TTSOpsMinimalTuple);
		state->nulls = MemoryContextAlloc(queryCtx, sizeof(bool) *
										  ExecGetResultType(state->child)->natts);
	}

	/* Split shared memory into one slot per chunk, shrinking chunks to fit. */
	shared = AnnWorkerSharedBuffer(&sharedSize);
	slotBytes = sharedSize / state->nchunks / sizeof(float) * sizeof(float);
	if (shared && slotBytes / (sizeof(float) * state->dimensions) >= (size_t) state->bound * 2)
	{
		state->chunkRows = Min(state->chunkRows, slotBytes / (sizeof(float) * state->dimensions));
		for (int i = 0; i < state->nchunks; i++)
			state->chunks[i].shared = shared + i * (slotBytes / sizeof(float));
	}
	else
		for (int i = 0; i < state->nchunks; i++)
			state->chunks[i].shared = NULL;
	state->filling = -1;
	state->inflight = 0;
	state->useWorker = true;
	state->workerUsed = true;
}

static void
CollectChildRows(AnnCustomScanState *state)
{
	ExprContext *context = state->css.ss.ps.ps_ExprContext;
	int			attno = state->distanceAttno - 1;
	int64		bound = state->bound;
	uint64		nullRows = 0;

	for (;;)
	{
		TupleTableSlot *slot;

		CHECK_FOR_INTERRUPTS();
		slot = ExecProcNode(state->child);
		if (TupIsNull(slot))
			break;
		state->eligibleRows++;
		slot_getallattrs(slot);
		if (state->useWorker)
		{
			Vector	   *vector;
			MemoryContext oldCtx;

			/* NULL vectors sort last, so only the first bound can matter. */
			if (slot->tts_isnull[attno])
			{
				if (nullRows++ < (uint64) bound)
					PutRow(state, slot);
				continue;
			}
			oldCtx = MemoryContextSwitchTo(context->ecxt_per_tuple_memory);
			vector = DatumGetVector(slot->tts_values[attno]);
			MemoryContextSwitchTo(oldCtx);

			/* Mismatched dimensions raise the operator's own error. */
			if (vector->dim == state->dimensions)
			{
				AddChunkRow(state, slot, vector);
				ResetExprContext(context);
				continue;
			}
		}
		PutRow(state, slot);
	}
	if (state->useWorker && state->filling >= 0)
		SendChunk(state);
	while (state->useWorker && state->inflight > 0)
		ReceiveChunk(state);
}

static void
CollectRows(AnnCustomScanState *state)
{
	CustomScanState *node = &state->css;
	ExprContext *context = node->ss.ps.ps_ExprContext;
	int64		bound = GetSortBound(state);

	state->sortstate = tuplesort_begin_heap(state->input->tts_tupleDescriptor, state->nkeys,
											state->sortColIdx, state->sortOperators,
											state->collations, state->nullsFirst,
											work_mem, NULL, bound > 0 ? TUPLESORT_ALLOWBOUNDED : TUPLESORT_NONE);
	if (bound > 0)
		tuplesort_set_bound(state->sortstate, bound);
	state->queryDatum = ExecEvalExprSwitchContext(state->query, context, &state->queryIsNull);
	if (!state->queryIsNull)
	{
		MemoryContext oldCtx = MemoryContextSwitchTo(node->ss.ps.state->es_query_cxt);

		state->queryDatum = PointerGetDatum(PG_DETOAST_DATUM_COPY(state->queryDatum));
		MemoryContextSwitchTo(oldCtx);
	}
	state->bound = bound;
	BeginWorkerRoute(state);
	if (!state->useWorker)
		CollectChildRows(state);
	else
	{
		PG_TRY();
		{
			CollectChildRows(state);
		}
		PG_FINALLY();
		{
			AnnWorkerEnd();
		}
		PG_END_TRY();
	}
	tuplesort_performsort(state->sortstate);
	if (!state->queryIsNull)
		pfree(DatumGetPointer(state->queryDatum));
}

static TupleTableSlot *
GetNextRow(ScanState *scan)
{
	AnnCustomScanState *state = (AnnCustomScanState *) scan;
	TupleTableSlot *slot = state->output;

	Assert(ScanDirectionIsForward(scan->ps.state->es_direction));
	if (!state->sortstate)
		CollectRows(state);
	if (!tuplesort_gettupleslot(state->sortstate, true, false, slot, NULL))
		return NULL;
	return slot;
}

static bool
RecheckRow(ScanState *scan, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot *
ExecCustomScan(CustomScanState *node)
{
	return ExecScan(&node->ss, GetNextRow, RecheckRow);
}

static void
EndCustomScan(CustomScanState *node)
{
	AnnCustomScanState *state = (AnnCustomScanState *) node;

	ExecClearTuple(node->ss.ss_ScanTupleSlot);
	ExecDropSingleTupleTableSlot(state->input);
	ExecDropSingleTupleTableSlot(state->output);
	if (state->chunkSlot)
		ExecDropSingleTupleTableSlot(state->chunkSlot);
	if (state->sortstate)
		tuplesort_end(state->sortstate);
	ExecEndNode(state->child);
#if PG_VERSION_NUM >= 170000
	/* Earlier versions release these in ExecEndCustomScan itself. */
	FreeExprContext(node->ss.ps.ps_ExprContext, true);
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
#endif
}

static void
ReScanCustomScan(CustomScanState *node)
{
	AnnCustomScanState *state = (AnnCustomScanState *) node;

	ExecClearTuple(node->ss.ss_ScanTupleSlot);
	ExecClearTuple(state->output);
	if (state->sortstate)
	{
		tuplesort_end(state->sortstate);
		state->sortstate = NULL;
	}
	if (node->ss.ps.chgParam)
		UpdateChangedParamSet(state->child, node->ss.ps.chgParam);
	ExecReScan(state->child);
}

static void
ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es)
{
	AnnCustomScanState *state = (AnnCustomScanState *) node;

	if (es->analyze && state->workerUsed)
		ExplainPropertyText("Search Route", "worker pipeline after predicates", es);
	else if (ann_worker_socket && ann_worker_socket[0] != '\0')
		ExplainPropertyText("Search Route", "worker pipeline or exact CPU after predicates", es);
	else
		ExplainPropertyText("Search Route", "exact CPU after predicates", es);
	if (es->analyze)
	{
		if (state->workerUsed)
		{
			ExplainPropertyInteger("Worker Chunks", NULL, state->workerChunks, es);
			ExplainPropertyInteger("Local Chunks", NULL, state->localChunks, es);
			ExplainPropertyInteger("Worker Candidates", NULL, state->workerCandidates, es);
		}
		ExplainPropertyInteger("Eligible Rows", NULL, state->eligibleRows, es);
		ExplainPropertyInteger("Distance Evaluations", NULL, state->distanceEvaluations, es);
		if (state->sortstate)
		{
			TuplesortInstrumentation stats;

			tuplesort_get_stats(state->sortstate, &stats);
			ExplainPropertyText("Sort Method", tuplesort_method_name(stats.sortMethod), es);
			ExplainPropertyInteger("Sort Space Used", "kB", stats.spaceUsed, es);
			ExplainPropertyText("Sort Space Type", tuplesort_space_type_name(stats.spaceType), es);
		}
	}
}
