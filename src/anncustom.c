#include "postgres.h"

#include "access/gin.h"
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

static void
CollectRows(AnnCustomScanState *state)
{
	CustomScanState *node = &state->css;
	ExprContext *context = node->ss.ps.ps_ExprContext;
	TupleTableSlot *input = state->input;
	int			attno = state->distanceAttno - 1;
	int64		bound = GetSortBound(state);
	bool		queryIsNull;
	Datum		query;

	state->sortstate = tuplesort_begin_heap(input->tts_tupleDescriptor, state->nkeys,
											state->sortColIdx, state->sortOperators,
											state->collations, state->nullsFirst,
											work_mem, NULL, bound > 0 ? TUPLESORT_ALLOWBOUNDED : TUPLESORT_NONE);
	if (bound > 0)
		tuplesort_set_bound(state->sortstate, bound);
	query = ExecEvalExprSwitchContext(state->query, context, &queryIsNull);
	if (!queryIsNull)
	{
		MemoryContext oldCtx = MemoryContextSwitchTo(node->ss.ps.state->es_query_cxt);

		query = PointerGetDatum(PG_DETOAST_DATUM_COPY(query));
		MemoryContextSwitchTo(oldCtx);
	}
	for (;;)
	{
		TupleTableSlot *slot;
		MemoryContext oldCtx;

		CHECK_FOR_INTERRUPTS();
		slot = ExecProcNode(state->child);
		if (TupIsNull(slot))
			break;
		slot_getallattrs(slot);
		ExecClearTuple(input);
		memcpy(input->tts_values, slot->tts_values, sizeof(Datum) * slot->tts_tupleDescriptor->natts);
		memcpy(input->tts_isnull, slot->tts_isnull, sizeof(bool) * slot->tts_tupleDescriptor->natts);
		oldCtx = MemoryContextSwitchTo(context->ecxt_per_tuple_memory);
		if (queryIsNull || input->tts_isnull[attno])
		{
			input->tts_isnull[attno] = true;
			input->tts_values[attno] = (Datum) 0;
		}
		else
		{
			input->tts_values[attno] = FunctionCall2Coll(&state->distance, InvalidOid,
														 PointerGetDatum(DatumGetVector(input->tts_values[attno])), query);
			state->distanceEvaluations++;
		}
		ExecStoreVirtualTuple(input);
		MemoryContextSwitchTo(oldCtx);
		tuplesort_puttupleslot(state->sortstate, input);
		ExecClearTuple(input);
		ResetExprContext(context);
		state->eligibleRows++;
	}
	tuplesort_performsort(state->sortstate);
	if (!queryIsNull)
		pfree(DatumGetPointer(query));
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

	ExplainPropertyText("Search Route", "exact CPU after predicates", es);
	if (es->analyze)
	{
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
