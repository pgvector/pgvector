#include "postgres.h"

#include "access/gin.h"
#include "ann.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

static bool ann_enable_filtered_search;
static create_upper_paths_hook_type previous_upper_paths_hook;

/*
 * A complete PostgreSQL predicate scan supplies visible, qualified rows. Keep
 * parameterized, parallel, and accelerator index scans out of this first path.
 */
static bool
IsPredicatePath(Path *path)
{
	if (path->param_info || path->parallel_aware)
		return false;
	if (IsA(path, ProjectionPath))
		return IsPredicatePath(((ProjectionPath *) path)->subpath);
	if (IsA(path, IndexPath))
		return ((IndexPath *) path)->indexinfo->relam == BTREE_AM_OID &&
			path->pathtype != T_IndexOnlyScan &&
			((IndexPath *) path)->indexorderbys == NIL;
	return path->pathtype == T_SeqScan || IsA(path, BitmapHeapPath);
}

static bool
IsLimitExpression(Node *node)
{
	return node == NULL || IsA(node, Const) ||
		(IsA(node, Param) && ((Param *) node)->paramkind == PARAM_EXTERN);
}

/*
 * Match the actual support function and indexed column, not search-path names.
 * Unsupported shapes retain their original PostgreSQL plans.
 */
static OpExpr *
GetDistanceExpression(PlannerInfo *root, Oid amoid)
{
	Query	   *query = root->parse;
	RangeTblRef *ref;
	RangeTblEntry *rte;
	RelOptInfo *rel;
	SortGroupClause *sort;
	Node	   *expression;
	OpExpr	   *distance;
	Var		   *column;
	Node	   *argument;
	ListCell   *lc;

	if (root->parent_root || query->commandType != CMD_SELECT ||
		query->sortClause == NIL || query->rowMarks || query->hasAggs ||
		query->hasWindowFuncs || query->hasTargetSRFs || query->hasSubLinks ||
		query->groupClause || query->groupingSets || query->havingQual ||
		query->distinctClause || query->setOperations || query->cteList ||
		list_length(query->jointree->fromlist) != 1 ||
		!IsLimitExpression(query->limitCount) || !IsLimitExpression(query->limitOffset) ||
		contain_volatile_functions((Node *) query->targetList) ||
		contain_volatile_functions(query->jointree->quals))
		return NULL;
	ref = linitial(query->jointree->fromlist);
	if (!IsA(ref, RangeTblRef))
		return NULL;
	rte = planner_rt_fetch(ref->rtindex, root);
	rel = root->simple_rel_array[ref->rtindex];
	if (!rel || rte->rtekind != RTE_RELATION || rte->relkind != RELKIND_RELATION ||
		rel->baserestrictinfo == NIL)
		return NULL;
	sort = linitial_node(SortGroupClause, query->sortClause);
	expression = (Node *) get_sortgroupclause_expr(sort, query->targetList);
	if (!IsA(expression, OpExpr) || sort->nulls_first || sort->sortop != Float8LessOperator)
		return NULL;
	distance = (OpExpr *) expression;
	if (list_length(distance->args) != 2)
		return NULL;
	column = linitial(distance->args);
	argument = lsecond(distance->args);
	if (!IsA(column, Var) || column->varno != ref->rtindex || column->varlevelsup ||
		column->varattno <= 0 || !(IsA(argument, Const) ||
								   (IsA(argument, Param) && ((Param *) argument)->paramkind == PARAM_EXTERN)))
		return NULL;
	foreach(lc, rel->indexlist)
	{
		IndexOptInfo *index = lfirst(lc);
		Oid			proc;

		if (index->relam != amoid || index->nkeycolumns != 1 ||
			index->indexkeys[0] != column->varattno || index->indpred || index->indexprs)
			continue;
		proc = get_opfamily_proc(index->opfamily[0], index->opcintype[0],
								 index->opcintype[0], ANN_DISTANCE_PROC);
		if (OidIsValid(proc) && proc == get_opcode(distance->opno) &&
			func_strict(proc) && func_volatile(proc) == PROVOLATILE_IMMUTABLE)
			return distance;
	}
	return NULL;
}

static void
AnnCreateUpperPaths(PlannerInfo *root, UpperRelationKind stage,
					RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra)
{
	Oid			amoid;
	OpExpr	   *distance;
	ListCell   *lc;

	if (previous_upper_paths_hook)
		previous_upper_paths_hook(root, stage, input_rel, output_rel, extra);
	if (!ann_enable_filtered_search || stage != UPPERREL_ORDERED || GinFuzzySearchLimit != 0)
		return;
	amoid = get_am_oid("ann", true);
	if (!OidIsValid(amoid) || !(distance = GetDistanceExpression(root, amoid)))
		return;

	/* Replace equivalent full sorts, preserving PostgreSQL's path costs. */
	foreach(lc, output_rel->pathlist)
	{
		Path	   *path = lfirst(lc);
		SortPath   *sort;
		CustomPath *custom;

		if (!IsA(path, SortPath))
			continue;
		sort = (SortPath *) path;
		if (!IsPredicatePath(sort->subpath))
			continue;
		custom = makeNode(CustomPath);
		custom->path = *path;
		NodeSetTag(custom, T_CustomPath);
		custom->path.pathtype = T_CustomScan;
		custom->path.parallel_safe = false;
		custom->flags = CUSTOMPATH_SUPPORT_PROJECTION;
		custom->custom_paths = list_make1(sort->subpath);
		custom->custom_private = list_make5(copyObject(root->parse->sortClause),
											copyObject(distance), copyObject(root->parse->limitCount),
											copyObject(root->parse->limitOffset),
											makeInteger(root->parse->limitOption == LIMIT_OPTION_WITH_TIES));
		custom->methods = &AnnCustomPathMethods;
		lfirst(lc) = custom;
	}
}

void
AnnInit(void)
{
	DefineCustomBoolVariable("ann.enable_filtered_search",
							 "Enable the experimental exact filtered-search path.",
							 NULL, &ann_enable_filtered_search, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	RegisterCustomScanMethods(&AnnCustomScanMethods);
	previous_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = AnnCreateUpperPaths;
}
