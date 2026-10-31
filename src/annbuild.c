#include "postgres.h"

#include "access/tableam.h"
#include "access/xloginsert.h"
#include "ann.h"
#include "utils/memutils.h"

typedef struct AnnBuildState
{
	double		indtuples;
	MemoryContext tmpCtx;
} AnnBuildState;

static void
BuildCallback(Relation index, ItemPointer tid, Datum *values, bool *isnull,
			  bool tupleIsAlive, void *state)
{
	AnnBuildState *buildstate = state;
	MemoryContext oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	AnnInsertTuple(index, values, isnull, tid, true);
	buildstate->indtuples++;
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
}

IndexBuildResult *
annbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	AnnBuildState buildstate = {0};
	IndexBuildResult *result = palloc0_object(IndexBuildResult);

	AnnInitMetaPage(index, MAIN_FORKNUM);
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext, "Ann build temporary context", ALLOCSET_DEFAULT_SIZES);
	result->heap_tuples = table_index_build_scan(heap, index, indexInfo, true, true,
												 BuildCallback, &buildstate, NULL);
	result->index_tuples = buildstate.indtuples;
	if (RelationNeedsWAL(index))
		log_newpage_range(index, MAIN_FORKNUM, 0, RelationGetNumberOfBlocks(index), true);
	MemoryContextDelete(buildstate.tmpCtx);
	return result;
}

void
annbuildempty(Relation index)
{
	AnnInitMetaPage(index, INIT_FORKNUM);
	log_newpage_range(index, INIT_FORKNUM, 0, 1, true);
}
