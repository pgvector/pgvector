#include "postgres.h"

#include "ivfflat.h"
#include "storage/bufmgr.h"
#include "vector.h"

#if PG_VERSION_NUM >= 120000
#include "commands/progress.h"
#endif

#if PG_VERSION_NUM >= 140000
#include "utils/backend_progress.h"
#elif PG_VERSION_NUM >= 120000
#include "pgstat.h"
#endif

/*
 * Allocate a vector array
 */
VectorArray
VectorArrayInit(int maxlen, int dimensions)
{
	VectorArray res = palloc0(VECTOR_ARRAY_SIZE(maxlen, dimensions));

	res->length = 0;
	res->maxlen = maxlen;
	res->dim = dimensions;
	return res;
}

/*
 * Print vector array - useful for debugging
 */
void
PrintVectorArray(char *msg, VectorArray arr)
{
	int			i;

	for (i = 0; i < arr->length; i++)
		PrintVector(msg, VectorArrayGet(arr, i));
}

/*
 * Get the number of lists in the index
 */
int
IvfflatGetLists(Relation index)
{
	IvfflatOptions *opts = (IvfflatOptions *) index->rd_options;

	if (opts)
		return opts->lists;

	return IVFFLAT_DEFAULT_LISTS;
}

/*
 * Get proc
 */
FmgrInfo *
IvfflatOptionalProcInfo(Relation rel, uint16 procnum)
{
	if (!OidIsValid(index_getprocid(rel, 1, procnum)))
		return NULL;

	return index_getprocinfo(rel, 1, procnum);
}

/*
 * Divide by the norm
 *
 * Returns false if value should not be indexed
 *
 * The caller needs to free the pointer stored in value
 * if it's different than the original value
 */
bool
IvfflatNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, Vector * result)
{
	Vector	   *v;
	int			i;
	double		norm;

	norm = DatumGetFloat8(FunctionCall1Coll(procinfo, collation, *value));

	if (norm > 0)
	{
		v = (Vector *) DatumGetPointer(*value);

		if (result == NULL)
			result = InitVector(v->dim);

		for (i = 0; i < v->dim; i++)
			result->x[i] = v->x[i] / norm;

		*value = PointerGetDatum(result);

		return true;
	}

	return false;
}

/*
 * New buffer
 */
Buffer
IvfflatNewBuffer(Relation index, ForkNumber forkNum)
{
	Buffer		buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	return buf;
}

/*
 * Init page
 */
void
IvfflatInitPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state)
{
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	PageInit(*page, BufferGetPageSize(*buf), sizeof(IvfflatPageOpaqueData));
	IvfflatPageGetOpaque(*page)->nextblkno = InvalidBlockNumber;
	IvfflatPageGetOpaque(*page)->page_id = IVFFLAT_PAGE_ID;
}

/*
 * Commit buffer
 */
void
IvfflatCommitBuffer(Buffer buf, GenericXLogState *state)
{
	MarkBufferDirty(buf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Add a new page
 *
 * The order is very important!!
 */
void
IvfflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum)
{
	Buffer		prevbuf = *buf;

	/* Get new buffer */
	*buf = IvfflatNewBuffer(index, forkNum);

	/* Update and commit previous buffer */
	IvfflatPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(*buf);
	IvfflatCommitBuffer(prevbuf, *state);

	/* Init new page */
	IvfflatInitPage(index, buf, page, state);
}

/*
 * Update the start or insert page of a list
 */
void
IvfflatUpdateList(Relation index, GenericXLogState *state, ListInfo listInfo,
				  BlockNumber insertPage, BlockNumber startPage, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	IvfflatList list;

	buf = ReadBufferExtended(index, forkNum, listInfo.blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	list = (IvfflatList) PageGetItem(page, PageGetItemId(page, listInfo.offno));

	if (BlockNumberIsValid(insertPage))
		list->insertPage = insertPage;

	if (BlockNumberIsValid(startPage))
		list->startPage = startPage;

	/* Could only commit if changed, but extra complexity isn't needed */
	IvfflatCommitBuffer(buf, state);
}

/*
 * Update build phase progress
 */
void
IvfflatUpdateProgress(int64 val)
{
#if PG_VERSION_NUM >= 120000
	pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE, val);
#endif
}
