#include "postgres.h"

#include "access/generic_xlog.h"
#include "catalog/pg_type.h"
#include "halfutils.h"
#include "halfvec.h"
#include "ivfflat.h"
#include "storage/bufmgr.h"
#include "vector.h"
#include "utils/syscache.h"

/*
 * Allocate a vector array
 */
VectorArray
VectorArrayInit(int maxlen, int dimensions, Size itemsize)
{
	VectorArray res = palloc(sizeof(VectorArrayData));

	res->length = 0;
	res->maxlen = maxlen;
	res->dim = dimensions;
	res->itemsize = itemsize;
	res->items = palloc_extended(maxlen * itemsize, MCXT_ALLOC_ZERO | MCXT_ALLOC_HUGE);
	return res;
}

/*
 * Free a vector array
 */
void
VectorArrayFree(VectorArray arr)
{
	pfree(arr->items);
	pfree(arr);
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
IvfflatOptionalProcInfo(Relation index, uint16 procnum)
{
	if (!OidIsValid(index_getprocid(index, 1, procnum)))
		return NULL;

	return index_getprocinfo(index, 1, procnum);
}

/*
 * Get type
 */
IvfflatType
IvfflatGetType(Relation index)
{
	Oid			typid = TupleDescAttr(index->rd_att, 0)->atttypid;
	HeapTuple	tuple;
	Form_pg_type type;
	IvfflatType result;

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type %u", typid);

	type = (Form_pg_type) GETSTRUCT(tuple);
	if (strcmp(NameStr(type->typname), "vector") == 0)
		result = IVFFLAT_TYPE_VECTOR;
	else if (strcmp(NameStr(type->typname), "halfvec") == 0)
		result = IVFFLAT_TYPE_HALFVEC;
	else
	{
		ReleaseSysCache(tuple);
		elog(ERROR, "type not supported for ivfflat index");
	}

	ReleaseSysCache(tuple);

	return result;
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
IvfflatNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, IvfflatType type)
{
	double		norm = DatumGetFloat8(FunctionCall1Coll(procinfo, collation, *value));

	if (norm > 0)
	{
		if (type == IVFFLAT_TYPE_VECTOR)
		{
			Vector	   *v = DatumGetVector(*value);
			Vector	   *result = InitVector(v->dim);

			for (int i = 0; i < v->dim; i++)
				result->x[i] = v->x[i] / norm;

			*value = PointerGetDatum(result);
		}
		else if (type == IVFFLAT_TYPE_HALFVEC)
		{
			HalfVector *v = DatumGetHalfVector(*value);
			HalfVector *result = InitHalfVector(v->dim);

			for (int i = 0; i < v->dim; i++)
				result->x[i] = Float4ToHalfUnchecked(HalfToFloat4(v->x[i]) / norm);

			*value = PointerGetDatum(result);
		}
		else
			elog(ERROR, "Unsupported type");

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
IvfflatInitPage(Buffer buf, Page page)
{
	PageInit(page, BufferGetPageSize(buf), sizeof(IvfflatPageOpaqueData));
	IvfflatPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
	IvfflatPageGetOpaque(page)->page_id = IVFFLAT_PAGE_ID;
}

/*
 * Init and register page
 */
void
IvfflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state)
{
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	IvfflatInitPage(*buf, *page);
}

/*
 * Commit buffer
 */
void
IvfflatCommitBuffer(Buffer buf, GenericXLogState *state)
{
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
	/* Get new buffer */
	Buffer		newbuf = IvfflatNewBuffer(index, forkNum);
	Page		newpage = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);

	/* Update the previous buffer */
	IvfflatPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

	/* Init new page */
	IvfflatInitPage(newbuf, newpage);

	/* Commit */
	GenericXLogFinish(*state);

	/* Unlock */
	UnlockReleaseBuffer(*buf);

	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);
	*buf = newbuf;
}

/*
 * Get the metapage info
 */
void
IvfflatGetMetaPageInfo(Relation index, int *lists, int *dimensions)
{
	Buffer		buf;
	Page		page;
	IvfflatMetaPage metap;

	buf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = IvfflatPageGetMeta(page);

	*lists = metap->lists;

	if (dimensions != NULL)
		*dimensions = metap->dimensions;

	UnlockReleaseBuffer(buf);
}

/*
 * Update the start or insert page of a list
 */
void
IvfflatUpdateList(Relation index, ListInfo listInfo,
				  BlockNumber insertPage, BlockNumber originalInsertPage,
				  BlockNumber startPage, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	IvfflatList list;
	bool		changed = false;

	buf = ReadBufferExtended(index, forkNum, listInfo.blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	list = (IvfflatList) PageGetItem(page, PageGetItemId(page, listInfo.offno));

	if (BlockNumberIsValid(insertPage) && insertPage != list->insertPage)
	{
		/* Skip update if insert page is lower than original insert page  */
		/* This is needed to prevent insert from overwriting vacuum */
		if (!BlockNumberIsValid(originalInsertPage) || insertPage >= originalInsertPage)
		{
			list->insertPage = insertPage;
			changed = true;
		}
	}

	if (BlockNumberIsValid(startPage) && startPage != list->startPage)
	{
		list->startPage = startPage;
		changed = true;
	}

	/* Only commit if changed */
	if (changed)
		IvfflatCommitBuffer(buf, state);
	else
	{
		GenericXLogAbort(state);
		UnlockReleaseBuffer(buf);
	}
}
