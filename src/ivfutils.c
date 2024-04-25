#include "postgres.h"

#include "access/generic_xlog.h"
#include "bitvec.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "halfutils.h"
#include "halfvec.h"
#include "ivfflat.h"
#include "storage/bufmgr.h"

/*
 * Allocate a vector array
 */
VectorArray
VectorArrayInit(int maxlen, int dimensions, Size itemsize)
{
	VectorArray res = palloc(sizeof(VectorArrayData));

	/* Ensure items are aligned to prevent UB */
	itemsize = MAXALIGN(itemsize);

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
 * Normalize value
 */
Datum
IvfflatNormValue(FmgrInfo *procinfo, Oid collation, Datum value)
{
	if (procinfo == NULL)
		return DirectFunctionCall1(l2_normalize, value);

	return FunctionCall1Coll(procinfo, collation, value);
}

/*
 * Check if non-zero norm
 */
bool
IvfflatCheckNorm(FmgrInfo *procinfo, Oid collation, Datum value)
{
	return DatumGetFloat8(FunctionCall1Coll(procinfo, collation, value)) > 0;
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

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_halfvec_max_dims);
Datum
ivfflat_halfvec_max_dims(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(IVFFLAT_MAX_DIM * 2);
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_bit_max_dims);
Datum
ivfflat_bit_max_dims(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(IVFFLAT_MAX_DIM * 32);
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_vector_update_center);
Datum
ivfflat_vector_update_center(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	int			dimensions = PG_GETARG_INT32(1);
	float	   *x = (float *) PG_GETARG_POINTER(2);

	SET_VARSIZE(vec, VECTOR_SIZE(dimensions));
	vec->dim = dimensions;

	for (int k = 0; k < dimensions; k++)
		vec->x[k] = x[k];

	PG_RETURN_VOID();
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_halfvec_update_center);
Datum
ivfflat_halfvec_update_center(PG_FUNCTION_ARGS)
{
	HalfVector *vec = PG_GETARG_HALFVEC_P(0);
	int			dimensions = PG_GETARG_INT32(1);
	float	   *x = (float *) PG_GETARG_POINTER(2);

	SET_VARSIZE(vec, HALFVEC_SIZE(dimensions));
	vec->dim = dimensions;

	for (int k = 0; k < dimensions; k++)
		vec->x[k] = Float4ToHalfUnchecked(x[k]);

	PG_RETURN_VOID();
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_bit_update_center);
Datum
ivfflat_bit_update_center(PG_FUNCTION_ARGS)
{
	VarBit	   *vec = PG_GETARG_VARBIT_P(0);
	int			dimensions = PG_GETARG_INT32(1);
	float	   *x = (float *) PG_GETARG_POINTER(2);
	unsigned char *nx = VARBITS(vec);

	SET_VARSIZE(vec, VARBITTOTALLEN(dimensions));
	VARBITLEN(vec) = dimensions;

	for (uint32 k = 0; k < VARBITBYTES(vec); k++)
		nx[k] = 0;

	for (int k = 0; k < dimensions; k++)
		nx[k / 8] |= (x[k] > 0.5 ? 1 : 0) << (7 - (k % 8));

	PG_RETURN_VOID();
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_vector_sum_center);
Datum
ivfflat_vector_sum_center(PG_FUNCTION_ARGS)
{
	Vector	   *vec = PG_GETARG_VECTOR_P(0);
	float	   *x = (float *) PG_GETARG_POINTER(1);

	for (int k = 0; k < vec->dim; k++)
		x[k] += vec->x[k];

	PG_RETURN_VOID();
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_halfvec_sum_center);
Datum
ivfflat_halfvec_sum_center(PG_FUNCTION_ARGS)
{
	HalfVector *vec = PG_GETARG_HALFVEC_P(0);
	float	   *x = (float *) PG_GETARG_POINTER(1);

	for (int k = 0; k < vec->dim; k++)
		x[k] += HalfToFloat4(vec->x[k]);

	PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_bit_sum_center);
Datum
ivfflat_bit_sum_center(PG_FUNCTION_ARGS)
{
	VarBit	   *vec = PG_GETARG_VARBIT_P(0);
	float	   *x = (float *) PG_GETARG_POINTER(1);

	for (int k = 0; k < VARBITLEN(vec); k++)
		x[k] += (float) (((VARBITS(vec)[k / 8]) >> (7 - (k % 8))) & 0x01);

	PG_RETURN_VOID();
}
