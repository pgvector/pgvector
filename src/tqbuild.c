#include "postgres.h"

#include <math.h>

#include "access/generic_xlog.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "fmgr.h"
#include "tq.h"
#include "utils/array.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "vector.h"

/*
 * Build state threaded through the heap scan callback.
 */
typedef struct TqBuildState
{
	Relation	heap;
	Relation	index;
	IndexInfo  *indexInfo;
	ForkNumber	forkNum;

	/* Model parameters */
	int			dim;
	int			bits;
	int			nLevels;
	TqMetric	metric;
	bool		tqProd;
	bool		fastRotation;
	int			dimCodes;		/* dim, or next_pow2(dim) in fast mode */
	TqModel		model;

	/* Type-info vtable (resolved from opclass support proc). */
	const TqTypeInfo *typeInfo;
	float	   *vecScratch;		/* dim floats, reused per tuple */

	/* Entry sizing */
	Size		entrySize;		/* MAXALIGNed size of one TqEntry on disk */
	int			codesBytes;		/* TQ_CODES_BYTES(dim, bits) */
	TqEntry    *entry;			/* scratch entry (entrySize bytes) */

	/* Data page write cursor */
	Buffer		buf;
	Page		page;
	GenericXLogState *state;

	/* Counters */
	double		reltuples;
	double		indtuples;

	MemoryContext tmpCtx;

	/* Blocked build staging */
	uint8	   *codeStage;		/* TQ_BLOCK_CODE_BYTES(dimCodes), reused per
								 * block */
	TqBlockSideRec sideStage;	/* staged side record for the current block */
	int			slot;			/* next free lane 0..TQ_BLOCK_WIDTH-1 */
	uint32		blockCount;		/* blocks flushed */

	/*
	 * Streaming code-plane chain cursor.  Each block's code-plane is appended
	 * straight to this page chain as it flushes, rather than accumulated in a
	 * single in-memory buffer: a StringInfo caps at MaxAllocSize (~1 GB) and
	 * would error out a large build (~2M rows at dim=768) well before the
	 * meta page's 2^32 nVectors limit.  The reassembled byte stream is
	 * independent of how blocks are split across page items (TqReadBytes
	 * concatenates item contents in chain order), so this is
	 * on-disk-identical to a one-shot write of the whole stream.
	 */
	BlockNumber codeStart;		/* first code-chain block (set on first
								 * append) */
	Buffer		codeBuf;		/* code-chain page cursor */
	Page		codePage;
	GenericXLogState *codeState;

	BlockNumber sideStart;		/* first side-chain block (set on first
								 * append) */
	Buffer		sideBuf;		/* side-chain page cursor */
	Page		sidePage;
	GenericXLogState *sideState;
} TqBuildState;

/*
 * Get a new buffer with an exclusive lock (mirrors IvfflatNewBuffer).
 */
Buffer
TqNewBuffer(Relation index, ForkNumber forkNum)
{
	Buffer		buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	return buf;
}

/*
 * Initialize a tqflat/tqivf page (mirrors IvfflatInitPage).
 * pageId is TQ_PAGE_ID for tqflat pages, TQIVF_PAGE_ID for tqivf pages.
 */
void
TqInitPage(Buffer buf, Page page, uint16 pageId)
{
	PageInit(page, BufferGetPageSize(buf), sizeof(TqPageOpaqueData));
	TqPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
	TqPageGetOpaque(page)->page_id = pageId;
}

/*
 * Init and register a page within a GenericXLog transaction.
 * pageId is TQ_PAGE_ID for tqflat pages, TQIVF_PAGE_ID for tqivf pages.
 */
void
TqInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state,
				   uint16 pageId)
{
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	TqInitPage(*buf, *page, pageId);
}

/*
 * Commit a buffer (finish WAL record + release lock).
 */
void
TqCommitBuffer(Buffer buf, GenericXLogState *state)
{
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Park a streaming build cursor: finish its WAL record and drop the buffer's
 * content lock, but keep the pin.  Called at the end of every block flush so
 * the tqflat build holds no buffer content lock across table_index_build_scan
 * -- otherwise a large CREATE INDEX would ignore cancel/statement_timeout
 * (LWLocks hold off interrupts).
 */
static void
TqParkCursor(Buffer buf, Page *page, GenericXLogState **state)
{
	GenericXLogFinish(*state);
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	*state = NULL;
	*page = NULL;
}

/*
 * Re-lock a parked cursor and reopen a WAL record for it (delta, not a full
 * image -- the page already exists on disk).
 */
static void
TqResumeCursor(Relation index, Buffer buf, Page *page, GenericXLogState **state)
{
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, buf, 0);
}

/*
 * Append a new page after the current one, linking via nextblkno.
 * pageId is TQ_PAGE_ID for tqflat pages, TQIVF_PAGE_ID for tqivf pages.
 *
 * The order is very important (mirrors IvfflatAppendPage).
 */
void
TqAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state,
			 ForkNumber forkNum, uint16 pageId)
{
	/* Get new buffer */
	Buffer		newbuf = TqNewBuffer(index, forkNum);
	Page		newpage = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);

	/* Update the previous buffer */
	TqPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

	/* Init new page */
	TqInitPage(newbuf, newpage, pageId);

	/* Commit */
	GenericXLogFinish(*state);

	/* Unlock */
	UnlockReleaseBuffer(*buf);

	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);
	*buf = newbuf;
}

/*
 * Usable bytes on a freshly-initialized tqflat page (after header + special).
 */
Size
TqPageCapacity(void)
{
	return BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(TqPageOpaqueData));
}

/*
 * Write the meta page at block TQ_METAPAGE_BLKNO.  The chain start blocks and
 * post-build counters are left invalid/zero here and backfilled by
 * TqUpdateMeta once the side and block pages are written.
 */
static void
TqCreateMetaPage(TqBuildState *buildstate)
{
	Relation	index = buildstate->index;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	TqMetaPage	metap;

	buf = TqNewBuffer(index, buildstate->forkNum);
	Assert(BufferGetBlockNumber(buf) == TQ_METAPAGE_BLKNO);
	TqInitRegisterPage(index, &buf, &page, &state, TQ_PAGE_ID);

	metap = TqPageGetMeta(page);
	metap->magicNumber = TQ_MAGIC_NUMBER;
	metap->version = TQ_VERSION;
	metap->dimensions = (uint16) buildstate->dim;
	metap->bits = (uint16) buildstate->bits;
	metap->metric = (uint16) buildstate->metric;
	metap->tqProd = (uint16) (buildstate->tqProd ? 1 : 0);
	metap->nLevels = (uint32) buildstate->nLevels;
	metap->nVectors = 0;
	metap->fastRotation = 0;
	metap->dimPadded = 0;
	metap->rotationStart = InvalidBlockNumber;
	metap->codebookStart = InvalidBlockNumber;
	metap->qjlStart = InvalidBlockNumber;
	metap->dataStart = InvalidBlockNumber;
	metap->codeStart = InvalidBlockNumber;
	metap->sideStart = InvalidBlockNumber;
	metap->tailStart = InvalidBlockNumber;
	metap->blockWidth = TQ_BLOCK_WIDTH;
	metap->blockCount = 0;
	metap->qjlScale = 0.0f;

	((PageHeader) page)->pd_lower =
		((char *) metap + sizeof(TqMetaPageData)) - (char *) page;

	TqCommitBuffer(buf, state);
}

/*
 * Write a raw byte buffer across as many linked pages as needed.
 * Returns the first block number of the chain.
 */
BlockNumber
TqWriteBytes(Relation index, ForkNumber forkNum, const char *bytes, Size nbytes,
			 uint16 pageId)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	BlockNumber startBlock;
	Size		offset = 0;

	buf = TqNewBuffer(index, forkNum);
	TqInitRegisterPage(index, &buf, &page, &state, pageId);
	startBlock = BufferGetBlockNumber(buf);

	while (offset < nbytes)
	{
		Size		avail = PageGetFreeSpace(page);
		Size		chunk;
		OffsetNumber offno;

		/* Reserve space for the item's line pointer + alignment */
		if (avail <= sizeof(ItemIdData))
		{
			TqAppendPage(index, &buf, &page, &state, forkNum, pageId);
			continue;
		}

		chunk = avail - sizeof(ItemIdData);
		chunk = chunk - (chunk % MAXIMUM_ALIGNOF);	/* keep items aligned */
		if (chunk == 0)
		{
			TqAppendPage(index, &buf, &page, &state, forkNum, pageId);
			continue;
		}
		if (chunk > nbytes - offset)
			chunk = nbytes - offset;

		offno = PageAddItem(page, (Pointer) (bytes + offset), chunk,
							InvalidOffsetNumber, false, false);
		if (offno == InvalidOffsetNumber)
			elog(ERROR, "failed to add side-page item to \"%s\"", RelationGetRelationName(index));

		offset += chunk;
	}

	TqCommitBuffer(buf, state);

	return startBlock;
}

/*
 * Append raw bytes to the streaming code-plane page chain, extending it across
 * as many linked pages as needed.  The chain is opened lazily on the first
 * call (recording its first block in buildstate->codeStart) and stays open
 * between calls; TqBuildIndex commits the final page after the scan.
 *
 * This packs bytes into page items using the same chunking as TqWriteBytes, so
 * a block's code-plane may be split across items / pages.  That is fine: the
 * reader (TqReadBytes) reassembles by concatenating item contents in chain
 * order, so the on-disk byte stream is identical to a one-shot write of the
 * full concatenation, regardless of item boundaries.
 */
static void
TqCodeAppend(TqBuildState *buildstate, const char *bytes, Size nbytes)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	Size		offset = 0;

	/* Open the code chain on the first flush; resume the parked cursor after. */
	if (buildstate->codePage == NULL)
	{
		if (BufferIsValid(buildstate->codeBuf))
			TqResumeCursor(index, buildstate->codeBuf, &buildstate->codePage,
						   &buildstate->codeState);
		else
		{
			buildstate->codeBuf = TqNewBuffer(index, forkNum);
			TqInitRegisterPage(index, &buildstate->codeBuf, &buildstate->codePage,
							   &buildstate->codeState, TQ_PAGE_ID);
			buildstate->codeStart = BufferGetBlockNumber(buildstate->codeBuf);
		}
	}

	while (offset < nbytes)
	{
		Size		avail = PageGetFreeSpace(buildstate->codePage);
		Size		chunk;
		OffsetNumber offno;

		/* Reserve space for the item's line pointer + alignment */
		if (avail <= sizeof(ItemIdData))
		{
			TqAppendPage(index, &buildstate->codeBuf, &buildstate->codePage,
						 &buildstate->codeState, forkNum, TQ_PAGE_ID);
			continue;
		}

		chunk = avail - sizeof(ItemIdData);
		chunk = chunk - (chunk % MAXIMUM_ALIGNOF);	/* keep items aligned */
		if (chunk == 0)
		{
			TqAppendPage(index, &buildstate->codeBuf, &buildstate->codePage,
						 &buildstate->codeState, forkNum, TQ_PAGE_ID);
			continue;
		}
		if (chunk > nbytes - offset)
			chunk = nbytes - offset;

		offno = PageAddItem(buildstate->codePage, (Pointer) (bytes + offset), chunk,
							InvalidOffsetNumber, false, false);
		if (offno == InvalidOffsetNumber)
			elog(ERROR, "failed to add code-plane item to \"%s\"", RelationGetRelationName(index));

		offset += chunk;
	}
}

/*
 * Position a byte stream at the head of a linked page chain.  The stream
 * reads the chain's items forward-only via TqByteStreamRead; no buffer pins
 * are held between reads.
 */
void
TqByteStreamInit(TqByteStream *bs, Relation index, BlockNumber startBlock)
{
	bs->index = index;
	bs->blkno = startBlock;
	bs->offno = FirstOffsetNumber;
	bs->itemOff = 0;
}

/*
 * Copy the next len bytes from the chain into dest, advancing the cursor
 * across items and pages as needed.  Errors out if the chain runs short.
 */
void
TqByteStreamRead(TqByteStream *bs, char *dest, Size len)
{
	Size		offset = 0;

	while (BlockNumberIsValid(bs->blkno) && offset < len)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;

		buf = ReadBuffer(bs->index, bs->blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		maxoff = PageGetMaxOffsetNumber(page);

		while (bs->offno <= maxoff && offset < len)
		{
			ItemId		iid = PageGetItemId(page, bs->offno);
			Size		itemLen = ItemIdGetLength(iid);
			char	   *item = (char *) PageGetItem(page, iid);
			Size		avail = itemLen - bs->itemOff;
			Size		chunk = Min(avail, len - offset);

			memcpy(dest + offset, item + bs->itemOff, chunk);
			offset += chunk;
			bs->itemOff += chunk;
			if (bs->itemOff >= itemLen)
			{
				bs->offno = OffsetNumberNext(bs->offno);
				bs->itemOff = 0;
			}
		}

		if (bs->offno > maxoff)
		{
			bs->blkno = TqPageGetOpaque(page)->nextblkno;
			bs->offno = FirstOffsetNumber;
			bs->itemOff = 0;
		}
		UnlockReleaseBuffer(buf);
	}

	if (offset != len)
		elog(ERROR, "index side page chain shorter than expected");
}

/*
 * Read a raw byte buffer back from a linked page chain into dest.
 */
void
TqReadBytes(Relation index, BlockNumber startBlock, char *dest, Size nbytes)
{
	TqByteStream bs;

	TqByteStreamInit(&bs, index, startBlock);
	TqByteStreamRead(&bs, dest, nbytes);
}

/*
 * Backfill the post-build meta fields: chain start blocks, nVectors,
 * fastRotation/dimPadded, block geometry, and qjlScale.
 */
static void
TqUpdateMeta(Relation index, ForkNumber forkNum, BlockNumber rotationStart,
			 BlockNumber codebookStart, BlockNumber qjlStart, float qjlScale,
			 BlockNumber dataStart, uint32 nVectors,
			 uint16 fastRotation, uint16 dimPadded,
			 BlockNumber codeStart, BlockNumber sideStart, BlockNumber tailStart,
			 uint16 blockWidth, uint32 blockCount)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	TqMetaPage	metap;

	buf = ReadBufferExtended(index, forkNum, TQ_METAPAGE_BLKNO, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	metap = TqPageGetMeta(page);

	metap->rotationStart = rotationStart;
	metap->codebookStart = codebookStart;
	metap->qjlStart = qjlStart;
	metap->qjlScale = qjlScale;
	metap->dataStart = dataStart;
	metap->nVectors = nVectors;
	metap->fastRotation = fastRotation;
	metap->dimPadded = dimPadded;
	metap->codeStart = codeStart;
	metap->sideStart = sideStart;
	metap->tailStart = tailStart;
	metap->blockWidth = blockWidth;
	metap->blockCount = blockCount;

	TqCommitBuffer(buf, state);
}

/*
 * Build the in-memory model (rotation + codebook, plus the QJL sketch when
 * tq_prod) and write its side pages.  Returns each chain's start block in
 * rotStart/cbStart/qjlStart (InvalidBlockNumber when absent, e.g. fast mode)
 * and the QJL estimator scale in qjlScale.
 */
static void
TqBuildModelAndSidePages(TqBuildState *buildstate, BlockNumber *rotStart,
						 BlockNumber *cbStart, BlockNumber *qjlStart, float *qjlScale)
{
	int			dim = buildstate->dim;
	int			bits = buildstate->bits;
	int			nLevels = buildstate->nLevels;
	int			nBnd = nLevels - 1;
	bool		fastRotation = buildstate->fastRotation;
	int			dimPadded = fastRotation ? TqNextPow2(dim) : dim;
	int			dimCodes = fastRotation ? dimPadded : dim;
	Size		rotBytes = (Size) sizeof(float) * dim * dim;
	Size		cbBytes = (Size) sizeof(float) * (nBnd + nLevels);
	char	   *cbBuf;

	/* Allocate model arrays */
	buildstate->model.dim = dim;
	buildstate->model.bits = bits;
	buildstate->model.nLevels = nLevels;
	buildstate->model.metric = buildstate->metric;
	buildstate->model.tqProd = buildstate->tqProd;
	buildstate->model.qjl = NULL;
	buildstate->model.qjlScale = 0.0f;
	buildstate->model.fastRotation = fastRotation;
	buildstate->model.dimPadded = dimPadded;
	buildstate->model.dimCodes = dimCodes;
	buildstate->model.rotation = NULL;
	buildstate->model.rotSeed = TQ_ROTATION_SEED;
	buildstate->model.qjlSeed = TQ_QJL_SEED;
	buildstate->model.boundaries = palloc(sizeof(float) * nBnd);
	buildstate->model.centroids = palloc(sizeof(float) * nLevels);

	/*
	 * Codebook coordinate density depends on the working dimension: dim in
	 * dense mode, the padded dim in fast mode (the RHT produces dimPadded
	 * near-uniform-on-sphere coordinates).
	 */
	TqBuildCodebook(dimCodes, bits, buildstate->model.boundaries, buildstate->model.centroids);

	if (fastRotation)
	{
		/*
		 * Fast mode: the rotation (and QJL) are structured RHTs computed on
		 * the fly from TQ_ROTATION_SEED / TQ_QJL_SEED, so no dense matrix is
		 * built or written.  The rotation/QJL side pages are absent.
		 */
		*rotStart = InvalidBlockNumber;
	}
	else
	{
		/*
		 * rotation is ~1 GB at TQ_MAX_DIM; relies on the index-build memory
		 * context being torn down after ambuild (mirrors ivfflat's pattern).
		 */
		buildstate->model.rotation = palloc(rotBytes);
		TqBuildRotation(dim, TQ_ROTATION_SEED, buildstate->model.rotation);
		*rotStart = TqWriteBytes(buildstate->index, buildstate->forkNum,
								 (const char *) buildstate->model.rotation, rotBytes,
								 TQ_PAGE_ID);
	}

	/* Write codebook: boundaries followed by centroids, contiguous */
	cbBuf = palloc(cbBytes);
	memcpy(cbBuf, buildstate->model.boundaries, sizeof(float) * nBnd);
	memcpy(cbBuf + sizeof(float) * nBnd, buildstate->model.centroids, sizeof(float) * nLevels);
	*cbStart = TqWriteBytes(buildstate->index, buildstate->forkNum, cbBuf, cbBytes,
							TQ_PAGE_ID);
	pfree(cbBuf);

	/*
	 * QJL matrix + estimator scale (tqProd only).  The sign estimator <q,r>
	 * ~= qjlScale * ||r|| * <S q, sign(S r)> is unbiased for a DENSE Gaussian
	 * sketch S iff qjlScale = sqrt(pi/2)/dimPadded (dimPadded == dimCodes
	 * here; derived from E[X sign(Y)] = sqrt(2/pi) Cov(X,Y)/sqrt(Var(Y)) for
	 * jointly Gaussian (X,Y), summed over dimPadded QJL coordinates). Matches
	 * turboquant TurboQuantProd.qjl_scale.  NOTE: in fast mode the structured
	 * RHT sketch is not i.i.d. Gaussian, so this estimate is biased (see the
	 * note in TqEncode); tq_prod defaults off accordingly.
	 */
	if (buildstate->tqProd)
	{
		if (fastRotation)
		{
			/* Structured QJL: no dense matrix written. */
			*qjlStart = InvalidBlockNumber;
		}
		else
		{
			buildstate->model.qjl = palloc(rotBytes);
			TqBuildQjl(dim, TQ_QJL_SEED, buildstate->model.qjl);
			*qjlStart = TqWriteBytes(buildstate->index, buildstate->forkNum,
									 (const char *) buildstate->model.qjl, rotBytes,
									 TQ_PAGE_ID);
		}
		*qjlScale = (float) (sqrt(M_PI / 2.0) / (double) dimPadded);
		buildstate->model.qjlScale = *qjlScale;
	}
	else
	{
		*qjlStart = InvalidBlockNumber;
		*qjlScale = 0.0f;
	}
}

/*
 * Append one TqBlockSideRec to the side-chain page cursor.  Records the first
 * block into *sideStart on the first append.  The side chain is page-addressable
 * (one PageAddItem item per block), unlike the raw code-plane byte stream.
 */
static void
TqAppendSideRec(TqBuildState *buildstate, BlockNumber *sideStart,
				const TqBlockSideRec *rec)
{
	Relation	index = buildstate->index;
	OffsetNumber offno;

	/* Open the side chain on first append; resume the parked cursor after. */
	if (buildstate->sidePage == NULL)
	{
		if (BufferIsValid(buildstate->sideBuf))
			TqResumeCursor(index, buildstate->sideBuf, &buildstate->sidePage,
						   &buildstate->sideState);
		else
		{
			buildstate->sideBuf = TqNewBuffer(index, buildstate->forkNum);
			TqInitRegisterPage(index, &buildstate->sideBuf, &buildstate->sidePage,
							   &buildstate->sideState, TQ_PAGE_ID);
			*sideStart = BufferGetBlockNumber(buildstate->sideBuf);
		}
	}

	if (PageGetFreeSpace(buildstate->sidePage) < sizeof(TqBlockSideRec))
		TqAppendPage(index, &buildstate->sideBuf, &buildstate->sidePage,
					 &buildstate->sideState, buildstate->forkNum, TQ_PAGE_ID);

	offno = PageAddItem(buildstate->sidePage, (Pointer) rec, sizeof(TqBlockSideRec),
						InvalidOffsetNumber, false, false);
	if (offno == InvalidOffsetNumber)
		elog(ERROR, "failed to add tqflat side record to \"%s\"",
			 RelationGetRelationName(index));
}

/*
 * Flush the currently-staged block: append its code-plane to the code-plane
 * page chain and its side record to the side chain.
 */
static void
TqFlushBlock(TqBuildState *buildstate)
{
	int			dc = buildstate->dimCodes;

	buildstate->sideStage.nvecs = (uint16) buildstate->slot;
	buildstate->sideStage.deletedMask = 0;
	buildstate->sideStage.pad = 0;

	/* Code plane -> streamed straight onto the code-plane page chain. */
	TqCodeAppend(buildstate, (char *) buildstate->codeStage,
				 TQ_BLOCK_CODE_BYTES(dc));

	/* Side record -> one addressable item in the side chain. */
	TqAppendSideRec(buildstate, &buildstate->sideStart, &buildstate->sideStage);

	/*
	 * Park both cursors (finish their WAL records, drop the content locks, keep
	 * the pins) so no buffer lock is held across the heap scan between blocks --
	 * the next flush resumes them.  This keeps a large CREATE INDEX cancellable.
	 */
	if (buildstate->codePage != NULL)
		TqParkCursor(buildstate->codeBuf, &buildstate->codePage,
					 &buildstate->codeState);
	if (buildstate->sidePage != NULL)
		TqParkCursor(buildstate->sideBuf, &buildstate->sidePage,
					 &buildstate->sideState);

	memset(buildstate->codeStage, 0, TQ_BLOCK_CODE_BYTES(dc));
	buildstate->slot = 0;
	buildstate->blockCount++;
	MemSet(&buildstate->sideStage, 0, sizeof(buildstate->sideStage));
}

/*
 * Callback for table_index_build_scan: encode one heap tuple and stage it into
 * the current block.  The block is flushed once it fills (TQ_BLOCK_WIDTH lanes).
 */
static void
TqBuildCallback(Relation index, ItemPointer tid, Datum *values,
				bool *isnull, bool tupleIsAlive, void *state)
{
	TqBuildState *buildstate = (TqBuildState *) state;
	MemoryContext oldCtx;
	int			slot;

	/* Cursors are parked between blocks, so cancel/timeout is honored here. */
	CHECK_FOR_INTERRUPTS();

	if (isnull[0])
		return;

	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	{
		const float *fv = buildstate->typeInfo->toFloat(values[0],
														buildstate->vecScratch,
														buildstate->dim);

		/*
		 * Skip zero-norm vectors under cosine (the operator returns NaN for
		 * them), mirroring hnsw/ivfflat.
		 */
		if (buildstate->model.metric == TQ_METRIC_COSINE &&
			!TqCheckNorm(fv, buildstate->dim))
		{
			MemoryContextSwitchTo(oldCtx);
			MemoryContextReset(buildstate->tmpCtx);
			return;
		}

		memset(buildstate->entry, 0, buildstate->entrySize);
		TqEncode(&buildstate->model, fv, buildstate->entry);
	}

	slot = buildstate->slot;
	TqScatterCodes(&buildstate->model, buildstate->entry->data, slot,
				   buildstate->codeStage);
	buildstate->sideStage.side[slot].heaptid = *tid;
	buildstate->sideStage.side[slot].norm = buildstate->entry->norm;
	buildstate->sideStage.side[slot].scale = buildstate->entry->scale;
	buildstate->slot++;

	MemoryContextSwitchTo(oldCtx);

	if (buildstate->slot == TQ_BLOCK_WIDTH)
		TqFlushBlock(buildstate);

	buildstate->indtuples++;
	MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Initialize the build state from index relation / options.
 */
static void
TqInitBuildState(TqBuildState *buildstate, Relation heap, Relation index,
				 IndexInfo *indexInfo, ForkNumber forkNum)
{
	TqOptions  *opts = (TqOptions *) index->rd_options;

	buildstate->heap = heap;
	buildstate->index = index;
	buildstate->indexInfo = indexInfo;
	buildstate->forkNum = forkNum;

	/*
	 * Rerank fetches the raw heap column (indkey.values[0]); an expression
	 * index has no backing attribute (attno 0) and cannot be reranked.
	 */
	if (indexInfo->ii_Expressions != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("tqflat indexes do not support expression index columns")));

	/* Dimensions from the index column typmod (mirror ivfflat) */
	buildstate->dim = TupleDescAttr(index->rd_att, 0)->atttypmod;
	if (buildstate->dim < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("column does not have dimensions")));
	if (buildstate->dim < 3)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("tqflat index requires at least 3 dimensions")));

	/* Options */
	buildstate->bits = opts ? opts->bits : TQ_DEFAULT_BITS;
	if (buildstate->bits != 4)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("tqflat blocked layout supports only bits = 4")));

	/*
	 * Defaults single-sourced with the reloption registration in tq.c so the
	 * no-WITH-clause path can't drift from `WITH (...)`.
	 */
	buildstate->tqProd = opts ? opts->tqProd : TQ_DEFAULT_TQPROD;
	if (buildstate->tqProd)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("tqflat blocked layout does not support tq_prod (QJL); use the default tq_prod = false")));
	buildstate->fastRotation = opts ? opts->fastRotation : TQ_DEFAULT_FAST_ROTATION;
	buildstate->nLevels = 1 << buildstate->bits;

	/*
	 * In fast mode all code/sign storage and quantization run over the padded
	 * dimension (next_pow2(dim)); in dense mode dimCodes == dim.
	 */
	buildstate->dimCodes = buildstate->fastRotation ?
		TqNextPow2(buildstate->dim) : buildstate->dim;

	/* Type-info vtable from the opclass support proc (default vector/L2). */
	buildstate->typeInfo = TqGetTypeInfo(index, TQ_TYPE_INFO_PROC);
	buildstate->metric = buildstate->typeInfo->metric;

	if (buildstate->dim > buildstate->typeInfo->maxDimensions)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("column cannot have more than %d dimensions for tqflat index",
						buildstate->typeInfo->maxDimensions)));

	/* heap == NULL on the ambuildempty path: no rows to densify, no warning. */
	if (buildstate->heap != NULL &&
		buildstate->typeInfo->toFloat == TqSparsevecToFloat)
		ereport(NOTICE,
				(errmsg("tq densifies sparsevec to %d dimensions; sparse storage advantage is not preserved",
						buildstate->dim)));

	buildstate->vecScratch = palloc(sizeof(float) * buildstate->dim);

	/* Entry sizing (over dimCodes: padded dim in fast mode, else dim) */
	buildstate->codesBytes = TQ_CODES_BYTES(buildstate->dimCodes, buildstate->bits);
	buildstate->entrySize = TqEntrySize(buildstate->dimCodes, buildstate->bits, buildstate->tqProd);

	/*
	 * Sanity: a row-major entry must fit on a page.  In fast_rotation mode the
	 * codes span next_pow2(dim), so a declared dim above the largest fitting
	 * power of two (8192 for 4-bit codes) overflows a page even though it is
	 * within the advertised maxDimensions -- give a specific, actionable error
	 * instead of the opaque generic one.  Dense mode (fast_rotation = false)
	 * stores codes over dim itself and reaches the full advertised range.
	 */
	if (buildstate->entrySize > TqPageCapacity() - sizeof(ItemIdData))
	{
		if (buildstate->fastRotation)
		{
			int			maxFastDim = 1;

			while (TqEntrySize(maxFastDim * 2, buildstate->bits, buildstate->tqProd)
				   <= TqPageCapacity() - sizeof(ItemIdData))
				maxFastDim *= 2;

			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("tqflat index with fast_rotation does not support more than %d dimensions",
							maxFastDim),
					 errhint("Use fast_rotation = false to index higher dimensions.")));
		}
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("tqflat entry too large for a page")));
	}

	buildstate->entry = palloc0(buildstate->entrySize);

	/* Blocked build staging */
	buildstate->codeStage = palloc0(TQ_BLOCK_CODE_BYTES(buildstate->dimCodes));
	buildstate->slot = 0;
	buildstate->blockCount = 0;
	buildstate->codeStart = InvalidBlockNumber;
	buildstate->codeBuf = InvalidBuffer;
	buildstate->codePage = NULL;
	buildstate->codeState = NULL;
	buildstate->sideStart = InvalidBlockNumber;
	buildstate->sideBuf = InvalidBuffer;
	buildstate->sidePage = NULL;
	buildstate->sideState = NULL;
	MemSet(&buildstate->sideStage, 0, sizeof(buildstate->sideStage));

	buildstate->reltuples = 0;
	buildstate->indtuples = 0;

	buildstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											   "Tqflat build temporary context",
											   ALLOCSET_DEFAULT_SIZES);
}

/*
 * Free build state resources.
 */
static void
TqFreeBuildState(TqBuildState *buildstate)
{
	MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Build the index (shared by tqbuild and tqbuildempty).
 */
static void
TqBuildIndex(Relation heap, Relation index, IndexInfo *indexInfo,
			 TqBuildState *buildstate, ForkNumber forkNum)
{
	BlockNumber rotStart;
	BlockNumber cbStart;
	BlockNumber qjlStart;
	float		qjlScale;
	BlockNumber dataStart = InvalidBlockNumber;

	TqInitBuildState(buildstate, heap, index, indexInfo, forkNum);

	/* Meta page first (block 0), then side pages */
	TqCreateMetaPage(buildstate);
	TqBuildModelAndSidePages(buildstate, &rotStart, &cbStart, &qjlStart, &qjlScale);

	/* Scan the heap and stage encoded entries into blocks (skip for empty) */
	if (heap != NULL)
		buildstate->reltuples = table_index_build_scan(heap, index, indexInfo,
													   true, true, TqBuildCallback,
													   (void *) buildstate, NULL);

	/* Flush any partial trailing block. */
	if (buildstate->slot > 0)
		TqFlushBlock(buildstate);

	/*
	 * Drop the parked cursors' pins.  TqFlushBlock parks both cursors (WAL
	 * record finished, lock released) at the end of every flush including the
	 * trailing one, so only the pins remain here; both stay InvalidBuffer when
	 * no block ever flushed (empty index, codeStart stays InvalidBlockNumber).
	 */
	if (BufferIsValid(buildstate->sideBuf))
		ReleaseBuffer(buildstate->sideBuf);
	if (BufferIsValid(buildstate->codeBuf))
		ReleaseBuffer(buildstate->codeBuf);

	/* Backfill meta page */
	TqUpdateMeta(index, forkNum, rotStart, cbStart, qjlStart, qjlScale, dataStart,
				 (uint32) buildstate->indtuples,
				 (uint16) (buildstate->model.fastRotation ? 1 : 0),
				 (uint16) buildstate->model.dimPadded,
				 buildstate->codeStart, buildstate->sideStart, InvalidBlockNumber,
				 TQ_BLOCK_WIDTH, buildstate->blockCount);

	/* Write WAL for init fork since GenericXLog does not */
	if (forkNum == INIT_FORKNUM)
		log_newpage_range(index, forkNum, 0, RelationGetNumberOfBlocksInFork(index, forkNum), true);

	TqFreeBuildState(buildstate);
}

/*
 * tqbuild -- build a tqflat index from scratch.
 */
IndexBuildResult *
tqbuild(Relation heap, Relation index, struct IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	TqBuildState buildstate;

	TqBuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}

/*
 * tqbuildempty -- create an empty tqflat index (for UNLOGGED tables).
 */
void
tqbuildempty(Relation index)
{
	IndexInfo  *indexInfo = BuildIndexInfo(index);
	TqBuildState buildstate;

	TqBuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}

/*
 * TqGetCachedModel -- load the TqModel from rd_amcache, or (re)load it.
 *
 * The model is allocated in index->rd_indexcxt (the per-index relcache memory
 * context) with a pointer cached in rd_amcache.  Postgres sets rd_amcache =
 * NULL when the relcache entry is invalidated, so the next call reloads
 * automatically (see the RelationData comments in utils/rel.h).
 *
 * TqLoadModel allocates the whole model -- header plus the boundaries,
 * centroids, rotation, and qjl arrays -- as ONE contiguous chunk (see
 * TqAllocModel), so rd_amcache points at a single palloc'd chunk as the
 * relcache contract requires: a relcache invalidation that pfree's rd_amcache
 * (or resets rd_indexcxt) frees everything with no leak.
 */
TqModel *
TqGetCachedModel(Relation index)
{
	if (index->rd_amcache != NULL)
		return (TqModel *) index->rd_amcache;

	index->rd_amcache = TqLoadModel(index, index->rd_indexcxt);
	return (TqModel *) index->rd_amcache;
}

/*
 * TqFindInsertPage -- walk the data-page chain and return the last page's
 * block number (the one to try inserting into first).
 *
 * Design note: we walk the whole chain from dataStart each time rather than
 * caching an insertPage in the meta page.  This is O(pages), not O(tuples)
 * (each page holds many entries), so it is acceptable.  An insertPage field
 * (like ivfflat's list->insertPage) would make it O(1) but needs a meta-format
 * change plus an insertPage update on every insert; left as future work.
 */
static BlockNumber
TqFindInsertPage(Relation index, BlockNumber dataStart)
{
	BlockNumber blkno = dataStart;
	BlockNumber lastblkno = dataStart;

	while (BlockNumberIsValid(blkno))
	{
		Buffer		buf;
		Page		page;
		BlockNumber next;

		buf = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		next = TqPageGetOpaque(page)->nextblkno;
		UnlockReleaseBuffer(buf);

		lastblkno = blkno;
		if (!BlockNumberIsValid(next))
			break;
		blkno = next;
	}

	return lastblkno;
}

/*
 * tqinsert -- insert one tuple into an existing tqflat index.
 *
 * Mirrors ivfflatinsert structure:
 *   1. Skip nulls.
 *   2. Load (or reuse cached) TqModel.
 *   3. Encode the vector into a TqEntry.
 *   4. Find the tail data page; if the entry fits, PageAddItem it there.
 *      If not, allocate a new page, link it, and add there.
 *   5. Increment nVectors in the meta page under a separate GenericXLog.
 *
 * Buffer lock ordering to avoid deadlock / assertion failures:
 *   We never hold more than one data-page buffer at a time during the
 *   page-link step.  The meta-page update is done after fully releasing
 *   the data-page buffer, matching ivfflat's IvfflatUpdateList pattern.
 */
bool
tqinsert(Relation index, Datum *values, bool *isnull,
		 ItemPointer heap_tid, Relation heap,
		 IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
		 ,bool indexUnchanged
#endif
		 ,struct IndexInfo *indexInfo
)
{
	TqModel    *model;
	TqEntry    *entry;
	Size		entrySize;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	BlockNumber tailStart;
	BlockNumber insertPage;
	MemoryContext insertCtx;
	MemoryContext oldCtx;

	/* Skip nulls (mirror ivfflat) */
	if (isnull[0])
		return false;

	/*
	 * Use a short-lived memory context for the scratch entry (mirrors
	 * ivfflatinsert's insertCtx).
	 */
	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Tqflat insert temporary context",
									  ALLOCSET_DEFAULT_SIZES);
	oldCtx = MemoryContextSwitchTo(insertCtx);

	/* Load (or reuse cached) model */
	model = TqGetCachedModel(index);

	/* Allocate and zero the scratch entry (sized over dimCodes) */
	entrySize = TqEntrySize(model->dimCodes, model->bits, model->tqProd);
	entry = palloc0(entrySize);
	entry->heaptid = *heap_tid;
	entry->deleted = 0;

	/* Encode: plain extraction (no normalize) so norm is stored correctly. */
	{
		const TqTypeInfo *ti = TqGetTypeInfo(index, TQ_TYPE_INFO_PROC);
		float	   *scratch = palloc(sizeof(float) * model->dim);
		const float *fv = ti->toFloat(values[0], scratch, model->dim);

		/*
		 * Skip zero-norm vectors under cosine (the operator returns NaN for
		 * them), mirroring hnsw/ivfflat.
		 */
		if (model->metric == TQ_METRIC_COSINE && !TqCheckNorm(fv, model->dim))
		{
			MemoryContextSwitchTo(oldCtx);
			MemoryContextDelete(insertCtx);
			return false;
		}

		TqEncode(model, fv, entry);
	}

	MemoryContextSwitchTo(oldCtx);

	/*
	 * ---- Find the head of the row-major insert tail chain ----
	 *
	 * In the blocked format the built rows live in the block code-plane /
	 * side chains; freshly inserted rows go to a separate row-major "tail"
	 * chain (metap->tailStart) that the scan reads with TqScoreEntry.  The
	 * tail chain is created lazily on the first insert, so tailStart is
	 * InvalidBlockNumber until then.
	 */
	{
		Buffer		metabuf;
		Page		metapage;
		TqMetaPage	metap;

		metabuf = ReadBuffer(index, TQ_METAPAGE_BLKNO);
		LockBuffer(metabuf, BUFFER_LOCK_SHARE);
		metapage = BufferGetPage(metabuf);
		metap = TqPageGetMeta(metapage);
		tailStart = metap->tailStart;
		UnlockReleaseBuffer(metabuf);
	}

	/*
	 * tqinsert only writes MAIN_FORKNUM; the init fork is written only by
	 * tqbuildempty.
	 */
	if (!BlockNumberIsValid(tailStart))
	{
		/* First insert: create the tail chain's first page. */
		Buffer		newbuf;
		Page		newpage;
		GenericXLogState *newstate;
		BlockNumber newblk;
		OffsetNumber offno;

		LockRelationForExtension(index, ExclusiveLock);
		newbuf = TqNewBuffer(index, MAIN_FORKNUM);
		UnlockRelationForExtension(index, ExclusiveLock);

		newblk = BufferGetBlockNumber(newbuf);
		newstate = GenericXLogStart(index);
		newpage = GenericXLogRegisterBuffer(newstate, newbuf, GENERIC_XLOG_FULL_IMAGE);
		TqInitPage(newbuf, newpage, TQ_PAGE_ID);
		TqPageGetOpaque(newpage)->nextblkno = InvalidBlockNumber;
		offno = PageAddItem(newpage, (Pointer) entry, entrySize,
							InvalidOffsetNumber, false, false);
		if (offno == InvalidOffsetNumber)
		{
			GenericXLogAbort(newstate);
			UnlockReleaseBuffer(newbuf);
			elog(ERROR, "failed to add tqflat tail item on first insert");
		}
		GenericXLogFinish(newstate);
		UnlockReleaseBuffer(newbuf);

		/* Record tailStart (and bump nVectors) in the meta page. */
		{
			Buffer		mbuf;
			Page		mpage;
			GenericXLogState *mstate;
			TqMetaPage	mp;

			mbuf = ReadBuffer(index, TQ_METAPAGE_BLKNO);
			LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
			mstate = GenericXLogStart(index);
			mpage = GenericXLogRegisterBuffer(mstate, mbuf, 0);
			mp = TqPageGetMeta(mpage);

			if (BlockNumberIsValid(mp->tailStart))
			{
				/*
				 * Another session won the concurrent first-insert race: it
				 * acquired this exclusive meta lock first and already set
				 * tailStart.  Our newblk page is committed but unreferenced;
				 * it becomes a stranded page reclaimed at REINDEX (same
				 * trade-off as ivfflat).  Abort our meta xlog, release, and
				 * fall through to the normal append path so our entry is not
				 * lost.
				 */
				tailStart = mp->tailStart;	/* capture before release */
				GenericXLogAbort(mstate);
				UnlockReleaseBuffer(mbuf);

				/* Append our entry via the normal tail-walk path. */
				insertPage = TqFindInsertPage(index, tailStart);
				goto append_entry;
			}

			mp->tailStart = newblk;
			mp->nVectors += 1;
			GenericXLogFinish(mstate);
			UnlockReleaseBuffer(mbuf);
		}

		goto insert_done;
	}

	/* Normal path: tail chain already exists. */
	insertPage = TqFindInsertPage(index, tailStart);

append_entry:

	/*
	 * Append to the tail page (or a new page), mirroring ivfinsert's loop.
	 * The for(;;) retry is needed because TqFindInsertPage walks the chain
	 * under share locks and releases them before we take the exclusive lock
	 * here.  Between that walk and our exclusive lock, a concurrent inserter
	 * may have already extended the tail (set nextblkno to a valid page).
	 * Re-checking nextblkno after the exclusive lock avoids overwriting that
	 * link (which would orphan the newer page and lose its entries).
	 */
	for (;;)
	{
		OffsetNumber offno;
		BlockNumber nextblkno;

		buf = ReadBuffer(index, insertPage);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		if (PageGetFreeSpace(page) >= entrySize)
		{
			/* Entry fits on this page */
			offno = PageAddItem(page, (Pointer) entry, entrySize,
								InvalidOffsetNumber, false, false);
			if (offno == InvalidOffsetNumber)
			{
				GenericXLogAbort(state);
				UnlockReleaseBuffer(buf);
				elog(ERROR, "failed to add tqflat data item to \"%s\"",
					 RelationGetRelationName(index));
			}

			GenericXLogFinish(state);
			UnlockReleaseBuffer(buf);
			break;
		}

		nextblkno = TqPageGetOpaque(page)->nextblkno;

		if (BlockNumberIsValid(nextblkno))
		{
			/*
			 * Concurrent inserter already extended the tail while we were
			 * waiting for the exclusive lock.  Abort our xlog state and
			 * follow the link instead of overwriting it (which would orphan
			 * the newer page).
			 */
			GenericXLogAbort(state);
			UnlockReleaseBuffer(buf);
			insertPage = nextblkno;
			/* Loop back and try the page the concurrent inserter allocated. */
			continue;
		}

		/*
		 * No room and no concurrent extension: allocate a new page and link
		 * it.
		 */
		{
			Buffer		newbuf;
			Page		newpage;
			BlockNumber newblkno;

			LockRelationForExtension(index, ExclusiveLock);
			newbuf = TqNewBuffer(index, MAIN_FORKNUM);
			UnlockRelationForExtension(index, ExclusiveLock);

			newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);
			TqInitPage(newbuf, newpage, TQ_PAGE_ID);

			newblkno = BufferGetBlockNumber(newbuf);
			TqPageGetOpaque(page)->nextblkno = newblkno;

			/* Commit the link on the old page and the init of the new page */
			GenericXLogFinish(state);
			UnlockReleaseBuffer(buf);

			/* Now insert into the new page */
			state = GenericXLogStart(index);
			buf = newbuf;
			page = GenericXLogRegisterBuffer(state, buf, 0);

			offno = PageAddItem(page, (Pointer) entry, entrySize,
								InvalidOffsetNumber, false, false);
			if (offno == InvalidOffsetNumber)
			{
				GenericXLogAbort(state);
				UnlockReleaseBuffer(buf);
				elog(ERROR, "failed to add tqflat data item to \"%s\"",
					 RelationGetRelationName(index));
			}

			GenericXLogFinish(state);
			UnlockReleaseBuffer(buf);
			break;
		}
	}

	/* ---- Increment nVectors in the meta page ---- */
	{
		Buffer		metabuf;
		Page		metapage;
		GenericXLogState *metastate;
		TqMetaPage	metap;

		metabuf = ReadBuffer(index, TQ_METAPAGE_BLKNO);
		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		metastate = GenericXLogStart(index);
		metapage = GenericXLogRegisterBuffer(metastate, metabuf, 0);
		metap = TqPageGetMeta(metapage);
		metap->nVectors++;
		GenericXLogFinish(metastate);
		UnlockReleaseBuffer(metabuf);
	}

insert_done:
	MemoryContextDelete(insertCtx);

	return false;
}

/*
 * TqLoadModel -- load the quantization model from index meta/side pages.
 */
TqModel *
TqLoadModel(Relation index, MemoryContext ctx)
{
	Buffer		buf;
	Page		page;
	TqMetaPage	metap;
	MemoryContext oldCtx;
	TqModel    *model;
	int			dim;
	int			bits;
	int			nLevels;
	int			nBnd;
	BlockNumber rotationStart;
	BlockNumber codebookStart;
	BlockNumber qjlStart;
	float		qjlScale;
	TqMetric	metric;
	bool		tqProd;
	bool		fastRotation;
	int			dimPadded;
	Size		rotBytes;
	char	   *cbBuf;
	Size		cbBytes;

	/* Read fixed fields from the meta page under a share lock */
	buf = ReadBuffer(index, TQ_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = TqPageGetMeta(page);

	if (unlikely(metap->magicNumber != TQ_MAGIC_NUMBER))
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqflat index is not valid");
	}

	if (unlikely(metap->version != TQ_VERSION))
	{
		uint32		v = metap->version;

		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqflat index version %u not supported (expected %u)", v, TQ_VERSION);
	}

	dim = metap->dimensions;
	bits = metap->bits;
	nLevels = metap->nLevels;
	metric = (TqMetric) metap->metric;
	tqProd = metap->tqProd != 0;
	fastRotation = metap->fastRotation ? true : false;
	rotationStart = metap->rotationStart;
	codebookStart = metap->codebookStart;
	qjlStart = metap->qjlStart;
	qjlScale = metap->qjlScale;

	/*
	 * The meta always stores a positive dimPadded (= dim in dense mode); the
	 * fallback is defensive for any zero-valued legacy/partial meta.
	 */
	Assert(metap->dimPadded > 0);
	dimPadded = metap->dimPadded ? (int) metap->dimPadded : dim;
	UnlockReleaseBuffer(buf);

	nBnd = nLevels - 1;
	rotBytes = (Size) sizeof(float) * dim * dim;
	cbBytes = (Size) sizeof(float) * (nBnd + nLevels);

	oldCtx = MemoryContextSwitchTo(ctx);

	/*
	 * One contiguous chunk (header + boundaries + centroids + rotation + qjl)
	 * so the cached model honors the rd_amcache single-chunk contract; the
	 * array fields are pre-pointed into the trailing bytes (NULL in fast mode
	 * for rotation/qjl).
	 */
	model = TqAllocModel(ctx, dim, nLevels, !fastRotation, tqProd && !fastRotation);
	model->dim = dim;
	model->bits = bits;
	model->nLevels = nLevels;
	model->metric = metric;
	model->tqProd = tqProd;
	model->fastRotation = fastRotation;
	model->dimPadded = dimPadded;
	model->dimCodes = fastRotation ? dimPadded : dim;
	model->rotSeed = TQ_ROTATION_SEED;
	model->qjlSeed = TQ_QJL_SEED;
	/* qjlScale is recomputed over dimPadded in fast mode (below). */
	model->qjlScale = tqProd ? qjlScale : 0.0f;

	MemoryContextSwitchTo(oldCtx);

	/*
	 * In fast mode qjlScale is not stored (the structured sketch spans
	 * dimPadded, not dim), so recompute it over dimPadded to match the build.
	 */
	if (fastRotation)
		model->qjlScale = tqProd ? (float) (sqrt(M_PI / 2.0) / (double) dimPadded) : 0.0f;

	/*
	 * Read the rotation side page back (dense mode only; absent in fast
	 * mode).
	 */
	if (!fastRotation)
	{
		if (!BlockNumberIsValid(rotationStart))
			elog(ERROR, "tqflat index has no rotation matrix");
		TqReadBytes(index, rotationStart, (char *) model->rotation, rotBytes);
	}

	if (!BlockNumberIsValid(codebookStart))
		elog(ERROR, "tqflat index has no codebook");

	/*
	 * cbBuf scratch lands in the caller's current context and is pfree'd
	 * before return.
	 */
	cbBuf = palloc(cbBytes);
	TqReadBytes(index, codebookStart, cbBuf, cbBytes);
	memcpy(model->boundaries, cbBuf, sizeof(float) * nBnd);
	memcpy(model->centroids, cbBuf + sizeof(float) * nBnd, sizeof(float) * nLevels);
	pfree(cbBuf);

	/* Read the QJL matrix back (tqProd, dense mode only). */
	if (tqProd && !fastRotation)
	{
		if (!BlockNumberIsValid(qjlStart))
			elog(ERROR, "tqflat index has tq_prod set but no QJL matrix");
		TqReadBytes(index, qjlStart, (char *) model->qjl, rotBytes);
	}

	return model;
}

/*
 * TqGetMetaInfo -- read fixed fields from the index meta page.
 */
void
TqGetMetaInfo(Relation index, int *dim, int *bits, TqMetric *metric, bool *tqProd)
{
	Buffer		buf;
	Page		page;
	TqMetaPage	metap;

	buf = ReadBuffer(index, TQ_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = TqPageGetMeta(page);

	if (unlikely(metap->magicNumber != TQ_MAGIC_NUMBER))
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqflat index is not valid");
	}

	if (unlikely(metap->version != TQ_VERSION))
	{
		uint32		v = metap->version;

		UnlockReleaseBuffer(buf);
		elog(ERROR, "tqflat index version %u not supported (expected %u)", v, TQ_VERSION);
	}

	if (dim != NULL)
		*dim = metap->dimensions;
	if (bits != NULL)
		*bits = metap->bits;
	if (metric != NULL)
		*metric = (TqMetric) metap->metric;
	if (tqProd != NULL)
		*tqProd = metap->tqProd != 0;

	UnlockReleaseBuffer(buf);
}

/*
 * tqflat_test_meta(regclass) RETURNS int[]
 *
 * Test-only wrapper: returns {dim, bits, metric, tqProd, nVectors,
 * fastRotation, dimPadded, blockWidth, blockCount} read from the meta page of a
 * tqflat index.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(tqflat_test_meta);
Datum
tqflat_test_meta(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	Relation	index;
	int			dim;
	int			bits;
	TqMetric	metric;
	bool		tqProd;
	uint32		nVectors;
	uint16		fastRotation;
	uint16		dimPadded;
	uint16		blockWidth;
	uint32		blockCount;
	Datum		elems[9];
	ArrayType  *result;
	Buffer		buf;
	Page		page;
	TqMetaPage	metap;

	index = index_open(indexoid, AccessShareLock);

	TqGetMetaInfo(index, &dim, &bits, &metric, &tqProd);

	/*
	 * nVectors, fastRotation, dimPadded not returned by TqGetMetaInfo; read
	 * directly
	 */
	buf = ReadBuffer(index, TQ_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = TqPageGetMeta(page);
	nVectors = metap->nVectors;
	fastRotation = metap->fastRotation;
	dimPadded = metap->dimPadded;
	blockWidth = metap->blockWidth;
	blockCount = metap->blockCount;
	UnlockReleaseBuffer(buf);

	index_close(index, AccessShareLock);

	elems[0] = Int32GetDatum(dim);
	elems[1] = Int32GetDatum(bits);
	elems[2] = Int32GetDatum((int) metric);
	elems[3] = Int32GetDatum(tqProd ? 1 : 0);
	elems[4] = Int32GetDatum((int) nVectors);
	elems[5] = Int32GetDatum((int) fastRotation);
	elems[6] = Int32GetDatum((int) dimPadded);
	elems[7] = Int32GetDatum((int) blockWidth);
	elems[8] = Int32GetDatum((int) blockCount);

	result = construct_array(elems, 9, INT4OID, sizeof(int32), true, TYPALIGN_INT);

	PG_RETURN_ARRAYTYPE_P(result);
}
