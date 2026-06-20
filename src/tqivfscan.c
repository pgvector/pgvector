#include "postgres.h"

#include <float.h>
#include <math.h>

#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/tupdesc.h"
#include "catalog/index.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "lib/pairingheap.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "tqivf.h"
#include "utils/float.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tuplesort.h"
#include "vector.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

#define GetTqivfScanList(ptr) pairingheap_container(TqivfScanList, ph_node, ptr)
#define GetTqivfScanListConst(ptr) pairingheap_const_container(TqivfScanList, ph_node, ptr)

/*
 * One scored candidate.  dist is the value we MINIMIZE (smaller == closer):
 * the exact FUNCTION 1 distance for the reranked subset, otherwise the
 * quantized estimate.  Mirrors tqscan's TqScanResult.
 */
typedef struct TqivfScanResult
{
	double		dist;
	ItemPointerData tid;
} TqivfScanResult;

/*
 * Scan state.  Mirrors tqscan's TqScanOpaqueData (model, per-query LUT, exact
 * distance procinfo + collation for rerank, ||q||^2, ordered results + cursor,
 * lazy heap-fetch handle) plus the IVF probe-selection fields (probes /
 * maxProbes / lists, list-directory head, the ordered probed-list array, and a
 * pairing heap used during probe selection).
 */
typedef struct TqivfScanOpaqueData
{
	TqModel    *model;
	TqMetric	metric;
	bool		first;

	/* Exact distance (FUNCTION 1) for rerank + probe selection. */
	FmgrInfo   *procinfo;
	Oid			collation;

	/* Per-query state (rebuilt on each rescan). */
	float	   *lut;			/* dimCodes * nLevels */
	uint8	   *lut8;			/* 8-bit query LUT for the block kernel */
	float		lutBias;		/* affine recovery: mse = lutScale*sum +
								 * dc*lutBias */
	float		lutScale;
	const TqTypeInfo *typeInfo; /* tqivf type-info vtable (extractor +
								 * normalize) */
	Datum		queryDatum;		/* native query (normalized for cosine), for
								 * rerank/probe */
	bool		haveQuery;
	float	   *vecScratch;		/* dim floats (rebuilt per rescan) for the LUT */
	double		qNormSq;		/* ||q||^2 (1.0 for cosine after normalize) */

	/*
	 * Per-batch candidate stream: a work_mem-bounded, disk-spilling tuplesort
	 * (mirrors tqscan / ivfscan) instead of an unbounded in-memory array, so
	 * scan memory does not grow with a probed-list batch.  Lives OUTSIDE tmpCtx
	 * -- rescan resets tmpCtx but only tuplesort_reset's the sort, and each
	 * TqivfLoadBatch tuplesort_reset's it for the next batch.
	 */
	TupleDesc	tupdesc;
	Tuplesortstate *sortstate;
	TupleTableSlot *vslot;		/* virtual slot for puttuple */
	TupleTableSlot *mslot;		/* minimal-tuple slot for gettuple */
	bool		streamExhausted;
	bool		streamHeadValid;
	TqivfScanResult streamHead; /* buffered head of the sorted stream */

	/*
	 * Reranked top-K with exact distances (K <= tqivf.rerank, so bounded by the
	 * GUC).  Sorted ascending; gettuple merges it with the remaining
	 * estimate-ordered stream, which yields the same per-batch order as the
	 * previous sort-everything-mixed approach.  Allocated in tmpCtx.
	 */
	TqivfScanResult *rerank;
	int64		nrerank;
	int64		rerankCursor;

	/* Heap access for rerank. */
	Relation	heapRel;		/* opened here iff heapOpened */
	bool		heapOpened;
	IndexFetchTableData *fetch;
	TupleTableSlot *slot;
	AttrNumber	heapAttno;		/* heap attribute backing the index column */

	/* IVF probe selection. */
	int			probes;			/* lists scored per batch */
	int			maxProbes;		/* total lists scored across all batches */
	int			lists;			/* total lists in the index */
	BlockNumber listStart;		/* first list-directory page */
	pairingheap *listQueue;		/* max-heap during probe selection */
	TqivfScanList *probeLists;	/* materialized, ascending distance */
	int			nProbeLists;	/* number of entries in probeLists */
	int			listIndex;		/* next probed list to score (iterative scan) */

	MemoryContext tmpCtx;
} TqivfScanOpaqueData;

typedef TqivfScanOpaqueData *TqivfScanOpaque;

/*
 * qsort comparator: ascending distance.
 */
static int
CompareResults(const void *a, const void *b)
{
	double		da = ((const TqivfScanResult *) a)->dist;
	double		db = ((const TqivfScanResult *) b)->dist;

	if (da < db)
		return -1;
	if (da > db)
		return 1;
	return 0;
}

/*
 * Feed one scored candidate into the scan's tuplesort (mirrors tqscan's
 * TqAddCandidate).  tid may point into a locked page: tuplesort copies the
 * slot into a minimal tuple immediately.
 */
static inline void
TqivfAddCandidate(TqivfScanOpaque so, double dist, ItemPointer tid)
{
	ExecClearTuple(so->vslot);
	so->vslot->tts_values[0] = Float8GetDatum(dist);
	so->vslot->tts_isnull[0] = false;
	so->vslot->tts_values[1] = PointerGetDatum(tid);
	so->vslot->tts_isnull[1] = false;
	ExecStoreVirtualTuple(so->vslot);

	tuplesort_puttupleslot(so->sortstate, so->vslot);
}

/*
 * Pull the next (dist, tid) off the sorted stream (mirrors tqscan's
 * TqStreamNext).  Decodes immediately: the slot's contents are only valid
 * until the next gettupleslot call.
 */
static bool
TqivfStreamNext(TqivfScanOpaque so, TqivfScanResult *out)
{
	bool		isnull;

	if (!tuplesort_gettupleslot(so->sortstate, true, false, so->mslot, NULL))
		return false;

	out->dist = DatumGetFloat8(slot_getattr(so->mslot, 1, &isnull));
	out->tid = *((ItemPointer) DatumGetPointer(slot_getattr(so->mslot, 2, &isnull)));
	return true;
}

/*
 * Pairing-heap comparator for probe selection: a max-heap keyed on centroid
 * distance, so the farthest kept list sits at the root and is the first
 * evicted.  Mirrors ivfscan's CompareLists.
 */
static int
CompareLists(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (GetTqivfScanListConst(a)->distance > GetTqivfScanListConst(b)->distance)
		return 1;
	if (GetTqivfScanListConst(a)->distance < GetTqivfScanListConst(b)->distance)
		return -1;
	return 0;
}

/*
 * Convert a raw inner-product estimate to a distance to MINIMIZE, taking the
 * stored norm directly.  MUST mirror tqscan's TqEstToDist / TqEstToDistSide
 * formulas EXACTLY: IP -> -est; L2 -> qNormSq + norm^2 - 2*est; cosine ->
 * 1 - est/norm.
 */
static inline double
TqivfEstToDist(TqMetric metric, double qNormSq, float norm, float est)
{
	switch (metric)
	{
		case TQ_METRIC_IP:
			return -(double) est;
		case TQ_METRIC_COSINE:
			{
				double		n = (double) norm;

				if (n < 1e-12)
					return 1.0;
				return 1.0 - (double) est / n;
			}
		case TQ_METRIC_L2:
		default:
			return qNormSq + (double) norm * (double) norm - 2.0 * (double) est;
	}
}

/*
 * Fetch the original vector for tid from the heap.  Returns a palloc'd copy in
 * the current memory context, or NULL if the tuple is no longer visible.
 * Replicates tqscan's TqHeapFetchDatum scoped to TqivfScanOpaque.
 */
static Datum
TqivfHeapFetchDatum(IndexScanDesc scan, TqivfScanOpaque so, ItemPointer tid)
{
	bool		call_again = false;
	bool		all_dead = false;
	Datum		datum;
	bool		isnull;

	if (so->fetch == NULL)
	{
		Relation	heap = scan->heapRelation;

		if (heap == NULL)
		{
			Oid			heapoid = IndexGetRelation(RelationGetRelid(scan->indexRelation), false);

			so->heapRel = table_open(heapoid, AccessShareLock);
			so->heapOpened = true;
			heap = so->heapRel;
		}
		else
			so->heapRel = heap;

		so->fetch = TqTableIndexFetchBegin(heap);
		so->slot = table_slot_create(heap, NULL);
	}

	ExecClearTuple(so->slot);
	if (!table_index_fetch_tuple(so->fetch, tid, scan->xs_snapshot, so->slot,
								 &call_again, &all_dead))
		return (Datum) 0;

	datum = slot_getattr(so->slot, so->heapAttno, &isnull);
	if (isnull)
		return (Datum) 0;

	/* palloc'd, fully-detoasted copy that outlives the slot. */
	return PointerGetDatum(PG_DETOAST_DATUM_COPY(datum));
}

/*
 * TqivfGetScanLists -- probe selection.  Walk the list-directory chain, compute
 * the exact query->centroid distance for each list, and keep the nearest
 * maxProbes in a max-heap (evicting the farthest).  Materialize the kept lists
 * into probeLists ordered by ascending distance.  Mirrors ivfscan's
 * GetScanLists.
 */
static void
TqivfGetScanLists(IndexScanDesc scan)
{
	TqivfScanOpaque so = (TqivfScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	BlockNumber blkno = so->listStart;
	int			listCount = 0;
	double		maxDistance = DBL_MAX;
	MemoryContext oldCtx;
	int			i;

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);

	so->probeLists = palloc(so->maxProbes * sizeof(TqivfScanList));
	pairingheap_reset(so->listQueue);

	while (BlockNumberIsValid(blkno))
	{
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber offno;
		BlockNumber nextblk;

		buf = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		maxoff = PageGetMaxOffsetNumber(page);

		for (offno = FirstOffsetNumber; offno <= maxoff; offno = OffsetNumberNext(offno))
		{
			TqivfList	list = (TqivfList) PageGetItem(page, PageGetItemId(page, offno));
			double		distance;

			/*
			 * Exact query->centroid distance via FUNCTION 1.  A NULL order-by
			 * key probes with zero distance for every list (mirrors ivfflat's
			 * ZeroDistance), keeping the first maxProbes lists.
			 */
			if (so->haveQuery)
				distance = DatumGetFloat8(FunctionCall2Coll(so->procinfo, so->collation,
															PointerGetDatum(&list->center),
															so->queryDatum));
			else
				distance = 0.0;

			if (listCount < so->maxProbes)
			{
				TqivfScanList *sl = &so->probeLists[listCount];

				sl->codeStart = list->codeStart;
				sl->sideStart = list->sideStart;
				sl->tailStart = list->tailStart;
				sl->blockCount = list->blockCount;
				sl->nvectors = list->nvectors;
				sl->distance = distance;
				listCount++;

				pairingheap_add(so->listQueue, &sl->ph_node);

				if (listCount == so->maxProbes)
					maxDistance = GetTqivfScanList(pairingheap_first(so->listQueue))->distance;
			}
			else if (distance < maxDistance)
			{
				TqivfScanList *sl;

				/* Evict the farthest, reuse its slot. */
				sl = GetTqivfScanList(pairingheap_remove_first(so->listQueue));
				sl->codeStart = list->codeStart;
				sl->sideStart = list->sideStart;
				sl->tailStart = list->tailStart;
				sl->blockCount = list->blockCount;
				sl->nvectors = list->nvectors;
				sl->distance = distance;
				pairingheap_add(so->listQueue, &sl->ph_node);

				maxDistance = GetTqivfScanList(pairingheap_first(so->listQueue))->distance;
			}
		}

		nextblk = TqPageGetOpaque(page)->nextblkno;
		UnlockReleaseBuffer(buf);
		blkno = nextblk;
	}

	/*
	 * Drain the max-heap into a temporary array (the heap nodes alias the
	 * probeLists slots), then copy back in ascending distance order. Removing
	 * from a max-heap yields descending distance, so fill from the back.
	 */
	{
		TqivfScanList *ordered = palloc(listCount * sizeof(TqivfScanList));

		for (i = listCount - 1; i >= 0; i--)
			ordered[i] = *GetTqivfScanList(pairingheap_remove_first(so->listQueue));

		Assert(pairingheap_is_empty(so->listQueue));

		for (i = 0; i < listCount; i++)
			so->probeLists[i] = ordered[i];

		pfree(ordered);
	}

	so->nProbeLists = listCount;

	MemoryContextSwitchTo(oldCtx);
}

/*
 * TqivfScoreList -- score one probed list.  Replicates tqscan's TqDoScan block
 * + tail loops, scoped to this list's chains, feeding each candidate into the
 * scan's per-batch tuplesort via TqivfAddCandidate.  Returns the number of
 * candidates fed (the caller sums these to cap the rerank top-K, mirroring
 * tqscan's inline candidate count).  Must be called with the scan's tmpCtx
 * current (caller switches).
 */
static int64
TqivfScoreList(IndexScanDesc scan, TqivfScanList *L)
{
	TqivfScanOpaque so = (TqivfScanOpaque) scan->opaque;
	TqModel    *model = so->model;
	Relation	index = scan->indexRelation;
	int			dc = model->dimCodes;
	int64		n = 0;

	/* Block scan: read this list's code-plane chain, walk its side chain. */
	if (L->blockCount > 0 && BlockNumberIsValid(L->codeStart))
	{
		Size		blockCodeBytes = TQ_BLOCK_CODE_BYTES(dc);
		char	   *planeBuf = NULL;
		TqByteStream codeStream;
		BlockNumber sblk = L->sideStart;
		uint32		b PG_USED_FOR_ASSERTS_ONLY = 0;

		/*
		 * Stream the code-plane chain one block at a time (mirrors tqscan).
		 * The side chain is walked in build order, so block b's plane is the
		 * b-th read; per-list scan memory stays one block regardless of list
		 * size, instead of materializing the whole plane.  Skipped for a NULL
		 * order-by key (nothing is scored).
		 */
		if (so->haveQuery)
		{
			planeBuf = (char *) palloc(blockCodeBytes);
			TqByteStreamInit(&codeStream, index, L->codeStart);
		}

		while (BlockNumberIsValid(sblk))
		{
			Buffer		sbuf;
			Page		spage;
			OffsetNumber soff,
						smax;
			BlockNumber nextblk;

			CHECK_FOR_INTERRUPTS();

			sbuf = ReadBuffer(index, sblk);
			LockBuffer(sbuf, BUFFER_LOCK_SHARE);
			spage = BufferGetPage(sbuf);
			smax = PageGetMaxOffsetNumber(spage);

			for (soff = FirstOffsetNumber; soff <= smax; soff = OffsetNumberNext(soff))
			{
				TqBlockSideRec *srec = (TqBlockSideRec *) PageGetItem(spage, PageGetItemId(spage, soff));
				const uint8 *plane = NULL;

				if (so->haveQuery)
				{
					TqByteStreamRead(&codeStream, planeBuf, blockCodeBytes);
					plane = (const uint8 *) planeBuf;
				}

				if (!so->haveQuery)
				{
					/*
					 * NULL order-by key: every distance is NULL, but rows
					 * must still be returned (mirrors ivfflat's ZeroDistance
					 * path).
					 */
					for (int j = 0; j < srec->nvecs; j++)
					{
						if (srec->deletedMask & (1u << j))
							continue;

						TqivfAddCandidate(so, 0.0, &srec->side[j].heaptid);
						n++;
					}
				}
				else if (tqivf_force_scalar)
				{
					int			nLevels = model->nLevels;
					int			j;

					for (j = 0; j < srec->nvecs; j++)
					{
						int			lane = j & 15;
						bool		high = j >= 16;
						double		mse = 0.0;
						float		est;
						TqBlockSide *sd;
						int			ii;

						if (srec->deletedMask & (1u << j))
							continue;

						for (ii = 0; ii < dc; ii++)
						{
							uint8		cellb = plane[(Size) ii * 16 + lane];
							uint8		code = high ? (uint8) (cellb >> 4) : (uint8) (cellb & 0x0F);

							mse += (double) so->lut[(Size) ii * nLevels + code];
						}
						sd = &srec->side[j];
						est = (float) ((double) sd->scale * mse);

						TqivfAddCandidate(so, TqivfEstToDist(so->metric, so->qNormSq, sd->norm, est), &sd->heaptid);
						n++;
					}
				}
				else
				{
					TqBlockAccum acc;
					int			j;

					TqBlockAccumInit(&acc);
					TqScoreBlockRange(so->lut8, plane, 0, dc, &acc);
					TqBlockAccumFinish(&acc);

					for (j = 0; j < srec->nvecs; j++)
					{
						double		mse;
						float		est;
						TqBlockSide *sd;

						if (srec->deletedMask & (1u << j))
							continue;

						sd = &srec->side[j];
						mse = (double) so->lutScale * acc.acc32[j] + (double) dc * so->lutBias;
						est = (float) ((double) sd->scale * mse);

						TqivfAddCandidate(so, TqivfEstToDist(so->metric, so->qNormSq, sd->norm, est), &sd->heaptid);
						n++;
					}
				}
				b++;
			}

			nextblk = TqPageGetOpaque(spage)->nextblkno;
			UnlockReleaseBuffer(sbuf);
			sblk = nextblk;
		}
		Assert(b == L->blockCount);

		if (planeBuf != NULL)
			pfree(planeBuf);
	}

	/* Tail scan: row-major insert tail (Invalid until first insert). */
	if (BlockNumberIsValid(L->tailStart))
	{
		BlockNumber tblk = L->tailStart;

		while (BlockNumberIsValid(tblk))
		{
			Buffer		tbuf;
			Page		tpage;
			OffsetNumber toff,
						tmax;
			BlockNumber nextblk;

			CHECK_FOR_INTERRUPTS();

			tbuf = ReadBuffer(index, tblk);
			LockBuffer(tbuf, BUFFER_LOCK_SHARE);
			tpage = BufferGetPage(tbuf);
			tmax = PageGetMaxOffsetNumber(tpage);

			for (toff = FirstOffsetNumber; toff <= tmax; toff = OffsetNumberNext(toff))
			{
				TqEntry    *entry = (TqEntry *) PageGetItem(tpage, PageGetItemId(tpage, toff));
				double		dist;

				if (entry->deleted)
					continue;

				if (so->haveQuery)
				{
					float		est = TqScoreEntry(model, so->lut, NULL, entry, entry->data);

					dist = TqivfEstToDist(so->metric, so->qNormSq, entry->norm, est);
				}
				else
					dist = 0.0;		/* NULL order-by key */
				TqivfAddCandidate(so, dist, &entry->heaptid);
				n++;
			}

			nextblk = TqPageGetOpaque(tpage)->nextblkno;
			UnlockReleaseBuffer(tbuf);
			tblk = nextblk;
		}
	}

	return n;
}

/*
 * TqivfLoadBatch -- score the next batch of probed lists (up to `probes` lists,
 * advancing listIndex) into the work_mem-bounded tuplesort, sort it, and rerank
 * the top-K.  Leaves the sort positioned so gettuple's merge consumes the
 * reranked array (so->rerank) ahead of the remaining estimate stream.
 *
 * Iterative scan / relaxed_order semantics: when tqivf.iterative_scan =
 * relaxed_order, maxProbes > probes and tqivfgettuple calls this function
 * repeatedly as each batch is consumed.  Results are sorted within each
 * batch (exact distances after rerank), but rows returned from later batches
 * may rank ahead of rows already returned from earlier batches -- the output
 * is approximately ordered across batches, not globally ordered.  This
 * mirrors ivfflat's relaxed_order behaviour and is acceptable for index-scan
 * callers that apply a top-level Sort node (e.g. ORDER BY ... LIMIT).
 *
 * With iterative scan OFF, maxProbes == probes so this runs exactly once and
 * the single batch covers every probed list -- non-iterative behaviour is
 * unchanged.
 */
static void
TqivfLoadBatch(IndexScanDesc scan)
{
	TqivfScanOpaque so = (TqivfScanOpaque) scan->opaque;
	int			batchEnd;
	int			i;
	int64		n = 0;
	MemoryContext oldCtx;

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);

	/*
	 * Iterative scan re-enters here once per exhausted batch.  Empty the
	 * per-batch tuplesort for reuse and drop the previous batch's rerank
	 * array (allocated in tmpCtx) so a long scan retains only one batch at a
	 * time; the rest of tmpCtx -- probe lists, LUTs, heap-fetch state -- must
	 * survive across batches and is freed at rescan/endscan.
	 */
	tuplesort_reset(so->sortstate);
	so->streamExhausted = false;
	so->streamHeadValid = false;
	if (so->rerank != NULL)
	{
		pfree(so->rerank);
		so->rerank = NULL;
	}
	so->nrerank = 0;
	so->rerankCursor = 0;

	/*
	 * Score this batch's lists into the work_mem-bounded tuplesort (spills
	 * past work_mem instead of growing an unbounded in-memory array).  Each
	 * TqivfScoreList returns the count it fed; summing gives this batch's
	 * candidate count, used only to cap the rerank top-K (mirrors tqscan's
	 * inline candidate count).
	 */
	batchEnd = Min(so->listIndex + so->probes, so->nProbeLists);
	for (i = so->listIndex; i < batchEnd; i++)
		n += TqivfScoreList(scan, &so->probeLists[i]);
	so->listIndex = batchEnd;

	/* Sort this batch's candidates by estimated distance. */
	tuplesort_performsort(so->sortstate);

	/*
	 * Rerank: pull the top-K candidates by estimate off the sorted stream,
	 * fetch the original vector, and replace the estimate with the exact
	 * FUNCTION 1 distance.  The bounded K-array is re-sorted and gettuple
	 * merges it with the rest of the stream -- merging two ascending
	 * sequences yields the same per-batch order as the previous
	 * sort-everything-mixed approach.  Mirrors tqscan's rerank block.
	 *
	 * Documented tradeoff (inherited from tqflat): the reranked
	 * subset carries an exact FUNCTION 1 distance while the rest carry the
	 * quantized estimate; the two scales match exactly for L2 and IP, and for
	 * cosine the exact -cos(q, x) is shifted by +1 below so it sorts on the
	 * estimate's 1 - cos(q, x) scale -- the remaining difference is
	 * quantization error only.  Distances are never surfaced to the caller
	 * (the AM does not set xs_orderbyvals), so this affects only internal
	 * ordering.
	 */
	if (tqivf_rerank > 0 && n > 0 && so->haveQuery &&
		AttributeNumberIsValid(so->heapAttno))
	{
		int64		k = Min((int64) tqivf_rerank, n);
		int64		nrerank = 0;

		/* Bounded by the tqivf.rerank GUC (TQ_MAX_RERANK). */
		so->rerank = (TqivfScanResult *) palloc(sizeof(TqivfScanResult) * k);

		while (nrerank < k && TqivfStreamNext(so, &so->rerank[nrerank]))
			nrerank++;

		for (int64 ri = 0; ri < nrerank; ri++)
		{
			Datum		heapDatum;

			CHECK_FOR_INTERRUPTS();

			heapDatum = TqivfHeapFetchDatum(scan, so, &so->rerank[ri].tid);

			if (heapDatum != (Datum) 0)
			{
				Datum		cmpDatum = heapDatum;
				double		d;

				/*
				 * For cosine, normalize the heap value (type-specific) so
				 * FUNCTION 1 computes -cos(q, x), ranking identically to the
				 * estimate-based distance 1 - cos(q, x).
				 */
				if (so->metric == TQ_METRIC_COSINE && so->typeInfo->normalize != NULL)
					cmpDatum = DirectFunctionCall1Coll(so->typeInfo->normalize,
													   so->collation, heapDatum);

				d = DatumGetFloat8(FunctionCall2Coll(so->procinfo, so->collation,
													 so->queryDatum, cmpDatum));

				/*
				 * Cosine: FUNCTION 1 yields -cos on normalized inputs while
				 * the quantized estimate scale is 1 - cos; shift by 1 so the
				 * two populations sort on the same scale in the merge.
				 */
				if (so->metric == TQ_METRIC_COSINE)
					d += 1.0;

				so->rerank[ri].dist = d;

				if (cmpDatum != heapDatum)
					pfree(DatumGetPointer(cmpDatum));
				pfree(DatumGetPointer(heapDatum));
			}
			else
			{
				/* No longer visible: push to the end so it is not returned. */
				so->rerank[ri].dist = get_float8_infinity();
			}
		}

		qsort(so->rerank, nrerank, sizeof(TqivfScanResult), CompareResults);
		so->nrerank = nrerank;
	}

	/*
	 * Reset the merge cursors.  The rerank loop already pulled the top-K off
	 * the front of the sorted stream via TqivfStreamNext, so gettuple's merge
	 * resumes the estimate stream at element K+1 -- the reranked rows are not
	 * re-emitted (mirrors tqscan: no tuplesort_rescan here).
	 */
	so->rerankCursor = 0;
	so->streamExhausted = false;
	so->streamHeadValid = false;

	MemoryContextSwitchTo(oldCtx);
}

/*
 * tqivfbeginscan -- initialize a scan descriptor.  Mirrors tqbeginscan +
 * ivfflatbeginscan: load the model, resolve metric / lists / listStart, set up
 * probes / maxProbes, and look up the exact-distance procinfo for probe
 * selection and rerank.
 */
IndexScanDesc
tqivfbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	TqivfScanOpaque so;
	int			lists;
	TqMetric	metric;
	BlockNumber listStart;
	int			probes = tqivf_probes;
	int			maxProbes;
	MemoryContext oldCtx;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	TqivfGetMetaInfo(index, NULL, &metric, &lists, &listStart);

	if (tqivf_iterative_scan != TQIVF_ITERATIVE_SCAN_OFF)
		maxProbes = Max(tqivf_max_probes, probes);
	else
		maxProbes = probes;

	if (probes > lists)
		probes = lists;
	if (maxProbes > lists)
		maxProbes = lists;

	so = (TqivfScanOpaque) palloc0(sizeof(TqivfScanOpaqueData));
	so->first = true;

	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Tqivf scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	/*
	 * Per-batch candidate stream (mirrors tqbeginscan / ivfflatbeginscan).
	 * Kept outside tmpCtx: rescan resets tmpCtx but reuses the sort via
	 * tuplesort_reset, and each batch tuplesort_reset's it.
	 */
	so->tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 1, "distance", FLOAT8OID, -1, 0);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 2, "heaptid", TIDOID, -1, 0);
#if PG_VERSION_NUM >= 190000
	TupleDescFinalize(so->tupdesc);
#endif

	{
		AttrNumber	attNums[] = {1};
		Oid			sortOperators[] = {Float8LessOperator};
		Oid			sortCollations[] = {InvalidOid};
		bool		nullsFirstFlags[] = {false};

		so->sortstate = tuplesort_begin_heap(so->tupdesc, 1, attNums,
											 sortOperators, sortCollations,
											 nullsFirstFlags, work_mem, NULL, false);
	}

	/* Need separate slots for puttuple and gettuple */
	so->vslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
	so->mslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);

	/* Cached model (owned by rd_indexcxt; must NOT be freed by the scan). */
	so->model = TqivfGetCachedModel(index);
	so->metric = metric;

	/* Exact distance (FUNCTION 1) + collation for probe selection / rerank. */
	so->procinfo = index_getprocinfo(index, 1, TQIVF_DISTANCE_PROC);
	so->collation = index->rd_indcollation[0];
	so->typeInfo = TqGetTypeInfo(index, TQIVF_TYPE_INFO_PROC);

	/* Map the index column to its backing heap attribute. */
	so->heapAttno = index->rd_index->indkey.values[0];

	so->probes = probes;
	so->maxProbes = maxProbes;
	so->lists = lists;
	so->listStart = listStart;
	so->listIndex = 0;
	so->nProbeLists = 0;
	so->probeLists = NULL;

	so->lut = NULL;
	so->lut8 = NULL;
	so->queryDatum = (Datum) 0;
	so->haveQuery = false;
	so->vecScratch = NULL;
	so->qNormSq = 0.0;
	so->streamExhausted = false;
	so->streamHeadValid = false;
	so->rerank = NULL;
	so->nrerank = 0;
	so->rerankCursor = 0;
	so->heapRel = NULL;
	so->heapOpened = false;
	so->fetch = NULL;
	so->slot = NULL;

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);
	so->listQueue = pairingheap_allocate(CompareLists, scan);
	MemoryContextSwitchTo(oldCtx);

	scan->opaque = so;

	return scan;
}

/*
 * tqivfrescan -- start or restart the scan: extract and (for cosine) normalize
 * the order-by query, compute ||q||^2, and build the per-query LUT (once,
 * reused across all probed lists).
 */
void
tqivfrescan(IndexScanDesc scan, ScanKey keys, int nkeys,
			ScanKey orderbys, int norderbys)
{
	TqivfScanOpaque so = (TqivfScanOpaque) scan->opaque;
	TqModel    *model;
	MemoryContext oldCtx;

	so->first = true;
	so->listIndex = 0;
	so->nProbeLists = 0;

	/* Empty the per-batch sort for reuse; reset the merge cursors. */
	tuplesort_reset(so->sortstate);
	so->streamExhausted = false;
	so->streamHeadValid = false;
	so->rerank = NULL;
	so->nrerank = 0;
	so->rerankCursor = 0;

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));
	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));

	/* Tear down any heap-fetch state from a prior scan (lives in tmpCtx). */
	if (so->fetch != NULL)
	{
		table_index_fetch_end(so->fetch);
		so->fetch = NULL;
	}
	if (so->slot != NULL)
	{
		ExecDropSingleTupleTableSlot(so->slot);
		so->slot = NULL;
	}
	if (so->heapOpened && so->heapRel != NULL)
	{
		table_close(so->heapRel, AccessShareLock);
		so->heapOpened = false;
	}
	so->heapRel = NULL;

	/*
	 * Reset per-query allocations (LUT, queryDatum, rerank array, probeLists,
	 * listQueue). The model lives in rd_indexcxt and is unaffected. listQueue
	 * and the rerank array live in tmpCtx and are freed by the reset, so
	 * re-null them (listQueue is re-allocated afterward).
	 */
	pairingheap_reset(so->listQueue);
	MemoryContextReset(so->tmpCtx);
	so->lut = NULL;
	so->lut8 = NULL;
	so->queryDatum = (Datum) 0;
	so->haveQuery = false;
	so->vecScratch = NULL;
	so->rerank = NULL;
	so->probeLists = NULL;

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);
	so->listQueue = pairingheap_allocate(CompareLists, scan);
	MemoryContextSwitchTo(oldCtx);

	/*
	 * Re-fetch the cached model: a relcache invalidation since beginscan (or
	 * the prior rescan) frees the rd_amcache chunk, so a pointer cached for the
	 * scan's lifetime could dangle here.
	 */
	so->model = TqivfGetCachedModel(scan->indexRelation);
	model = so->model;

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);
	if (scan->orderByData != NULL &&
		!(scan->orderByData->sk_flags & SK_ISNULL))
	{
		Datum		value = scan->orderByData->sk_argument;
		const float *qfloat;
		int			i;
		double		s = 0.0;

		Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
		Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

		/*
		 * Cosine: normalize the NATIVE query (type-specific) so
		 * probe/rerank's FUNCTION 1 sees a unit query and the float LUT is
		 * built from it.
		 */
		if (so->metric == TQ_METRIC_COSINE && so->typeInfo->normalize != NULL)
			value = DirectFunctionCall1Coll(so->typeInfo->normalize, so->collation, value);

		/* Keep the native (normalized for cosine) query for probe + rerank. */
		so->queryDatum = value;
		so->haveQuery = true;

		/* Dense float query for the LUT/estimate path. */
		so->vecScratch = palloc(sizeof(float) * model->dim);
		qfloat = so->typeInfo->toFloat(value, so->vecScratch, model->dim);

		for (i = 0; i < model->dim; i++)
			s += (double) qfloat[i] * qfloat[i];
		so->qNormSq = s;

		/* Build the LUT (sized over dimCodes), then the 8-bit block LUT. */
		so->lut = palloc(sizeof(float) * model->dimCodes * model->nLevels);
		TqBuildQueryLut(model, qfloat, so->lut, NULL);

		so->lut8 = palloc(model->dimCodes * model->nLevels);
		TqBuildLut8(model, so->lut, so->lut8, &so->lutBias, &so->lutScale);
	}

	MemoryContextSwitchTo(oldCtx);
}

/*
 * tqivfgettuple -- on the first call, select probes + score the first batch +
 * rerank; then return one heap tid per call in ascending distance order.
 */
bool
tqivfgettuple(IndexScanDesc scan, ScanDirection dir)
{
	TqivfScanOpaque so = (TqivfScanOpaque) scan->opaque;

	Assert(ScanDirectionIsForward(dir));

	if (so->first)
	{
		pgstat_count_index_scan(scan->indexRelation);
#if PG_VERSION_NUM >= 180000
		if (scan->instrument)
			scan->instrument->nsearches++;
#endif

		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan tqivf index without order");

		if (!IsMVCCSnapshot(scan->xs_snapshot))
			elog(ERROR, "non-MVCC snapshots are not supported with tqivf");

		/*
		 * A NULL order-by key still probes and returns tuples (with zero
		 * distance), matching ivfflat's ZeroDistance path.
		 */
		TqivfGetScanLists(scan);
		TqivfLoadBatch(scan);
		so->first = false;
	}

	/*
	 * Emit one tid per call by 2-way merging this batch's reranked top-K
	 * (exact distances) with the remaining estimate-ordered stream; both are
	 * ascending, so emitting the smaller head reproduces a per-batch sort of
	 * the mixed distances.  Ties prefer the reranked (exact) entry.  Mirrors
	 * tqscan's gettuple merge.
	 *
	 * When the current batch is fully drained (rerank array consumed AND the
	 * stream exhausted), advance to the next batch of probed lists if any
	 * remain (iterative scan).  With iterative scan OFF, maxProbes == probes so
	 * the single batch covers every probed list and the outer loop re-enters
	 * only to return false.
	 */
	for (;;)
	{
		/* Keep the head of the sorted stream buffered for the merge. */
		if (!so->streamHeadValid && !so->streamExhausted)
		{
			if (TqivfStreamNext(so, &so->streamHead))
				so->streamHeadValid = true;
			else
				so->streamExhausted = true;
		}

		/* Batch exhausted: rerank array drained AND estimate stream empty. */
		if (so->rerankCursor >= so->nrerank && !so->streamHeadValid)
		{
			if (so->listIndex >= so->nProbeLists)
				return false;

			TqivfLoadBatch(scan);
			continue;
		}

		if (so->rerankCursor < so->nrerank &&
			(!so->streamHeadValid ||
			 so->rerank[so->rerankCursor].dist <= so->streamHead.dist))
		{
			scan->xs_heaptid = so->rerank[so->rerankCursor].tid;
			so->rerankCursor++;
		}
		else
		{
			scan->xs_heaptid = so->streamHead.tid;
			so->streamHeadValid = false;
		}

		scan->xs_recheck = false;
		scan->xs_recheckorderby = false;

		return true;
	}
}

/*
 * tqivfendscan -- release scan resources (and the heap relation if we opened
 * it).  Mirrors tqendscan.
 */
void
tqivfendscan(IndexScanDesc scan)
{
	TqivfScanOpaque so = (TqivfScanOpaque) scan->opaque;

	if (so->fetch != NULL)
		table_index_fetch_end(so->fetch);
	if (so->slot != NULL)
		ExecDropSingleTupleTableSlot(so->slot);
	if (so->heapOpened && so->heapRel != NULL)
		table_close(so->heapRel, AccessShareLock);

	/* Free any temporary files */
	tuplesort_end(so->sortstate);

	ExecDropSingleTupleTableSlot(so->vslot);
	ExecDropSingleTupleTableSlot(so->mslot);

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}
