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
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "tq.h"
#include "utils/float.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tuplesort.h"
#include "vector.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

/*
 * One scored candidate.  dist is the value we MINIMIZE (smaller == closer);
 * for the reranked subset it is the exact FUNCTION 1 distance, otherwise the
 * quantized estimate.
 */
typedef struct TqScanResult
{
	double		dist;
	ItemPointerData tid;
} TqScanResult;

/*
 * Scan state.  Mirrors IvfflatScanOpaqueData: the loaded model, the per-query
 * LUT (+ qjlQuery when tqProd), the metric, the exact-distance procinfo +
 * collation used for rerank, ||q||^2, and the sorted candidate stream.
 */
typedef struct TqScanOpaqueData
{
	TqModel    *model;
	TqMetric	metric;
	bool		first;

	/* Exact distance (FUNCTION 1) for rerank. */
	FmgrInfo   *procinfo;
	Oid			collation;

	/* Type-info vtable (resolved from opclass support proc). */
	const TqTypeInfo *typeInfo;

	/* Per-query state (rebuilt on each rescan). */
	float	   *lut;			/* dimCodes * nLevels */
	uint8	   *lut8;			/* 8-bit query LUT for the block kernel */
	float		lutBias;		/* affine recovery: mse = lutScale*sum +
								 * dc*lutBias */
	float		lutScale;
	float	   *qjlQuery;		/* dimCodes, or NULL when !tqProd */
	Datum		queryDatum;		/* native query (normalized for cosine), for
								 * rerank */
	bool		haveQuery;
	float	   *vecScratch;		/* dim floats (rebuilt per rescan) */
	double		qNormSq;		/* ||q||^2 (1.0 for cosine after normalize) */

	/*
	 * Sorted candidates: a work_mem-bounded, disk-spilling tuplesort (mirrors
	 * ivfscan.c) instead of an in-memory array, so scan memory does not grow
	 * with the index.  Lives OUTSIDE tmpCtx -- rescan resets tmpCtx but only
	 * tuplesort_reset's the sort.
	 */
	TupleDesc	tupdesc;
	Tuplesortstate *sortstate;
	TupleTableSlot *vslot;		/* virtual slot for puttuple */
	TupleTableSlot *mslot;		/* minimal-tuple slot for gettuple */
	bool		streamExhausted;
	bool		streamHeadValid;
	TqScanResult streamHead;	/* buffered head of the sorted stream */

	/*
	 * Reranked top-K with exact distances (K <= tqflat.rerank, so bounded by
	 * the GUC).  Sorted ascending; gettuple merges it with the remaining
	 * estimate-ordered stream, which yields the same total order as the
	 * previous sort-everything-mixed approach.  Allocated in tmpCtx.
	 */
	TqScanResult *rerank;
	int64		nrerank;
	int64		rerankCursor;

	/* Heap access for rerank. */
	Relation	heapRel;		/* opened here iff heapOpened */
	bool		heapOpened;
	IndexFetchTableData *fetch;
	TupleTableSlot *slot;
	AttrNumber	heapAttno;		/* heap attribute backing the index column */

	MemoryContext tmpCtx;
} TqScanOpaqueData;

typedef TqScanOpaqueData *TqScanOpaque;

/*
 * qsort comparator: ascending distance.
 */
static int
CompareResults(const void *a, const void *b)
{
	double		da = ((const TqScanResult *) a)->dist;
	double		db = ((const TqScanResult *) b)->dist;

	if (da < db)
		return -1;
	if (da > db)
		return 1;
	return 0;
}

/*
 * Feed one scored candidate into the scan's tuplesort (mirrors ivfscan.c's
 * virtual-tuple add).  tid may point into a locked page: tuplesort copies the
 * slot into a minimal tuple immediately.
 */
static inline void
TqAddCandidate(TqScanOpaque so, double dist, ItemPointer tid)
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
 * Pull the next (dist, tid) off the sorted stream.  Decodes immediately: the
 * slot's contents are only valid until the next gettupleslot call.
 */
static bool
TqStreamNext(TqScanOpaque so, TqScanResult *out)
{
	bool		isnull;

	if (!tuplesort_gettupleslot(so->sortstate, true, false, so->mslot, NULL))
		return false;

	out->dist = DatumGetFloat8(slot_getattr(so->mslot, 1, &isnull));
	out->tid = *((ItemPointer) DatumGetPointer(slot_getattr(so->mslot, 2, &isnull)));
	return true;
}

/*
 * tqbeginscan -- initialize a scan descriptor.
 *
 * Loads the model (via the same rd_amcache path inserts use, falling back to
 * TqLoadModel), reads the metric from the model, and looks up the exact
 * distance procinfo (FUNCTION 1) + collation for rerank.  Mirrors
 * ivfflatbeginscan.
 */
IndexScanDesc
tqbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	TqScanOpaque so;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	so = (TqScanOpaque) palloc0(sizeof(TqScanOpaqueData));
	so->first = true;

	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Tqflat scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	/*
	 * Sort state for the candidate stream (mirrors ivfflatbeginscan).  Kept
	 * outside tmpCtx: rescan resets tmpCtx but reuses the sort via
	 * tuplesort_reset.
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

	/*
	 * Load the model from the relcache (rd_amcache / rd_indexcxt).  The
	 * cached model is owned by rd_indexcxt and must NOT be freed by the scan
	 * or live in tmpCtx.
	 */
	so->model = TqGetCachedModel(index);

	so->metric = so->model->metric;

	/* Exact distance (FUNCTION 1) + collation for rerank. */
	so->procinfo = index_getprocinfo(index, 1, TQ_DISTANCE_PROC);
	so->collation = index->rd_indcollation[0];

	/* Type-info vtable from the opclass support proc. */
	so->typeInfo = TqGetTypeInfo(index, TQ_TYPE_INFO_PROC);
	so->haveQuery = false;

	/* Map the index column to its backing heap attribute. */
	so->heapAttno = index->rd_index->indkey.values[0];

	so->lut = NULL;
	so->lut8 = NULL;
	so->qjlQuery = NULL;
	so->vecScratch = NULL;
	so->streamExhausted = false;
	so->streamHeadValid = false;
	so->rerank = NULL;
	so->nrerank = 0;
	so->rerankCursor = 0;
	so->heapRel = NULL;
	so->heapOpened = false;
	so->fetch = NULL;
	so->slot = NULL;

	scan->opaque = so;

	return scan;
}

/*
 * tqrescan -- start or restart the scan: extract and (for cosine) normalize the
 * order-by query, compute ||q||^2, and build the per-query LUT.
 */
void
tqrescan(IndexScanDesc scan, ScanKey keys, int nkeys,
		 ScanKey orderbys, int norderbys)
{
	TqScanOpaque so = (TqScanOpaque) scan->opaque;
	TqModel    *model;
	MemoryContext oldCtx;

	so->first = true;

	/* Empty the sort for reuse; reset the merge cursors. */
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
	 * Reset per-query allocations (LUT, qjlQuery, vecScratch, rerank array).
	 * The model lives in rd_indexcxt (via TqGetCachedModel) and must NOT be
	 * freed here.
	 */
	MemoryContextReset(so->tmpCtx);
	so->lut = NULL;
	so->lut8 = NULL;
	so->qjlQuery = NULL;
	so->haveQuery = false;
	so->vecScratch = NULL;

	/*
	 * Re-fetch the cached model: a relcache invalidation processed since
	 * beginscan (or the prior rescan) frees the rd_amcache chunk, so a pointer
	 * cached for the scan's lifetime could dangle here.
	 */
	so->model = TqGetCachedModel(scan->indexRelation);
	model = so->model;

	/* Build the LUT from the order-by query (a tqflat scan always has one). */
	oldCtx = MemoryContextSwitchTo(so->tmpCtx);
	if (scan->orderByData != NULL &&
		!(scan->orderByData->sk_flags & SK_ISNULL))
	{
		Datum		value = scan->orderByData->sk_argument;
		const float *qfloat;

		Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
		Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

		/*
		 * Cosine: normalize the NATIVE query (type-specific) so rerank's
		 * FUNCTION 1 sees a unit query and the float LUT is built from it.
		 */
		if (so->metric == TQ_METRIC_COSINE && so->typeInfo->normalize != NULL)
			value = DirectFunctionCall1Coll(so->typeInfo->normalize, so->collation, value);

		/*
		 * Keep the native (normalized for cosine) query for native-Datum
		 * rerank.
		 */
		so->queryDatum = value;
		so->haveQuery = true;

		/* Dense float query for the LUT/estimate path. */
		so->vecScratch = palloc(sizeof(float) * model->dim);
		qfloat = so->typeInfo->toFloat(value, so->vecScratch, model->dim);

		/* ||q||^2 (1 for cosine after normalize, but compute generally). */
		{
			double		s = 0.0;

			for (int i = 0; i < model->dim; i++)
				s += (double) qfloat[i] * qfloat[i];
			so->qNormSq = s;
		}

		so->lut = palloc(sizeof(float) * model->dimCodes * model->nLevels);
		if (model->tqProd)
			so->qjlQuery = palloc(sizeof(float) * model->dimCodes);
		TqBuildQueryLut(model, qfloat, so->lut, so->qjlQuery);

		so->lut8 = palloc(model->dimCodes * model->nLevels);
		TqBuildLut8(model, so->lut, so->lut8, &so->lutBias, &so->lutScale);
	}

	MemoryContextSwitchTo(oldCtx);
}

/*
 * Convert a raw inner-product estimate to a distance to MINIMIZE.
 */
static inline double
TqEstToDist(TqScanOpaque so, const TqEntry *entry, float est)
{
	switch (so->metric)
	{
		case TQ_METRIC_IP:
			return -(double) est;
		case TQ_METRIC_COSINE:
			{
				double		n = (double) entry->norm;

				if (n < 1e-12)
					return 1.0;
				return 1.0 - (double) est / n;
			}
		case TQ_METRIC_L2:
		default:
			return so->qNormSq + (double) entry->norm * (double) entry->norm
				- 2.0 * (double) est;
	}
}

/*
 * Like TqEstToDist but takes the stored norm directly (the block path has no
 * TqEntry, only the side record's norm).  MUST mirror TqEstToDist's formulas.
 */
static inline double
TqEstToDistSide(TqScanOpaque so, float norm, float est)
{
	switch (so->metric)
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
			return so->qNormSq + (double) norm * (double) norm
				- 2.0 * (double) est;
	}
}

/*
 * Fetch the original datum for tid from the heap.  Returns a palloc'd
 * fully-detoasted copy in the current memory context, or (Datum) 0 if the
 * tuple is no longer visible.
 */
static Datum
TqHeapFetchDatum(IndexScanDesc scan, TqScanOpaque so, ItemPointer tid)
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
 * Perform the full scan: score every live entry into the work_mem-bounded
 * tuplesort, sort it, and optionally pull + rerank the top candidates against
 * full-precision heap vectors (gettuple then merges the two sorted sources).
 */
static void
TqDoScan(IndexScanDesc scan)
{
	TqScanOpaque so = (TqScanOpaque) scan->opaque;
	TqModel    *model = so->model;
	Relation	index = scan->indexRelation;
	int			dc = model->dimCodes;
	BlockNumber codeStart;
	BlockNumber sideStart;
	BlockNumber tailStart;
	uint32		blockCount;
	int64		n = 0;
	MemoryContext oldCtx;

	/* Block-layout chain heads + counts from the meta page. */
	{
		Buffer		metabuf;
		Page		metapage;
		TqMetaPage	metap;

		metabuf = ReadBuffer(index, TQ_METAPAGE_BLKNO);
		LockBuffer(metabuf, BUFFER_LOCK_SHARE);
		metapage = BufferGetPage(metabuf);
		metap = TqPageGetMeta(metapage);
		codeStart = metap->codeStart;
		sideStart = metap->sideStart;
		tailStart = metap->tailStart;
		blockCount = metap->blockCount;
		UnlockReleaseBuffer(metabuf);
	}

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);

	/*
	 * Block scan: walk the side chain in block order, streaming each block's
	 * code plane (dc*16 bytes) from the code chain as it is consumed -- the
	 * blocks were written in the same order, so a forward-only byte stream
	 * suffices and scan memory stays one block, not the whole plane.  For
	 * each block, fold its code-plane into the 32-lane accumulator with the
	 * SIMD kernel and recover each live lane's estimate from the side
	 * record's per-lane scale/norm.
	 */
	if (blockCount > 0 && BlockNumberIsValid(codeStart))
	{
		Size		blockCodeBytes = TQ_BLOCK_CODE_BYTES(dc);
		char	   *planeBuf = NULL;
		TqByteStream codeStream;
		BlockNumber sblk = sideStart;
		uint32		b PG_USED_FOR_ASSERTS_ONLY = 0;

		/* Skipped for a NULL order-by key (nothing is scored). */
		if (so->haveQuery)
		{
			planeBuf = (char *) palloc(blockCodeBytes);
			TqByteStreamInit(&codeStream, index, codeStart);
		}

		while (BlockNumberIsValid(sblk))
		{
			Buffer		sbuf = ReadBuffer(index, sblk);
			Page		spage;
			OffsetNumber soff,
						smax;
			BlockNumber nextblk;

			CHECK_FOR_INTERRUPTS();

			LockBuffer(sbuf, BUFFER_LOCK_SHARE);
			spage = BufferGetPage(sbuf);
			smax = PageGetMaxOffsetNumber(spage);

			for (soff = FirstOffsetNumber; soff <= smax; soff = OffsetNumberNext(soff))
			{
				TqBlockSideRec *srec = (TqBlockSideRec *) PageGetItem(spage, PageGetItemId(spage, soff));
				const uint8 *plane = NULL;

				/*
				 * Stream this block's code plane.  The code chain holds only
				 * share-locked page reads, taken while the side page is also
				 * share-locked; no path locks these chains in the reverse
				 * order.
				 */
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

						TqAddCandidate(so, 0.0, &srec->side[j].heaptid);
						n++;
					}
				}
				else if (tqflat_force_scalar)
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

						TqAddCandidate(so, TqEstToDistSide(so, sd->norm, est), &sd->heaptid);
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

						TqAddCandidate(so, TqEstToDistSide(so, sd->norm, est), &sd->heaptid);
						n++;
					}
				}
				b++;
			}

			nextblk = TqPageGetOpaque(spage)->nextblkno;
			UnlockReleaseBuffer(sbuf);
			sblk = nextblk;
		}
		Assert(b == blockCount);
	}

	/*
	 * Tail scan: rows inserted post-build live in the row-major tail chain
	 * (Invalid until the first insert).  Score them with the scalar kernel
	 * over the float LUT, exactly as the pre-block layout did.
	 */
	if (BlockNumberIsValid(tailStart))
	{
		BlockNumber tblk = tailStart;

		while (BlockNumberIsValid(tblk))
		{
			Buffer		tbuf = ReadBuffer(index, tblk);
			Page		tpage;
			OffsetNumber toff,
						tmax;
			BlockNumber nextblk;

			CHECK_FOR_INTERRUPTS();

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
					float		est = TqScoreEntry(model, so->lut, so->qjlQuery, entry, entry->data);

					dist = TqEstToDist(so, entry, est);
				}
				else
					dist = 0.0; /* NULL order-by key */
				TqAddCandidate(so, dist, &entry->heaptid);
				n++;
			}

			nextblk = TqPageGetOpaque(tpage)->nextblkno;
			UnlockReleaseBuffer(tbuf);
			tblk = nextblk;
		}
	}

	/* Sort all candidates by estimated distance (spills past work_mem). */
	tuplesort_performsort(so->sortstate);

	/*
	 * Rerank: pull the top-K candidates by estimate off the sorted stream,
	 * fetch the original vectors, and replace the estimates with the exact
	 * FUNCTION 1 distance.  The K-array is re-sorted and gettuple merges it
	 * with the rest of the stream -- merging two ascending sequences yields
	 * the same total order as the previous sort-everything-mixed approach.
	 *
	 * Approach (a documented tradeoff): exact and estimated distances
	 * are not perfectly comparable, but in the top-K region the exact values
	 * dominate, which is what matters for the returned neighbors.
	 */
	if (tqflat_rerank > 0 && n > 0 && so->haveQuery &&
		AttributeNumberIsValid(so->heapAttno))
	{
		int64		k = Min((int64) tqflat_rerank, n);
		int64		nrerank = 0;

		/* Bounded by the tqflat.rerank GUC (TQ_MAX_RERANK). */
		so->rerank = (TqScanResult *) palloc(sizeof(TqScanResult) * k);

		while (nrerank < k && TqStreamNext(so, &so->rerank[nrerank]))
			nrerank++;

		for (int64 i = 0; i < nrerank; i++)
		{
			Datum		heapDatum = TqHeapFetchDatum(scan, so, &so->rerank[i].tid);

			CHECK_FOR_INTERRUPTS();

			if (heapDatum != (Datum) 0)
			{
				Datum		cmpDatum = heapDatum;
				double		d;

				/*
				 * Cosine: normalize the heap value (type-specific) so
				 * FUNCTION 1 (-inner_product) computes -cos, matching the
				 * estimate ranking.
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

				so->rerank[i].dist = d;

				if (cmpDatum != heapDatum)
					pfree(DatumGetPointer(cmpDatum));
				pfree(DatumGetPointer(heapDatum));
			}
			else
				so->rerank[i].dist = get_float8_infinity();
		}

		qsort(so->rerank, nrerank, sizeof(TqScanResult), CompareResults);
		so->nrerank = nrerank;
	}

	so->rerankCursor = 0;
	so->streamExhausted = false;
	so->streamHeadValid = false;

	MemoryContextSwitchTo(oldCtx);
}

/*
 * tqgettuple -- on the first call, run the full scan + rerank; then return one
 * heap tid per call in ascending distance order.
 */
bool
tqgettuple(IndexScanDesc scan, ScanDirection dir)
{
	TqScanOpaque so = (TqScanOpaque) scan->opaque;

	Assert(ScanDirectionIsForward(dir));

	if (so->first)
	{
		pgstat_count_index_scan(scan->indexRelation);
#if PG_VERSION_NUM >= 180000
		if (scan->instrument)
			scan->instrument->nsearches++;
#endif

		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan tqflat index without order");

		if (!IsMVCCSnapshot(scan->xs_snapshot))
			elog(ERROR, "non-MVCC snapshots are not supported with tqflat");

		/*
		 * A NULL order-by key still returns every tuple (with zero distance),
		 * matching ivfflat's ZeroDistance path -- TqDoScan handles it.
		 */
		TqDoScan(scan);
		so->first = false;
	}

	/* Keep the head of the sorted stream buffered for the merge. */
	if (!so->streamHeadValid && !so->streamExhausted)
	{
		if (TqStreamNext(so, &so->streamHead))
			so->streamHeadValid = true;
		else
			so->streamExhausted = true;
	}

	/*
	 * 2-way merge of the reranked top-K (exact distances) and the remaining
	 * estimate-ordered stream; both are ascending, so emitting the smaller
	 * head reproduces a global sort of the mixed distances.  Ties prefer the
	 * reranked (exact) entry.
	 */
	if (so->rerankCursor < so->nrerank &&
		(!so->streamHeadValid ||
		 so->rerank[so->rerankCursor].dist <= so->streamHead.dist))
	{
		scan->xs_heaptid = so->rerank[so->rerankCursor].tid;
		so->rerankCursor++;
	}
	else if (so->streamHeadValid)
	{
		scan->xs_heaptid = so->streamHead.tid;
		so->streamHeadValid = false;
	}
	else
		return false;

	scan->xs_recheck = false;
	scan->xs_recheckorderby = false;

	return true;
}

/*
 * tqendscan -- release scan resources (and the heap relation if we opened it).
 */
void
tqendscan(IndexScanDesc scan)
{
	TqScanOpaque so = (TqScanOpaque) scan->opaque;

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
