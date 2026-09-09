#include "postgres.h"

#include <float.h>

#include "access/genam.h"
#include "access/itup.h"
#include "access/relscan.h"
#include "access/tupdesc.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "lib/pairingheap.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

#include "vector.h"

/*
 * Distance support functions of the vector opclasses, defined in
 * src/vector.c via PG_FUNCTION_INFO_V1. Declared here for the fn_addr
 * dispatch in ivfflatbeginscan (Task 2 O1).
 *
 * These are the FUNCTION 1 (IVFFLAT_DISTANCE_PROC) entries of the ivfflat
 * opclasses in sql/vector--*.sql:
 *   vector_l2_ops     -> vector_l2_squared_distance
 *   vector_ip_ops     -> vector_negative_inner_product
 *   vector_cosine_ops -> vector_negative_inner_product (inputs normalized
 *                        by the FUNCTION 2 norm proc)
 */
extern Datum	vector_l2_squared_distance(PG_FUNCTION_ARGS);
extern Datum	vector_negative_inner_product(PG_FUNCTION_ARGS);

/*
 * Task 2 O1: extract (dim, values) from a vector Datum without detoasting.
 *
 * Index tuples store small vectors with a 1-byte SHORT varlena header
 * (see VARATT_CAN_MAKE_SHORT). Such a datum has the layout
 *   [1B header][int16 dim][int16 unused][float x[dim]]
 * while a 4-byte-header datum has
 *   [4B header][int16 dim][int16 unused][float x[dim]].
 * Casting a short-header datum to Vector * reads dim out of the first
 * float, which produced garbage dimensions, NaN distances and, for large
 * garbage dims, out-of-bounds reads (reproduced as a segfault on the
 * cosine opclass with dim 8). The fmgr path hides this because
 * PG_GETARG_VECTOR_P detoasts.
 *
 * Returns false for header forms we do not handle inline (external or
 * compressed), in which case the caller must use the fmgr path.
 */
static inline bool
IvfflatVectorParts(Datum d, int *dim, const float **x)
{
	struct varlena *vl = (struct varlena *) DatumGetPointer(d);

	if (VARATT_IS_4B_U(vl))
	{
		Vector	   *v = (Vector *) vl;

		*dim = v->dim;
		*x = v->x;
		return true;
	}
	else if (VARATT_IS_1B(vl) && !VARATT_IS_1B_E(vl))
	{
		unsigned char *p = (unsigned char *) vl;
		int16		rawDim;

		/* Copy the unaligned int16 instead of dereferencing it directly. */
		memcpy(&rawDim, p + 1, sizeof(int16));

		*dim = (int) rawDim;
		*x = (const float *) (p + 5);
		return true;
	}

	return false;
}

#define GetScanList(ptr) pairingheap_container(IvfflatScanList, ph_node, ptr)
#define GetScanListConst(ptr) pairingheap_const_container(IvfflatScanList, ph_node, ptr)

/*
 * Task 2 O2: comparator for the candidate array. Distance ascending;
 * exact ties are broken by TID (block, then offset) so the emitted order
 * is fully deterministic across runs.
 */
static int
CompareCands(const void *a, const void *b)
{
	const IvfflatCandData *x = (const IvfflatCandData *) a;
	const IvfflatCandData *y = (const IvfflatCandData *) b;
	BlockNumber xb;
	BlockNumber yb;

	if (x->distance < y->distance)
		return -1;
	if (x->distance > y->distance)
		return 1;

	xb = ItemPointerGetBlockNumber(&x->tid);
	yb = ItemPointerGetBlockNumber(&y->tid);
	if (xb != yb)
		return (xb < yb) ? -1 : 1;

	{
		OffsetNumber xo = ItemPointerGetOffsetNumber(&x->tid);
		OffsetNumber yo = ItemPointerGetOffsetNumber(&y->tid);

		return (xo < yo) ? -1 : (xo > yo) ? 1 : 0;
	}
}

static void
SortCands(IvfflatScanOpaque so)
{
	if (so->candCount > 1)
		qsort(so->cands, so->candCount, sizeof(IvfflatCandData), CompareCands);
}

/*
 * Task 2 O2: append a candidate to the array. Returns false when the
 * work_mem-derived capacity is reached and the scan must fall back to
 * the original tuplesort path.
 */
static bool
AppendCand(IvfflatScanOpaque so, double distance, ItemPointer tid)
{
	if (so->candCount >= so->candCapacity)
	{
		int			newcap;

		if (so->candCapacity >= so->candMax)
			return false;

		newcap = (so->candCapacity > 0) ? so->candCapacity * 2 : Min(1024, so->candMax);
		if (newcap > so->candMax)
			newcap = so->candMax;

		if (so->cands == NULL)
			so->cands = (IvfflatCand) palloc((Size) newcap * sizeof(IvfflatCandData));
		else
			so->cands = (IvfflatCand) repalloc(so->cands, (Size) newcap * sizeof(IvfflatCandData));
		so->candCapacity = newcap;
	}

	so->cands[so->candCount].distance = distance;
	so->cands[so->candCount].tid = *tid;
	so->candCount++;
	return true;
}

/*
 * Task 2 O2 fallback: move already-collected candidates into the tuplesort
 * and switch this scan to the original code path. The caller keeps
 * appending through the tuplesort afterwards; performsort is deferred to
 * the end of GetScanItems.
 */
static void
FallbackToSort(IvfflatScanOpaque so)
{
	TupleTableSlot *slot = so->vslot;

	tuplesort_reset(so->sortstate);

	for (int i = 0; i < so->candCount; i++)
	{
		ExecClearTuple(slot);
		slot->tts_values[0] = Float8GetDatum(so->cands[i].distance);
		slot->tts_isnull[0] = false;
		slot->tts_values[1] = PointerGetDatum(&so->cands[i].tid);
		slot->tts_isnull[1] = false;
		ExecStoreVirtualTuple(slot);

		tuplesort_puttupleslot(so->sortstate, slot);
	}

	so->candCount = 0;
	so->fastPath = false;
}

/*
 * Compare list distances
 */
static int
CompareLists(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (GetScanListConst(a)->distance > GetScanListConst(b)->distance)
		return 1;

	if (GetScanListConst(a)->distance < GetScanListConst(b)->distance)
		return -1;

	return 0;
}

/*
 * Get lists and sort by distance
 */
static void
GetScanLists(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	BlockNumber nextblkno = IVFFLAT_HEAD_BLKNO;
	int			listCount = 0;
	double		maxDistance = DBL_MAX;

	/* Search all list pages */
	while (BlockNumberIsValid(nextblkno))
	{
		Buffer		cbuf;
		Page		cpage;
		OffsetNumber maxoffno;

		cbuf = ReadBuffer(scan->indexRelation, nextblkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);

		maxoffno = PageGetMaxOffsetNumber(cpage);

		for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			IvfflatList list = (IvfflatList) PageGetItem(cpage, PageGetItemId(cpage, offno));
			double		distance;

		/* Use procinfo from the index instead of scan key for performance */
		if (so->fastPath)
		{
			/* Task 2 O1: bypass fmgr for list-center distances */
			int			cdim;
			const float *cx;

			/*
			 * List centers are embedded Vectors with a regular 4-byte
			 * header, but fall back to fmgr if that ever changes.
			 */
			if (IvfflatVectorParts(PointerGetDatum(&list->center), &cdim, &cx) &&
				cdim == so->qdim)
				distance = so->kernel(cdim, cx, so->qx);
			else
				distance = DatumGetFloat8(so->distfunc(so->procinfo, so->collation,
													   PointerGetDatum(&list->center), value));
		}
		else
			{
				distance = DatumGetFloat8(so->distfunc(so->procinfo, so->collation, PointerGetDatum(&list->center), value));
			}

			if (listCount < so->maxProbes)
			{
				IvfflatScanList *scanlist;

				scanlist = &so->lists[listCount];
				scanlist->startPage = list->startPage;
				scanlist->distance = distance;
				listCount++;

				/* Add to heap */
				pairingheap_add(so->listQueue, &scanlist->ph_node);

				/* Calculate max distance */
				if (listCount == so->maxProbes)
					maxDistance = GetScanList(pairingheap_first(so->listQueue))->distance;
			}
			else if (distance < maxDistance)
			{
				IvfflatScanList *scanlist;

				/* Remove */
				scanlist = GetScanList(pairingheap_remove_first(so->listQueue));

				/* Reuse */
				scanlist->startPage = list->startPage;
				scanlist->distance = distance;
				pairingheap_add(so->listQueue, &scanlist->ph_node);

				/* Update max distance */
				maxDistance = GetScanList(pairingheap_first(so->listQueue))->distance;
			}
		}

		nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

		UnlockReleaseBuffer(cbuf);
	}

	for (int i = listCount - 1; i >= 0; i--)
		so->listPages[i] = GetScanList(pairingheap_remove_first(so->listQueue))->startPage;

	Assert(pairingheap_is_empty(so->listQueue));
}

/*
 * Get items
 */
static void
GetScanItems(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	TupleTableSlot *slot = so->vslot;
	int			batchProbes = 0;
	MemoryContext oldCtx = CurrentMemoryContext;

	if (so->fastPath)
	{
		/* Task 2 O2: candidate array replaces the tuplesort batch */
		so->candCount = 0;
		so->emitIndex = 0;
		oldCtx = MemoryContextSwitchTo(so->tmpCtx);
	}
	else
	{
		tuplesort_reset(so->sortstate);
	}

	/* Search closest probes lists */
	while (so->listIndex < so->maxProbes && (++batchProbes) <= so->probes)
	{
		BlockNumber searchPage = so->listPages[so->listIndex++];

		/* Search all entry pages for list */
		while (BlockNumberIsValid(searchPage))
		{
			Buffer		buf;
			Page		page;
			OffsetNumber maxoffno;

			buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, so->bas);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			maxoffno = PageGetMaxOffsetNumber(page);

			for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
			{
				IndexTuple	itup;
				Datum		datum;
				bool		isnull;
				ItemId		itemid = PageGetItemId(page, offno);

				itup = (IndexTuple) PageGetItem(page, itemid);
				datum = index_getattr(itup, 1, tupdesc, &isnull);

				if (so->fastPath)
				{
					/*
					 * Task 2 O1/O2: direct C kernel + candidate array, no
					 * fmgr dispatch and no slot machinery per tuple.
					 */
					int			edim;
					const float *ex;

					/*
					 * Any entry whose header form or dimension we cannot
					 * handle inline falls back: the candidates collected so
					 * far are flushed into the tuplesort and this tuple is
					 * appended below by the original path.
					 */
					if (!IvfflatVectorParts(datum, &edim, &ex) || edim != so->qdim)
						FallbackToSort(so);
					else
					{
						double		distance = so->kernel(edim, ex, so->qx);

						if (!AppendCand(so, distance, &itup->t_tid))
						{
							/*
							 * Capacity reached: flush collected candidates
							 * into the tuplesort and continue on the original
							 * path for the rest of this scan.
							 */
							FallbackToSort(so);
						}
					}
				}

				if (!so->fastPath)
				{
					/*
					 * Add virtual tuple
					 *
					 * Use procinfo from the index instead of scan key for
					 * performance
					 */
					ExecClearTuple(slot);
					slot->tts_values[0] = so->distfunc(so->procinfo, so->collation, datum, value);
					slot->tts_isnull[0] = false;
					slot->tts_values[1] = PointerGetDatum(&itup->t_tid);
					slot->tts_isnull[1] = false;
					ExecStoreVirtualTuple(slot);

					tuplesort_puttupleslot(so->sortstate, slot);
				}
			}

			searchPage = IvfflatPageGetOpaque(page)->nextblkno;

			UnlockReleaseBuffer(buf);
		}
	}

	if (oldCtx != CurrentMemoryContext)
		MemoryContextSwitchTo(oldCtx);

	if (!so->fastPath)
		tuplesort_performsort(so->sortstate);

#if defined(IVFFLAT_MEMORY)
	elog(INFO, "memory: %zu MB", MemoryContextMemAllocated(CurrentMemoryContext, true) / (1024 * 1024));
#endif
}

/*
 * Zero distance
 */
static Datum
ZeroDistance(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
{
	return Float8GetDatum(0.0);
}

/*
 * Get scan value
 */
static Datum
GetScanValue(IndexScanDesc scan)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	Datum		value;

	if (scan->orderByData->sk_flags & SK_ISNULL)
	{
		value = PointerGetDatum(NULL);
		so->distfunc = ZeroDistance;

		/* Task 2: no kernel on NULL order-by value */
		so->fastPath = false;
	}
	else
	{
		value = scan->orderByData->sk_argument;
		so->distfunc = FunctionCall2Coll;

		/* Value should not be compressed or toasted */
		Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
		Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

		/* Normalize if needed */
		if (so->normprocinfo != NULL)
		{
			MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

			value = IvfflatNormValue(so->typeInfo, so->collation, value);

			MemoryContextSwitchTo(oldCtx);
		}

		/*
		 * Task 2: enable the fast path when a kernel was resolved and the
		 * scan value can be read without detoasting. The parts are resolved
		 * once per scan (after normalization) and reused for every entry.
		 */
		so->qdim = 0;
		so->qx = NULL;
		so->fastPath = (so->kernel != NULL) &&
			IvfflatVectorParts(value, &so->qdim, &so->qx) &&
			so->qdim == so->dimensions;
	}

	return value;
}

/*
 * Initialize scan sort state
 */
static Tuplesortstate *
InitScanSortState(TupleDesc tupdesc)
{
	AttrNumber	attNums[] = {1};
	Oid			sortOperators[] = {Float8LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};

	return tuplesort_begin_heap(tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, work_mem, NULL, false);
}

/*
 * Prepare for an index scan
 */
IndexScanDesc
ivfflatbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	IvfflatScanOpaque so;
	int			lists;
	int			dimensions;
	int			probes = ivfflat_probes;
	int			maxProbes;
	MemoryContext oldCtx;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	/* Get lists and dimensions from metapage */
	IvfflatGetMetaPageInfo(index, &lists, &dimensions);

	if (ivfflat_iterative_scan != IVFFLAT_ITERATIVE_SCAN_OFF)
		maxProbes = Max(ivfflat_max_probes, probes);
	else
		maxProbes = probes;

	if (probes > lists)
		probes = lists;

	if (maxProbes > lists)
		maxProbes = lists;

	so = palloc_object(IvfflatScanOpaqueData);
	so->typeInfo = IvfflatGetTypeInfo(index);
	so->first = true;
	so->probes = probes;
	so->maxProbes = maxProbes;
	so->dimensions = dimensions;
	so->value = PointerGetDatum(NULL);

	/* Set support functions */
	so->procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
	so->normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	so->collation = index->rd_indcollation[0];

	/*
	 * Task 2 O1: resolve the C-level distance kernel for this index by
	 * comparing fn_addr. Both the support function and the kernels come
	 * from this extension's shared library, so the addresses are
	 * comparable within a single loaded .so.
	 *
	 * The mapping follows IVFFLAT_DISTANCE_PROC (FUNCTION 1) of each
	 * opclass - see the extern declarations at the top of this file. Note
	 * that ip_ops and cosine_ops share vector_negative_inner_product: for
	 * cosine the norm proc normalizes the entries and the scan value, so
	 * -dot orders the same as cosine distance and one kernel serves both.
	 *
	 * Anything unrecognised leaves kernel = NULL, which keeps the scan on
	 * the original fmgr path.
	 */
	so->kernel = NULL;
	if (so->procinfo->fn_addr == vector_l2_squared_distance)
		so->kernel = IvfflatFastL2SquaredDistance;
	else if (so->procinfo->fn_addr == vector_negative_inner_product)
		so->kernel = IvfflatFastNegInnerProduct;

	/* Enabled per scan in GetScanValue once the order-by value is known */
	so->fastPath = false;

	/* Task 2 O2: candidate array state, bounded by work_mem */
	so->cands = NULL;
	so->candCount = 0;
	so->candCapacity = 0;
	so->emitIndex = 0;
	so->candMax = (int) Min((double) work_mem * 1024 / sizeof(IvfflatCandData), (double) INT_MAX);

	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Ivfflat scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);

	/* Create tuple description for sorting */
	so->tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 1, "distance", FLOAT8OID, -1, 0);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 2, "heaptid", TIDOID, -1, 0);
#if PG_VERSION_NUM >= 190000
	TupleDescFinalize(so->tupdesc);
#endif

	/* Prep sort */
	so->sortstate = InitScanSortState(so->tupdesc);

	/* Need separate slots for puttuple and gettuple */
	so->vslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
	so->mslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);

	/*
	 * Reuse same set of shared buffers for scan
	 *
	 * See postgres/src/backend/storage/buffer/README for description
	 */
	so->bas = GetAccessStrategy(BAS_BULKREAD);

	so->listQueue = pairingheap_allocate(CompareLists, scan);
	so->listPages = palloc_array_checked(BlockNumber, (Size) maxProbes);
	so->listIndex = 0;
	so->lists = palloc_array_checked(IvfflatScanList, (Size) maxProbes);

	MemoryContextSwitchTo(oldCtx);

	scan->opaque = so;

	return scan;
}

/*
 * Start or restart an index scan
 */
void
ivfflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;

	so->first = true;
	pairingheap_reset(so->listQueue);
	so->listIndex = 0;

	if (so->normprocinfo != NULL && DatumGetPointer(so->value) != NULL)
	{
		pfree(DatumGetPointer(so->value));
		so->value = PointerGetDatum(NULL);
	}

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, (Size) scan->numberOfKeys * sizeof(ScanKeyData));

	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, (Size) scan->numberOfOrderBys * sizeof(ScanKeyData));
}

/*
 * Fetch the next tuple in the given scan
 */
bool
ivfflatgettuple(IndexScanDesc scan, ScanDirection dir)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	ItemPointer heaptid;
	bool		isnull;

	/*
	 * Index can be used to scan backward, but Postgres doesn't support
	 * backward scan on operators
	 */
	Assert(ScanDirectionIsForward(dir));

	if (so->first)
	{
		Datum		value;

		/* Count index scan for stats */
		pgstat_count_index_scan(scan->indexRelation);
#if PG_VERSION_NUM >= 180000
		if (scan->instrument)
			scan->instrument->nsearches++;
#endif

		/* Safety check */
		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan ivfflat index without order");

		/* Requires MVCC-compliant snapshot as not able to pin during sorting */
		/* https://www.postgresql.org/docs/current/index-locking.html */
		if (!IsMVCCSnapshot(scan->xs_snapshot))
			elog(ERROR, "non-MVCC snapshots are not supported with ivfflat");

		value = GetScanValue(scan);
		IvfflatBench("GetScanLists", GetScanLists(scan, value));
		IvfflatBench("GetScanItems", GetScanItems(scan, value));
		if (so->fastPath)
			SortCands(so);
		so->first = false;
		so->value = value;
	}

	if (so->fastPath)
	{
		/*
		 * Task 2 O2: emit directly from the sorted candidate array.
		 */
		while (true)
		{
			if (so->emitIndex < so->candCount)
			{
				scan->xs_heaptid = so->cands[so->emitIndex].tid;
				scan->xs_recheck = false;
				scan->xs_recheckorderby = false;
				so->emitIndex++;
				return true;
			}

			if (so->listIndex == so->maxProbes)
				return false;

			IvfflatBench("GetScanItems", GetScanItems(scan, so->value));

			if (so->fastPath)
				SortCands(so);
			else
				break;			/* fell back to the tuplesort mid-batch */
		}
	}

	while (!tuplesort_gettupleslot(so->sortstate, true, false, so->mslot, NULL))
	{
		if (so->listIndex == so->maxProbes)
			return false;

		IvfflatBench("GetScanItems", GetScanItems(scan, so->value));
	}

	heaptid = (ItemPointer) DatumGetPointer(slot_getattr(so->mslot, 2, &isnull));

	scan->xs_heaptid = *heaptid;
	scan->xs_recheck = false;
	scan->xs_recheckorderby = false;
	return true;
}

/*
 * End a scan and release resources
 */
void
ivfflatendscan(IndexScanDesc scan)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;

	/* Free any temporary files */
	tuplesort_end(so->sortstate);

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}
