#include "postgres.h"

#include <math.h>

#include "common/hashfn.h"
#include "lib/pairingheap.h"
#include "nodes/pg_list.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "tqhnsw.h"
#include "halfutils.h"			/* HalfToFloat4 / Float4ToHalf / Halfvec*Distance */

/* murmurhash64 became public in common/hashfn.h in PG17; shim for older. */
#if PG_VERSION_NUM < 170000
static inline uint64
murmurhash64(uint64 data)
{
	uint64		h = data;

	h ^= h >> 33;
	h *= 0xff51afd7ed558ccd;
	h ^= h >> 33;
	h *= 0xc4ceb9fe1a85ec53;
	h ^= h >> 33;

	return h;
}
#endif

typedef struct TqhnswPointerHashEntry
{
	uintptr_t	ptr;
	char		status;
} TqhnswPointerHashEntry;
typedef struct TqhnswOffsetHashEntry
{
	Size		offset;
	char		status;
} TqhnswOffsetHashEntry;

static uint32
tqhnsw_hash_pointer(uintptr_t ptr)
{
#if SIZEOF_VOID_P == 8
	return murmurhash64((uint64) ptr);
#else
	return murmurhash32((uint32) ptr);
#endif
}

static uint32
tqhnsw_hash_offset(Size offset)
{
#if SIZEOF_SIZE_T == 8
	return murmurhash64((uint64) offset);
#else
	return murmurhash32((uint32) offset);
#endif
}

#define SH_PREFIX		tqpointerhash
#define SH_ELEMENT_TYPE	TqhnswPointerHashEntry
#define SH_KEY_TYPE		uintptr_t
#define SH_KEY			ptr
#define SH_HASH_KEY(tb, key)	tqhnsw_hash_pointer(key)
#define SH_EQUAL(tb, a, b)		(a == b)
#define SH_SCOPE		static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

#define SH_PREFIX		tqoffsethash
#define SH_ELEMENT_TYPE	TqhnswOffsetHashEntry
#define SH_KEY_TYPE		Size
#define SH_KEY			offset
#define SH_HASH_KEY(tb, key)	tqhnsw_hash_offset(key)
#define SH_EQUAL(tb, a, b)		(a == b)
#define SH_SCOPE		static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

typedef union
{
	struct tqpointerhash_hash *pointers;
	struct tqoffsethash_hash *offsets;
} TqhnswVisited;

/*
 * Reconstruct rhat[i] = norm*scale*centroids[code_i] from the packed codes into
 * the caller's float `scratch`, optionally cosine unit-normalize it, then pack to
 * fp16 `out`.  rhat is stored fp16 to halve the bytes streamed per build distance
 * (the parallel build's dominant, memory-bandwidth-bound traffic).  Reconstruct
 * and normalize run in float and round once at the pack, preserving precision.
 * The RHT rotation is orthonormal, so L2/IP between two rhat equal the distances
 * between the original vectors -- the graph is built in rotated space.
 *
 * scratch and out are both model->dimCodes long and caller-owned.
 */
void
TqhnswReconstructHalf(const TqModel *model, const char *codes,
					  float norm, float scale, bool normalize,
					  float *scratch, half *out)
{
	int			dc = model->dimCodes;
	int			codesBytes = TQ_CODES_BYTES(dc, model->bits);
	float		s = norm * scale;
	int			i;

	for (i = 0; i < dc; i++)
	{
		uint8		code = TqUnpackCode(codes, codesBytes, i, model->bits);

		scratch[i] = s * model->centroids[code];
	}

	if (normalize)
	{
		double		n = 0.0;

		for (i = 0; i < dc; i++)
			n += (double) scratch[i] * (double) scratch[i];
		n = sqrt(n);
		if (n > 1e-20)
		{
			float		inv = (float) (1.0 / n);

			for (i = 0; i < dc; i++)
				scratch[i] *= inv;
		}
	}

	for (i = 0; i < dc; i++)
	{
		float		v = scratch[i];

		/*
		 * Clamp finite floats to ±HALF_MAX before conversion.
		 * Float4ToHalfUnchecked maps finite-but-too-large floats to ±infinity;
		 * infinity in rhat breaks distance ordering (all inf nodes look
		 * equidistant).  Clamping preserves the "very large but finite"
		 * ordering without corrupting the distance metric.  Genuine infinities
		 * (isinf(v)) pass through unchanged -- HalfvecL2/IP handle them
		 * consistently.
		 */
		if (!isinf(v))
		{
			if (v > HALF_MAX)
				v = HALF_MAX;
			else if (v < -HALF_MAX)
				v = -HALF_MAX;
		}
		out[i] = Float4ToHalfUnchecked(v);
	}
}

/*
 * Build-time distance on the reconstructed fp16 rotated vectors.  Smaller is
 * nearer (HNSW convention): L2 -> squared Euclidean; IP/cosine -> negative inner
 * product (rhat is pre-normalized for cosine so -IP orders by cosine distance).
 *
 * Dispatches to pgvector's F16C-probed halfvec kernels (installed in HalfvecInit),
 * which accumulate in float -- matching the prior float-accumulated build kernel.
 */
double
TqhnswBuildDistance(half *a, half *b, int dc, TqMetric metric)
{
	Assert(a != NULL && b != NULL);

	if (metric == TQ_METRIC_L2)
		return (double) HalfvecL2SquaredDistance(dc, a, b);
	return -(double) HalfvecInnerProduct(dc, a, b);
}

/* ------------------------------------------------------------------------- *
 * In-memory serial graph build (absolute pointers, no relptr, no LWLocks).  *
 * Ported from hnswutils.c's in-memory branch: HnswSearchLayer (Alg 2),      *
 * SelectNeighbors (Alg 4), HnswFindElementNeighbors (Alg 1) + reciprocal    *
 * pruning (HnswUpdateConnection).  The only seam vs HNSW is the distance     *
 * call, which becomes TqhnswBuildDistance on the nodes' rhat vectors.       *
 * ------------------------------------------------------------------------- */

/*
 * A search candidate: an element plus its distance to the query, with the two
 * pairing-heap link nodes (c_node for the nearest/min heap C, w_node for the
 * furthest/max heap W).  Mirrors HnswSearchCandidate.
 */
typedef struct TqhnswSearchCandidate
{
	pairingheap_node c_node;
	pairingheap_node w_node;
	TqhnswElement *element;
	double		distance;
} TqhnswSearchCandidate;

#define TqhnswGetSearchCandidate(membername, ptr) \
	pairingheap_container(TqhnswSearchCandidate, membername, ptr)
#define TqhnswGetSearchCandidateConst(membername, ptr) \
	pairingheap_const_container(TqhnswSearchCandidate, membername, ptr)

/* C heap: nearest first (min by distance). */
static int
CompareNearestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	double		da = TqhnswGetSearchCandidateConst(c_node, a)->distance;
	double		db = TqhnswGetSearchCandidateConst(c_node, b)->distance;

	if (da < db)
		return 1;
	if (da > db)
		return -1;
	return 0;
}

/* W heap: furthest first (max by distance). */
static int
CompareFurthestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	double		da = TqhnswGetSearchCandidateConst(w_node, a)->distance;
	double		db = TqhnswGetSearchCandidateConst(w_node, b)->distance;

	if (da < db)
		return -1;
	if (da > db)
		return 1;
	return 0;
}

static TqhnswSearchCandidate *
TqhnswInitSearchCandidate(TqhnswElement *element, double distance)
{
	TqhnswSearchCandidate *sc = palloc(sizeof(TqhnswSearchCandidate));

	sc->element = element;
	sc->distance = distance;
	return sc;
}

/*
 * Whether element e counts toward ef during search.  Mirrors hnswutils.c
 * CountElement: when skipElement is non-NULL (vacuum repair), elements being
 * deleted (invalid heaptid) do NOT count toward ef, so the beam is sized over
 * survivors.  For build/insert (skipElement == NULL) every element counts.
 *
 * Two intentional divergences from hnswutils.c CountElement:
 *   1. Live-ness check uses ItemPointerIsValid(&e->heaptid) (single heaptid)
 *      rather than heaptidsLength > 0, because tqhnsw stores exactly one heap
 *      TID per element (no multi-TID deduplication).
 *   2. The pg_memory_barrier() that hnswutils.c places here is omitted: tqhnsw
 *      has no concurrent shared-memory in-memory build phase (single writer),
 *      so no barrier is needed to order visibility of heaptid writes.
 */
static inline bool
TqhnswCountElement(TqhnswElement *skipElement, TqhnswElement *e)
{
	if (skipElement == NULL)
		return true;
	return ItemPointerIsValid(&e->heaptid);
}

/*
 * Init a per-search visited set.  Thread-local: never writes shared element
 * state, so concurrent searches over shared in-memory elements (parallel build)
 * do not clobber each other.  Mirrors hnswutils.c InitVisited.
 */
static inline void
TqhnswInitVisited(char *base, TqhnswVisited *v, int ef, int m)
{
	if (base != NULL)
		v->offsets = tqoffsethash_create(CurrentMemoryContext, ef * m * 2, NULL);
	else
		v->pointers = tqpointerhash_create(CurrentMemoryContext, ef * m * 2, NULL);
}

/* Returns true if element was already visited (and marks it visited). */
static inline bool
TqhnswAddToVisited(char *base, TqhnswVisited *v, TqhnswElement *element)
{
	bool		found;

	if (base != NULL)
		tqoffsethash_insert(v->offsets, (Size) ((char *) element - base), &found);
	else
		tqpointerhash_insert(v->pointers, (uintptr_t) element, &found);
	return found;
}

/*
 * Algorithm 2 from the paper (in-memory branch of HnswSearchLayer).
 *
 * ep is a List of TqhnswSearchCandidate * entry points (already distance-scored).
 * Returns a List of TqhnswSearchCandidate * (the W set), ordered furthest-first
 * as the source returns it (callers do not rely on the order, they re-sort).
 */
static List *
TqhnswSearchLayer(char *base, Relation index, const TqModel *model, HTAB *cache,
				  MemoryContext ctx, TqhnswElement *query, List *ep, int ef, int lc,
				  int m, int dc, TqMetric metric, TqhnswElement *skipElement)
{
	List	   *w = NIL;
	pairingheap *C = pairingheap_allocate(CompareNearestCandidates, NULL);
	pairingheap *W = pairingheap_allocate(CompareFurthestCandidates, NULL);
	int			wlen = 0;
	ListCell   *lc2;
	TqhnswVisited visited;
	TqhnswNeighborArray *localNa = NULL;
	Size		localNaSize = 0;

	TqhnswInitVisited(base, &visited, ef, m);

	/*
	 * In-memory path: a reusable buffer to snapshot each candidate's neighbor
	 * array under its shared lock so the distance work below runs lock-free
	 * (see the loop).  lc is fixed for this call, so one size fits every
	 * copy.
	 */
	if (index == NULL)
	{
		localNaSize = TQHNSW_NEIGHBOR_ARRAY_SIZE(TqhnswGetLayerM(m, lc));
		localNa = palloc(localNaSize);
	}

	/* Add entry points to C and W, marking them visited. */
	foreach(lc2, ep)
	{
		TqhnswSearchCandidate *sc = (TqhnswSearchCandidate *) lfirst(lc2);

		(void) TqhnswAddToVisited(base, &visited, sc->element);
		pairingheap_add(C, &sc->c_node);
		pairingheap_add(W, &sc->w_node);
		if (TqhnswCountElement(skipElement, sc->element))
			wlen++;
	}

	while (!pairingheap_is_empty(C))
	{
		TqhnswSearchCandidate *c = TqhnswGetSearchCandidate(c_node, pairingheap_remove_first(C));
		TqhnswSearchCandidate *f = TqhnswGetSearchCandidate(w_node, pairingheap_first(W));
		TqhnswElement *cElement = c->element;
		int			i;

		if (c->distance > f->distance)
			break;

		/* Skip the candidate if it does not reach layer lc. */
		if (lc > cElement->level)
			continue;

		/* On-disk path: load neighbors for this layer lazily. */
		if (index != NULL)
		{
			TqhnswNeighborArrayPtr *neighborList = TqhnswPtrAccess(base, cElement->neighbors);

			if (TqhnswPtrIsNull(base, neighborList[lc]))
				TqhnswLoadNeighbors(index, model, metric, cElement, lc, m, ctx);
		}

		{
			TqhnswNeighborArray *na;
			bool		locked = (index == NULL);	/* in-memory graph: lock
													 * shared */

			na = TqhnswGetNeighbors(base, cElement, lc);

			if (locked)
			{
				/*
				 * Snapshot the neighbor array under a brief shared lock, then
				 * do all the distance work below lock-free (mirrors hnsw's
				 * HnswLoadUnvisitedFromMemory).  Holding cElement->lock
				 * across the distance kernels would block a concurrent
				 * reciprocal connect (EXCLUSIVE) on this node for the whole
				 * inner loop -- the hottest path of a parallel build.
				 */
				LWLockAcquire(&cElement->lock, LW_SHARED);
				memcpy(localNa, na, localNaSize);
				LWLockRelease(&cElement->lock);
				na = localNa;
			}

			for (i = 0; i < na->count; i++)
			{
				TqhnswElement *eElement = TqhnswPtrAccess(base, na->items[i].element);
				TqhnswSearchCandidate *e;
				double		eDistance;
				bool		alwaysAdd;

				/* On-disk path: resolve TID to element lazily. */
				if (index != NULL && eElement == NULL)
				{
					eElement = TqhnswLoadElement(index, model, metric,
												 &na->items[i].tid,
												 ctx, cache);
					TqhnswPtrStore(base, na->items[i].element, eElement);
				}

				if (TqhnswAddToVisited(base, &visited, eElement))
					continue;

				f = TqhnswGetSearchCandidate(w_node, pairingheap_first(W));
				alwaysAdd = wlen < ef;

				eDistance = TqhnswBuildDistance(TqhnswPtrAccess(base, query->rhat), TqhnswPtrAccess(base, eElement->rhat), dc, metric);

				if (!(eDistance < f->distance || alwaysAdd))
					continue;

				/*
				 * Make robust to issues (mirrors hnswutils.c): a stale edge
				 * can point at an element that no longer reaches this layer,
				 * so skip any neighbor whose level is below lc.
				 */
				if (eElement->level < lc)
					continue;

				e = TqhnswInitSearchCandidate(eElement, eDistance);
				pairingheap_add(C, &e->c_node);
				pairingheap_add(W, &e->w_node);
				if (TqhnswCountElement(skipElement, eElement))
				{
					wlen++;

					/*
					 * tqhnsw keeps the textbook Alg-2 invariant wlen == |W|
					 * (decrement on eviction).  This intentionally differs
					 * from hnswutils.c, which never decrements wlen because
					 * it tracks "admitted-live-elements" for its
					 * discarded-heap / iterative-scan path, which tqhnsw
					 * lacks.  For build/insert (skipElement==NULL) the two
					 * are equivalent; on the vacuum repair path
					 * (skipElement!=NULL) repaired-node recall is validated
					 * separately by the vacuum recall TAP test.
					 */
					if (wlen > ef)
					{
						pairingheap_remove_first(W);
						wlen--;
					}
				}
			}
		}
	}

	/* Drain W into a List. */
	while (!pairingheap_is_empty(W))
	{
		TqhnswSearchCandidate *sc = TqhnswGetSearchCandidate(w_node, pairingheap_remove_first(W));

		w = lappend(w, sc);
	}

	return w;
}

/* Compare two TqhnswCandidate by distance desc (furthest first), ptr tie-break. */
static int
CompareCandidateDistances(const ListCell *a, const ListCell *b)
{
	TqhnswCandidate *ca = lfirst(a);
	TqhnswCandidate *cb = lfirst(b);

	if (ca->distance < cb->distance)
		return 1;
	if (ca->distance > cb->distance)
		return -1;

	/*
	 * Tie-break on raw pointer (build) or relptr offset (disk) — both use
	 * .ptr for consistent ordering; the exact value is immaterial, only
	 * stability matters.
	 */
	if (ca->element.ptr < cb->element.ptr)
		return 1;
	if (ca->element.ptr > cb->element.ptr)
		return -1;
	return 0;
}

/*
 * Check if candidate e is closer to the query than to any element already in r.
 * Mirrors CheckElementCloser; distance(e, ri) via TqhnswBuildDistance.
 */
static bool
CheckElementCloser(char *base, TqhnswCandidate *e, List *r, int dc, TqMetric metric)
{
	ListCell   *lc2;
	TqhnswElement *eElement = TqhnswPtrAccess(base, e->element);

	Assert(eElement != NULL);

	foreach(lc2, r)
	{
		TqhnswCandidate *ri = lfirst(lc2);
		TqhnswElement *riElement = TqhnswPtrAccess(base, ri->element);
		double		distance;

		Assert(riElement != NULL);
		distance = TqhnswBuildDistance(TqhnswPtrAccess(base, eElement->rhat),
									   TqhnswPtrAccess(base, riElement->rhat),
									   dc, metric);

		if (distance <= e->distance)
			return false;
	}

	return true;
}

/*
 * Algorithm 4 from the paper (SelectNeighbors heuristic).  Returns up to lm
 * selected neighbors as a List of TqhnswCandidate *.  c is a List of
 * TqhnswCandidate *.
 *
 * If pruned is non-NULL, it receives one candidate that was NOT selected
 * (mirrors hnswutils.c SelectNeighbors); NULL when nothing was pruned.
 */
static List *
TqhnswSelectNeighbors(char *base, List *c, int lm, int dc, TqMetric metric,
					  TqhnswCandidate **pruned)
{
	List	   *r = NIL;
	List	   *w = list_copy(c);
	TqhnswCandidate **wd;
	int			wdlen = 0;
	int			wdoff = 0;

	if (pruned != NULL)
		*pruned = NULL;

	if (list_length(w) <= lm)
		return w;

	wd = palloc(sizeof(TqhnswCandidate *) * list_length(w));

	/* Order descending by distance so llast() is the nearest. */
	list_sort(w, CompareCandidateDistances);

	while (list_length(w) > 0 && list_length(r) < lm)
	{
		/* w is ordered desc; llast is the current nearest. */
		TqhnswCandidate *e = llast(w);
		bool		closer;

		w = list_delete_last(w);

		closer = CheckElementCloser(base, e, r, dc, metric);

		if (closer)
			r = lappend(r, e);
		else
			wd[wdlen++] = e;
	}

	/* Keep pruned connections to fill up to lm. */
	while (wdoff < wdlen && list_length(r) < lm)
		r = lappend(r, wd[wdoff++]);

	/* Return one unselected candidate for update connections. */
	if (pruned != NULL)
	{
		if (wdoff < wdlen)
			*pruned = wd[wdoff];
		else if (list_length(w) > 0)
			*pruned = linitial(w);
	}

	return r;
}

/*
 * Add element as a neighbor of target at layer lc, pruning target's neighbor
 * list back to lm via SelectNeighbors if it overflows.  Mirrors
 * HnswUpdateConnection: when the list is full, the candidate pruned OUT is
 * replaced IN PLACE, so existing items keep their slot positions.
 *
 * If updateIdx is non-NULL it reports what changed: -2 when element was
 * appended to a free slot; the (original-order) index of the replaced item
 * when the list was full and element won the prune; untouched (caller
 * initializes to -1) when element lost the prune.  The on-disk single-slot
 * write in tqhnswinsert.c relies on this index matching the slot order of the
 * live neighbor tuple.
 */
void
TqhnswUpdateConnection(char *base, TqhnswElement *target, TqhnswElement *element,
					   double distance, int lm, int lc, int dc, TqMetric metric,
					   int *updateIdx)
{
	TqhnswNeighborArray *na;

	LWLockAcquire(&target->lock, LW_EXCLUSIVE);
	na = TqhnswGetNeighbors(base, target, lc);

	if (na->count < lm)
	{
		TqhnswPtrStore(base, na->items[na->count].element, element);
		na->items[na->count].distance = distance;
		na->count++;

		if (updateIdx != NULL)
			*updateIdx = -2;
	}
	else
	{
		/* Full: rebuild the candidate set (existing + new) and re-select. */
		List	   *c = NIL;
		int			i;
		TqhnswCandidate *newCand;
		TqhnswCandidate *pruned = NULL;

		for (i = 0; i < na->count; i++)
		{
			TqhnswCandidate *hc = palloc(sizeof(TqhnswCandidate));
			TqhnswElement *ne = TqhnswPtrAccess(base, na->items[i].element);

			TqhnswPtrStore(base, hc->element, ne);
			hc->distance = TqhnswBuildDistance(TqhnswPtrAccess(base, target->rhat), TqhnswPtrAccess(base, ne->rhat),
											   dc, metric);
			c = lappend(c, hc);
		}
		newCand = palloc(sizeof(TqhnswCandidate));
		TqhnswPtrStore(base, newCand->element, element);
		newCand->distance = distance;
		c = lappend(c, newCand);

		(void) TqhnswSelectNeighbors(base, c, lm, dc, metric, &pruned);

		/* Should not happen */
		if (pruned == NULL)
		{
			LWLockRelease(&target->lock);
			return;
		}

		/*
		 * element lost the prune: leave the list untouched (updateIdx stays
		 * -1).
		 */
		if (pruned != newCand)
		{
			/* Replace the pruned element in place. */
			for (i = 0; i < na->count; i++)
			{
				if (TqhnswPtrAccess(base, na->items[i].element) ==
					TqhnswPtrAccess(base, pruned->element))
				{
					TqhnswPtrStore(base, na->items[i].element, element);
					na->items[i].distance = distance;

					if (updateIdx != NULL)
						*updateIdx = i;

					break;
				}
			}
		}
	}

	LWLockRelease(&target->lock);
}

/* ------------------------------------------------------------------------- *
 * Disk-load helpers (on-disk insert / scan path; build path never calls     *
 * these because index==NULL on that path).                                  *
 * ------------------------------------------------------------------------- */

/*
 * Create the TID->element cache.  The entry layout is TqhnswElementCacheEntry
 * (key first, as required by dynahash).
 */
HTAB *
TqhnswCreateElementCache(MemoryContext ctx)
{
	HASHCTL		hashctl;

	memset(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize = sizeof(ItemPointerData);
	hashctl.entrysize = sizeof(TqhnswElementCacheEntry);
	hashctl.hcxt = ctx;
	return hash_create("tqhnsw element cache", 256, &hashctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Materialize a disk element into ctx, caching it by TID in cache.
 * Loads the element tuple, reconstructs rhat, and lazily allocates the
 * neighbors pointer array (each layer populated by TqhnswLoadNeighbors).
 */
TqhnswElement *
TqhnswLoadElement(Relation index, const TqModel *model, TqMetric metric,
				  ItemPointer tid, MemoryContext ctx, HTAB *cache)
{
	char	   *base = NULL;
	bool		found;
	TqhnswElementCacheEntry *entry;
	TqhnswElement *e;
	Buffer		buf;
	Page		page;
	TqhnswElementTuple etup;
	char	   *codes;
	half	   *rhat;
	float	   *rhatScratch;
	int			codesBytes = TQ_CODES_BYTES(model->dimCodes, model->bits);
	MemoryContext old;

	/* Cache keyed by ItemPointerData; dynahash copies tid into entry->tid. */
	entry = (TqhnswElementCacheEntry *) hash_search(cache, tid, HASH_ENTER, &found);
	if (found)
		return entry->element;

	old = MemoryContextSwitchTo(ctx);
	e = palloc0(sizeof(TqhnswElement));
	LWLockInitialize(&e->lock, tqhnsw_lock_tranche_id);

	buf = ReadBuffer(index, ItemPointerGetBlockNumber(tid));
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	etup = (TqhnswElementTuple) PageGetItem(page,
											PageGetItemId(page,
														  ItemPointerGetOffsetNumber(tid)));

	if (unlikely(etup->deleted))
		elog(ERROR, "cannot load deleted tqhnsw element");

	e->level = etup->level;
	e->version = etup->version;
	e->norm = etup->norm;
	e->scale = etup->scale;
	e->heaptid = etup->heaptid;
	e->blkno = ItemPointerGetBlockNumber(tid);
	e->offno = ItemPointerGetOffsetNumber(tid);
	e->neighborPage = ItemPointerGetBlockNumber(&etup->neighbortid);
	e->neighborOffno = ItemPointerGetOffsetNumber(&etup->neighbortid);
	codes = (char *) palloc(codesBytes);
	memcpy(codes, etup->codes, codesBytes);
	TqhnswPtrStore(base, e->codes, codes);

	UnlockReleaseBuffer(buf);

	rhat = (half *) palloc(sizeof(half) * model->dimCodes);
	rhatScratch = (float *) palloc(sizeof(float) * model->dimCodes);
	TqhnswPtrStore(base, e->rhat, rhat);
	TqhnswReconstructHalf(model, codes, e->norm, e->scale,
						  metric == TQ_METRIC_COSINE, rhatScratch, rhat);
	pfree(rhatScratch);			/* only needed during reconstruct; rhat lives on */

	/* Neighbor arrays populated lazily per layer by TqhnswLoadNeighbors. */
	TqhnswPtrStore(base, e->neighbors,
				   (TqhnswNeighborArrayPtr *) palloc0(sizeof(TqhnswNeighborArrayPtr) * (e->level + 1)));

	MemoryContextSwitchTo(old);
	entry->element = e;
	return e;
}

/*
 * Load element's layer-lc neighbor TIDs into a TqhnswNeighborArray.  The
 * items[].element pointers start NULL and are resolved lazily in
 * TqhnswSearchLayer; items[].tid carry the on-disk TIDs.
 * Mirrors TqhnswLoadNeighborTids (tqhnswscan.c) but produces a NeighborArray
 * rather than a raw TID array.
 */
TqhnswNeighborArray *
TqhnswLoadNeighbors(Relation index, const TqModel *model, TqMetric metric,
					TqhnswElement *element, int lc, int m, MemoryContext ctx)
{
	char	   *base = NULL;
	Buffer		buf;
	Page		page;
	TqhnswNeighborTuple ntup;
	int			lm = TqhnswGetLayerM(m, lc);
	int			level;
	int			start;
	TqhnswNeighborArray *na;
	int			i;
	MemoryContext old = MemoryContextSwitchTo(ctx);

	na = palloc(TQHNSW_NEIGHBOR_ARRAY_SIZE(lm));
	na->count = 0;

	buf = ReadBuffer(index, element->neighborPage);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	ntup = (TqhnswNeighborTuple) PageGetItem(page,
											 PageGetItemId(page,
														   element->neighborOffno));

	/*
	 * Reject a slot that vacuum tombstoned and an insert reused since the
	 * element was loaded (mirrors TqhnswLoadNeighborTids / HnswLoadNeighborTids):
	 * the stale edge would otherwise read a wrong-sized indextids[] slice from a
	 * bare ntup->count derivation.  Returning the empty array just drops the
	 * stale edge from this search.
	 */
	if (ntup->type != TQHNSW_NEIGHBOR_TUPLE_TYPE ||
		ntup->version != element->version ||
		(int) ntup->count != (element->level + 2) * m)
	{
		UnlockReleaseBuffer(buf);
		MemoryContextSwitchTo(old);

		{
			TqhnswNeighborArrayPtr *neighborList = TqhnswPtrAccess(base, element->neighbors);

			TqhnswPtrStore(base, neighborList[lc], na);
		}
		return na;
	}

	level = element->level;
	start = (level - lc) * m;

	for (i = 0; i < lm; i++)
	{
		ItemPointer t = &ntup->indextids[start + i];

		if (!ItemPointerIsValid(t))
			continue;
		na->items[na->count].tid = *t;
		na->items[na->count].element.ptr = NULL;	/* lazy */
		na->items[na->count].distance = 0;
		na->count++;
	}
	UnlockReleaseBuffer(buf);
	MemoryContextSwitchTo(old);

	{
		TqhnswNeighborArrayPtr *neighborList = TqhnswPtrAccess(base, element->neighbors);

		TqhnswPtrStore(base, neighborList[lc], na);
	}
	return na;
}

/*
 * Remove self (skipElement, matched by block/offset) and any elements being
 * deleted (invalid heaptid) from a candidate list before neighbor selection.
 * Mirrors hnswutils.c RemoveElements.  Disk path only.
 *
 * As with TqhnswCountElement, live-ness is checked via a single heaptid
 * (ItemPointerIsValid) rather than heaptidsLength, and no pg_memory_barrier()
 * is needed (single-writer build; no concurrent in-memory graph).
 */
static List *
TqhnswRemoveElements(List *w, TqhnswElement *skipElement)
{
	char	   *base = NULL;
	ListCell   *lc2;
	List	   *w2 = NIL;

	foreach(lc2, w)
	{
		TqhnswCandidate *hc = (TqhnswCandidate *) lfirst(lc2);
		TqhnswElement *hce = TqhnswPtrAccess(base, hc->element);

		if (skipElement != NULL &&
			hce->blkno == skipElement->blkno && hce->offno == skipElement->offno)
			continue;

		if (ItemPointerIsValid(&hce->heaptid))
			w2 = lappend(w2, hc);
	}
	return w2;
}

/*
 * Algorithm 1 from the paper (HnswFindElementNeighbors): greedy descent then
 * per-layer search + select + reciprocal connect.  element already has its level,
 * rhat, codes, and zero-initialized neighbor arrays.
 *
 * build path: index=NULL, model=NULL, cache=NULL (base is NULL serial, the
 * DSM area for a parallel build).  existing=true is used by vacuum repair:
 * element is already in the graph and needs its neighbors re-selected (skip
 * self, widen beam, no inline reciprocal).
 */
void
TqhnswInsertElement(char *base, Relation index, const TqModel *model, HTAB *cache,
					MemoryContext ctx, TqhnswElement *element,
					TqhnswElement *entryPoint, int m,
					int efConstruction, int dc, TqMetric metric, bool existing)
{
	List	   *ep;
	List	   *w;
	int			level = element->level;
	int			entryLevel;
	int			lc;
	TqhnswElement *skipElement = existing ? element : NULL;

	/*
	 * No neighbors to select if there is no entry point (mirrors
	 * HnswFindElementNeighbors).  Repair may pass NULL when the graph's entry
	 * point was deleted with no surviving replacement; the element then gets
	 * an empty neighbor list, consistent with HNSW.
	 */
	if (entryPoint == NULL)
		return;

	entryLevel = entryPoint->level;

	/* Entry point candidate. */
	ep = list_make1(TqhnswInitSearchCandidate(entryPoint,
											  TqhnswBuildDistance(TqhnswPtrAccess(base, element->rhat),
																  TqhnswPtrAccess(base, entryPoint->rhat),
																  dc, metric)));

	/* 1st phase: greedy descent (ef=1) down to the element's level + 1. */
	for (lc = entryLevel; lc >= level + 1; lc--)
	{
		w = TqhnswSearchLayer(base, index, model, cache, ctx, element, ep, 1, lc, m, dc, metric,
							  skipElement);
		ep = w;
	}

	if (level > entryLevel)
		level = entryLevel;

	/*
	 * Add one for existing element (it will be filtered from its own
	 * candidate set).
	 */
	if (existing)
		efConstruction++;

	/*
	 * The unlocked phase-A forward writes below assume element is private to
	 * this backend.  That holds for a fresh insert (existing=false) and for
	 * the only existing=true caller (vacuum repair), which runs on-disk
	 * (base==NULL) and single-backed.  An existing=true element in a
	 * shared/in-memory graph would already be discoverable, so the unlocked
	 * reset would drop its edges mid-update.
	 */
	Assert(base == NULL || !existing);

	/*
	 * 2nd phase A: per-layer search + select, storing the forward neighbors
	 * into element's own (still-private) neighbor lists.  No reciprocal edges
	 * are added here, so element stays undiscoverable to other workers'
	 * searches -- these forward writes therefore need no lock.  Deferring the
	 * reciprocal connections to phase B mirrors hnsw's split of
	 * HnswFindElementNeighbors (forward) from UpdateNeighborsInMemory
	 * (reciprocal).  It is essential for the parallel build: it guarantees
	 * element's own lists are COMPLETE before element is published, so a
	 * concurrent worker never races this worker's unlocked forward writes
	 * against its own locked TqhnswUpdateConnection(element, ...).
	 */
	for (lc = level; lc >= 0; lc--)
	{
		int			lm = TqhnswGetLayerM(m, lc);
		List	   *lw = NIL;
		List	   *selected;
		ListCell   *lc2;
		TqhnswNeighborArray *na;

		w = TqhnswSearchLayer(base, index, model, cache, ctx, element, ep, efConstruction,
							  lc, m, dc, metric, skipElement);

		/* Convert search candidates to plain candidates. */
		foreach(lc2, w)
		{
			TqhnswSearchCandidate *sc = lfirst(lc2);
			TqhnswCandidate *hc = palloc(sizeof(TqhnswCandidate));

			TqhnswPtrStore(base, hc->element, sc->element);
			hc->distance = sc->distance;
			lw = lappend(lw, hc);
		}

		/* Disk path: drop self + deleted elements before selecting neighbors. */
		if (index != NULL)
			lw = TqhnswRemoveElements(lw, skipElement);

		selected = TqhnswSelectNeighbors(base, lw, lm, dc, metric, NULL);

		/*
		 * Store the forward neighbors (element -> selected) in element's own
		 * list.
		 */
		na = TqhnswGetNeighbors(base, element, lc);
		na->count = 0;
		foreach(lc2, selected)
		{
			TqhnswCandidate *hc = lfirst(lc2);

			TqhnswPtrStore(base, na->items[na->count].element,
						   TqhnswPtrAccess(base, hc->element));
			na->items[na->count].distance = hc->distance;
			na->count++;
		}

		ep = w;
	}

	/*
	 * 2nd phase B: add the reciprocal edges (selected -> element).  Adding
	 * the first reciprocal edge is what publishes element into the searchable
	 * graph, so this must run only after ALL forward lists are complete
	 * (phase A). Mirrors UpdateNeighborsInMemory: snapshot element's layer-lc
	 * list under its shared lock (once element is discoverable, a concurrent
	 * insert may add a reciprocal edge into element), then add element to
	 * each neighbor under that neighbor's exclusive lock (taken inside
	 * TqhnswUpdateConnection).  Only the in-memory build path (index == NULL)
	 * runs this: the on-disk paths (single-tuple insert and vacuum repair,
	 * index != NULL) write their reciprocity to disk via
	 * TqhnswUpdateNeighborsOnDisk, which reloads each neighbor's edges from
	 * disk and re-prunes -- so mutating the per-insert cached copies here is
	 * wasted work (~2x reciprocal-connect cost per on-disk insert).
	 */
	if (!existing && index == NULL)
	{
		for (lc = level; lc >= 0; lc--)
		{
			int			lm = TqhnswGetLayerM(m, lc);
			Size		naSize = TQHNSW_NEIGHBOR_ARRAY_SIZE(lm);
			TqhnswNeighborArray *snapshot = palloc(naSize);
			int			i;

			LWLockAcquire(&element->lock, LW_SHARED);
			memcpy(snapshot, TqhnswGetNeighbors(base, element, lc), naSize);
			LWLockRelease(&element->lock);

			for (i = 0; i < snapshot->count; i++)
			{
				TqhnswElement *neighbor = TqhnswPtrAccess(base, snapshot->items[i].element);

				TqhnswUpdateConnection(base, neighbor, element, snapshot->items[i].distance,
									   lm, lc, dc, metric, NULL);
			}
		}
	}
}
