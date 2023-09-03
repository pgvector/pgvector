#include "postgres.h"

#include <math.h>

#include "hnsw.h"
#include "storage/bufmgr.h"
#include "vector.h"

/*
 * Get the max number of connections in an upper layer for each element in the index
 */
int
HnswGetM(Relation index)
{
	HnswOptions *opts = (HnswOptions *) index->rd_options;

	if (opts)
		return opts->m;

	return HNSW_DEFAULT_M;
}

/*
 * Get the size of the dynamic candidate list in the index
 */
int
HnswGetEfConstruction(Relation index)
{
	HnswOptions *opts = (HnswOptions *) index->rd_options;

	if (opts)
		return opts->efConstruction;

	return HNSW_DEFAULT_EF_CONSTRUCTION;
}

/*
 * Get proc
 */
FmgrInfo *
HnswOptionalProcInfo(Relation rel, uint16 procnum)
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
HnswNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, Vector * result)
{
	double		norm = DatumGetFloat8(FunctionCall1Coll(procinfo, collation, *value));

	if (norm > 0)
	{
		Vector	   *v = DatumGetVector(*value);

		if (result == NULL)
			result = InitVector(v->dim);

		for (int i = 0; i < v->dim; i++)
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
HnswNewBuffer(Relation index, ForkNumber forkNum)
{
	Buffer		buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	return buf;
}

/*
 * Init page
 */
void
HnswInitPage(Buffer buf, Page page)
{
	PageInit(page, BufferGetPageSize(buf), sizeof(HnswPageOpaqueData));
	HnswPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
	HnswPageGetOpaque(page)->page_id = HNSW_PAGE_ID;
}

/*
 * Init and register page
 */
void
HnswInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state)
{
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	HnswInitPage(*buf, *page);
}

/*
 * Commit buffer
 */
void
HnswCommitBuffer(Buffer buf, GenericXLogState *state)
{
	MarkBufferDirty(buf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Allocate neighbors
 */
void
HnswInitNeighbors(HnswElement element, int m)
{
	int			level = element->level;

	element->neighbors = palloc(sizeof(HnswNeighborArray) * (level + 1));

	for (int lc = 0; lc <= level; lc++)
	{
		HnswNeighborArray *a;
		int			lm = HnswGetLayerM(m, lc);

		a = &element->neighbors[lc];
		a->length = 0;
		a->items = palloc(sizeof(HnswCandidate) * lm);
	}
}

/*
 * Allocate an element
 */
HnswElement
HnswInitElement(ItemPointer heaptid, int m, double ml, int maxLevel)
{
	HnswElement element = palloc(sizeof(HnswElementData));

	int			level = (int) (-log(RandomDouble()) * ml);

	/* Cap level */
	if (level > maxLevel)
		level = maxLevel;

	element->heaptids = NIL;
	HnswAddHeapTid(element, heaptid);

	element->level = level;
	element->deleted = 0;

	HnswInitNeighbors(element, m);

	return element;
}

/*
 * Free an element
 */
void
HnswFreeElement(HnswElement element)
{
	list_free_deep(element->heaptids);
	for (int lc = 0; lc <= element->level; lc++)
		pfree(element->neighbors[lc].items);
	pfree(element->neighbors);
	pfree(element->vec);
	pfree(element);
}

/*
 * Add a heap TID to an element
 */
void
HnswAddHeapTid(HnswElement element, ItemPointer heaptid)
{
	ItemPointer copy = palloc(sizeof(ItemPointerData));

	ItemPointerCopy(heaptid, copy);
	element->heaptids = lappend(element->heaptids, copy);
}

/*
 * Allocate an element from block and offset numbers
 */
HnswElement
HnswInitElementFromBlock(BlockNumber blkno, OffsetNumber offno)
{
	HnswElement element = palloc(sizeof(HnswElementData));

	element->blkno = blkno;
	element->offno = offno;
	element->neighbors = NULL;
	element->vec = NULL;
	return element;
}

/*
 * Get the entry point
 */
HnswElement
HnswGetEntryPoint(Relation index)
{
	Buffer		buf;
	Page		page;
	HnswMetaPage metap;
	HnswElement entryPoint = NULL;

	buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = HnswPageGetMeta(page);

	if (BlockNumberIsValid(metap->entryBlkno))
		entryPoint = HnswInitElementFromBlock(metap->entryBlkno, metap->entryOffno);

	UnlockReleaseBuffer(buf);

	return entryPoint;
}

/*
 * Update the metapage info
 */
static void
HnswUpdateMetaPageInfo(Page page, int updateEntry, HnswElement entryPoint, BlockNumber insertPage)
{
	HnswMetaPage metap = HnswPageGetMeta(page);

	if (updateEntry)
	{
		if (entryPoint == NULL)
		{
			metap->entryBlkno = InvalidBlockNumber;
			metap->entryOffno = InvalidOffsetNumber;
			metap->entryLevel = -1;
		}
		else if (entryPoint->level > metap->entryLevel || updateEntry == HNSW_UPDATE_ENTRY_ALWAYS)
		{
			metap->entryBlkno = entryPoint->blkno;
			metap->entryOffno = entryPoint->offno;
			metap->entryLevel = entryPoint->level;
		}
	}

	if (BlockNumberIsValid(insertPage))
		metap->insertPage = insertPage;
}

/*
 * Update the metapage
 */
void
HnswUpdateMetaPage(Relation index, int updateEntry, HnswElement entryPoint, BlockNumber insertPage, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;

	buf = ReadBufferExtended(index, forkNum, HNSW_METAPAGE_BLKNO, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	HnswUpdateMetaPageInfo(page, updateEntry, entryPoint, insertPage);

	HnswCommitBuffer(buf, state);
}

/*
 * Set element tuple, except for neighbor info
 */
void
HnswSetElementTuple(HnswElementTuple etup, HnswElement element)
{
	etup->type = HNSW_ELEMENT_TUPLE_TYPE;
	etup->level = element->level;
	etup->deleted = 0;
	for (int i = 0; i < HNSW_HEAPTIDS; i++)
	{
		if (i < list_length(element->heaptids))
			etup->heaptids[i] = *((ItemPointer) list_nth(element->heaptids, i));
		else
			ItemPointerSetInvalid(&etup->heaptids[i]);
	}
	memcpy(&etup->vec, element->vec, VECTOR_SIZE(element->vec->dim));
}

/*
 * Set neighbor tuple
 */
void
HnswSetNeighborTuple(HnswNeighborTuple ntup, HnswElement e, int m)
{
	int			idx = 0;

	ntup->type = HNSW_NEIGHBOR_TUPLE_TYPE;

	for (int lc = e->level; lc >= 0; lc--)
	{
		HnswNeighborArray *neighbors = &e->neighbors[lc];
		int			lm = HnswGetLayerM(m, lc);

		for (int i = 0; i < lm; i++)
		{
			ItemPointer indextid = &ntup->indextids[idx++];

			if (i < neighbors->length)
			{
				HnswCandidate *hc = &neighbors->items[i];

				ItemPointerSet(indextid, hc->element->blkno, hc->element->offno);
			}
			else
				ItemPointerSetInvalid(indextid);
		}
	}

	ntup->count = idx;
}

/*
 * Load neighbors from page
 */
static void
LoadNeighborsFromPage(HnswElement element, Relation index, Page page)
{
	HnswNeighborTuple ntup = (HnswNeighborTuple) PageGetItem(page, PageGetItemId(page, element->neighborOffno));
	int			m = HnswGetM(index);
	int			neighborCount = (element->level + 2) * m;

	Assert(HnswIsNeighborTuple(ntup));

	HnswInitNeighbors(element, m);

	/* Ensure expected neighbors */
	if (ntup->count != neighborCount)
		return;

	for (int i = 0; i < neighborCount; i++)
	{
		HnswElement e;
		int			level;
		HnswCandidate *hc;
		ItemPointer indextid;
		HnswNeighborArray *neighbors;

		indextid = &ntup->indextids[i];

		if (!ItemPointerIsValid(indextid))
			continue;

		e = HnswInitElementFromBlock(ItemPointerGetBlockNumber(indextid), ItemPointerGetOffsetNumber(indextid));

		/* Calculate level based on offset */
		level = element->level - i / m;
		if (level < 0)
			level = 0;

		neighbors = &element->neighbors[level];
		hc = &neighbors->items[neighbors->length++];
		hc->element = e;
	}
}

/*
 * Load neighbors
 */
void
HnswLoadNeighbors(HnswElement element, Relation index)
{
	Buffer		buf;
	Page		page;

	buf = ReadBuffer(index, element->neighborPage);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	LoadNeighborsFromPage(element, index, page);

	UnlockReleaseBuffer(buf);
}

/*
 * Load an element from a tuple
 */
void
HnswLoadElementFromTuple(HnswElement element, HnswElementTuple etup, bool loadHeaptids, bool loadVec)
{
	element->level = etup->level;
	element->deleted = etup->deleted;
	element->neighborPage = ItemPointerGetBlockNumber(&etup->neighbortid);
	element->neighborOffno = ItemPointerGetOffsetNumber(&etup->neighbortid);
	element->heaptids = NIL;

	if (loadHeaptids)
	{
		for (int i = 0; i < HNSW_HEAPTIDS; i++)
		{
			/* Can stop at first invalid */
			if (!ItemPointerIsValid(&etup->heaptids[i]))
				break;

			HnswAddHeapTid(element, &etup->heaptids[i]);
		}
	}

	if (loadVec)
	{
		element->vec = palloc(VECTOR_SIZE(etup->vec.dim));
		memcpy(element->vec, &etup->vec, VECTOR_SIZE(etup->vec.dim));
	}
}

/*
 * Load an element and optionally get its distance from q
 */
void
HnswLoadElement(HnswElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadVec)
{
	Buffer		buf;
	Page		page;
	HnswElementTuple etup;

	/* Read vector */
	buf = ReadBuffer(index, element->blkno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	etup = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, element->offno));

	Assert(HnswIsElementTuple(etup));

	/* Load element */
	HnswLoadElementFromTuple(element, etup, true, loadVec);

	/* Calculate distance */
	if (distance != NULL)
		*distance = (float) DatumGetFloat8(FunctionCall2Coll(procinfo, collation, *q, PointerGetDatum(&etup->vec)));

	UnlockReleaseBuffer(buf);
}

/*
 * Get the distance for a candidate
 */
static float
GetCandidateDistance(HnswCandidate * hc, Datum q, FmgrInfo *procinfo, Oid collation)
{
	return DatumGetFloat8(FunctionCall2Coll(procinfo, collation, q, PointerGetDatum(hc->element->vec)));
}

/*
 * Create a candidate for the entry point
 */
HnswCandidate *
HnswEntryCandidate(HnswElement entryPoint, Datum q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadVec)
{
	HnswCandidate *hc = palloc(sizeof(HnswCandidate));

	hc->element = entryPoint;
	if (index == NULL)
		hc->distance = GetCandidateDistance(hc, q, procinfo, collation);
	else
		HnswLoadElement(hc->element, &hc->distance, &q, index, procinfo, collation, loadVec);
	return hc;
}

/*
 * Compare candidate distances
 */
static int
CompareNearestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (((const HnswPairingHeapNode *) a)->inner->distance < ((const HnswPairingHeapNode *) b)->inner->distance)
		return 1;

	if (((const HnswPairingHeapNode *) a)->inner->distance > ((const HnswPairingHeapNode *) b)->inner->distance)
		return -1;

	return 0;
}

/*
 * Compare candidate distances
 */
static int
CompareFurthestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (((const HnswPairingHeapNode *) a)->inner->distance < ((const HnswPairingHeapNode *) b)->inner->distance)
		return -1;

	if (((const HnswPairingHeapNode *) a)->inner->distance > ((const HnswPairingHeapNode *) b)->inner->distance)
		return 1;

	return 0;
}

/*
 * Create a pairing heap node for a candidate
 */
static HnswPairingHeapNode *
CreatePairingHeapNode(HnswCandidate * c)
{
	HnswPairingHeapNode *node = palloc(sizeof(HnswPairingHeapNode));

	node->inner = c;
	return node;
}

/*
 * Add to visited
 */
static inline void
AddToVisited(HTAB *v, HnswCandidate * hc, Relation index, bool *found)
{
	if (index == NULL)
		hash_search(v, &hc->element, HASH_ENTER, found);
	else
	{
		ItemPointerData indextid;

		ItemPointerSet(&indextid, hc->element->blkno, hc->element->offno);
		hash_search(v, &indextid, HASH_ENTER, found);
	}
}

/*
 * Algorithm 2 from paper
 */
List *
HnswSearchLayer(Datum q, List *ep, int ef, int lc, Relation index, FmgrInfo *procinfo, Oid collation, bool inserting, HnswElement skipElement)
{
	ListCell   *lc2;

	List	   *w = NIL;
	pairingheap *C = pairingheap_allocate(CompareNearestCandidates, NULL);
	pairingheap *W = pairingheap_allocate(CompareFurthestCandidates, NULL);
	int			wlen = 0;
	HASHCTL		hash_ctl;
	HTAB	   *v;

	/* Create hash table */
	if (index == NULL)
	{
		hash_ctl.keysize = sizeof(HnswElement *);
		hash_ctl.entrysize = sizeof(HnswElement *);
	}
	else
	{
		hash_ctl.keysize = sizeof(ItemPointerData);
		hash_ctl.entrysize = sizeof(ItemPointerData);
	}

	hash_ctl.hcxt = CurrentMemoryContext;
	v = hash_create("hnsw visited", 256, &hash_ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Add entry points to v, C, and W */
	foreach(lc2, ep)
	{
		HnswCandidate *hc = (HnswCandidate *) lfirst(lc2);

		AddToVisited(v, hc, index, NULL);

		pairingheap_add(C, &(CreatePairingHeapNode(hc)->ph_node));
		pairingheap_add(W, &(CreatePairingHeapNode(hc)->ph_node));

		/*
		 * Do not count elements being deleted towards ef when vacuuming. It
		 * would be ideal to do this for inserts as well, but this could
		 * affect insert performance.
		 */
		if (skipElement == NULL || list_length(hc->element->heaptids) != 0)
			wlen++;
	}

	while (!pairingheap_is_empty(C))
	{
		HnswNeighborArray *neighborhood;
		HnswCandidate *c = ((HnswPairingHeapNode *) pairingheap_remove_first(C))->inner;
		HnswCandidate *f = ((HnswPairingHeapNode *) pairingheap_first(W))->inner;

		if (c->distance > f->distance)
			break;

		if (c->element->neighbors == NULL)
			HnswLoadNeighbors(c->element, index);

		/* Get the neighborhood at layer lc */
		neighborhood = &c->element->neighbors[lc];

		for (int i = 0; i < neighborhood->length; i++)
		{
			HnswCandidate *e = &neighborhood->items[i];
			bool		visited;

			AddToVisited(v, e, index, &visited);

			if (!visited)
			{
				float		eDistance;

				f = ((HnswPairingHeapNode *) pairingheap_first(W))->inner;

				if (index == NULL)
					eDistance = GetCandidateDistance(e, q, procinfo, collation);
				else
					HnswLoadElement(e->element, &eDistance, &q, index, procinfo, collation, inserting);

				Assert(!e->element->deleted);

				/* Make robust to issues */
				if (e->element->level < lc)
					continue;

				if (eDistance < f->distance || wlen < ef)
				{
					/* Copy e */
					HnswCandidate *ec = palloc(sizeof(HnswCandidate));

					ec->element = e->element;
					ec->distance = eDistance;

					pairingheap_add(C, &(CreatePairingHeapNode(ec)->ph_node));
					pairingheap_add(W, &(CreatePairingHeapNode(ec)->ph_node));

					/*
					 * Do not count elements being deleted towards ef when
					 * vacuuming. It would be ideal to do this for inserts as
					 * well, but this could affect insert performance.
					 */
					if (skipElement == NULL || list_length(e->element->heaptids) != 0)
					{
						wlen++;

						/* No need to decrement wlen */
						if (wlen > ef)
							pairingheap_remove_first(W);
					}
				}
			}
		}
	}

	/* Add each element of W to w */
	while (!pairingheap_is_empty(W))
	{
		HnswCandidate *hc = ((HnswPairingHeapNode *) pairingheap_remove_first(W))->inner;

		w = lappend(w, hc);
	}

	return w;
}

/*
 * Calculate the distance between elements
 */
static float
HnswGetDistance(HnswElement a, HnswElement b, int lc, FmgrInfo *procinfo, Oid collation)
{
	/* Look for cached distance */
	if (a->neighbors != NULL)
	{
		Assert(a->level >= lc);

		for (int i = 0; i < a->neighbors[lc].length; i++)
		{
			if (a->neighbors[lc].items[i].element == b)
				return a->neighbors[lc].items[i].distance;
		}
	}

	if (b->neighbors != NULL)
	{
		Assert(b->level >= lc);

		for (int i = 0; i < b->neighbors[lc].length; i++)
		{
			if (b->neighbors[lc].items[i].element == a)
				return b->neighbors[lc].items[i].distance;
		}
	}

	return DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(a->vec), PointerGetDatum(b->vec)));
}

/*
 * Check if an element is closer to q than any element from R
 */
static bool
CheckElementCloser(HnswCandidate * e, List *r, int lc, FmgrInfo *procinfo, Oid collation)
{
	ListCell   *lc2;

	foreach(lc2, r)
	{
		HnswCandidate *ri = lfirst(lc2);
		float		distance = HnswGetDistance(e->element, ri->element, lc, procinfo, collation);

		if (distance <= e->distance)
			return false;
	}

	return true;
}

/*
 * Algorithm 4 from paper
 */
static List *
SelectNeighbors(List *c, int m, int lc, FmgrInfo *procinfo, Oid collation, HnswCandidate * *pruned)
{
	List	   *r = NIL;
	List	   *w = list_copy(c);
	pairingheap *wd;

	if (list_length(w) <= m)
		return w;

	wd = pairingheap_allocate(CompareNearestCandidates, NULL);

	while (list_length(w) > 0 && list_length(r) < m)
	{
		/* Assumes w is already ordered desc */
		HnswCandidate *e = llast(w);
		bool		closer;

		w = list_delete_last(w);

		closer = CheckElementCloser(e, r, lc, procinfo, collation);

		if (closer)
			r = lappend(r, e);
		else
			pairingheap_add(wd, &(CreatePairingHeapNode(e)->ph_node));
	}

	/* Keep pruned connections */
	while (!pairingheap_is_empty(wd) && list_length(r) < m)
		r = lappend(r, ((HnswPairingHeapNode *) pairingheap_remove_first(wd))->inner);

	/* Return pruned for update connections */
	if (pruned != NULL)
	{
		if (!pairingheap_is_empty(wd))
			*pruned = ((HnswPairingHeapNode *) pairingheap_first(wd))->inner;
		else
			*pruned = linitial(w);
	}

	return r;
}

/*
 * Find duplicate element
 */
HnswElement
HnswFindDuplicate(HnswElement e)
{
	HnswNeighborArray *neighbors = &e->neighbors[0];

	for (int i = 0; i < neighbors->length; i++)
	{
		HnswCandidate *neighbor = &neighbors->items[i];

		/* Exit early since ordered by distance */
		if (vector_cmp_internal(e->vec, neighbor->element->vec) != 0)
			break;

		/* Check for space */
		if (list_length(neighbor->element->heaptids) < HNSW_HEAPTIDS)
			return neighbor->element;
	}

	return NULL;
}

/*
 * Add connections
 */
static void
AddConnections(HnswElement element, List *neighbors, int m, int lc)
{
	ListCell   *lc2;
	HnswNeighborArray *a = &element->neighbors[lc];

	foreach(lc2, neighbors)
		a->items[a->length++] = *((HnswCandidate *) lfirst(lc2));
}

/*
 * Compare candidate distances
 */
static int
#if PG_VERSION_NUM >= 130000
CompareCandidateDistances(const ListCell *a, const ListCell *b)
#else
CompareCandidateDistances(const void *a, const void *b)
#endif
{
	HnswCandidate *hca = lfirst((ListCell *) a);
	HnswCandidate *hcb = lfirst((ListCell *) b);

	if (hca->distance < hcb->distance)
		return 1;

	if (hca->distance > hcb->distance)
		return -1;

	return 0;
}

/*
 * Update connections
 */
void
HnswUpdateConnection(HnswElement element, HnswCandidate * hc, int m, int lc, int *updateIdx, Relation index, FmgrInfo *procinfo, Oid collation)
{
	HnswNeighborArray *currentNeighbors = &hc->element->neighbors[lc];

	HnswCandidate hc2;

	hc2.element = element;
	hc2.distance = hc->distance;

	if (currentNeighbors->length < m)
	{
		currentNeighbors->items[currentNeighbors->length++] = hc2;

		/* Track update */
		if (updateIdx != NULL)
			*updateIdx = -2;
	}
	else
	{
		/* Shrink connections */
		HnswCandidate *pruned = NULL;

		/* Load elements on insert */
		if (index != NULL)
		{
			Datum		q = PointerGetDatum(hc->element->vec);

			for (int i = 0; i < currentNeighbors->length; i++)
			{
				HnswCandidate *hc3 = &currentNeighbors->items[i];

				if (hc3->element->vec == NULL)
					HnswLoadElement(hc3->element, &hc3->distance, &q, index, procinfo, collation, true);
				else
					hc3->distance = GetCandidateDistance(hc3, q, procinfo, collation);

				/* Prune element if being deleted */
				if (list_length(hc3->element->heaptids) == 0)
				{
					pruned = &currentNeighbors->items[i];
					break;
				}
			}
		}

		if (pruned == NULL)
		{
			List	   *c = NIL;

			/* Add and sort candidates */
			for (int i = 0; i < currentNeighbors->length; i++)
				c = lappend(c, &currentNeighbors->items[i]);
			c = lappend(c, &hc2);
			list_sort(c, CompareCandidateDistances);

			SelectNeighbors(c, m, lc, procinfo, collation, &pruned);

			/* Should not happen */
			if (pruned == NULL)
				return;
		}

		/* Find and replace the pruned element */
		for (int i = 0; i < currentNeighbors->length; i++)
		{
			if (currentNeighbors->items[i].element == pruned->element)
			{
				currentNeighbors->items[i] = hc2;

				/* Track update */
				if (updateIdx != NULL)
					*updateIdx = i;

				break;
			}
		}
	}
}

/*
 * Remove elements being deleted or skipped
 */
static List *
RemoveElements(List *w, HnswElement skipElement)
{
	ListCell   *lc2;
	List	   *w2 = NIL;

	foreach(lc2, w)
	{
		HnswCandidate *hc = (HnswCandidate *) lfirst(lc2);

		/* Skip self for vacuuming update */
		if (skipElement != NULL && hc->element->blkno == skipElement->blkno && hc->element->offno == skipElement->offno)
			continue;

		if (list_length(hc->element->heaptids) != 0)
			w2 = lappend(w2, hc);
	}

	return w2;
}

/*
 * Algorithm 1 from paper
 */
void
HnswInsertElement(HnswElement element, HnswElement entryPoint, Relation index, FmgrInfo *procinfo, Oid collation, int m, int efConstruction, bool existing)
{
	List	   *ep;
	List	   *w;
	int			level = element->level;
	int			entryLevel;
	Datum		q = PointerGetDatum(element->vec);
	HnswElement skipElement = existing ? element : NULL;

	/* No neighbors if no entry point */
	if (entryPoint == NULL)
		return;

	/* Get entry point and level */
	ep = list_make1(HnswEntryCandidate(entryPoint, q, index, procinfo, collation, true));
	entryLevel = entryPoint->level;

	/* 1st phase: greedy search to insert level */
	for (int lc = entryLevel; lc >= level + 1; lc--)
	{
		w = HnswSearchLayer(q, ep, 1, lc, index, procinfo, collation, true, skipElement);
		ep = w;
	}

	if (level > entryLevel)
		level = entryLevel;

	/* Add one for existing element */
	if (existing)
		efConstruction++;

	/* 2nd phase */
	for (int lc = level; lc >= 0; lc--)
	{
		int			lm = HnswGetLayerM(m, lc);
		List	   *neighbors;
		List	   *lw;

		w = HnswSearchLayer(q, ep, efConstruction, lc, index, procinfo, collation, true, skipElement);

		/* Elements being deleted or skipped can help with search */
		/* but should be removed before selecting neighbors */
		if (index != NULL)
			lw = RemoveElements(w, skipElement);
		else
			lw = w;

		neighbors = SelectNeighbors(lw, lm, lc, procinfo, collation, NULL);

		AddConnections(element, neighbors, lm, lc);

		ep = w;
	}
}
