#include "postgres.h"

#include <math.h>

#include "hnsw.h"
#include "storage/bufmgr.h"
#include "vector.h"

/*
 * Get the number of connection in the index
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
 * Create an element from block and offset
 */
static HnswElement
CreateElementFromBlock(BlockNumber blkno, OffsetNumber offno)
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
GetEntryPoint(Relation index)
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
		entryPoint = CreateElementFromBlock(metap->entryBlkno, metap->entryOffno);

	UnlockReleaseBuffer(buf);

	return entryPoint;
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
 * Calculate the distance between elements
 */
static float
HnswGetDistance(HnswElement a, HnswElement b, int lc, FmgrInfo *procinfo, Oid collation)
{
	/* Look for cached distance */
	if (a->neighbors != NULL)
	{
		for (int i = 0; i < a->neighbors[lc].length; i++)
		{
			if (a->neighbors[lc].items[i].element == b)
				return a->neighbors[lc].items[i].distance;
		}
	}

	if (b->neighbors != NULL)
	{
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

	if (list_length(w) < m)
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
 * Create update
 */
static HnswUpdate *
CreateUpdate(HnswCandidate * hc, int level, int index)
{
	HnswUpdate *update = palloc(sizeof(HnswUpdate));

	update->hc = *hc;
	update->level = level;
	update->index = index;
	return update;
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
 * Load an element and optionally get its distance from q
 */
void
HnswLoadElement(HnswElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadvec)
{
	Buffer		buf;
	Page		page;
	HnswElementTuple item;

	/* Read vector */
	buf = ReadBuffer(index, element->blkno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	item = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, element->offno));

	/* Load element */
	element->heaptids = NIL;
	for (int i = 0; i < HNSW_HEAPTIDS; i++)
	{
		/* Can stop at first invalid */
		if (!ItemPointerIsValid(&item->heaptids[i]))
			break;

		HnswAddHeapTid(element, &item->heaptids[i]);
	}
	element->level = item->level;
	element->neighborPage = item->neighborPage;
	element->deleted = item->deleted;

	if (loadvec)
	{
		element->vec = palloc(VECTOR_SIZE(item->vec.dim));
		memcpy(element->vec, &item->vec, VECTOR_SIZE(item->vec.dim));
	}

	/* Calculate distance */
	if (distance != NULL)
		*distance = (float) DatumGetFloat8(FunctionCall2Coll(procinfo, collation, *q, PointerGetDatum(&item->vec)));

	UnlockReleaseBuffer(buf);
}

/*
 * Update connections
 */
static void
UpdateConnections(HnswElement element, List *neighbors, int m, int lc, List **updates, Relation index, FmgrInfo *procinfo, Oid collation)
{
	ListCell   *lc2;

	foreach(lc2, neighbors)
	{
		HnswCandidate *hc = (HnswCandidate *) lfirst(lc2);
		HnswNeighborArray *currentNeighbors = &hc->element->neighbors[lc];

		HnswCandidate hc2;

		hc2.element = element;
		hc2.distance = hc->distance;

		if (currentNeighbors->length < m)
		{
			currentNeighbors->items[currentNeighbors->length++] = hc2;

			/* Track updates */
			if (updates != NULL)
				*updates = lappend(*updates, CreateUpdate(hc, lc, currentNeighbors->length - 1));
		}
		else
		{
			/* Shrink connections */
			HnswCandidate *pruned = NULL;
			List	   *c = NIL;

			/* Add and sort candidates */
			for (int i = 0; i < currentNeighbors->length; i++)
				c = lappend(c, &currentNeighbors->items[i]);
			c = lappend(c, &hc2);
			list_sort(c, CompareCandidateDistances);

			/* Load elements on insert */
			if (index != NULL)
			{
				for (int i = 0; i < currentNeighbors->length; i++)
				{
					if (currentNeighbors->items[i].element->vec == NULL)
					{
						HnswLoadElement(currentNeighbors->items[i].element, NULL, NULL, index, procinfo, collation, true);

						/* Prune deleted element */
						if (currentNeighbors->items[i].element->deleted)
						{
							pruned = &currentNeighbors->items[i];
							break;
						}
					}
				}
			}

			if (pruned == NULL)
			{
				SelectNeighbors(c, m, lc, procinfo, collation, &pruned);

				/* Should not happen */
				if (pruned == NULL)
					continue;
			}

			/* Find and replace the pruned element */
			for (int i = 0; i < currentNeighbors->length; i++)
			{
				if (currentNeighbors->items[i].element == pruned->element)
				{
					currentNeighbors->items[i] = hc2;

					/* Track updates */
					if (updates != NULL)
						*updates = lappend(*updates, CreateUpdate(hc, lc, i));

					break;
				}
			}
		}
	}
}

/*
 * Initialize neighbors
 */
void
HnswInitNeighbors(HnswElement element, int m)
{
	int			level = element->level;

	element->neighbors = palloc(sizeof(HnswNeighborArray) * (level + 1));

	for (int lc = 0; lc <= level; lc++)
	{
		HnswNeighborArray *a;
		int			lm = GetLayerM(m, lc);

		a = &element->neighbors[lc];
		a->length = 0;
		a->items = palloc(sizeof(HnswCandidate) * lm);
	}
}

/*
 * Load neighbors
 */
static void
LoadNeighbors(HnswCandidate * c, Relation index)
{
	Buffer		buf;
	Page		page;
	OffsetNumber offno;
	OffsetNumber maxoffno;
	HnswNeighborTuple neighbor;
	HnswNeighborArray *neighbors;
	int			m = HnswGetM(index);

	buf = ReadBuffer(index, c->element->neighborPage);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	maxoffno = PageGetMaxOffsetNumber(page);

	HnswInitNeighbors(c->element, m);

	/* If not, neighbor page represents new item */
	/* Only caught if item has a different level */
	/* TODO Use versioning to fix this? */
	if (maxoffno == (c->element->level + 2) * m)
	{
		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			HnswElement element;
			int			level;
			HnswCandidate *hc;

			neighbor = (HnswNeighborTuple) PageGetItem(page, PageGetItemId(page, offno));

			if (!ItemPointerIsValid(&neighbor->indextid))
				continue;

			element = CreateElementFromBlock(ItemPointerGetBlockNumber(&neighbor->indextid), ItemPointerGetOffsetNumber(&neighbor->indextid));

			/* Calculate level based on offset */
			level = c->element->level - (offno - FirstOffsetNumber) / m;
			if (level < 0)
				level = 0;

			neighbors = &c->element->neighbors[level];
			hc = &neighbors->items[neighbors->length];
			hc->element = element;
			hc->distance = neighbor->distance;
			neighbors->length++;
		}
	}

	UnlockReleaseBuffer(buf);
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
	list_free(element->heaptids);
	for (int lc = 0; lc <= element->level; lc++)
		pfree(element->neighbors[lc].items);
	pfree(element->neighbors);
	pfree(element->vec);
	pfree(element);
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
SearchLayer(Datum q, List *ep, int ef, int lc, Relation index, FmgrInfo *procinfo, Oid collation, bool inserting, BlockNumber *skipPage)
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
			LoadNeighbors(c, index);

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

				/* Skip if fully deleted */
				if (e->element->deleted)
					continue;

				/* Skip for inserts if deleting */
				if (inserting && list_length(e->element->heaptids) == 0)
					continue;

				/* Skip self for vacuuming update */
				if (skipPage != NULL && e->element->neighborPage == *skipPage)
					continue;

				/* Stale read */
				if (e->element->level < lc)
					continue;

				if (eDistance < f->distance || wlen < ef)
				{
					/* copy e */
					HnswCandidate *e2 = palloc(sizeof(HnswCandidate));

					e2->element = e->element;
					e2->distance = eDistance;

					pairingheap_add(C, &(CreatePairingHeapNode(e2)->ph_node));
					pairingheap_add(W, &(CreatePairingHeapNode(e2)->ph_node));
					wlen++;

					if (wlen > ef)
					{
						pairingheap_remove_first(W);
						wlen--;
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
 * Create a candidate for the entry point
 */
HnswCandidate *
EntryCandidate(HnswElement entryPoint, Datum q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadvec)
{
	HnswCandidate *hc = palloc(sizeof(HnswCandidate));

	hc->element = entryPoint;
	if (index == NULL)
		hc->distance = GetCandidateDistance(hc, q, procinfo, collation);
	else
		HnswLoadElement(hc->element, &hc->distance, &q, index, procinfo, collation, loadvec);
	return hc;
}

/*
 * Find duplicate element
 */
static HnswElement
HnswFindDuplicate(HnswElement e, List *neighbors)
{
	ListCell   *lc;

	foreach(lc, neighbors)
	{
		HnswCandidate *neighbor = lfirst(lc);

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
 * Algorithm 1 from paper
 */
HnswElement
HnswInsertElement(HnswElement element, HnswElement entryPoint, Relation index, FmgrInfo *procinfo, Oid collation, int m, int efConstruction, List **updates, bool vacuuming)
{
	List	   *ep = NIL;
	List	   *w;
	int			level = element->level;
	int			entryLevel;
	List	  **newNeighbors = palloc(sizeof(List *) * (level + 1));
	Datum		q = PointerGetDatum(element->vec);
	HnswElement dup;
	BlockNumber *skipPage = vacuuming ? &element->neighborPage : NULL;

	/* Get entry point and level */
	if (entryPoint != NULL)
	{
		ep = lappend(ep, EntryCandidate(entryPoint, q, index, procinfo, collation, true));
		entryLevel = entryPoint->level;
	}
	else
		entryLevel = -1;

	for (int lc = entryLevel; lc >= level + 1; lc--)
	{
		w = SearchLayer(q, ep, 1, lc, index, procinfo, collation, true, skipPage);
		ep = w;
	}

	if (level > entryLevel)
		level = entryLevel;

	for (int lc = level; lc >= 0; lc--)
	{
		int			lm = GetLayerM(m, lc);

		w = SearchLayer(q, ep, efConstruction, lc, index, procinfo, collation, true, skipPage);
		newNeighbors[lc] = SelectNeighbors(w, lm, lc, procinfo, collation, NULL);
		ep = w;
	}

	if (level >= 0 && !vacuuming)
	{
		dup = HnswFindDuplicate(element, newNeighbors[0]);
		if (dup != NULL)
			return dup;
	}

	/* Update connections */
	for (int lc = level; lc >= 0; lc--)
	{
		int			lm = GetLayerM(m, lc);

		AddConnections(element, newNeighbors[lc], lm, lc);

		if (!vacuuming)
			UpdateConnections(element, newNeighbors[lc], lm, lc, updates, index, procinfo, collation);
	}

	return NULL;
}

/*
 * Update the metapage
 */
void
UpdateMetaPage(Relation index, bool updateEntry, HnswElement entryPoint, BlockNumber insertPage, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	HnswMetaPage metap;

	buf = ReadBufferExtended(index, forkNum, HNSW_METAPAGE_BLKNO, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	metap = HnswPageGetMeta(page);

	if (updateEntry)
	{
		if (entryPoint == NULL)
		{
			metap->entryBlkno = InvalidBlockNumber;
			metap->entryOffno = InvalidOffsetNumber;
		}
		else
		{
			metap->entryBlkno = entryPoint->blkno;
			metap->entryOffno = entryPoint->offno;
		}
	}

	if (BlockNumberIsValid(insertPage))
		metap->insertPage = insertPage;

	HnswCommitBuffer(buf, state);
}

/*
 * Add neighbors to page
 */
void
AddNeighborsToPage(Relation index, Page page, HnswElement e, HnswNeighborTuple neighbor, Size neighborsz, int m)
{
	for (int lc = e->level; lc >= 0; lc--)
	{
		HnswNeighborArray *neighbors = &e->neighbors[lc];
		int			lm = GetLayerM(m, lc);

		for (int i = 0; i < lm; i++)
		{
			if (i < neighbors->length)
			{
				HnswCandidate *hc = &neighbors->items[i];

				ItemPointerSet(&neighbor->indextid, hc->element->blkno, hc->element->offno);
				neighbor->distance = hc->distance;
			}
			else
			{
				ItemPointerSetInvalid(&neighbor->indextid);
				neighbor->distance = NAN;
			}

			if (PageAddItem(page, (Item) neighbor, neighborsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
				elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
		}
	}
}
