#include "postgres.h"

#include <math.h>

#include "access/generic_xlog.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "hnsw.h"
#include "lib/pairingheap.h"
#include "sparsevec.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/memdebug.h"
#include "utils/rel.h"

#if PG_VERSION_NUM >= 130000
#include "common/hashfn.h"
#else
#include "utils/hashutils.h"
#endif

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

/* TID hash table */
static uint32
hash_tid(ItemPointerData tid)
{
	union
	{
		uint64		i;
		ItemPointerData tid;
	}			x;

	/* Initialize unused bytes */
	x.i = 0;
	x.tid = tid;

	return murmurhash64(x.i);
}

#define SH_PREFIX		tidhash
#define SH_ELEMENT_TYPE	TidHashEntry
#define SH_KEY_TYPE		ItemPointerData
#define	SH_KEY			tid
#define SH_HASH_KEY(tb, key)	hash_tid(key)
#define SH_EQUAL(tb, a, b)		ItemPointerEquals(&a, &b)
#define	SH_SCOPE		extern
#define SH_DEFINE
#include "lib/simplehash.h"

/* Pointer hash table */
static uint32
hash_pointer(uintptr_t ptr)
{
#if SIZEOF_VOID_P == 8
	return murmurhash64((uint64) ptr);
#else
	return murmurhash32((uint32) ptr);
#endif
}

#define SH_PREFIX		pointerhash
#define SH_ELEMENT_TYPE	PointerHashEntry
#define SH_KEY_TYPE		uintptr_t
#define	SH_KEY			ptr
#define SH_HASH_KEY(tb, key)	hash_pointer(key)
#define SH_EQUAL(tb, a, b)		(a == b)
#define	SH_SCOPE		extern
#define SH_DEFINE
#include "lib/simplehash.h"

/* Offset hash table */
static uint32
hash_offset(Size offset)
{
#if SIZEOF_SIZE_T == 8
	return murmurhash64((uint64) offset);
#else
	return murmurhash32((uint32) offset);
#endif
}

#define SH_PREFIX		offsethash
#define SH_ELEMENT_TYPE	OffsetHashEntry
#define SH_KEY_TYPE		Size
#define	SH_KEY			offset
#define SH_HASH_KEY(tb, key)	hash_offset(key)
#define SH_EQUAL(tb, a, b)		(a == b)
#define	SH_SCOPE		extern
#define SH_DEFINE
#include "lib/simplehash.h"

typedef union
{
	pointerhash_hash *pointers;
	offsethash_hash *offsets;
	tidhash_hash *tids;
}			visited_hash;

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
HnswOptionalProcInfo(Relation index, uint16 procnum)
{
	if (!OidIsValid(index_getprocid(index, 1, procnum)))
		return NULL;

	return index_getprocinfo(index, 1, procnum);
}

/*
 * Normalize value
 */
Datum
HnswNormValue(const HnswTypeInfo * typeInfo, Oid collation, Datum value)
{
	return DirectFunctionCall1Coll(typeInfo->normalize, collation, value);
}

/*
 * Check if non-zero norm
 */
bool
HnswCheckNorm(FmgrInfo *procinfo, Oid collation, Datum value)
{
	return DatumGetFloat8(FunctionCall1Coll(procinfo, collation, value)) > 0;
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
 * Allocate a neighbor array
 */
static HnswNeighborArray *
HnswInitNeighborArray(int lm, HnswAllocator * allocator)
{
	HnswNeighborArray *a = HnswAlloc(allocator, HNSW_NEIGHBOR_ARRAY_SIZE(lm));

	a->length = 0;
	a->closerSet = false;
	return a;
}

/*
 * Allocate neighbors
 */
void
HnswInitNeighbors(char *base, HnswElement element, int m, HnswAllocator * allocator)
{
	int			level = element->level;
	HnswNeighborArrayPtr *neighborList = (HnswNeighborArrayPtr *) HnswAlloc(allocator, sizeof(HnswNeighborArrayPtr) * (level + 1));

	HnswPtrStore(base, element->neighbors, neighborList);

	for (int lc = 0; lc <= level; lc++)
		HnswPtrStore(base, neighborList[lc], HnswInitNeighborArray(HnswGetLayerM(m, lc), allocator));
}

/*
 * Allocate memory from the allocator
 */
void *
HnswAlloc(HnswAllocator * allocator, Size size)
{
	if (allocator)
		return (*(allocator)->alloc) (size, (allocator)->state);

	return palloc(size);
}

/*
 * Allocate an element
 */
HnswElement
HnswInitElement(char *base, ItemPointer heaptid, int m, double ml, int maxLevel, HnswAllocator * allocator)
{
	HnswElement element = HnswAlloc(allocator, sizeof(HnswElementData));

	int			level = (int) (-log(RandomDouble()) * ml);

	/* Cap level */
	if (level > maxLevel)
		level = maxLevel;

	element->heaptidsLength = 0;
	HnswAddHeapTid(element, heaptid);

	element->level = level;
	element->deleted = 0;

	HnswInitNeighbors(base, element, m, allocator);

	HnswPtrStore(base, element->value, (Pointer) NULL);

	return element;
}

/*
 * Add a heap TID to an element
 */
void
HnswAddHeapTid(HnswElement element, ItemPointer heaptid)
{
	element->heaptids[element->heaptidsLength++] = *heaptid;
}

/*
 * Allocate an element from block and offset numbers
 */
HnswElement
HnswInitElementFromBlock(BlockNumber blkno, OffsetNumber offno)
{
	HnswElement element = palloc(sizeof(HnswElementData));
	char	   *base = NULL;

	element->blkno = blkno;
	element->offno = offno;
	HnswPtrStore(base, element->neighbors, (HnswNeighborArrayPtr *) NULL);
	HnswPtrStore(base, element->value, (Pointer) NULL);
	return element;
}

/*
 * Get the metapage info
 */
void
HnswGetMetaPageInfo(Relation index, int *m, HnswElement * entryPoint)
{
	Buffer		buf;
	Page		page;
	HnswMetaPage metap;

	buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = HnswPageGetMeta(page);

	if (unlikely(metap->magicNumber != HNSW_MAGIC_NUMBER))
		elog(ERROR, "hnsw index is not valid");

	if (m != NULL)
		*m = metap->m;

	if (entryPoint != NULL)
	{
		if (BlockNumberIsValid(metap->entryBlkno))
		{
			*entryPoint = HnswInitElementFromBlock(metap->entryBlkno, metap->entryOffno);
			(*entryPoint)->level = metap->entryLevel;
		}
		else
			*entryPoint = NULL;
	}

	UnlockReleaseBuffer(buf);
}

/*
 * Get the entry point
 */
HnswElement
HnswGetEntryPoint(Relation index)
{
	HnswElement entryPoint;

	HnswGetMetaPageInfo(index, NULL, &entryPoint);

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
HnswUpdateMetaPage(Relation index, int updateEntry, HnswElement entryPoint, BlockNumber insertPage, ForkNumber forkNum, bool building)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;

	buf = ReadBufferExtended(index, forkNum, HNSW_METAPAGE_BLKNO, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	if (building)
	{
		state = NULL;
		page = BufferGetPage(buf);
	}
	else
	{
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);
	}

	HnswUpdateMetaPageInfo(page, updateEntry, entryPoint, insertPage);

	if (building)
		MarkBufferDirty(buf);
	else
		GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Set element tuple, except for neighbor info
 */
void
HnswSetElementTuple(char *base, HnswElementTuple etup, HnswElement element)
{
	Pointer		valuePtr = HnswPtrAccess(base, element->value);

	etup->type = HNSW_ELEMENT_TUPLE_TYPE;
	etup->level = element->level;
	etup->deleted = 0;
	for (int i = 0; i < HNSW_HEAPTIDS; i++)
	{
		if (i < element->heaptidsLength)
			etup->heaptids[i] = element->heaptids[i];
		else
			ItemPointerSetInvalid(&etup->heaptids[i]);
	}
	memcpy(&etup->data, valuePtr, VARSIZE_ANY(valuePtr));
}

/*
 * Set neighbor tuple
 */
void
HnswSetNeighborTuple(char *base, HnswNeighborTuple ntup, HnswElement e, int m)
{
	int			idx = 0;

	ntup->type = HNSW_NEIGHBOR_TUPLE_TYPE;

	for (int lc = e->level; lc >= 0; lc--)
	{
		HnswNeighborArray *neighbors = HnswGetNeighbors(base, e, lc);
		int			lm = HnswGetLayerM(m, lc);

		for (int i = 0; i < lm; i++)
		{
			ItemPointer indextid = &ntup->indextids[idx++];

			if (i < neighbors->length)
			{
				HnswCandidate *hc = &neighbors->items[i];
				HnswElement hce = HnswPtrAccess(base, hc->element);

				ItemPointerSet(indextid, hce->blkno, hce->offno);
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
LoadNeighborsFromPage(HnswElement element, Relation index, Page page, int m)
{
	char	   *base = NULL;

	HnswNeighborTuple ntup = (HnswNeighborTuple) PageGetItem(page, PageGetItemId(page, element->neighborOffno));
	int			neighborCount = (element->level + 2) * m;

	Assert(HnswIsNeighborTuple(ntup));

	HnswInitNeighbors(base, element, m, NULL);

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

		neighbors = HnswGetNeighbors(base, element, level);
		hc = &neighbors->items[neighbors->length++];
		HnswPtrStore(base, hc->element, e);
	}
}

/*
 * Load neighbors
 */
void
HnswLoadNeighbors(HnswElement element, Relation index, int m)
{
	Buffer		buf;
	Page		page;

	buf = ReadBuffer(index, element->neighborPage);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	LoadNeighborsFromPage(element, index, page, m);

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
	element->heaptidsLength = 0;

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
		char	   *base = NULL;
		Datum		value = datumCopy(PointerGetDatum(&etup->data), false, -1);

		HnswPtrStore(base, element->value, DatumGetPointer(value));
	}
}

/*
 * Load an element and optionally get its distance from q
 */
void
HnswLoadElement(HnswElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadVec, float *maxDistance)
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

	/* Calculate distance */
	if (distance != NULL)
	{
		if (DatumGetPointer(*q) == NULL)
			*distance = 0;
		else
			*distance = (float) DatumGetFloat8(FunctionCall2Coll(procinfo, collation, *q, PointerGetDatum(&etup->data)));
	}

	/* Load element */
	if (distance == NULL || maxDistance == NULL || *distance < *maxDistance)
		HnswLoadElementFromTuple(element, etup, true, loadVec);

	UnlockReleaseBuffer(buf);
}

/*
 * Get the distance for a candidate
 */
static float
GetCandidateDistance(char *base, HnswCandidate * hc, Datum q, FmgrInfo *procinfo, Oid collation)
{
	HnswElement hce = HnswPtrAccess(base, hc->element);
	Datum		value = HnswGetValue(base, hce);

	return DatumGetFloat8(FunctionCall2Coll(procinfo, collation, q, value));
}

/*
 * Create a candidate for the entry point
 */
HnswCandidate *
HnswEntryCandidate(char *base, HnswElement entryPoint, Datum q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadVec)
{
	HnswCandidate *hc = palloc(sizeof(HnswCandidate));

	HnswPtrStore(base, hc->element, entryPoint);
	if (index == NULL)
		hc->distance = GetCandidateDistance(base, hc, q, procinfo, collation);
	else
		HnswLoadElement(entryPoint, &hc->distance, &q, index, procinfo, collation, loadVec, NULL);
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
 * Init visited
 */
static inline void
InitVisited(char *base, visited_hash * v, Relation index, int ef, int m)
{
	if (index != NULL)
		v->tids = tidhash_create(CurrentMemoryContext, ef * m * 2, NULL);
	else if (base != NULL)
		v->offsets = offsethash_create(CurrentMemoryContext, ef * m * 2, NULL);
	else
		v->pointers = pointerhash_create(CurrentMemoryContext, ef * m * 2, NULL);
}

/*
 * Add to visited
 */
static inline void
AddToVisited(char *base, visited_hash * v, HnswCandidate * hc, Relation index, bool *found)
{
	if (index != NULL)
	{
		HnswElement element = HnswPtrAccess(base, hc->element);
		ItemPointerData indextid;

		ItemPointerSet(&indextid, element->blkno, element->offno);
		tidhash_insert(v->tids, indextid, found);
	}
	else if (base != NULL)
	{
#if PG_VERSION_NUM >= 130000
		HnswElement element = HnswPtrAccess(base, hc->element);

		offsethash_insert_hash(v->offsets, HnswPtrOffset(hc->element), element->hash, found);
#else
		offsethash_insert(v->offsets, HnswPtrOffset(hc->element), found);
#endif
	}
	else
	{
#if PG_VERSION_NUM >= 130000
		HnswElement element = HnswPtrAccess(base, hc->element);

		pointerhash_insert_hash(v->pointers, (uintptr_t) HnswPtrPointer(hc->element), element->hash, found);
#else
		pointerhash_insert(v->pointers, (uintptr_t) HnswPtrPointer(hc->element), found);
#endif
	}
}

/*
 * Count element towards ef
 */
static inline bool
CountElement(char *base, HnswElement skipElement, HnswCandidate * hc)
{
	HnswElement e;

	if (skipElement == NULL)
		return true;

	/* Ensure does not access heaptidsLength during in-memory build */
	pg_memory_barrier();

	e = HnswPtrAccess(base, hc->element);
	return e->heaptidsLength != 0;
}

/*
 * Algorithm 2 from paper
 */
List *
HnswSearchLayer(char *base, Datum q, List *ep, int ef, int lc, Relation index, FmgrInfo *procinfo, Oid collation, int m, bool inserting, HnswElement skipElement)
{
	List	   *w = NIL;
	pairingheap *C = pairingheap_allocate(CompareNearestCandidates, NULL);
	pairingheap *W = pairingheap_allocate(CompareFurthestCandidates, NULL);
	int			wlen = 0;
	visited_hash v;
	ListCell   *lc2;
	HnswNeighborArray *neighborhoodData = NULL;
	Size		neighborhoodSize;

	InitVisited(base, &v, index, ef, m);

	/* Create local memory for neighborhood if needed */
	if (index == NULL)
	{
		neighborhoodSize = HNSW_NEIGHBOR_ARRAY_SIZE(HnswGetLayerM(m, lc));
		neighborhoodData = palloc(neighborhoodSize);
	}

	/* Add entry points to v, C, and W */
	foreach(lc2, ep)
	{
		HnswCandidate *hc = (HnswCandidate *) lfirst(lc2);
		bool		found;

		AddToVisited(base, &v, hc, index, &found);

		pairingheap_add(C, &(CreatePairingHeapNode(hc)->ph_node));
		pairingheap_add(W, &(CreatePairingHeapNode(hc)->ph_node));

		/*
		 * Do not count elements being deleted towards ef when vacuuming. It
		 * would be ideal to do this for inserts as well, but this could
		 * affect insert performance.
		 */
		if (CountElement(base, skipElement, hc))
			wlen++;
	}

	while (!pairingheap_is_empty(C))
	{
		HnswNeighborArray *neighborhood;
		HnswCandidate *c = ((HnswPairingHeapNode *) pairingheap_remove_first(C))->inner;
		HnswCandidate *f = ((HnswPairingHeapNode *) pairingheap_first(W))->inner;
		HnswElement cElement;

		if (c->distance > f->distance)
			break;

		cElement = HnswPtrAccess(base, c->element);

		if (HnswPtrIsNull(base, cElement->neighbors))
			HnswLoadNeighbors(cElement, index, m);

		/* Get the neighborhood at layer lc */
		neighborhood = HnswGetNeighbors(base, cElement, lc);

		/* Copy neighborhood to local memory if needed */
		if (index == NULL)
		{
			LWLockAcquire(&cElement->lock, LW_SHARED);
			memcpy(neighborhoodData, neighborhood, neighborhoodSize);
			LWLockRelease(&cElement->lock);
			neighborhood = neighborhoodData;
		}

		for (int i = 0; i < neighborhood->length; i++)
		{
			HnswCandidate *e = &neighborhood->items[i];
			bool		visited;

			AddToVisited(base, &v, e, index, &visited);

			if (!visited)
			{
				float		eDistance;
				HnswElement eElement = HnswPtrAccess(base, e->element);
				bool		alwaysAdd = wlen < ef;

				f = ((HnswPairingHeapNode *) pairingheap_first(W))->inner;

				if (index == NULL)
					eDistance = GetCandidateDistance(base, e, q, procinfo, collation);
				else
					HnswLoadElement(eElement, &eDistance, &q, index, procinfo, collation, inserting, alwaysAdd ? NULL : &f->distance);

				if (eDistance < f->distance || alwaysAdd)
				{
					HnswCandidate *ec;

					Assert(!eElement->deleted);

					/* Make robust to issues */
					if (eElement->level < lc)
						continue;

					/* Copy e */
					ec = palloc(sizeof(HnswCandidate));
					HnswPtrStore(base, ec->element, eElement);
					ec->distance = eDistance;

					pairingheap_add(C, &(CreatePairingHeapNode(ec)->ph_node));
					pairingheap_add(W, &(CreatePairingHeapNode(ec)->ph_node));

					/*
					 * Do not count elements being deleted towards ef when
					 * vacuuming. It would be ideal to do this for inserts as
					 * well, but this could affect insert performance.
					 */
					if (CountElement(base, skipElement, e))
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
 * Compare candidate distances with pointer tie-breaker
 */
static int
#if PG_VERSION_NUM >= 130000
CompareCandidateDistances(const ListCell *a, const ListCell *b)
{
	HnswCandidate *hca = lfirst(a);
	HnswCandidate *hcb = lfirst(b);
#else
CompareCandidateDistances(const void *a, const void *b)
{
	HnswCandidate *hca = lfirst(*(ListCell **) a);
	HnswCandidate *hcb = lfirst(*(ListCell **) b);
#endif

	if (hca->distance < hcb->distance)
		return 1;

	if (hca->distance > hcb->distance)
		return -1;

	if (HnswPtrPointer(hca->element) < HnswPtrPointer(hcb->element))
		return 1;

	if (HnswPtrPointer(hca->element) > HnswPtrPointer(hcb->element))
		return -1;

	return 0;
}

/*
 * Compare candidate distances with offset tie-breaker
 */
static int
#if PG_VERSION_NUM >= 130000
CompareCandidateDistancesOffset(const ListCell *a, const ListCell *b)
{
	HnswCandidate *hca = lfirst(a);
	HnswCandidate *hcb = lfirst(b);
#else
CompareCandidateDistancesOffset(const void *a, const void *b)
{
	HnswCandidate *hca = lfirst(*(ListCell **) a);
	HnswCandidate *hcb = lfirst(*(ListCell **) b);
#endif

	if (hca->distance < hcb->distance)
		return 1;

	if (hca->distance > hcb->distance)
		return -1;

	if (HnswPtrOffset(hca->element) < HnswPtrOffset(hcb->element))
		return 1;

	if (HnswPtrOffset(hca->element) > HnswPtrOffset(hcb->element))
		return -1;

	return 0;
}

/*
 * Calculate the distance between elements
 */
static float
HnswGetDistance(char *base, HnswElement a, HnswElement b, FmgrInfo *procinfo, Oid collation)
{
	Datum		aValue = HnswGetValue(base, a);
	Datum		bValue = HnswGetValue(base, b);

	return DatumGetFloat8(FunctionCall2Coll(procinfo, collation, aValue, bValue));
}

/*
 * Check if an element is closer to q than any element from R
 */
static bool
CheckElementCloser(char *base, HnswCandidate * e, List *r, FmgrInfo *procinfo, Oid collation)
{
	HnswElement eElement = HnswPtrAccess(base, e->element);
	ListCell   *lc2;

	foreach(lc2, r)
	{
		HnswCandidate *ri = lfirst(lc2);
		HnswElement riElement = HnswPtrAccess(base, ri->element);
		float		distance = HnswGetDistance(base, eElement, riElement, procinfo, collation);

		if (distance <= e->distance)
			return false;
	}

	return true;
}

/*
 * Algorithm 4 from paper
 */
static List *
SelectNeighbors(char *base, List *c, int lm, int lc, FmgrInfo *procinfo, Oid collation, HnswElement e2, HnswCandidate * newCandidate, HnswCandidate * *pruned, bool sortCandidates)
{
	List	   *r = NIL;
	List	   *w = list_copy(c);
	HnswCandidate **wd;
	int			wdlen = 0;
	int			wdoff = 0;
	HnswNeighborArray *neighbors = HnswGetNeighbors(base, e2, lc);
	bool		mustCalculate = !neighbors->closerSet;
	List	   *added = NIL;
	bool		removedAny = false;

	if (list_length(w) <= lm)
		return w;

	wd = palloc(sizeof(HnswCandidate *) * list_length(w));

	/* Ensure order of candidates is deterministic for closer caching */
	if (sortCandidates)
	{
		if (base == NULL)
			list_sort(w, CompareCandidateDistances);
		else
			list_sort(w, CompareCandidateDistancesOffset);
	}

	while (list_length(w) > 0 && list_length(r) < lm)
	{
		/* Assumes w is already ordered desc */
		HnswCandidate *e = llast(w);

		w = list_delete_last(w);

		/* Use previous state of r and wd to skip work when possible */
		if (mustCalculate)
			e->closer = CheckElementCloser(base, e, r, procinfo, collation);
		else if (list_length(added) > 0)
		{
			/* Keep Valgrind happy for in-memory, parallel builds */
			if (base != NULL)
				VALGRIND_MAKE_MEM_DEFINED(&e->closer, 1);

			/*
			 * If the current candidate was closer, we only need to compare it
			 * with the other candidates that we have added.
			 */
			if (e->closer)
			{
				e->closer = CheckElementCloser(base, e, added, procinfo, collation);

				if (!e->closer)
					removedAny = true;
			}
			else
			{
				/*
				 * If we have removed any candidates from closer, a candidate
				 * that was not closer earlier might now be.
				 */
				if (removedAny)
				{
					e->closer = CheckElementCloser(base, e, r, procinfo, collation);
					if (e->closer)
						added = lappend(added, e);
				}
			}
		}
		else if (e == newCandidate)
		{
			e->closer = CheckElementCloser(base, e, r, procinfo, collation);
			if (e->closer)
				added = lappend(added, e);
		}

		/* Keep Valgrind happy for in-memory, parallel builds */
		if (base != NULL)
			VALGRIND_MAKE_MEM_DEFINED(&e->closer, 1);

		if (e->closer)
			r = lappend(r, e);
		else
			wd[wdlen++] = e;
	}

	/* Cached value can only be used in future if sorted deterministically */
	neighbors->closerSet = sortCandidates;

	/* Keep pruned connections */
	while (wdoff < wdlen && list_length(r) < lm)
		r = lappend(r, wd[wdoff++]);

	/* Return pruned for update connections */
	if (pruned != NULL)
	{
		if (wdoff < wdlen)
			*pruned = wd[wdoff];
		else
			*pruned = linitial(w);
	}

	return r;
}

/*
 * Add connections
 */
static void
AddConnections(char *base, HnswElement element, List *neighbors, int lc)
{
	ListCell   *lc2;
	HnswNeighborArray *a = HnswGetNeighbors(base, element, lc);

	foreach(lc2, neighbors)
		a->items[a->length++] = *((HnswCandidate *) lfirst(lc2));
}

/*
 * Update connections
 */
void
HnswUpdateConnection(char *base, HnswElement element, HnswCandidate * hc, int lm, int lc, int *updateIdx, Relation index, FmgrInfo *procinfo, Oid collation)
{
	HnswElement hce = HnswPtrAccess(base, hc->element);
	HnswNeighborArray *currentNeighbors = HnswGetNeighbors(base, hce, lc);
	HnswCandidate hc2;

	HnswPtrStore(base, hc2.element, element);
	hc2.distance = hc->distance;

	if (currentNeighbors->length < lm)
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
			Datum		q = HnswGetValue(base, hce);

			for (int i = 0; i < currentNeighbors->length; i++)
			{
				HnswCandidate *hc3 = &currentNeighbors->items[i];
				HnswElement hc3Element = HnswPtrAccess(base, hc3->element);

				if (HnswPtrIsNull(base, hc3Element->value))
					HnswLoadElement(hc3Element, &hc3->distance, &q, index, procinfo, collation, true, NULL);
				else
					hc3->distance = GetCandidateDistance(base, hc3, q, procinfo, collation);

				/* Prune element if being deleted */
				if (hc3Element->heaptidsLength == 0)
				{
					pruned = &currentNeighbors->items[i];
					break;
				}
			}
		}

		if (pruned == NULL)
		{
			List	   *c = NIL;

			/* Add candidates */
			for (int i = 0; i < currentNeighbors->length; i++)
				c = lappend(c, &currentNeighbors->items[i]);
			c = lappend(c, &hc2);

			SelectNeighbors(base, c, lm, lc, procinfo, collation, hce, &hc2, &pruned, true);

			/* Should not happen */
			if (pruned == NULL)
				return;
		}

		/* Find and replace the pruned element */
		for (int i = 0; i < currentNeighbors->length; i++)
		{
			if (HnswPtrEqual(base, currentNeighbors->items[i].element, pruned->element))
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
RemoveElements(char *base, List *w, HnswElement skipElement)
{
	ListCell   *lc2;
	List	   *w2 = NIL;

	/* Ensure does not access heaptidsLength during in-memory build */
	pg_memory_barrier();

	foreach(lc2, w)
	{
		HnswCandidate *hc = (HnswCandidate *) lfirst(lc2);
		HnswElement hce = HnswPtrAccess(base, hc->element);

		/* Skip self for vacuuming update */
		if (skipElement != NULL && hce->blkno == skipElement->blkno && hce->offno == skipElement->offno)
			continue;

		if (hce->heaptidsLength != 0)
			w2 = lappend(w2, hc);
	}

	return w2;
}

#if PG_VERSION_NUM >= 130000
/*
 * Precompute hash
 */
static void
PrecomputeHash(char *base, HnswElement element)
{
	HnswElementPtr ptr;

	HnswPtrStore(base, ptr, element);

	if (base == NULL)
		element->hash = hash_pointer((uintptr_t) HnswPtrPointer(ptr));
	else
		element->hash = hash_offset(HnswPtrOffset(ptr));
}
#endif

/*
 * Algorithm 1 from paper
 */
void
HnswFindElementNeighbors(char *base, HnswElement element, HnswElement entryPoint, Relation index, FmgrInfo *procinfo, Oid collation, int m, int efConstruction, bool existing)
{
	List	   *ep;
	List	   *w;
	int			level = element->level;
	int			entryLevel;
	Datum		q = HnswGetValue(base, element);
	HnswElement skipElement = existing ? element : NULL;

#if PG_VERSION_NUM >= 130000
	/* Precompute hash */
	if (index == NULL)
		PrecomputeHash(base, element);
#endif

	/* No neighbors if no entry point */
	if (entryPoint == NULL)
		return;

	/* Get entry point and level */
	ep = list_make1(HnswEntryCandidate(base, entryPoint, q, index, procinfo, collation, true));
	entryLevel = entryPoint->level;

	/* 1st phase: greedy search to insert level */
	for (int lc = entryLevel; lc >= level + 1; lc--)
	{
		w = HnswSearchLayer(base, q, ep, 1, lc, index, procinfo, collation, m, true, skipElement);
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

		w = HnswSearchLayer(base, q, ep, efConstruction, lc, index, procinfo, collation, m, true, skipElement);

		/* Elements being deleted or skipped can help with search */
		/* but should be removed before selecting neighbors */
		if (index != NULL)
			lw = RemoveElements(base, w, skipElement);
		else
			lw = w;

		/*
		 * Candidates are sorted, but not deterministically. Could set
		 * sortCandidates to true for in-memory builds to enable closer
		 * caching, but there does not seem to be a difference in performance.
		 */
		neighbors = SelectNeighbors(base, lw, lm, lc, procinfo, collation, element, NULL, NULL, false);

		AddConnections(base, element, neighbors, lc);

		ep = w;
	}
}

PGDLLEXPORT Datum l2_normalize(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum halfvec_l2_normalize(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum sparsevec_l2_normalize(PG_FUNCTION_ARGS);

static void
SparsevecCheckValue(Pointer v)
{
	SparseVector *vec = (SparseVector *) v;

	if (vec->nnz > HNSW_MAX_NNZ)
		elog(ERROR, "sparsevec cannot have more than %d non-zero elements for hnsw index", HNSW_MAX_NNZ);
}

/*
 * Get type info
 */
const		HnswTypeInfo *
HnswGetTypeInfo(Relation index)
{
	FmgrInfo   *procinfo = HnswOptionalProcInfo(index, HNSW_TYPE_INFO_PROC);

	if (procinfo == NULL)
	{
		static const HnswTypeInfo typeInfo = {
			.maxDimensions = HNSW_MAX_DIM,
			.normalize = l2_normalize,
			.checkValue = NULL
		};

		return (&typeInfo);
	}
	else
		return (const HnswTypeInfo *) DatumGetPointer(FunctionCall0Coll(procinfo, InvalidOid));
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(hnsw_halfvec_support);
Datum
hnsw_halfvec_support(PG_FUNCTION_ARGS)
{
	static const HnswTypeInfo typeInfo = {
		.maxDimensions = HNSW_MAX_DIM * 2,
		.normalize = halfvec_l2_normalize,
		.checkValue = NULL
	};

	PG_RETURN_POINTER(&typeInfo);
};

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(hnsw_bit_support);
Datum
hnsw_bit_support(PG_FUNCTION_ARGS)
{
	static const HnswTypeInfo typeInfo = {
		.maxDimensions = HNSW_MAX_DIM * 32,
		.normalize = NULL,
		.checkValue = NULL
	};

	PG_RETURN_POINTER(&typeInfo);
};

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(hnsw_sparsevec_support);
Datum
hnsw_sparsevec_support(PG_FUNCTION_ARGS)
{
	static const HnswTypeInfo typeInfo = {
		.maxDimensions = SPARSEVEC_MAX_DIM,
		.normalize = sparsevec_l2_normalize,
		.checkValue = SparsevecCheckValue
	};

	PG_RETURN_POINTER(&typeInfo);
};
