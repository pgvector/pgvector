#include "postgres.h"

#include <float.h>
#include <math.h>

#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "lib/pairingheap.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "tqhnsw.h"
#include "utils/float.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

/*
 * One scored candidate kept in the final result array.  dist is the value we
 * MINIMIZE (smaller == closer): the quantized estimate during traversal, then
 * the exact FUNCTION 1 distance for the reranked subset.  blkno/offno locate the
 * element tuple (codes); heaptid is the heap row used for rerank.
 */
typedef struct TqhnswScanResult
{
	double		dist;
	ItemPointerData heaptid;
	BlockNumber blkno;
	OffsetNumber offno;
} TqhnswScanResult;

/* One scored neighbor produced by TqhnswExpandNode (block path). */
typedef struct TqhnswNeighborScore
{
	double		dist;			/* value to MINIMIZE (quantized estimate) */
	ItemPointerData heaptid;
	BlockNumber blkno;
	OffsetNumber offno;
	uint8		level;
	uint8		version;
	ItemPointerData neighbortid;
} TqhnswNeighborScore;

/*
 * Scan state.  Combines tqscan.c's LUT/rerank fields with HNSW scan state (the
 * graph entry point + m).
 */
typedef struct TqhnswScanOpaqueData
{
	TqModel    *model;
	TqMetric	metric;
	bool		first;
	int			m;
	BlockNumber entryBlkno;
	OffsetNumber entryOffno;
	int			entryLevel;

	/* Exact distance (FUNCTION 1) + collation for rerank. */
	FmgrInfo   *procinfo;
	Oid			collation;
	AttrNumber	heapAttno;		/* heap attribute backing the index column */

	/* Type-info vtable (resolved from opclass support proc). */
	const TqTypeInfo *typeInfo;

	/* Per-query state (rebuilt on each rescan). */
	float	   *lut;			/* dimCodes * nLevels */
	Datum		queryDatum;		/* native query (normalized for cosine), for
								 * rerank */
	bool		haveQuery;
	float	   *vecScratch;		/* dim floats (rebuilt per rescan) */
	double		qNormSq;		/* ||q||^2 (1.0 for cosine after normalize) */

	/*
	 * 8-bit query LUT for the block kernel (built each rescan, mirrors
	 * tqivf).
	 */
	uint8	   *lut8;			/* dimCodes * nLevels */
	float		lutBias;
	float		lutScale;

	/* Block-expansion scratch, allocated once per rescan, reused per node. */
	TqhnswNeighborScore *expandOut; /* >= TQHNSW_MAX_M*2 entries */
	char	   *expandCodes;	/* codesBytes * TQHNSW_MAX_M*2 (gathered
								 * row-major codes) */
	uint8	   *expandPlane;	/* TQ_BLOCK_CODE_BYTES(dimCodes) */

	/* Results + rerank. */
	TqhnswScanResult *results;
	int			nresults;
	int			cursor;

	/* Heap access for rerank. */
	Relation	heapRel;
	bool		heapOpened;
	IndexFetchTableData *fetch;
	TupleTableSlot *slot;

	MemoryContext tmpCtx;
} TqhnswScanOpaqueData;

typedef TqhnswScanOpaqueData *TqhnswScanOpaque;

/*
 * One traversal candidate.  Lives in both the min-heap C (nearest first) and the
 * max-heap W (furthest first), exactly as HnswSearchCandidate does.
 */
typedef struct TqhnswScanCandidate
{
	pairingheap_node c_node;	/* min-heap by distance */
	pairingheap_node w_node;	/* max-heap by distance */
	double		distance;
	BlockNumber blkno;
	OffsetNumber offno;
	uint8		level;			/* element level (neighbor-slice math) */
	uint8		version;		/* element version (slot-reuse detection) */
	ItemPointerData heaptid;
	ItemPointerData neighbortid;	/* the candidate's neighbor-tuple location */
} TqhnswScanCandidate;

#define TqhnswGetCandidate(membername, ptr) \
	pairingheap_container(TqhnswScanCandidate, membername, ptr)
#define TqhnswGetCandidateConst(membername, ptr) \
	pairingheap_const_container(TqhnswScanCandidate, membername, ptr)

/* Visited-set key: an element tuple's (blkno, offno). */
typedef struct TqhnswVisitedKey
{
	BlockNumber blkno;
	OffsetNumber offno;
} TqhnswVisitedKey;

/*
 * qsort comparator: ascending distance.  Replicated from CompareResults in
 * tqscan.c.
 */
static int
CompareResults(const void *a, const void *b)
{
	double		da = ((const TqhnswScanResult *) a)->dist;
	double		db = ((const TqhnswScanResult *) b)->dist;

	if (da < db)
		return -1;
	if (da > db)
		return 1;
	return 0;
}

/* Min-heap on distance (nearest at the root). */
static int
CompareNearestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (TqhnswGetCandidateConst(c_node, a)->distance < TqhnswGetCandidateConst(c_node, b)->distance)
		return 1;
	if (TqhnswGetCandidateConst(c_node, a)->distance > TqhnswGetCandidateConst(c_node, b)->distance)
		return -1;
	return 0;
}

/* Max-heap on distance (furthest at the root). */
static int
CompareFurthestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (TqhnswGetCandidateConst(w_node, a)->distance < TqhnswGetCandidateConst(w_node, b)->distance)
		return -1;
	if (TqhnswGetCandidateConst(w_node, a)->distance > TqhnswGetCandidateConst(w_node, b)->distance)
		return 1;
	return 0;
}

/*
 * Convert a raw inner-product estimate to a distance to MINIMIZE.  Replicated
 * verbatim from TqEstToDist (tqscan.c) — formulas MUST stay identical.
 */
static inline double
TqhnswEstToDist(TqMetric metric, double qNormSq, float norm, float est)
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
 * Read the element tuple at (blkno,offno); return its quantized distance to the
 * query and (optionally) its heaptid/level/neighbortid/version.
 *
 * TqScoreEntry needs a TqEntry-shaped header for scale (+ norm/residualNorm only
 * when tqProd, which the tqhnsw model never enables); a stack TqEntry with those
 * fields filled from the element tuple is sufficient.
 */
static double
TqhnswScoreElement(TqhnswScanOpaque so, Relation index, BlockNumber blkno,
				   OffsetNumber offno, ItemPointer heaptid_out, uint8 *level_out,
				   ItemPointer neighbortid_out, uint8 *version_out)
{
	Buffer		buf = ReadBuffer(index, blkno);
	Page		page;
	TqhnswElementTuple etup;
	double		dist;

	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	etup = (TqhnswElementTuple) PageGetItem(page, PageGetItemId(page, offno));

	if (so->haveQuery)
	{
		float		est;
		TqEntry		hdr;

		hdr.norm = etup->norm;
		hdr.scale = etup->scale;
		hdr.residualNorm = 0;

		est = TqScoreEntry(so->model, so->lut, NULL, &hdr, etup->codes);
		dist = TqhnswEstToDist(so->metric, so->qNormSq, etup->norm, est);
	}
	else
		dist = 0.0;				/* NULL order-by key (mirrors hnsw) */

	if (heaptid_out)
		*heaptid_out = etup->heaptid;
	if (level_out)
		*level_out = etup->level;
	if (neighbortid_out)
		*neighbortid_out = etup->neighbortid;
	if (version_out)
		*version_out = etup->version;

	UnlockReleaseBuffer(buf);
	return dist;
}

/*
 * Read the (level-lc)*m slice (size TqhnswGetLayerM(m,lc)) of an element's
 * neighbor tuple into indextids.  Mirrors HnswLoadNeighborTids: the slice math
 * matches what TqhnswFlushGraph wrote (lc from level..0, lc's slice at
 * (level-lc)*m), keyed by the level/version the caller read from the ELEMENT
 * tuple.  If the neighbor slot was reused or repaired since (type/version/count
 * mismatch, or the expected level is below lc), return 0 ("no neighbors")
 * rather than reading a slice that no longer exists.
 */
static int
TqhnswLoadNeighborTids(Relation index, BlockNumber neighborPage,
					   OffsetNumber neighborOffno, int m, int lc,
					   int level, uint8 version,
					   ItemPointerData *indextids)
{
	Buffer		buf;
	Page		page;
	TqhnswNeighborTuple ntup;
	int			lm;
	int			start;

	if (lc > level)
		return 0;

	buf = ReadBuffer(index, neighborPage);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	ntup = (TqhnswNeighborTuple) PageGetItem(page, PageGetItemId(page, neighborOffno));

	/* Reject a reused/repaired slot (mirrors HnswLoadNeighborTids). */
	if (ntup->type != TQHNSW_NEIGHBOR_TUPLE_TYPE ||
		ntup->version != version ||
		(int) ntup->count != (level + 2) * m)
	{
		UnlockReleaseBuffer(buf);
		return 0;
	}

	lm = TqhnswGetLayerM(m, lc);
	start = (level - lc) * m;

	memcpy(indextids, ntup->indextids + start, lm * sizeof(ItemPointerData));

	UnlockReleaseBuffer(buf);
	return lm;
}

/*
 * Score a node's neighbor slice (indextids[0..lm), stopping at the first invalid
 * TID) with the 8-bit block kernel.  Gathers each neighbor's codes page-by-page
 * (one SHARE lock at a time, codes copied out), scatters them into a transient
 * coordinate-strided plane in chunks of TQ_BLOCK_WIDTH, runs TqScoreBlockRange,
 * and converts each lane sum to a distance.  Writes one TqhnswNeighborScore per
 * valid neighbor into so->expandOut and returns the count.
 */
static int
TqhnswExpandNode(TqhnswScanOpaque so, Relation index,
				 const ItemPointerData *indextids, int lm)
{
	TqModel    *model = so->model;
	int			dc = model->dimCodes;
	int			codesBytes = TQ_CODES_BYTES(dc, model->bits);
	Size		planeBytes = TQ_BLOCK_CODE_BYTES(dc);
	TqhnswNeighborScore *out = so->expandOut;
	float		norms[TQHNSW_MAX_M * 2];
	float		scales[TQHNSW_MAX_M * 2];
	int			order[TQHNSW_MAX_M * 2];
	int			nv = 0;
	int			i,
				k,
				chunk;

	/*
	 * 1. Collect valid TIDs (stop at first invalid, matching the scalar
	 * loops).
	 */
	for (i = 0; i < lm; i++)
	{
		if (!ItemPointerIsValid(&indextids[i]))
			break;
		order[nv++] = i;
	}
	if (nv == 0)
		return 0;

	/*
	 * 2. Sort the collected indices by block number (insertion sort; nv is
	 * small).
	 */
	for (i = 1; i < nv; i++)
	{
		int			key = order[i];
		BlockNumber kb = ItemPointerGetBlockNumber(&indextids[key]);
		int			j = i - 1;

		while (j >= 0 && ItemPointerGetBlockNumber(&indextids[order[j]]) > kb)
		{
			order[j + 1] = order[j];
			j--;
		}
		order[j + 1] = key;
	}

	/*
	 * 3. Page-grouped gather: one buffer SHARE lock per distinct page at a
	 * time.
	 */
	k = 0;
	while (k < nv)
	{
		BlockNumber blk = ItemPointerGetBlockNumber(&indextids[order[k]]);
		Buffer		buf = ReadBuffer(index, blk);
		Page		page;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		while (k < nv && ItemPointerGetBlockNumber(&indextids[order[k]]) == blk)
		{
			int			orig = order[k];
			OffsetNumber off = ItemPointerGetOffsetNumber(&indextids[orig]);
			TqhnswElementTuple etup =
			(TqhnswElementTuple) PageGetItem(page, PageGetItemId(page, off));
			TqhnswNeighborScore *o = &out[orig];

			memcpy(so->expandCodes + (Size) orig * codesBytes, etup->codes, codesBytes);
			norms[orig] = etup->norm;
			scales[orig] = etup->scale;
			o->heaptid = etup->heaptid;
			o->blkno = blk;
			o->offno = off;
			o->level = etup->level;
			o->version = etup->version;
			o->neighbortid = etup->neighbortid;
			k++;
		}
		UnlockReleaseBuffer(buf);
	}

	/*
	 * 4. Scatter + score in chunks of TQ_BLOCK_WIDTH; convert lane -> est ->
	 * dist.
	 */
	for (chunk = 0; chunk < nv; chunk += TQ_BLOCK_WIDTH)
	{
		int			n = Min(TQ_BLOCK_WIDTH, nv - chunk);
		TqBlockAccum acc;
		int			lane;

		memset(so->expandPlane, 0, planeBytes);
		for (lane = 0; lane < n; lane++)
			TqScatterCodes(model,
						   so->expandCodes + (Size) (chunk + lane) * codesBytes,
						   lane, so->expandPlane);

		TqBlockAccumInit(&acc);
		TqScoreBlockRange(so->lut8, so->expandPlane, 0, dc, &acc);
		TqBlockAccumFinish(&acc);

		for (lane = 0; lane < n; lane++)
		{
			int			idx = chunk + lane;
			double		mse = (double) so->lutScale * acc.acc32[lane]
			+ (double) dc * so->lutBias;
			float		est = (float) ((double) scales[idx] * mse);

			out[idx].dist = TqhnswEstToDist(so->metric, so->qNormSq, norms[idx], est);
		}
	}

	/*
	 * expandOut[i] corresponds to indextids[i] (original neighbor order), so
	 * the block beam admits neighbors in the same order as the scalar path.
	 */
	return nv;
}

/*
 * Run the quantized graph traversal: greedy ef=1 descent through the upper
 * levels, then a level-0 ef beam.  Fills so->results with up to ef_search
 * candidates scored by the ADC LUT.  Mirrors GetScanItems + HnswSearchLayer's
 * on-disk branch.
 */
static void
TqhnswSearchGraph(IndexScanDesc scan)
{
	TqhnswScanOpaque so = (TqhnswScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	int			m;
	int			ef = tqhnsw_ef_search;
	BlockNumber curBlkno;
	OffsetNumber curOffno;
	ItemPointerData curHeaptid;
	ItemPointerData curNeighbortid;
	uint8		curLevel;
	uint8		curVersion;
	double		curDist;
	pairingheap *C;
	pairingheap *W;
	HASHCTL		hashctl;
	HTAB	   *visited;
	TqhnswVisitedKey key;
	bool		found;
	int			wlen;
	TqhnswScanCandidate *entry;
	int			lc;
	int			n;
	int			dim;

	so->results = NULL;
	so->nresults = 0;

	/*
	 * Re-read the entry point under the scan lock (the caller holds
	 * TQHNSW_SCAN_LOCK).  Reading it once at beginscan would let a vacuum
	 * between rescans replace the entry point -- a stale entry both degrades
	 * recall and, if the old slot was reused by a lower-level element, breaks
	 * the descent's slice math.  Mirrors hnsw, which re-reads the meta page
	 * inside every search.
	 */
	TqhnswGetMetaInfo(index, &dim, &so->metric, &so->m,
					  &so->entryBlkno, &so->entryOffno, &so->entryLevel,
					  NULL, NULL);
	m = so->m;

	/* Empty index: no entry point. */
	if (!BlockNumberIsValid(so->entryBlkno) || so->entryLevel < 0)
		return;

	/* Score the entry point. */
	curBlkno = so->entryBlkno;
	curOffno = so->entryOffno;
	curDist = TqhnswScoreElement(so, index, curBlkno, curOffno,
								 &curHeaptid, &curLevel, &curNeighbortid,
								 &curVersion);

	/*
	 * Upper-level greedy descent (lc from the entry element's level down to
	 * 1): move to the nearest improving neighbor until none improves.
	 */
	for (lc = curLevel; lc >= 1; lc--)
	{
		bool		changed = true;

		while (changed)
		{
			ItemPointerData indextids[TQHNSW_MAX_M * 2];
			BlockNumber nbrPage = ItemPointerGetBlockNumber(&curNeighbortid);
			OffsetNumber nbrOffno = ItemPointerGetOffsetNumber(&curNeighbortid);
			int			lm;
			int			i;

			changed = false;
			lm = TqhnswLoadNeighborTids(index, nbrPage, nbrOffno, m, lc,
										curLevel, curVersion, indextids);

			if (tqhnsw_force_scalar || !so->haveQuery)
			{
				for (i = 0; i < lm; i++)
				{
					ItemPointer indextid = &indextids[i];
					BlockNumber nblk;
					OffsetNumber noff;
					ItemPointerData nheaptid;
					ItemPointerData nneighbortid;
					uint8		nlevel;
					uint8		nversion;
					double		ndist;

					if (!ItemPointerIsValid(indextid))
						break;

					nblk = ItemPointerGetBlockNumber(indextid);
					noff = ItemPointerGetOffsetNumber(indextid);
					ndist = TqhnswScoreElement(so, index, nblk, noff,
											   &nheaptid, &nlevel, &nneighbortid,
											   &nversion);

					if (ndist < curDist)
					{
						curDist = ndist;
						curBlkno = nblk;
						curOffno = noff;
						curHeaptid = nheaptid;
						curNeighbortid = nneighbortid;
						curLevel = nlevel;
						curVersion = nversion;
						changed = true;
					}
				}
			}
			else
			{
				int			nv = TqhnswExpandNode(so, index, indextids, lm);

				for (i = 0; i < nv; i++)
				{
					if (so->expandOut[i].dist < curDist)
					{
						curDist = so->expandOut[i].dist;
						curBlkno = so->expandOut[i].blkno;
						curOffno = so->expandOut[i].offno;
						curHeaptid = so->expandOut[i].heaptid;
						curNeighbortid = so->expandOut[i].neighbortid;
						curLevel = so->expandOut[i].level;
						curVersion = so->expandOut[i].version;
						changed = true;
					}
				}
			}

			/*
			 * If a neighbor moved us to a HIGHER level than the current
			 * layer, the slice math stays valid (lc <= curLevel); a
			 * lower-level element can only be reached via slot reuse, which
			 * the version/count check in TqhnswLoadNeighborTids rejects.
			 */
		}
	}

	/* Level-0 beam search (ef = tqhnsw_ef_search). */
	C = pairingheap_allocate(CompareNearestCandidates, NULL);
	W = pairingheap_allocate(CompareFurthestCandidates, NULL);

	MemSet(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize = sizeof(TqhnswVisitedKey);
	hashctl.entrysize = sizeof(TqhnswVisitedKey);
	hashctl.hcxt = CurrentMemoryContext;
	visited = hash_create("tqhnsw visited", ef * 2 + 16, &hashctl,
						  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Seed with the descent's best node. */
	entry = palloc(sizeof(TqhnswScanCandidate));
	entry->distance = curDist;
	entry->blkno = curBlkno;
	entry->offno = curOffno;
	entry->level = curLevel;
	entry->version = curVersion;
	entry->heaptid = curHeaptid;
	entry->neighbortid = curNeighbortid;
	pairingheap_add(C, &entry->c_node);
	pairingheap_add(W, &entry->w_node);
	wlen = 1;

	/* Zero once so the struct's trailing padding hashes consistently */
	MemSet(&key, 0, sizeof(key));
	key.blkno = curBlkno;
	key.offno = curOffno;
	hash_search(visited, &key, HASH_ENTER, &found);

	while (!pairingheap_is_empty(C))
	{
		TqhnswScanCandidate *c = TqhnswGetCandidate(c_node, pairingheap_remove_first(C));
		TqhnswScanCandidate *f = TqhnswGetCandidate(w_node, pairingheap_first(W));
		ItemPointerData indextids[TQHNSW_MAX_M * 2];
		BlockNumber nbrPage;
		OffsetNumber nbrOffno;
		int			lm;
		int			i;
		int			nv;

		CHECK_FOR_INTERRUPTS();

		if (c->distance > f->distance)
			break;

		nbrPage = ItemPointerGetBlockNumber(&c->neighbortid);
		nbrOffno = ItemPointerGetOffsetNumber(&c->neighbortid);
		lm = TqhnswLoadNeighborTids(index, nbrPage, nbrOffno, m, 0,
									c->level, c->version, indextids);

		if (tqhnsw_force_scalar || !so->haveQuery)
		{
			for (i = 0; i < lm; i++)
			{
				ItemPointer indextid = &indextids[i];
				BlockNumber nblk;
				OffsetNumber noff;
				ItemPointerData nheaptid;
				ItemPointerData nneighbortid;
				uint8		nlevel;
				uint8		nversion;
				double		ndist;
				bool		alwaysAdd = wlen < ef;
				TqhnswScanCandidate *e;

				if (!ItemPointerIsValid(indextid))
					break;

				nblk = ItemPointerGetBlockNumber(indextid);
				noff = ItemPointerGetOffsetNumber(indextid);

				key.blkno = nblk;
				key.offno = noff;
				hash_search(visited, &key, HASH_ENTER, &found);
				if (found)
					continue;

				f = TqhnswGetCandidate(w_node, pairingheap_first(W));

				ndist = TqhnswScoreElement(so, index, nblk, noff, &nheaptid,
										   &nlevel, &nneighbortid, &nversion);

				if (!(ndist < f->distance || alwaysAdd))
					continue;

				e = palloc(sizeof(TqhnswScanCandidate));
				e->distance = ndist;
				e->blkno = nblk;
				e->offno = noff;
				e->level = nlevel;
				e->version = nversion;
				e->heaptid = nheaptid;
				e->neighbortid = nneighbortid;
				pairingheap_add(C, &e->c_node);
				pairingheap_add(W, &e->w_node);
				wlen++;

				if (wlen > ef)
				{
					(void) pairingheap_remove_first(W);
					wlen--;
				}
			}
		}
		else
		{
			nv = TqhnswExpandNode(so, index, indextids, lm);

			for (i = 0; i < nv; i++)
			{
				TqhnswNeighborScore *o = &so->expandOut[i];
				bool		alwaysAdd = wlen < ef;
				TqhnswScanCandidate *e;

				key.blkno = o->blkno;
				key.offno = o->offno;
				hash_search(visited, &key, HASH_ENTER, &found);
				if (found)
					continue;

				f = TqhnswGetCandidate(w_node, pairingheap_first(W));
				if (!(o->dist < f->distance || alwaysAdd))
					continue;

				e = palloc(sizeof(TqhnswScanCandidate));
				e->distance = o->dist;
				e->blkno = o->blkno;
				e->offno = o->offno;
				e->level = o->level;
				e->version = o->version;
				e->heaptid = o->heaptid;
				e->neighbortid = o->neighbortid;
				pairingheap_add(C, &e->c_node);
				pairingheap_add(W, &e->w_node);
				wlen++;
				if (wlen > ef)
				{
					(void) pairingheap_remove_first(W);
					wlen--;
				}
			}
		}
	}

	/* Drain W into the results array. */
	so->results = palloc(sizeof(TqhnswScanResult) * Max(wlen, 1));
	n = 0;
	while (!pairingheap_is_empty(W))
	{
		TqhnswScanCandidate *sc = TqhnswGetCandidate(w_node, pairingheap_remove_first(W));

		/*
		 * Vacuum pass 1 invalidates heap TIDs without draining scans; such
		 * elements are being deleted and must not reach rerank (heap fetch on
		 * an invalid TID) or be emitted.  Mirrors hnswgettuple's skip of
		 * elements with no valid heap TIDs.
		 */
		if (!ItemPointerIsValid(&sc->heaptid))
			continue;

		so->results[n].dist = sc->distance;
		so->results[n].heaptid = sc->heaptid;
		so->results[n].blkno = sc->blkno;
		so->results[n].offno = sc->offno;
		n++;
	}
	so->nresults = n;

	hash_destroy(visited);
	pairingheap_free(C);
	pairingheap_free(W);
}

/*
 * Fetch the original datum for tid from the heap.  Returns a palloc'd
 * fully-detoasted copy in the current memory context, or (Datum) 0 if the
 * tuple is no longer visible.  Mirrors TqHeapFetchDatum (tqscan.c).
 */
static Datum
TqhnswHeapFetchDatum(IndexScanDesc scan, TqhnswScanOpaque so, ItemPointer tid)
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

IndexScanDesc
tqhnswbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	TqhnswScanOpaque so;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	so = (TqhnswScanOpaque) palloc0(sizeof(TqhnswScanOpaqueData));
	so->first = true;

	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Tqhnsw scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	/* Model is owned by rd_indexcxt; do not free it from the scan. */
	so->model = TqhnswGetCachedModel(index);
	so->metric = so->model->metric;

	/* Exact distance (FUNCTION 1) + collation for rerank. */
	so->procinfo = index_getprocinfo(index, 1, TQHNSW_DISTANCE_PROC);
	so->collation = index->rd_indcollation[0];
	so->heapAttno = index->rd_index->indkey.values[0];

	/* Type-info vtable from the opclass support proc. */
	so->typeInfo = TqGetTypeInfo(index, TQHNSW_TYPE_INFO_PROC);
	so->haveQuery = false;

	/*
	 * Entry point + m are read per search inside TqhnswSearchGraph, under the
	 * scan lock -- reading them here would go stale across rescans if vacuum
	 * replaces the entry point.
	 */

	scan->opaque = so;

	return scan;
}

void
tqhnswrescan(IndexScanDesc scan, ScanKey keys, int nkeys,
			 ScanKey orderbys, int norderbys)
{
	TqhnswScanOpaque so = (TqhnswScanOpaque) scan->opaque;
	TqModel    *model;
	MemoryContext oldCtx;

	so->first = true;
	so->cursor = 0;
	so->nresults = 0;

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));
	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));

	/* Tear down any heap-fetch state from a prior scan. */
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

	/* Reset per-query allocations; the model lives in rd_indexcxt. */
	MemoryContextReset(so->tmpCtx);
	so->lut = NULL;
	so->haveQuery = false;
	so->vecScratch = NULL;
	so->results = NULL;
	so->lut8 = NULL;
	so->expandOut = NULL;
	so->expandCodes = NULL;
	so->expandPlane = NULL;

	/*
	 * Re-fetch the cached model: a relcache invalidation since beginscan (or
	 * the prior rescan) frees the rd_amcache chunk, so a pointer cached for the
	 * scan's lifetime could dangle here.
	 */
	so->model = TqhnswGetCachedModel(scan->indexRelation);
	model = so->model;

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
			int			i;

			for (i = 0; i < model->dim; i++)
				s += (double) qfloat[i] * qfloat[i];
			so->qNormSq = s;
		}

		so->lut = palloc(sizeof(float) * model->dimCodes * model->nLevels);
		TqBuildQueryLut(model, qfloat, so->lut, NULL);

		so->lut8 = palloc(model->dimCodes * model->nLevels);
		TqBuildLut8(model, so->lut, so->lut8, &so->lutBias, &so->lutScale);

		{
			int			codesBytes = TQ_CODES_BYTES(model->dimCodes, model->bits);
			int			maxNbr = TQHNSW_MAX_M * 2;

			so->expandOut = palloc(sizeof(TqhnswNeighborScore) * maxNbr);
			so->expandCodes = palloc((Size) codesBytes * maxNbr);
			so->expandPlane = palloc(TQ_BLOCK_CODE_BYTES(model->dimCodes));
		}
	}

	MemoryContextSwitchTo(oldCtx);
}

bool
tqhnswgettuple(IndexScanDesc scan, ScanDirection dir)
{
	TqhnswScanOpaque so = (TqhnswScanOpaque) scan->opaque;

	Assert(ScanDirectionIsForward(dir));

	if (so->first)
	{
		MemoryContext oldCtx;

		pgstat_count_index_scan(scan->indexRelation);
#if PG_VERSION_NUM >= 180000
		if (scan->instrument)
			scan->instrument->nsearches++;
#endif

		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan tqhnsw index without order");

		if (!IsMVCCSnapshot(scan->xs_snapshot))
			elog(ERROR, "non-MVCC snapshots are not supported with tqhnsw");

		/*
		 * A NULL order-by key still traverses the graph with zero distances
		 * and returns up to ef candidates (mirrors hnsw's NULL-value path).
		 */
		oldCtx = MemoryContextSwitchTo(so->tmpCtx);

		/*
		 * Acquire a shared scan lock before reading the index graph.  This
		 * allows a future vacuum MarkDeleted pass to take ExclusiveLock and
		 * drain all in-flight scans before reclaiming tuple slots, preventing
		 * a concurrent scan from resolving a neighbor TID to a slot-reused
		 * element.  Mirrors HNSW's HNSW_SCAN_LOCK pattern.
		 *
		 * The lock is released immediately after graph traversal, before any
		 * heap fetches, to avoid holding it across heap buffer locks (which
		 * could deadlock).
		 */
		LockPage(scan->indexRelation, TQHNSW_SCAN_LOCK, ShareLock);

		/* Quantized graph traversal. */
		TqhnswSearchGraph(scan);

		/* Release the scan lock; heap rerank fetches happen outside it. */
		UnlockPage(scan->indexRelation, TQHNSW_SCAN_LOCK, ShareLock);

		/*
		 * Rerank the top candidates against full-precision heap vectors. Rows
		 * whose heap tuple is no longer visible get +inf and drop out (this
		 * is how deletes are handled without graph mutation).
		 */
		if (tqhnsw_rerank > 0 && so->nresults > 0 && so->haveQuery &&
			AttributeNumberIsValid(so->heapAttno))
		{
			int			k = Min(tqhnsw_rerank, so->nresults);
			int			i;

			qsort(so->results, so->nresults, sizeof(TqhnswScanResult), CompareResults);

			for (i = 0; i < k; i++)
			{
				Datum		heapDatum = TqhnswHeapFetchDatum(scan, so, &so->results[i].heaptid);

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

					d = DatumGetFloat8(FunctionCall2Coll(so->procinfo,
														 so->collation,
														 so->queryDatum,
														 cmpDatum));

					/*
					 * Cosine: FUNCTION 1 yields -cos on normalized inputs
					 * while the quantized estimate scale is 1 - cos; shift by
					 * 1 so the two populations sort on the same scale.
					 */
					if (so->metric == TQ_METRIC_COSINE)
						d += 1.0;

					so->results[i].dist = d;

					if (cmpDatum != heapDatum)
						pfree(DatumGetPointer(cmpDatum));
					pfree(DatumGetPointer(heapDatum));
				}
				else
				{
					so->results[i].dist = get_float8_infinity();
				}
			}
		}

		if (so->nresults > 0)
			qsort(so->results, so->nresults, sizeof(TqhnswScanResult), CompareResults);

		MemoryContextSwitchTo(oldCtx);
		so->first = false;
	}

	/* Drop any +inf (invisible) tail. */
	if (so->cursor >= so->nresults)
		return false;
	if (so->results[so->cursor].dist == get_float8_infinity())
		return false;

	scan->xs_heaptid = so->results[so->cursor].heaptid;
	scan->xs_recheck = false;
	scan->xs_recheckorderby = false;
	so->cursor++;

	return true;
}

void
tqhnswendscan(IndexScanDesc scan)
{
	TqhnswScanOpaque so = (TqhnswScanOpaque) scan->opaque;

	if (so->fetch != NULL)
		table_index_fetch_end(so->fetch);
	if (so->slot != NULL)
		ExecDropSingleTupleTableSlot(so->slot);
	if (so->heapOpened && so->heapRel != NULL)
		table_close(so->heapRel, AccessShareLock);

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}
