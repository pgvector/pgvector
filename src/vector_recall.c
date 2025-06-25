#include "postgres.h"

#include <float.h>

#include "access/heapam.h"  /* for heap_getnext / heap_getattr */
#include "access/table.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"   /* for ReturnSetInfo, SFRM_Materialize */
#include "executor/tuptable.h"
#include "fmgr.h"
#include "miscadmin.h"      /* for work_mem */
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "vector.h"
#include "vector_recall.h"

/* GUC variables */
bool	pgvector_track_recall = false;
int		pgvector_recall_sample_rate = 100;	/* Sample every 100th query */
int		pgvector_recall_max_scan_tuples = 10000;	/* Max tuples to scan for recall estimation */

static HTAB *recall_stats_hash = NULL;
static MemoryContext recall_context = NULL;

void
InitVectorRecallTracking(void)
{
	HASHCTL		info;

	if (recall_context == NULL)
	{
		recall_context = AllocSetContextCreate(TopMemoryContext,
											   "Vector Recall Tracking",
											   ALLOCSET_DEFAULT_SIZES);
	}

	if (recall_stats_hash == NULL)
	{
		MemSet(&info, 0, sizeof(info));
		info.keysize = sizeof(Oid);
		info.entrysize = sizeof(RecallStatsEntry);
		info.hcxt = recall_context;

		recall_stats_hash = hash_create("Vector Recall Stats",
										32,
										&info,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/* Define GUC parameters */
	DefineCustomBoolVariable("pgvector.track_recall",
							 "Enables recall tracking for vector queries",
							 "When enabled, pgvector will sample queries to measure recall quality.",
							 &pgvector_track_recall,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("pgvector.recall_sample_rate",
							"Sets the sampling rate for recall tracking (1 in N queries)",
							"Higher values mean less frequent sampling, lower overhead.",
							&pgvector_recall_sample_rate,
							100, 1, 10000,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pgvector.recall_max_scan_tuples",
							"Sets the maximum number of tuples to scan for recall estimation",
							"Higher values provide more accurate recall estimates but may impact performance on large tables. Set to -1 for unlimited scanning.",
							&pgvector_recall_max_scan_tuples,
							10000, -1, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);
}

/*
 * Track a vector query with safe recall estimation
 */
void
TrackVectorQuery(Relation index, VectorRecallTracker *tracker, FmgrInfo *distance_proc, Oid collation)
{
	RecallStatsEntry *entry;
	bool	found;

	/* Early exits for disabled tracking or invalid data */
	if (!pgvector_track_recall || recall_stats_hash == NULL)
		return;

	if (tracker->result_count == 0)
		return;

	entry = (RecallStatsEntry *) hash_search(recall_stats_hash,
											  &index->rd_id,
											  HASH_ENTER,
											  &found);

	if (!found)
	{
		MemSet(&entry->stats, 0, sizeof(VectorRecallStats));
		entry->stats.last_updated = GetCurrentTimestamp();
	}

	entry->stats.total_queries++;
	entry->stats.total_results_returned += tracker->result_count;

	if (entry->stats.total_queries % pgvector_recall_sample_rate == 0)
	{
		int estimated_expected = tracker->result_count;
		int estimated_correct = tracker->result_count;

		entry->stats.sampled_queries++;

		/* Enhanced estimation using distance-threshold scan */
		if (tracker->max_distance > 0)
		{
			/* Identify heap relation and attribute number */
			Oid heapOid = index->rd_index->indrelid;
			int16 attnum = index->rd_index->indkey.values[0];
			Relation heapRel = NULL;
			TableScanDesc heapScan = NULL;
			volatile int count_within = 0;
			volatile bool exceeded = false;
			volatile int tuples_scanned = 0;

			PG_TRY();
			{
				HeapTuple tuple;
				TupleDesc tupDesc;
				Datum distanceDatum;
				bool isnull;
				double dist;

				heapRel = table_open(heapOid, AccessShareLock);
				heapScan = table_beginscan(heapRel, GetActiveSnapshot(), 0, NULL);
				tupDesc = RelationGetDescr(heapRel);

				while ((tuple = heap_getnext(heapScan, ForwardScanDirection)) != NULL)
				{
					Datum value;

					tuples_scanned++;

					/* Limit scan size to prevent performance issues on large tables */
					if (pgvector_recall_max_scan_tuples > 0 && tuples_scanned > pgvector_recall_max_scan_tuples)
						break;

					value = heap_getattr(tuple, attnum, tupDesc, &isnull);
					if (isnull)
						continue;

					if (distance_proc != NULL) {
						distanceDatum = FunctionCall2Coll(distance_proc, collation, tracker->query_value, value);
						dist = DatumGetFloat8(distanceDatum);
						if (dist <= tracker->max_distance + DBL_EPSILON)
						{
							count_within++;

							/*
							 * Early termination: we only need to know if there
							 * are more than K matches
							 */
							if (count_within > tracker->result_count)
							{
								exceeded = true;
								break;
							}
						}
					} else {
						elog(ERROR, "distance_proc is NULL");
					}
				}

				if (exceeded)
					estimated_expected = tracker->result_count + 1; /* conservative lower bound */
				else
					estimated_expected = count_within;

				/*
				 * Correct matches is the minimum of what we returned vs what
				 * exists within threshold
				 */
				estimated_correct = (estimated_expected < tracker->result_count) ? estimated_expected : tracker->result_count;
			}
			PG_CATCH();
			{
				/* Ensure cleanup on error */
				if (heapScan != NULL)
					table_endscan(heapScan);
				if (heapRel != NULL)
					table_close(heapRel, AccessShareLock);

				PG_RE_THROW();
			}
			PG_END_TRY();

			/* Clean up resources */
			if (heapScan != NULL)
				table_endscan(heapScan);
			if (heapRel != NULL)
				table_close(heapRel, AccessShareLock);
		}

		entry->stats.correct_matches += estimated_correct;
		entry->stats.total_expected += estimated_expected;

		if (entry->stats.total_expected > 0)
			entry->stats.current_recall = (double) entry->stats.correct_matches / entry->stats.total_expected;

		entry->stats.last_updated = GetCurrentTimestamp();
	}
}

double
GetCurrentRecall(Oid indexoid)
{
	RecallStatsEntry *entry;

	if (recall_stats_hash == NULL)
		return -1.0;

	entry = (RecallStatsEntry *) hash_search(recall_stats_hash,
											  &indexoid,
											  HASH_FIND,
											  NULL);

	if (entry == NULL || entry->stats.total_expected == 0)
		return -1.0;

	return entry->stats.current_recall;
}

void
ResetRecallStats(Oid indexoid)
{
	RecallStatsEntry *entry;

	if (recall_stats_hash == NULL)
		return;

	entry = (RecallStatsEntry *) hash_search(recall_stats_hash,
											  &indexoid,
											  HASH_FIND,
											  NULL);

	if (entry != NULL)
	{
		MemSet(&entry->stats, 0, sizeof(VectorRecallStats));
		entry->stats.last_updated = GetCurrentTimestamp();
	}
}

/*
 * SQL function to get recall statistics
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(pg_vector_recall_stats);
Datum
pg_vector_recall_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS status;
	RecallStatsEntry *entry;

	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	tupdesc = CreateTemplateTupleDesc(8);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "indexoid", OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "total_queries", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "sampled_queries", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "total_results_returned", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "correct_matches", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "total_expected", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7, "current_recall", FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 8, "last_updated", TIMESTAMPTZOID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (recall_stats_hash == NULL)
		return (Datum) 0;

	hash_seq_init(&status, recall_stats_hash);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		Datum		values[8];
		bool		nulls[8];

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = ObjectIdGetDatum(entry->indexoid);
		values[1] = Int64GetDatum(entry->stats.total_queries);
		values[2] = Int64GetDatum(entry->stats.sampled_queries);
		values[3] = Int64GetDatum(entry->stats.total_results_returned);
		values[4] = Int64GetDatum(entry->stats.correct_matches);
		values[5] = Int64GetDatum(entry->stats.total_expected);
		values[6] = Float8GetDatum(entry->stats.current_recall);
		values[7] = TimestampTzGetDatum(entry->stats.last_updated);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	return (Datum) 0;
}

/*
 * SQL function to reset recall statistics for a specific index
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(pg_vector_recall_reset);
Datum
pg_vector_recall_reset(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);

	ResetRecallStats(indexoid);

	PG_RETURN_VOID();
}

/*
 * SQL function to get current recall for a specific index
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(pg_vector_recall_get);
Datum
pg_vector_recall_get(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	double		recall;

	recall = GetCurrentRecall(indexoid);

	if (recall < 0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(recall);
}
