#ifndef VECTOR_RECALL_H
#define VECTOR_RECALL_H

#include "postgres.h"

typedef struct VectorRecallStats
{
	int64		total_queries;
	int64		sampled_queries;
	int64		total_results_returned;
	int64		correct_matches;
	int64		total_expected;
	double		current_recall;
	TimestampTz	last_updated;
} VectorRecallStats;

double GetCurrentRecall(Oid indexoid);
void ResetRecallStats(Oid indexoid);

typedef struct RecallStatsEntry
{
	Oid			indexoid;
	VectorRecallStats stats;
} RecallStatsEntry;

typedef struct VectorRecallTracker
{
	Datum		        query_value;
	int			        result_count;     /* number of results returned */
	double		      max_distance;     /* distance of the farthest (k-th) result */
} VectorRecallTracker;

/*
 * Recall tracking configuration
 */
extern bool pgvector_track_recall;
extern int pgvector_recall_sample_rate;
extern int pgvector_recall_max_scan_tuples;

void InitVectorRecallTracking(void);

void TrackVectorQuery(Relation index, VectorRecallTracker *tracker, FmgrInfo *distance_proc, Oid collation);

#endif
