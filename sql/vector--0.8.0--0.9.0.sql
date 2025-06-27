-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.9.0'" to load this file. \quit

-- Function to get recall statistics for all vector indexes
CREATE OR REPLACE FUNCTION pg_vector_recall_stats()
RETURNS TABLE (
    indexoid oid,
    total_queries bigint,
    sampled_queries bigint,
    total_results_returned bigint,
    correct_matches bigint,
    total_expected bigint,
    current_recall double precision,
    last_updated timestamptz
)
AS 'MODULE_PATHNAME', 'pg_vector_recall_stats'
LANGUAGE C STRICT VOLATILE;

-- Function to reset recall statistics for a specific index
CREATE OR REPLACE FUNCTION pg_vector_recall_reset(indexoid oid)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_vector_recall_reset'
LANGUAGE C STRICT VOLATILE;

-- Function to get current recall for a specific index
CREATE OR REPLACE FUNCTION pg_vector_recall_get(indexoid oid)
RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_vector_recall_get'
LANGUAGE C STRICT VOLATILE;

-- Function to get recall statistics summary with index names
CREATE OR REPLACE FUNCTION pg_vector_recall_summary()
RETURNS TABLE (
    indexoid oid,
    schema_name name,
    index_name name,
    total_queries bigint,
    sampled_queries bigint,
    total_results_returned bigint,
    correct_matches bigint,
    total_expected bigint,
    current_recall double precision,
    last_updated timestamptz
)
AS $$
SELECT
    s.indexoid,
    n.nspname AS schema_name,
    c.relname AS index_name,
    s.total_queries,
    s.sampled_queries,
    s.total_results_returned,
    s.correct_matches,
    s.total_expected,
    s.current_recall,
    s.last_updated
FROM pg_vector_recall_stats() s
JOIN pg_class c ON s.indexoid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind = 'i'
ORDER BY schema_name, index_name;
$$ LANGUAGE SQL STRICT VOLATILE;
