-- Test-only C wrappers from vector.so, defined per test file rather than
-- shipped in the extension SQL (they read index internals and are not part
-- of the public surface).
CREATE OR REPLACE FUNCTION tqhnsw_test_meta(regclass) RETURNS text
	AS 'vector' LANGUAGE C STABLE;
CREATE OR REPLACE FUNCTION tqhnsw_test_graph(regclass) RETURNS text
	AS 'vector' LANGUAGE C STABLE;

SET enable_seqscan = off;

-- Empty (UNLOGGED -> ambuildempty path) index has a valid meta page.
CREATE UNLOGGED TABLE tqhnsw_e (v vector(8));
CREATE INDEX tqhnsw_e_idx ON tqhnsw_e USING tqhnsw (v vector_l2_ops);
SELECT tqhnsw_test_meta('tqhnsw_e_idx');   -- dim=8 m=16 ef_construction=64 bits=4 metric=0 nvectors=0 entry_level=-1
DROP TABLE tqhnsw_e;

-- Bad options rejected.
CREATE TABLE tqhnsw_bad (v vector(4));
CREATE INDEX ON tqhnsw_bad USING tqhnsw (v vector_l2_ops) WITH (m = 1);              -- ERROR: out of range
CREATE INDEX ON tqhnsw_bad USING tqhnsw (v vector_l2_ops) WITH (ef_construction = 2); -- ERROR: out of range
DROP TABLE tqhnsw_bad;

-- Real build: 500 rows -> a graph with an entry point and level-0 neighbors.
CREATE TABLE tqhnsw_b (id int, v vector(8));
INSERT INTO tqhnsw_b SELECT g, ARRAY[g,g+1,g+2,g+3,g+4,g+5,g+6,g+7]::real[]::vector
  FROM generate_series(1, 500) g;
CREATE INDEX tqhnsw_b_idx ON tqhnsw_b USING tqhnsw (v vector_l2_ops) WITH (m = 8, ef_construction = 32);
-- nvectors is a stable count; entry_level varies run-to-run so it is omitted here.
SELECT regexp_replace(tqhnsw_test_meta('tqhnsw_b_idx'), ' entry_level=.*', '');
-- entry node has >0 level-0 neighbors and the graph has 500 nodes (stable predicates):
SELECT tqhnsw_test_graph('tqhnsw_b_idx');

-- Post-build insert: verify that inserting after a real build does not corrupt the
-- codebook page (element tuples must land on element pages, not block 1).
INSERT INTO tqhnsw_b VALUES (501, ARRAY[1,1,1,1,1,1,1,1]::real[]::vector);
-- Verify the post-build insert was recorded in the index: nvectors and the graph node
-- count grow to 501 and the graph stays structurally valid.  This deterministically
-- confirms the element tuple landed on an element page (not the codebook page).  We do
-- NOT assert the result of a LIMIT-1 NN query: tqhnsw is an approximate index and does
-- not guarantee that the exact match is returned -- only that the index stays queryable
-- and the planner uses it.
SELECT regexp_replace(tqhnsw_test_meta('tqhnsw_b_idx'), ' entry_level=.*', '');
SELECT tqhnsw_test_graph('tqhnsw_b_idx');
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM tqhnsw_b ORDER BY v <-> ARRAY[1,1,1,1,1,1,1,1]::real[]::vector LIMIT 1;
RESET enable_seqscan;

DROP TABLE tqhnsw_b;

-- Parallel build: forced workers produce a usable index with correct recall.
SET max_parallel_maintenance_workers = 4;
SET min_parallel_table_scan_size = 1;
CREATE TABLE tqhnsw_par (id int, v vector(32));
INSERT INTO tqhnsw_par SELECT g,
  ARRAY[g%7,g%11,g%13,g%17,g%19,g%23,g%29,g%31,
        g%37,g%41,g%43,g%47,g%53,g%59,g%61,g%67,
        g%71,g%73,g%79,g%83,g%89,g%97,g%101,g%103,
        g%107,g%109,g%113,g%127,g%131,g%137,g%139,g%149]::real[]::vector(32)
  FROM generate_series(1, 2000) g;
CREATE INDEX tqhnsw_par_idx ON tqhnsw_par USING tqhnsw (v vector_l2_ops);
-- Verify the graph is fully built.
SELECT regexp_replace(tqhnsw_test_meta('tqhnsw_par_idx'), ' entry_level=.*', '');
SELECT tqhnsw_test_graph('tqhnsw_par_idx');
-- Every one of the first 10 query rows finds itself as the nearest neighbor.
SELECT count(*) = 10 AS all_self_match FROM (
  SELECT q.id, (SELECT p.id FROM tqhnsw_par p ORDER BY p.v <-> q.v LIMIT 1) AS nn
  FROM tqhnsw_par q WHERE q.id <= 10
) s WHERE nn = id;
RESET min_parallel_table_scan_size;
RESET max_parallel_maintenance_workers;
DROP TABLE tqhnsw_par;

-- halfvec opclass tests: l2, ip, cosine metrics.
-- dim = 4 (>= 3 required by tq).  Small fixed data for deterministic neighbors.
CREATE TABLE tqhnsw_hv (v halfvec(4));
INSERT INTO tqhnsw_hv VALUES ('[1,2,3,4]'), ('[2,3,4,5]'), ('[0,0,0,1]'), ('[5,4,3,2]'), ('[1,1,1,1]');
CREATE INDEX ON tqhnsw_hv USING tqhnsw (v halfvec_l2_ops);
CREATE INDEX ON tqhnsw_hv USING tqhnsw (v halfvec_ip_ops);
CREATE INDEX ON tqhnsw_hv USING tqhnsw (v halfvec_cosine_ops);
SET enable_seqscan = off;
-- L2: nearest to [1,2,3,4] should be itself first
SELECT v FROM tqhnsw_hv ORDER BY v <-> '[1,2,3,4]' LIMIT 3;
-- IP: nearest by inner product (query [1,2,3,5] separates all candidates: no IP tie)
SELECT v FROM tqhnsw_hv ORDER BY v <#> '[1,2,3,5]' LIMIT 3;
-- Cosine: nearest to [1,2,3,4] by cosine distance
SELECT v FROM tqhnsw_hv ORDER BY v <=> '[1,2,3,4]' LIMIT 3;
RESET enable_seqscan;
DROP TABLE tqhnsw_hv;

-- sparsevec opclass tests: l2, ip, cosine. Emits a densification NOTICE per CREATE INDEX.
-- Data: 5-dim vectors; query '{1:1,3:2}/5' = [1,0,2,0,0] (1-based indices).
-- L2 distances from query: 0, 4, 7, 14, 55 (all distinct).
-- IP scores with query '{1:4,2:3,3:2,4:1}/5': 12, 10, 8, 5, 3 (all distinct).
-- Cosine nearest to '{1:1,3:2}/5': self (dist=0), then '{1:1,2:1,3:1,4:1,5:1}/5' (dist=0.4).
CREATE TABLE tqhnsw_sv (v sparsevec(5));
INSERT INTO tqhnsw_sv VALUES
	('{1:1,3:2}/5'),
	('{2:1,5:3}/5'),
	('{1:2,2:1,4:1}/5'),
	('{4:5,5:5}/5'),
	('{1:1,2:1,3:1,4:1,5:1}/5');
CREATE INDEX ON tqhnsw_sv USING tqhnsw (v sparsevec_l2_ops);
CREATE INDEX ON tqhnsw_sv USING tqhnsw (v sparsevec_ip_ops);
CREATE INDEX ON tqhnsw_sv USING tqhnsw (v sparsevec_cosine_ops);
SET enable_seqscan = off;
SELECT v FROM tqhnsw_sv ORDER BY v <-> '{1:1,3:2}/5' LIMIT 2;
SELECT v FROM tqhnsw_sv ORDER BY v <#> '{1:4,2:3,3:2,4:1}/5' LIMIT 2;
SELECT v FROM tqhnsw_sv ORDER BY v <=> '{1:1,3:2}/5' LIMIT 2;
RESET enable_seqscan;
DROP TABLE tqhnsw_sv;

-- sparsevec declared dim above TQ_MAX_DIM is rejected at build (tqhnsw path).
CREATE TABLE tqhnsw_sv_big (v sparsevec(20000));
INSERT INTO tqhnsw_sv_big VALUES ('{1:1,20000:2}/20000');
\set ON_ERROR_STOP 0
CREATE INDEX ON tqhnsw_sv_big USING tqhnsw (v sparsevec_l2_ops);  -- expect ERROR
\set ON_ERROR_STOP 1
DROP TABLE tqhnsw_sv_big;
