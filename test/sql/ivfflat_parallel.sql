SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS vector;
--SET force_parallel_mode = on;
SET parallel_setup_cost = 10;
SET parallel_tuple_cost = 0.001;
SET min_parallel_table_scan_size = 0;
SET min_parallel_index_scan_size = 0;

CREATE TABLE t (id integer, val vector(3));
INSERT INTO t (id, val) SELECT n, ARRAY[random(), random(), random()] FROM generate_series(1,1000000) n;
CREATE INDEX ON t USING ivfflat (val) WITH (lists = 10);
SET ivfflat.probes = 2;

EXPLAIN SELECT * FROM t ORDER BY val <-> '[0.5,0.5,0.5]' LIMIT 5;
SELECT * FROM t ORDER BY val <-> '[0.5,0.5,0.5]' LIMIT 5;

DROP TABLE t;
