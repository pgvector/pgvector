-- SET force_parallel_mode = on;
SET parallel_setup_cost = 10;
SET parallel_tuple_cost = 0.000001;
SET min_parallel_table_scan_size = 1;
SET min_parallel_index_scan_size = 1;

CREATE TABLE t (id integer, val vector(3));
ALTER TABLE t ALTER COLUMN val SET STORAGE PLAIN;
INSERT INTO t (id, val) SELECT n, ARRAY[random(), random(), random()] FROM generate_series(1,1000000) n;
CREATE INDEX ON t USING ivfflat (val) WITH (lists = 10);
SET ivfflat.probes = 4;

EXPLAIN SELECT * FROM t ORDER BY val <-> '[0.5,0.5,0.5]' LIMIT 5;
SELECT * FROM t ORDER BY val <-> '[0.5,0.5,0.5]' LIMIT 5;

DROP TABLE t;
