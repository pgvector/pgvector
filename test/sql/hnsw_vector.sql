SET enable_seqscan = off;

-- L2

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_l2_ops);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <-> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <-> (SELECT NULL::vector)) t2;
SELECT COUNT(*) FROM t;

TRUNCATE t;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

DROP TABLE t;

-- inner product

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_ip_ops);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <#> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <#> (SELECT NULL::vector)) t2;

DROP TABLE t;

-- cosine

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_cosine_ops);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <=> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> '[0,0,0]') t2;
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> (SELECT NULL::vector)) t2;

DROP TABLE t;

-- L1

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_l1_ops);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <+> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <+> (SELECT NULL::vector)) t2;

DROP TABLE t;

-- iterative

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_l2_ops);

SET hnsw.iterative_scan = strict_order;
SET hnsw.ef_search = 1;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

SET hnsw.iterative_scan = relaxed_order;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

TRUNCATE t;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

RESET hnsw.iterative_scan;
RESET hnsw.ef_search;
DROP TABLE t;

-- unlogged

CREATE UNLOGGED TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_l2_ops);

SELECT * FROM t ORDER BY val <-> '[3,3,3]';

DROP TABLE t;

-- options

CREATE TABLE t (val vector(3));
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (m = 1);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (m = 101);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (ef_construction = 3);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (ef_construction = 1001);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (m = 16, ef_construction = 31);

SHOW hnsw.ef_search;

SET hnsw.ef_search = 0;
SET hnsw.ef_search = 1001;

SHOW hnsw.iterative_scan;

SET hnsw.iterative_scan = on;

SHOW hnsw.max_scan_tuples;

SET hnsw.max_scan_tuples = 0;

SHOW hnsw.scan_mem_multiplier;

SET hnsw.scan_mem_multiplier = 0;
SET hnsw.scan_mem_multiplier = 1001;

DROP TABLE t;

-- default_ef_search option tests

-- Test: Create index with default_ef_search
CREATE TABLE t_des (val vector(3));
INSERT INTO t_des (val) SELECT ARRAY[random(), random(), random()]::vector FROM generate_series(1, 100);
CREATE INDEX idx_des ON t_des USING hnsw (val vector_l2_ops) WITH (default_ef_search = 100);

-- Test: Query uses index default (100 ef_search)
SET enable_seqscan = off;
SELECT COUNT(*) FROM (SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;

-- Test: Explicit SET overrides index default
SET hnsw.ef_search = 50;
SELECT COUNT(*) FROM (SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;

-- Test: RESET returns to index default
RESET hnsw.ef_search;
SELECT COUNT(*) FROM (SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;

-- Test: ALTER INDEX changes default
ALTER INDEX idx_des SET (default_ef_search = 200);
SELECT COUNT(*) FROM (SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;

-- Test: ALTER INDEX RESET removes default (falls back to GUC default)
ALTER INDEX idx_des RESET (default_ef_search);
SELECT COUNT(*) FROM (SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;

-- Test: default_ef_search = 0 acts as unset (use GUC default)
CREATE INDEX idx_des_zero ON t_des USING hnsw (val vector_l2_ops) WITH (default_ef_search = 0);
SELECT COUNT(*) FROM (SELECT * FROM t_des ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;
DROP INDEX idx_des_zero;

-- Test: Invalid values rejected
CREATE INDEX ON t_des USING hnsw (val vector_l2_ops) WITH (default_ef_search = -1);

DROP TABLE t_des;
