SET enable_seqscan = off;

-- L2

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 1);

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
CREATE INDEX ON t USING ivfflat (val vector_ip_ops) WITH (lists = 1);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <#> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <#> (SELECT NULL::vector)) t2;

DROP TABLE t;

-- cosine

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING ivfflat (val vector_cosine_ops) WITH (lists = 1);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <=> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> '[0,0,0]') t2;
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> (SELECT NULL::vector)) t2;

DROP TABLE t;

-- iterative

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 3);

SET ivfflat.iterative_scan = relaxed_order;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

SET ivfflat.max_probes = 1;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

SET ivfflat.max_probes = 2;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

TRUNCATE t;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

RESET ivfflat.iterative_scan;
RESET ivfflat.max_probes;
DROP TABLE t;

-- unlogged

CREATE UNLOGGED TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 1);

SELECT * FROM t ORDER BY val <-> '[3,3,3]';

DROP TABLE t;

-- options

CREATE TABLE t (val vector(3));
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 0);
CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 32769);

SHOW ivfflat.probes;

SET ivfflat.probes = 0;
SET ivfflat.probes = 32769;

SHOW ivfflat.iterative_scan;

SET ivfflat.iterative_scan = on;

SHOW ivfflat.max_probes;

SET ivfflat.max_probes = 0;
SET ivfflat.max_probes = 32769;

DROP TABLE t;

-- default_probes option tests

-- Test: Create index with default_probes
CREATE TABLE t_dp (val vector(3));
INSERT INTO t_dp (val) SELECT ARRAY[random(), random(), random()]::vector FROM generate_series(1, 100);
CREATE INDEX idx_dp ON t_dp USING ivfflat (val vector_l2_ops) WITH (lists = 10, default_probes = 5);

-- Test: Query uses index default (5 probes)
SET enable_seqscan = off;
SELECT COUNT(*) FROM (SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;

-- Test: Explicit SET overrides index default
SET ivfflat.probes = 3;
SELECT COUNT(*) FROM (SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;

-- Test: RESET returns to index default
RESET ivfflat.probes;
SELECT COUNT(*) FROM (SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;

-- Test: ALTER INDEX changes default
ALTER INDEX idx_dp SET (default_probes = 8);
SELECT COUNT(*) FROM (SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;

-- Test: ALTER INDEX RESET removes default (falls back to GUC default)
ALTER INDEX idx_dp RESET (default_probes);
SELECT COUNT(*) FROM (SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;

-- Test: default_probes = 0 acts as unset (use GUC default)
CREATE INDEX idx_dp_zero ON t_dp USING ivfflat (val vector_l2_ops) WITH (lists = 10, default_probes = 0);
SELECT COUNT(*) FROM (SELECT * FROM t_dp ORDER BY val <-> '[0.5, 0.5, 0.5]' LIMIT 5) t;
DROP INDEX idx_dp_zero;

-- Test: Invalid values rejected
CREATE INDEX ON t_dp USING ivfflat (val vector_l2_ops) WITH (default_probes = -1);

DROP TABLE t_dp;
