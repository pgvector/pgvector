SET enable_seqscan = off;

-- L2

CREATE TABLE t (val sparsevec(3));
INSERT INTO t (val) VALUES ('{}/3'), ('{1:1,2:2,3:3}/3'), ('{1:1,2:1,3:1}/3'), (NULL);
CREATE INDEX ON t USING hnsw (val sparsevec_l2_ops);

INSERT INTO t (val) VALUES ('{1:1,2:2,3:4}/3');

SELECT * FROM t ORDER BY val <-> '{1:3,2:3,3:3}/3';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <-> (SELECT NULL::sparsevec)) t2;
SELECT COUNT(*) FROM t;

TRUNCATE t;
SELECT * FROM t ORDER BY val <-> '{1:3,2:3,3:3}/3';

DROP TABLE t;

-- inner product

CREATE TABLE t (val sparsevec(3));
INSERT INTO t (val) VALUES ('{}/3'), ('{1:1,2:2,3:3}/3'), ('{1:1,2:1,3:1}/3'), (NULL);
CREATE INDEX ON t USING hnsw (val sparsevec_ip_ops);

INSERT INTO t (val) VALUES ('{1:1,2:2,3:4}/3');

SELECT * FROM t ORDER BY val <#> '{1:3,2:3,3:3}/3';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <#> (SELECT NULL::sparsevec)) t2;

DROP TABLE t;

-- cosine

CREATE TABLE t (val sparsevec(3));
INSERT INTO t (val) VALUES ('{}/3'), ('{1:1,2:2,3:3}/3'), ('{1:1,2:1,3:1}/3'), (NULL);
CREATE INDEX ON t USING hnsw (val sparsevec_cosine_ops);

INSERT INTO t (val) VALUES ('{1:1,2:2,3:4}/3');

SELECT * FROM t ORDER BY val <=> '{1:3,2:3,3:3}/3';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> '{}/3') t2;
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> (SELECT NULL::sparsevec)) t2;

DROP TABLE t;

-- L1

CREATE TABLE t (val sparsevec(3));
INSERT INTO t (val) VALUES ('{}/3'), ('{1:1,2:2,3:3}/3'), ('{1:1,2:1,3:1}/3'), (NULL);
CREATE INDEX ON t USING hnsw (val sparsevec_l1_ops);

INSERT INTO t (val) VALUES ('{1:1,2:2,3:4}/3');

SELECT * FROM t ORDER BY val <+> '{1:3,2:3,3:3}/3';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <+> (SELECT NULL::sparsevec)) t2;

DROP TABLE t;

-- non-zero elements

CREATE TABLE t (val sparsevec(1001));
INSERT INTO t (val) VALUES (array_fill(1, ARRAY[1001])::vector::sparsevec);
CREATE INDEX ON t USING hnsw (val sparsevec_l2_ops);
TRUNCATE t;
CREATE INDEX ON t USING hnsw (val sparsevec_l2_ops);
INSERT INTO t (val) VALUES (array_fill(1, ARRAY[1001])::vector::sparsevec);
DROP TABLE t;
