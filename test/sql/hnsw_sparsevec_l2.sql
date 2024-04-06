SET enable_seqscan = off;

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

-- TODO move
CREATE TABLE t (val sparsevec(1001));
INSERT INTO t (val) VALUES (array_fill(1, ARRAY[1001])::vector::sparsevec);
CREATE INDEX ON t USING hnsw (val sparsevec_l2_ops);
TRUNCATE t;
CREATE INDEX ON t USING hnsw (val sparsevec_l2_ops);
INSERT INTO t (val) VALUES (array_fill(1, ARRAY[1001])::vector::sparsevec);
DROP TABLE t;
