SET enable_seqscan = off;

CREATE TABLE t (val sparsevec(3));
INSERT INTO t (val) VALUES ('{}/3'), ('{1:1,2:2,3:3}/3'), ('{1:1,2:1,3:1}/3'), (NULL);
CREATE INDEX ON t (val);

SELECT * FROM t WHERE val = '{1:1,2:2,3:3}/3';
SELECT * FROM t ORDER BY val;

DROP TABLE t;
