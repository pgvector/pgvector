SET enable_seqscan = off;

CREATE TABLE t (val halfvec(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t (val);

SELECT * FROM t WHERE val = '[1,2,3]';
SELECT * FROM t ORDER BY val;

DROP TABLE t;
