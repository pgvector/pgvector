SET enable_seqscan = off;

-- vector

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t (val);

SELECT * FROM t WHERE val = '[1,2,3]';
SELECT * FROM t ORDER BY val;

DROP TABLE t;

-- halfvec

CREATE TABLE t (val halfvec(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t (val);

SELECT * FROM t WHERE val = '[1,2,3]';
SELECT * FROM t ORDER BY val;

DROP TABLE t;

-- sparsevec

CREATE TABLE t (val sparsevec(3));
INSERT INTO t (val) VALUES ('{}/3'), ('{1:1,2:2,3:3}/3'), ('{1:1,2:1,3:1}/3'), (NULL);
CREATE INDEX ON t (val);

SELECT * FROM t WHERE val = '{1:1,2:2,3:3}/3';
SELECT * FROM t ORDER BY val;

DROP TABLE t;
