SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE t (val vector(3));

TRUNCATE t;
INSERT INTO t (val) VALUES ('[1,2,3]'), ('[2,4,6]'), ('[3,6,9]'), (NULL);
SELECT sum(val) FROM t;
SELECT avg(val) FROM t;

TRUNCATE t;
INSERT INTO t (val) VALUES (NULL), ('[1,2,3]'), ('[2,4,6]'), ('[3,6,9]');
SELECT sum(val) FROM t;
SELECT avg(val) FROM t;

SELECT vector_avg_accum(('[2,3,4]',10), '[1,2,3]');
SELECT vector_avg_accum(('[2,3,4]',10), NULL);
SELECT vector_avg_accum(NULL, '[1,2,3]');
SELECT vector_avg_accum(NULL, NULL);

SELECT vector_avg_combine(('[2,3,4]',10), ('[2,3,4]',10));
SELECT vector_avg_combine(('[2,3,4]',10), NULL);
SELECT vector_avg_combine(NULL, ('[2,3,4]',10));
SELECT vector_avg_combine(NULL, NULL);

DROP TABLE t;
