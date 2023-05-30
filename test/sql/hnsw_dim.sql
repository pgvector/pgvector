SET enable_seqscan = off;

CREATE TABLE t (val vector);
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX t_val_1 ON t USING hnsw (val vector_cosine_ops) WITH (dims = 3);

INSERT INTO t (val) VALUES ('[1,2,4]');
INSERT INTO t (val) VALUES ('[1,2,4,5]');

SELECT * FROM t ORDER BY val <=> '[3,3,3]';

INSERT INTO t (val) VALUES ('[1,2,4,5]');
CREATE INDEX t_val_2 ON t USING hnsw(val vector_cosine_ops) WITH (dims = 4);

TRUNCATE t;
CREATE INDEX t_val_3 ON t USING hnsw(val vector_cosine_ops) WITH (dims = 4);
INSERT INTO t (val) VALUES ('[1,2,4]');

DROP TABLE t;
