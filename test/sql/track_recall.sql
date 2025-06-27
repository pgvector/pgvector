SET pgvector.track_recall = on;
SET pgvector.recall_sample_rate = 1;
SET pgvector.recall_max_scan_tuples = -1;

SELECT pg_vector_recall_stats();
SELECT pg_vector_recall_summary();

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX t_idx ON t USING hnsw (val vector_l2_ops);

SELECT pg_vector_recall_get('t_idx'::regclass::oid);
SELECT pg_vector_recall_reset('t_idx'::regclass::oid);

DROP TABLE t;

SET pgvector.track_recall = off;
SET pgvector.recall_sample_rate = 100;
SET pgvector.recall_max_scan_tuples = 10000;
