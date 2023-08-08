SET enable_seqscan = off;

CREATE TABLE t (val vector(3));
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (m = 3);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (m = 101);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (ef_construction = 9);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (ef_construction = 1001);

SHOW hnsw.ef_search;

SET hnsw.ef_search = 9;
SET hnsw.ef_search = 1001;

DROP TABLE t;
