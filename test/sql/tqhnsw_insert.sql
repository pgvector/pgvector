-- Insert into an existing tqhnsw index, then query.
SET enable_seqscan = off;
CREATE TABLE tqh_ins (id int, v vector(4));
INSERT INTO tqh_ins SELECT g, ARRAY[g, g+1, g+2, g+3]::vector(4) FROM generate_series(1, 200) g;
CREATE INDEX tqh_ins_idx ON tqh_ins USING tqhnsw (v vector_l2_ops) WITH (m = 16, ef_construction = 64);
INSERT INTO tqh_ins VALUES (201, '[201,202,203,204]'), (202, '[202,203,204,205]');
SET tqhnsw.ef_search = 100;
SET tqhnsw.rerank = 100;
EXPLAIN (COSTS OFF) SELECT id FROM tqh_ins ORDER BY v <-> '[201,202,203,204]' LIMIT 1;
SELECT id FROM tqh_ins ORDER BY v <-> '[201,202,203,204]' LIMIT 1;
SELECT id FROM tqh_ins ORDER BY v <-> '[10,11,12,13]' LIMIT 1;
DROP TABLE tqh_ins;
