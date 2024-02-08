CREATE TABLE t (val svector(3));
CREATE INDEX ON t USING shnsw (val svector_l2_ops) WITH (m = 1);
CREATE INDEX ON t USING shnsw (val svector_l2_ops) WITH (m = 101);
CREATE INDEX ON t USING shnsw (val svector_l2_ops) WITH (ef_construction = 3);
CREATE INDEX ON t USING shnsw (val svector_l2_ops) WITH (ef_construction = 1001);
CREATE INDEX ON t USING shnsw (val svector_l2_ops) WITH (m = 16, ef_construction = 31);

SHOW shnsw.ef_search;

SET shnsw.ef_search = 0;
SET shnsw.ef_search = 1001;

DROP TABLE t;
