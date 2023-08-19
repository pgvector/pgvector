CREATE TABLE t (val vector(3));
CREATE INDEX ON t USING ivf (val vector_l2_ops) WITH (lists = 0);
CREATE INDEX ON t USING ivf (val vector_l2_ops) WITH (lists = 32769);
CREATE INDEX ON t USING ivf (val vector_l2_ops) WITH (lists = 1, quantizer = 'invalid');

SHOW ivf.probes;

DROP TABLE t;
