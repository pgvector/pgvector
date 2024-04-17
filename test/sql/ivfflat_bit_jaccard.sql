SET enable_seqscan = off;

CREATE TABLE t (val bit(4));
INSERT INTO t (val) VALUES (B'0000'), (B'1100'), (B'1111'), (NULL);
CREATE INDEX ON t USING ivfflat (val bit_jaccard_ops) WITH (lists = 1);

INSERT INTO t (val) VALUES (B'1110');

SELECT * FROM t ORDER BY val <%> B'1111';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <%> (SELECT NULL::bit)) t2;

DROP TABLE t;
