SET enable_seqscan = off;

-- hamming

CREATE TABLE t (val bit(3));
INSERT INTO t (val) VALUES (B'000'), (B'100'), (B'111'), (NULL);
CREATE INDEX ON t USING ivfflat (val bit_hamming_ops) WITH (lists = 1);

INSERT INTO t (val) VALUES (B'110');

SELECT * FROM t ORDER BY val <~> B'111';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <~> (SELECT NULL::bit)) t2;

DROP TABLE t;

-- varbit

CREATE TABLE t (val varbit(3));
CREATE INDEX ON t USING ivfflat (val bit_hamming_ops) WITH (lists = 1);
CREATE INDEX ON t USING ivfflat ((val::bit(3)) bit_hamming_ops) WITH (lists = 1);
CREATE INDEX ON t USING ivfflat ((val::bit(64001)) bit_hamming_ops) WITH (lists = 1);
CREATE INDEX ON t USING ivfflat ((val::bit(2)) bit_hamming_ops) WITH (lists = 5);
DROP TABLE t;

-- dimensions

CREATE TABLE t (val bit(64000));
CREATE INDEX ON t USING ivfflat (val bit_hamming_ops);
DROP TABLE t;

CREATE TABLE t (val bit(64001));
CREATE INDEX ON t USING ivfflat (val bit_hamming_ops);
DROP TABLE t;

-- memory

SET maintenance_work_mem = '1MB';
CREATE TABLE t (val bit(64000));
CREATE INDEX ON t USING ivfflat (val bit_hamming_ops);
DROP TABLE t;
RESET maintenance_work_mem;

SET maintenance_work_mem = '29MB';
CREATE TABLE t (val bit(64000));
INSERT INTO t (val) VALUES (B'0'::bit(64000));
CREATE INDEX ON t USING ivfflat (val bit_hamming_ops);
DROP TABLE t;
RESET maintenance_work_mem;
