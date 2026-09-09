SET enable_seqscan = off;

-- Fast-path correctness for the direct-call IVFFlat scan.
--
-- Index tuples for vectors with 1 + 4 + 4*dim <= 127 (dim <= 30) use a
-- 1-byte short varlena header; larger dimensions use the standard 4-byte
-- header. Both layouts must produce the same ordering as the original
-- fmgr path. The generators below use primes larger than the row count,
-- so every vector is distinct.

-- L2, short header (dim = 8)

CREATE TABLE t (id serial, val vector(8));
INSERT INTO t (val)
SELECT ('[' || string_agg(ROUND((((g * 37 + d * 11) % 211)::numeric / 8), 3)::text, ',' ORDER BY d) || ']')::vector
FROM generate_series(1, 200) g, generate_series(0, 7) d
GROUP BY g;

CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 5);

SET ivfflat.probes = 5;

SELECT id, ROUND((val <-> '[3,7,1,9,2,11,5,8]')::numeric, 6) FROM t ORDER BY val <-> '[3,7,1,9,2,11,5,8]' LIMIT 10;

RESET ivfflat.probes;
DROP TABLE t;

-- inner product, short header (dim = 8)

CREATE TABLE t (id serial, val vector(8));
INSERT INTO t (val)
SELECT ('[' || string_agg(ROUND((((g * 41 + d * 7) % 211)::numeric / 7), 3)::text, ',' ORDER BY d) || ']')::vector
FROM generate_series(1, 200) g, generate_series(0, 7) d
GROUP BY g;

CREATE INDEX ON t USING ivfflat (val vector_ip_ops) WITH (lists = 5);

SET ivfflat.probes = 5;

SELECT id, ROUND((val <#> '[3,7,1,9,2,11,5,8]')::numeric, 6) FROM t ORDER BY val <#> '[3,7,1,9,2,11,5,8]' LIMIT 10;

RESET ivfflat.probes;
DROP TABLE t;

-- cosine, short header (dim = 8)

CREATE TABLE t (id serial, val vector(8));
INSERT INTO t (val)
SELECT ('[' || string_agg(ROUND((((g * 43 + d * 13) % 211)::numeric / 6), 3)::text, ',' ORDER BY d) || ']')::vector
FROM generate_series(1, 200) g, generate_series(0, 7) d
GROUP BY g;

CREATE INDEX ON t USING ivfflat (val vector_cosine_ops) WITH (lists = 5);

SET ivfflat.probes = 5;

SELECT id, ROUND((val <=> '[3,7,1,9,2,11,5,8]')::numeric, 6) FROM t ORDER BY val <=> '[3,7,1,9,2,11,5,8]' LIMIT 10;

-- lateral rescan keeps the fast path

SELECT t.id, l.id FROM t CROSS JOIN LATERAL (SELECT id FROM t t2 WHERE t2.id != t.id ORDER BY t2.val <=> t.val LIMIT 1) l ORDER BY t.id LIMIT 5;

RESET ivfflat.probes;
DROP TABLE t;

-- candidate-array overflow falls back to the tuplesort path
-- (work_mem = 64kB caps the candidate array at 4096 entries)

CREATE TABLE t (id serial, val vector(8));
INSERT INTO t (val)
SELECT ('[' || string_agg(ROUND((((g * 37 + d * 11) % 9973)::numeric / 8), 3)::text, ',' ORDER BY d) || ']')::vector
FROM generate_series(1, 5000) g, generate_series(0, 7) d
GROUP BY g;

CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 5);

SET ivfflat.probes = 5;
SET work_mem = '64kB';

-- exact distance ties exist in this data set; the fast path breaks ties by
-- TID, but the tuplesort fallback does not guarantee a tie order, so order
-- by id as a secondary key to keep the snapshot deterministic

SELECT id, ROUND((val <-> '[3,7,1,9,2,11,5,8]')::numeric, 6) FROM t ORDER BY val <-> '[3,7,1,9,2,11,5,8]', id LIMIT 10;

RESET work_mem;
RESET ivfflat.probes;
DROP TABLE t;

-- 4-byte varlena header (dim = 64)

CREATE TABLE t (id serial, val vector(64));
INSERT INTO t (val)
SELECT ('[' || string_agg(ROUND((((g * 31 + d * 17) % 311)::numeric / 9), 3)::text, ',' ORDER BY d) || ']')::vector
FROM generate_series(1, 300) g, generate_series(0, 63) d
GROUP BY g;

CREATE INDEX ON t USING ivfflat (val vector_l2_ops) WITH (lists = 5);

SET ivfflat.probes = 5;

SELECT id, ROUND((val <-> '[3,7,1,9,2,11,5,8,4,6,10,2,12,1,15,9,3,7,1,9,2,11,5,8,4,6,10,2,12,1,15,9,3,7,1,9,2,11,5,8,4,6,10,2,12,1,15,9,3,7,1,9,2,11,5,8,4,6,10,2,12,1,15,9]')::numeric, 6) FROM t ORDER BY val <-> '[3,7,1,9,2,11,5,8,4,6,10,2,12,1,15,9,3,7,1,9,2,11,5,8,4,6,10,2,12,1,15,9,3,7,1,9,2,11,5,8,4,6,10,2,12,1,15,9,3,7,1,9,2,11,5,8,4,6,10,2,12,1,15,9]' LIMIT 10;

RESET ivfflat.probes;
DROP TABLE t;
