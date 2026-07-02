-- Test-only C wrappers from vector.so, defined per test file rather than
-- shipped in the extension SQL (they read index internals and are not part
-- of the public surface).
CREATE OR REPLACE FUNCTION tqflat_test_meta(regclass) RETURNS int[]
	AS 'vector' LANGUAGE C STRICT;

-- tqflat build tests: meta/side pages + loader, heap scan + data pages.
-- Blocked 4-bit layout: bits MUST be 4, tq_prod MUST be false (QJL unsupported).
-- fast_rotation in {on (default), off} are both supported.
-- tqflat_test_meta returns 9 ints:
--   {dim, bits, metric, tqProd, nVectors, fastRotation, dimPadded, blockWidth, blockCount}
-- blockWidth is always 32; blockCount = ceil(nVectorsBuiltAtBuildTime / 32).

CREATE TABLE tqbuild (id serial, v vector(8));
INSERT INTO tqbuild (v)
	SELECT ('[' || array_to_string(array(SELECT (i * 7 + g) % 13 - 6 FROM generate_series(1, 8) i), ',') || ']')::vector
	FROM generate_series(1, 500) g;

-- (a) default (no options): bits = 4, tq_prod = off, fast_rotation = on.
-- 500 rows -> blockCount = ceil(500/32) = 16.
CREATE INDEX tqbuild_idxdef ON tqbuild USING tqflat (v vector_l2_ops);
SELECT tqflat_test_meta('tqbuild_idxdef'::regclass);  -- {8,4,0,0,500,1,8,32,16}
SELECT pg_relation_size('tqbuild_idxdef') > 0 AS idxdef_nonempty;

-- (b) explicit bits = 4 (same as default).
CREATE INDEX tqbuild_idx4 ON tqbuild USING tqflat (v vector_l2_ops) WITH (bits = 4);
SELECT tqflat_test_meta('tqbuild_idx4'::regclass);    -- {8,4,0,0,500,1,8,32,16}
SELECT pg_relation_size('tqbuild_idx4') > 0 AS idx4_nonempty;

-- (c) bits = 4, fast_rotation = false (dense rotation path).
-- fastRotation = 0; dimPadded = dim = 8.
CREATE INDEX tqbuild_idxdense ON tqbuild USING tqflat (v vector_l2_ops) WITH (bits = 4, fast_rotation = false);
SELECT tqflat_test_meta('tqbuild_idxdense'::regclass);  -- {8,4,0,0,500,0,8,32,16}
SELECT pg_relation_size('tqbuild_idxdense') > 0 AS idxdense_nonempty;

-- tq_prod = true is rejected by the blocked layout (QJL unsupported).
\set ON_ERROR_STOP 0
CREATE INDEX tqbuild_idxprod ON tqbuild USING tqflat (v vector_l2_ops) WITH (bits = 4, tq_prod = true);
\set ON_ERROR_STOP 1

-- Bad bits must error at reloption validation: only bits = 4 is valid.
\set ON_ERROR_STOP 0
CREATE INDEX tqbuild_idxbad1 ON tqbuild USING tqflat (v vector_l2_ops) WITH (bits = 1);
CREATE INDEX tqbuild_idxbad2 ON tqbuild USING tqflat (v vector_l2_ops) WITH (bits = 2);
CREATE INDEX tqbuild_idxbad3 ON tqbuild USING tqflat (v vector_l2_ops) WITH (bits = 3);
CREATE INDEX tqbuild_idxbad5 ON tqbuild USING tqflat (v vector_l2_ops) WITH (bits = 5);
\set ON_ERROR_STOP 1

-- single-row insert (tqinsert) -- build then insert, nVectors must increase
-- while blockCount stays the same (inserts go to the row-major tail, not new blocks).
-- A single INSERT updates ALL indexes on the table simultaneously, so one batch of
-- 100 rows increments nVectors on every index from 500 to 600.
INSERT INTO tqbuild (v)
	SELECT ('[' || array_to_string(array(SELECT (i * 3 + g) % 7 - 3 FROM generate_series(1, 8) i), ',') || ']')::vector
	FROM generate_series(1, 100) g;

-- nVectors 500 -> 600 (5th element); blockCount stays 16 (9th element).
SELECT tqflat_test_meta('tqbuild_idxdef'::regclass);    -- {8,4,0,0,600,1,8,32,16}
SELECT tqflat_test_meta('tqbuild_idx4'::regclass);      -- {8,4,0,0,600,1,8,32,16}
-- dense path: nVectors 500 -> 600; blockCount stays 16; fastRotation = 0.
SELECT tqflat_test_meta('tqbuild_idxdense'::regclass);  -- {8,4,0,0,600,0,8,32,16}

-- index size must be non-zero after inserts
SELECT pg_relation_size('tqbuild_idx4') > 0 AS idx4_nonempty_after_insert;

DROP TABLE tqbuild;

-- halfvec opclass tests: l2, ip, cosine metrics.
-- dim = 4 (>= 3 required by tq).  Small fixed data for deterministic neighbors.
CREATE TABLE tqflat_hv (v halfvec(4));
INSERT INTO tqflat_hv VALUES ('[1,2,3,4]'), ('[2,3,4,5]'), ('[0,0,0,1]'), ('[5,4,3,2]'), ('[1,1,1,1]');
CREATE INDEX ON tqflat_hv USING tqflat (v halfvec_l2_ops);
CREATE INDEX ON tqflat_hv USING tqflat (v halfvec_ip_ops);
CREATE INDEX ON tqflat_hv USING tqflat (v halfvec_cosine_ops);
SET enable_seqscan = off;
-- L2: nearest to [1,2,3,4] should be itself first
SELECT v FROM tqflat_hv ORDER BY v <-> '[1,2,3,4]' LIMIT 3;
-- IP: nearest by inner product (query [1,2,3,5] separates all candidates: no IP tie)
SELECT v FROM tqflat_hv ORDER BY v <#> '[1,2,3,5]' LIMIT 3;
-- Cosine: nearest to [1,2,3,4] by cosine distance
SELECT v FROM tqflat_hv ORDER BY v <=> '[1,2,3,4]' LIMIT 3;
RESET enable_seqscan;
DROP TABLE tqflat_hv;

-- sparsevec opclass tests: l2, ip, cosine. Emits a densification NOTICE per CREATE INDEX.
-- Data: 5-dim vectors; query '{1:1,3:2}/5' = [1,0,2,0,0] (1-based indices).
-- L2 distances from query: 0, 4, 7, 14, 55 (all distinct).
-- IP scores with query '{1:4,2:3,3:2,4:1}/5': 12, 10, 8, 5, 3 (all distinct).
-- Cosine nearest to '{1:1,3:2}/5': self (dist=0), then '{1:1,2:1,3:1,4:1,5:1}/5' (dist=0.4).
CREATE TABLE tqflat_sv (v sparsevec(5));
INSERT INTO tqflat_sv VALUES
	('{1:1,3:2}/5'),
	('{2:1,5:3}/5'),
	('{1:2,2:1,4:1}/5'),
	('{4:5,5:5}/5'),
	('{1:1,2:1,3:1,4:1,5:1}/5');
CREATE INDEX ON tqflat_sv USING tqflat (v sparsevec_l2_ops);
CREATE INDEX ON tqflat_sv USING tqflat (v sparsevec_ip_ops);
CREATE INDEX ON tqflat_sv USING tqflat (v sparsevec_cosine_ops);
SET enable_seqscan = off;
SELECT v FROM tqflat_sv ORDER BY v <-> '{1:1,3:2}/5' LIMIT 2;
SELECT v FROM tqflat_sv ORDER BY v <#> '{1:4,2:3,3:2,4:1}/5' LIMIT 2;
SELECT v FROM tqflat_sv ORDER BY v <=> '{1:1,3:2}/5' LIMIT 2;
RESET enable_seqscan;
DROP TABLE tqflat_sv;

-- sparsevec declared dim above TQ_MAX_DIM is rejected
\set ON_ERROR_STOP 0
CREATE TABLE tqflat_sv_big (v sparsevec(20000));
INSERT INTO tqflat_sv_big VALUES ('{1:1,20000:2}/20000');
CREATE INDEX ON tqflat_sv_big USING tqflat (v sparsevec_l2_ops);
\set ON_ERROR_STOP 1
DROP TABLE tqflat_sv_big;

-- Unlogged build emits exactly ONE densification NOTICE: the ambuild (MAIN fork)
-- path warns; the ambuildempty (INIT fork, heap == NULL) path is guarded.
CREATE UNLOGGED TABLE tqflat_sv_ul (v sparsevec(5));
INSERT INTO tqflat_sv_ul VALUES ('{1:1,3:2}/5');
CREATE INDEX ON tqflat_sv_ul USING tqflat (v sparsevec_l2_ops);
DROP TABLE tqflat_sv_ul;
