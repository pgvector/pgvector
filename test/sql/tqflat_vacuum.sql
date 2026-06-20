-- Test-only C wrappers from vector.so, defined per test file rather than
-- shipped in the extension SQL (they read index internals and are not part
-- of the public surface).
CREATE OR REPLACE FUNCTION tqflat_test_meta(regclass) RETURNS int[]
	AS 'vector' LANGUAGE C STRICT;

-- tqflat vacuum tests: tombstone-based bulk delete.
--
-- Strategy: small, deterministic data where the nearest-neighbor order is
-- unambiguous after reranking.  We delete a known subset of rows, run VACUUM,
-- then confirm:
--   1. Deleted ids no longer appear in any kNN result.
--   2. Remaining rows still return correct results.
--   3. nVectors in the meta page is unchanged (physical count, not live count).
--
-- nVectors semantics: nVectors is the physical count of entries written
-- (live + tombstoned).  DELETE + VACUUM only sets the tombstone flag; it does
-- NOT decrement nVectors.  Space is not reclaimed in v1 (tombstone-only);
-- VACUUM FULL / REINDEX rebuilds from scratch and does reclaim space.

CREATE TABLE tqvac (id int, v vector(8));
INSERT INTO tqvac VALUES
	(1,  '[10,0,0,0,0,0,0,0]'),
	(2,  '[9,1,0,0,0,0,0,0]'),
	(3,  '[0,10,0,0,0,0,0,0]'),
	(4,  '[0,0,10,0,0,0,0,0]'),
	(5,  '[0,0,0,10,0,0,0,0]'),
	(6,  '[-10,0,0,0,0,0,0,0]'),
	(7,  '[5,5,0,0,0,0,0,0]'),
	(8,  '[0,0,0,0,0,0,0,10]'),
	(9,  '[3,0,0,0,0,0,0,0]'),
	(10, '[0,3,0,0,0,0,0,0]');

CREATE INDEX tqvac_idx ON tqvac USING tqflat (v vector_l2_ops) WITH (bits = 4, tq_prod = false);

SET enable_seqscan = off;
SET tqflat.rerank = 100;

-- Baseline: before any deletes, id 1 is the nearest neighbour of [10,0,...].
SELECT id FROM tqvac ORDER BY v <-> '[10,0,0,0,0,0,0,0]'::vector LIMIT 3;

-- nVectors before delete: 10 physical entries.
-- meta = {dim,bits,metric,tqProd,nVectors,fastRotation,dimPadded,blockWidth,blockCount}
-- = {8,4,0,0,10,1,8,32,1} (blockCount = ceil(10/32) = 1).
SELECT tqflat_test_meta('tqvac_idx'::regclass);

-- Delete ids 1, 2, and 6 (the ones nearest to [10,0,...] and [-10,0,...]).
DELETE FROM tqvac WHERE id IN (1, 2, 6);

-- Run VACUUM to invoke tqbulkdelete (tombstones the deleted heap tids).
VACUUM tqvac;

-- After vacuum, query near [10,0,...]: ids 1 and 2 must NOT appear.
-- The next closest remaining vectors are id 9 ([3,0,...]) and id 7 ([5,5,...]).
-- With deterministic well-separated data the unambiguous nearest is id 9.
SELECT id FROM tqvac ORDER BY v <-> '[10,0,0,0,0,0,0,0]'::vector LIMIT 3;

-- Confirm id 1 is completely absent from the top-10 results.
SELECT count(*) AS deleted_id_1_count
	FROM (SELECT id FROM tqvac ORDER BY v <-> '[10,0,0,0,0,0,0,0]'::vector LIMIT 10) s
	WHERE id = 1;

-- Confirm id 2 is completely absent from the top-10 results.
SELECT count(*) AS deleted_id_2_count
	FROM (SELECT id FROM tqvac ORDER BY v <-> '[10,0,0,0,0,0,0,0]'::vector LIMIT 10) s
	WHERE id = 2;

-- nVectors after vacuum: still 10 (physical count unchanged; tombstones remain).
-- {8,4,0,0,10,1,8,32,1}
SELECT tqflat_test_meta('tqvac_idx'::regclass);

-- Remaining rows still return correct results:
-- Nearest to [0,10,0,...] is id 3 (the only big value in dim 2).
SELECT id FROM tqvac ORDER BY v <-> '[0,10,0,0,0,0,0,0]'::vector LIMIT 1;

-- Nearest to [0,0,0,0,0,0,0,10] is id 8.
SELECT id FROM tqvac ORDER BY v <-> '[0,0,0,0,0,0,0,10]'::vector LIMIT 1;

-- Test VACUUM FULL: rebuilds from scratch (ambuild), space is reclaimed.
-- After VACUUM FULL the live count becomes the new nVectors (7 remaining rows).
VACUUM FULL tqvac;

-- After VACUUM FULL, deleted ids are gone from the rebuilt index.
SELECT id FROM tqvac ORDER BY v <-> '[10,0,0,0,0,0,0,0]'::vector LIMIT 3;

-- nVectors after VACUUM FULL: 7 (rebuilt from 7 live rows).
-- {8,4,0,0,7,1,8,32,1} (blockCount = ceil(7/32) = 1).
SELECT tqflat_test_meta('tqvac_idx'::regclass);

-- Delete all remaining rows, vacuum, empty index must return no results.
DELETE FROM tqvac;
VACUUM tqvac;
SELECT id FROM tqvac ORDER BY v <-> '[10,0,0,0,0,0,0,0]'::vector LIMIT 3;

RESET tqflat.rerank;
RESET enable_seqscan;
DROP TABLE tqvac;
