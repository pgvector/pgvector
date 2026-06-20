-- tqivf vacuum tests: tombstone-based bulk delete for the tqivf index AM.
--
-- Strategy: delete a known row, run VACUUM (tombstones the deleted heap tid),
-- confirm the deleted id does not appear in kNN results.  Also test the tail
-- path (post-build inserts) separately.
--
-- Key assertions:
--   1. Block-path delete: id 5 ABSENT from results after VACUUM.
--   2. Tail-path delete: id 999 ABSENT after VACUUM; nearest to [42,0,0,0] is 42.
--   3. VACUUM FULL (ambuild) rebuilds clean; deleted ids still absent.

SET enable_seqscan = off;

-- ---- Block-path delete test ----
CREATE TABLE tqivf_v (id int, v vector(4));
INSERT INTO tqivf_v SELECT g, ARRAY[g,0,0,0]::real[]::vector FROM generate_series(1, 200) g;
CREATE INDEX ON tqivf_v USING tqivf (v vector_l2_ops) WITH (lists = 10);
SET tqivf.probes = 10;

DELETE FROM tqivf_v WHERE id = 5;
VACUUM tqivf_v;

-- id 5 must NOT appear; nearest to [5,0,0,0] are {4,6} (dist 1) then the TIE
-- {3,7} (dist 2).  LIMIT 4 captures both tie pairs so the tie does not
-- straddle the cutoff; re-sort by id.
SELECT id FROM (
  SELECT id FROM tqivf_v ORDER BY v <-> '[5,0,0,0]' LIMIT 4
) q ORDER BY id;

-- Confirm id 5 is absent from the top-10.
SELECT count(*) AS deleted_id_5_count
	FROM (SELECT id FROM tqivf_v ORDER BY v <-> '[5,0,0,0]' LIMIT 10) s
	WHERE id = 5;

VACUUM FULL tqivf_v;

-- After VACUUM FULL (rebuild), id 5 still absent (same tie-safe shape).
SELECT id FROM (
  SELECT id FROM tqivf_v ORDER BY v <-> '[5,0,0,0]' LIMIT 4
) q ORDER BY id;

SELECT count(*) AS deleted_id_5_count_after_full
	FROM (SELECT id FROM tqivf_v ORDER BY v <-> '[5,0,0,0]' LIMIT 10) s
	WHERE id = 5;

DROP TABLE tqivf_v;

-- ---- Tail-path delete test ----
CREATE TABLE tqivf_vt (id int, v vector(4));
INSERT INTO tqivf_vt SELECT g, ARRAY[g,0,0,0]::real[]::vector FROM generate_series(1, 100) g;
CREATE INDEX ON tqivf_vt USING tqivf (v vector_l2_ops) WITH (lists = 5);
SET tqivf.probes = 5;

-- Post-build insert lands in the tail chain.
INSERT INTO tqivf_vt VALUES (999, '[42,0,0,0]');
DELETE FROM tqivf_vt WHERE id = 999;
VACUUM tqivf_vt;

-- Nearest to [42,0,0,0] must be id 42, NOT 999.
SELECT id FROM tqivf_vt ORDER BY v <-> '[42,0,0,0]' LIMIT 1;

-- Confirm id 999 is absent.
SELECT count(*) AS deleted_id_999_count
	FROM (SELECT id FROM tqivf_vt ORDER BY v <-> '[42,0,0,0]' LIMIT 10) s
	WHERE id = 999;

DROP TABLE tqivf_vt;

RESET tqivf.probes;
RESET enable_seqscan;
