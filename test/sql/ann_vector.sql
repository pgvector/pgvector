SET enable_seqscan = off;

-- Exact CPU staging: NULLs are retained and predicates run in the executor.
CREATE TABLE t (id integer, tenant text, val vector(3));
INSERT INTO t VALUES (1, 'a', '[0,0,0]'), (2, 'b', '[1,2,3]'),
    (3, 'a', '[1,1,1]'), (4, 'a', NULL);
CREATE INDEX t_ann ON t USING ann (val vector_l2_ops);
INSERT INTO t VALUES (5, 'a', '[1,2,4]');
SELECT id FROM t ORDER BY val <-> '[3,3,3]';
SELECT id FROM t WHERE tenant = 'a' ORDER BY val <-> '[3,3,3]' LIMIT 3;
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <-> (SELECT NULL::vector)) s;
SELECT pg_index_has_property('t_ann', 'index_scan'),
       pg_index_column_has_property('t_ann', 1, 'distance_orderable');

-- Rescans use a new query vector for each outer row.
SELECT t.id, s.id FROM t CROSS JOIN LATERAL
    (SELECT id FROM t t2 ORDER BY val <-> t.val LIMIT 1) s
    WHERE t.val IS NOT NULL ORDER BY t.id;
UPDATE t SET val = '[3,3,3]' WHERE id = 1;
DELETE FROM t WHERE id = 2;
VACUUM t;
SELECT id FROM t ORDER BY val <-> '[3,3,3]';
REINDEX INDEX t_ann;
SELECT id FROM t ORDER BY val <-> '[3,3,3]';
TRUNCATE t;
SELECT id FROM t ORDER BY val <-> '[3,3,3]';
INSERT INTO t VALUES (6, 'a', '[3,3,3]');
SELECT id FROM t ORDER BY val <-> '[3,3,3]';
DROP TABLE t;

CREATE TABLE t (val vector);
CREATE INDEX ON t USING ann (val vector_l2_ops);
DROP TABLE t;
CREATE TABLE t (val vector(2001));
CREATE INDEX ON t USING ann (val vector_l2_ops);
DROP TABLE t;
RESET enable_seqscan;
