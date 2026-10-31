-- Explicit loading also installs hooks when the first query has no vector literal.
LOAD 'vector';
SET ann.enable_filtered_search = on;

CREATE TABLE t (id integer, tenant text, val vector(3));
INSERT INTO t VALUES (1, 'a', '[0,0,0]'), (2, 'b', '[1,2,3]'),
    (3, 'a', '[1,1,1]'), (4, 'a', NULL), (5, 'a', '[1,2,4]');
CREATE INDEX ON t USING ann (val vector_l2_ops);
ANALYZE t;

SELECT id FROM t WHERE tenant = 'a' ORDER BY val <-> '[3,3,3]', id LIMIT 3;
SELECT id FROM t WHERE tenant = 'a' ORDER BY val <-> '[3,3,3]', id LIMIT 2 OFFSET 1;
SELECT id FROM t WHERE tenant = 'missing' ORDER BY val <-> '[3,3,3]', id LIMIT 3;

SET plan_cache_mode = force_generic_plan;
PREPARE filtered_query(vector, bigint, bigint) AS
    SELECT id FROM t WHERE tenant = 'a' ORDER BY val <-> $1, id LIMIT $2 OFFSET $3;
EXECUTE filtered_query('[3,3,3]', 2, 1);
EXECUTE filtered_query(NULL, 3, 0);
EXECUTE filtered_query('[0,0,0]', NULL, 0);
DEALLOCATE filtered_query;
RESET plan_cache_mode;

-- Unsupported shapes retain ordinary PostgreSQL execution.
SELECT id FROM t WHERE tenant = 'a' ORDER BY val <-> '[3,3,3]' NULLS FIRST, id LIMIT 3;
BEGIN;
DECLARE c SCROLL CURSOR FOR SELECT id FROM t WHERE tenant = 'a' ORDER BY val <-> '[3,3,3]', id;
FETCH 3 FROM c;
FETCH BACKWARD 2 FROM c;
CLOSE c;
COMMIT;

DROP TABLE t;
RESET ann.enable_filtered_search;
