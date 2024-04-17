SELECT t, 
Array[t,t+1,t+2]::vector(3) AS val 
INTO test_where 
FROM generate_series(1, 2000) as t;
SELECT t FROM test_where WHERE t>1500 ORDER BY val <-> '[1000.2,1000,1000.1]' LIMIT 2;
CREATE INDEX ON test_where USING hnsw (val vector_l2_ops) WITH (m=10);
set hnsw.use_relaxed=off;
set hnsw.ef_search=100;
SELECT t FROM test_where WHERE t>1500 ORDER BY val <-> '[1000.2,1000,1000.1]' LIMIT 2;
set hnsw.use_relaxed=on;
SELECT t FROM test_where WHERE t>1500 ORDER BY val <-> '[1000.2,1000,1000.1]' LIMIT 2;
SELECT t FROM test_where WHERE t>2000 ORDER BY val <-> '[1000.2,1000,1000.1]' LIMIT 2;
DELETE FROM test_where;
SELECT t FROM test_where WHERE t>1500 ORDER BY val <-> '[1000.2,1000,1000.1]' LIMIT 2;
TRUNCATE test_where;
SELECT t FROM test_where WHERE t>1500 ORDER BY val <-> '[1000.2,1000,1000.1]' LIMIT 2;
set hnsw.use_relaxed=off;
