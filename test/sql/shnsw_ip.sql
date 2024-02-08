SET enable_seqscan = off;

CREATE TABLE t (val svector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING shnsw (val svector_ip_ops);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <#> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <#> (SELECT NULL::svector)) t2;

DROP TABLE t;

CREATE TABLE t (val svector(4));
INSERT INTO t (val) VALUES ('[0,0,0,1]'), ('[3,4,0,2]'), ('[0,2,0,1]'), ('[0,4,0,0]');
CREATE INDEX ON t USING shnsw (val svector_ip_ops);
SELECT * FROM t ORDER BY val <#> '[3,3,0,3]';

DROP TABLE t;

CREATE TABLE t (val svector(4));
INSERT INTO t (val) VALUES ('{0,0,0,1}'::float4[]::svector), ('{3,4,0,2}'::float4[]::svector), ('{0,2,0,1}'::float4[]::svector);
CREATE INDEX ON t USING shnsw (val svector_ip_ops);
SELECT * FROM t ORDER BY val <#> '[3,3,0,3]';

DROP TABLE t;
