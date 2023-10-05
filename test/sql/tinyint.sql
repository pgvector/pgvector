SELECT '127'::tinyint;
SELECT '128'::tinyint;
SELECT '-128'::tinyint;
SELECT '-129'::tinyint;

SELECT ''::tinyint;
SELECT ' 1'::tinyint;
SELECT '1 '::tinyint;
SELECT '1a'::tinyint;

SELECT '{1,2,3}'::tinyint[];

SELECT '128'::numeric::tinyint;
SELECT 'NaN'::numeric::tinyint;

SELECT l2_distance('{0,0}'::tinyint[], '{3,4}'::tinyint[]);
SELECT l2_distance('{0,0}'::tinyint[], '{0,1}'::tinyint[]);
SELECT l2_distance('{1,2}'::tinyint[], '{3}'::tinyint[]);
SELECT l2_distance('{3e38}'::tinyint[], '{-3e38}'::tinyint[]);
SELECT '{0,0}'::tinyint[] <-> '{3,4}'::tinyint[];

SELECT inner_product('{1,2}'::tinyint[], '{3,4}'::tinyint[]);
SELECT inner_product('{1,2}'::tinyint[], '{3}'::tinyint[]);
SELECT inner_product('{127}'::tinyint[], '{127}'::tinyint[]);
SELECT '{1,2}'::tinyint[] <#> '{3,4}'::tinyint[];

SELECT cosine_distance('{1,2}'::tinyint[], '{2,4}'::tinyint[]);
SELECT cosine_distance('{1,2}'::tinyint[], '{0,0}'::tinyint[]);
SELECT cosine_distance('{1,1}'::tinyint[], '{1,1}'::tinyint[]);
SELECT cosine_distance('{1,0}'::tinyint[], '{0,2}'::tinyint[]);
SELECT cosine_distance('{1,1}'::tinyint[], '{-1,-1}'::tinyint[]);
SELECT cosine_distance('{1,2}'::tinyint[], '{3}'::tinyint[]);
SELECT cosine_distance('{3e38}'::tinyint[], '{3e38}'::tinyint[]);
SELECT '{1,2}'::tinyint[] <=> '{2,4}'::tinyint[];
