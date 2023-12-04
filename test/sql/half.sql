SELECT '1.5'::half;
SELECT '65504'::half;
SELECT '65505'::half;
SELECT '-65504'::half;
SELECT '-65505'::half;

SELECT ''::half;
SELECT ' '::half;
SELECT '-'::half;
SELECT ' 1.5'::half;
SELECT '1.5 '::half;
SELECT '1.5a'::half;

SELECT '{1,2,3}'::half[];
SELECT '{1,2,3}'::half[]::real[];

SELECT '65505'::integer::half;
SELECT 'NaN'::real::half;
SELECT 'Infinity'::real::half;

SELECT '1.5'::half::real;
SELECT '1.5'::real::half;

SELECT '1.5'::half::double precision;
SELECT '1.5'::double precision::half;

SELECT '1.5'::half::numeric;
SELECT '1.5'::numeric::half;

SELECT l2_distance('{0,0}'::half[], '{3,4}'::half[]);
SELECT l2_distance('{0,0}'::half[], '{0,1}'::half[]);
SELECT l2_distance('{1,2}'::half[], '{3}'::half[]);
SELECT '{0,0}'::half[] <-> '{3,4}'::half[];

SELECT inner_product('{1,2}'::half[], '{3,4}'::half[]);
SELECT inner_product('{1,2}'::half[], '{3}'::half[]);
SELECT inner_product('{65504}'::half[], '{65504}'::half[]);
SELECT '{1,2}'::half[] <#> '{3,4}'::half[];

SELECT cosine_distance('{1,2}'::half[], '{2,4}'::half[]);
SELECT cosine_distance('{1,2}'::half[], '{0,0}'::half[]);
SELECT cosine_distance('{1,1}'::half[], '{1,1}'::half[]);
SELECT cosine_distance('{1,0}'::half[], '{0,2}'::half[]);
SELECT cosine_distance('{1,1}'::half[], '{-1,-1}'::half[]);
SELECT cosine_distance('{1,2}'::half[], '{3}'::half[]);
SELECT cosine_distance('{1,1}'::half[], '{1.1,1.1}'::half[]);
SELECT cosine_distance('{1,1}'::half[], '{-1.1,-1.1}'::half[]);
SELECT '{1,2}'::half[] <=> '{2,4}'::half[];

SELECT l1_distance('{0,0}'::half[], '{3,4}');
SELECT l1_distance('{0,0}'::half[], '{0,1}');
SELECT l1_distance('{1,2}'::half[], '{3}');
