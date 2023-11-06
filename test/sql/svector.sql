SELECT '(0,1.5),(2,3.5)|5|'::svector;
SELECT '(0,1.5),(2,3.5)|5|'::svector::vector;
SELECT '(0,1.5),(2,3.5)|5|'::svector::vector(5);
SELECT '(0,1.5),(2,3.5)|5|'::svector::vector(4);
SELECT '[0,1.5,0,3.5,0]'::vector::svector;

SELECT '(0,0),(1,1),(2,0)|3|'::svector;

SELECT '|5|'::svector;
SELECT '|-1|'::svector;
SELECT '|100001|'::svector;
SELECT '|16001|'::svector::vector;

SELECT '(-1,1)|1|'::svector;
SELECT '(1,1)|1|'::svector;

SELECT '|1|'::svector(2);

SELECT l2_distance('|2|'::svector, '(0,3),(1,4)|2|');
SELECT l2_distance('|2|'::svector, '(1,1)|2|');
SELECT '|2|'::svector <-> '(0,3),(1,4)|2|';

SELECT inner_product('(0,1),(1,2)|2|'::svector, '(0,2),(1,4)|2|');
SELECT svector_negative_inner_product('(0,1),(1,2)|2|', '(0,2),(1,4)|2|');

SELECT cosine_distance('(0,1),(1,2)|2|'::svector, '(0,2),(1,4)|2|');
SELECT cosine_distance('(0,1),(1,2)|2|'::svector, '|2|');
SELECT cosine_distance('(0,1),(1,1)|2|'::svector, '(0,-1),(1,-1)|2|');
SELECT cosine_distance('(0,1)|2|'::svector, '(1,2)|2|');
SELECT cosine_distance('|1|'::svector, '|1|');
SELECT cosine_distance('(0,1)|2|'::svector, '(0,1)|3|');

SELECT jaccard_distance('(0,1)|2|', '(0,1)|2|');
SELECT jaccard_distance('(0,1)|2|', '(1,1)|2|');
SELECT jaccard_distance('|1|', '|1|');
SELECT jaccard_distance('(0,1)|2|', '(0,1)|3|');
