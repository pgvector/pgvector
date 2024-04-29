SELECT '[1,2,3]'::vector;
SELECT '[-1,-2,-3]'::vector;
SELECT '[1.,2.,3.]'::vector;
SELECT ' [ 1,  2 ,    3  ] '::vector;
SELECT '[1.23456]'::vector;
SELECT '[hello,1]'::vector;
SELECT '[NaN,1]'::vector;
SELECT '[Infinity,1]'::vector;
SELECT '[-Infinity,1]'::vector;
SELECT '[1.5e38,-1.5e38]'::vector;
SELECT '[1.5e+38,-1.5e+38]'::vector;
SELECT '[1.5e-38,-1.5e-38]'::vector;
SELECT '[4e38,1]'::vector;
SELECT '[-4e38,1]'::vector;
SELECT '[1e-46,1]'::vector;
SELECT '[-1e-46,1]'::vector;
SELECT '[1,2,3'::vector;
SELECT '[1,2,3]9'::vector;
SELECT '1,2,3'::vector;
SELECT ''::vector;
SELECT '['::vector;
SELECT '[ '::vector;
SELECT '[,'::vector;
SELECT '[]'::vector;
SELECT '[ ]'::vector;
SELECT '[,]'::vector;
SELECT '[1,]'::vector;
SELECT '[1a]'::vector;
SELECT '[1,,3]'::vector;
SELECT '[1, ,3]'::vector;

SELECT '[1,2,3]'::vector(3);
SELECT '[1,2,3]'::vector(2);
SELECT '[1,2,3]'::vector(3, 2);
SELECT '[1,2,3]'::vector('a');
SELECT '[1,2,3]'::vector(0);
SELECT '[1,2,3]'::vector(16001);

SELECT unnest('{"[1,2,3]", "[4,5,6]"}'::vector[]);
SELECT '{"[1,2,3]"}'::vector(2)[];


SELECT '[1,2,3]'::vector + '[4,5,6]';
SELECT '[3e38]'::vector + '[3e38]';
SELECT '[1,2]'::vector + '[3]';

SELECT '[1,2,3]'::vector - '[4,5,6]';
SELECT '[-3e38]'::vector - '[3e38]';
SELECT '[1,2]'::vector - '[3]';

SELECT '[1,2,3]'::vector * '[4,5,6]';
SELECT '[1e37]'::vector * '[1e37]';
SELECT '[1e-37]'::vector * '[1e-37]';
SELECT '[1,2]'::vector * '[3]';

SELECT '[1,2,3]'::vector || '[4,5]';
SELECT array_fill(0, ARRAY[16000])::vector || '[1]';

SELECT '[1,2,3]'::vector < '[1,2,3]';
SELECT '[1,2,3]'::vector < '[1,2]';
SELECT '[1,2,3]'::vector <= '[1,2,3]';
SELECT '[1,2,3]'::vector <= '[1,2]';
SELECT '[1,2,3]'::vector = '[1,2,3]';
SELECT '[1,2,3]'::vector = '[1,2]';
SELECT '[1,2,3]'::vector != '[1,2,3]';
SELECT '[1,2,3]'::vector != '[1,2]';
SELECT '[1,2,3]'::vector >= '[1,2,3]';
SELECT '[1,2,3]'::vector >= '[1,2]';
SELECT '[1,2,3]'::vector > '[1,2,3]';
SELECT '[1,2,3]'::vector > '[1,2]';

SELECT vector_cmp('[1,2,3]', '[1,2,3]');
SELECT vector_cmp('[1,2,3]', '[0,0,0]');
SELECT vector_cmp('[0,0,0]', '[1,2,3]');
SELECT vector_cmp('[1,2]', '[1,2,3]');
SELECT vector_cmp('[1,2,3]', '[1,2]');
SELECT vector_cmp('[1,2]', '[2,3,4]');
SELECT vector_cmp('[2,3]', '[1,2,3]');

SELECT vector_dims('[1,2,3]'::vector);

SELECT round(vector_norm('[1,1]')::numeric, 5);
SELECT vector_norm('[3,4]');
SELECT vector_norm('[0,1]');
SELECT vector_norm('[3e37,4e37]')::real;
SELECT vector_norm('[0,0]');
SELECT vector_norm('[2]');

SELECT l2_distance('[0,0]'::vector, '[3,4]');
SELECT l2_distance('[0,0]'::vector, '[0,1]');
SELECT l2_distance('[1,2]'::vector, '[3]');
SELECT l2_distance('[3e38]'::vector, '[-3e38]');
SELECT l2_distance('[1,1,1,1,1,1,1,1,1]'::vector, '[1,1,1,1,1,1,1,4,5]');
SELECT '[0,0]'::vector <-> '[3,4]';

SELECT inner_product('[1,2]'::vector, '[3,4]');
SELECT inner_product('[1,2]'::vector, '[3]');
SELECT inner_product('[3e38]'::vector, '[3e38]');
SELECT inner_product('[1,1,1,1,1,1,1,1,1]'::vector, '[1,2,3,4,5,6,7,8,9]');
SELECT '[1,2]'::vector <#> '[3,4]';

SELECT cosine_distance('[1,2]'::vector, '[2,4]');
SELECT cosine_distance('[1,2]'::vector, '[0,0]');
SELECT cosine_distance('[1,1]'::vector, '[1,1]');
SELECT cosine_distance('[1,0]'::vector, '[0,2]');
SELECT cosine_distance('[1,1]'::vector, '[-1,-1]');
SELECT cosine_distance('[1,2]'::vector, '[3]');
SELECT cosine_distance('[1,1]'::vector, '[1.1,1.1]');
SELECT cosine_distance('[1,1]'::vector, '[-1.1,-1.1]');
SELECT cosine_distance('[3e38]'::vector, '[3e38]');
SELECT cosine_distance('[1,2,3,4,5,6,7,8,9]'::vector, '[1,2,3,4,5,6,7,8,9]');
SELECT cosine_distance('[1,2,3,4,5,6,7,8,9]'::vector, '[-1,-2,-3,-4,-5,-6,-7,-8,-9]');
SELECT '[1,2]'::vector <=> '[2,4]';

SELECT l1_distance('[0,0]'::vector, '[3,4]');
SELECT l1_distance('[0,0]'::vector, '[0,1]');
SELECT l1_distance('[1,2]'::vector, '[3]');
SELECT l1_distance('[3e38]'::vector, '[-3e38]');
SELECT l1_distance('[1,2,3,4,5,6,7,8,9]'::vector, '[1,2,3,4,5,6,7,8,9]');
SELECT l1_distance('[1,2,3,4,5,6,7,8,9]'::vector, '[0,3,2,5,4,7,6,9,8]');
SELECT '[0,0]'::vector <+> '[3,4]';

SELECT l2_normalize('[3,4]'::vector);
SELECT l2_normalize('[3,0]'::vector);
SELECT l2_normalize('[0,0.1]'::vector);
SELECT l2_normalize('[0,0]'::vector);
SELECT l2_normalize('[3e38]'::vector);

SELECT binary_quantize('[1,0,-1]'::vector);
SELECT binary_quantize('[0,0.1,-0.2,-0.3,0.4,0.5,0.6,-0.7,0.8,-0.9,1]'::vector);

SELECT subvector('[1,2,3,4,5]'::vector, 1, 3);
SELECT subvector('[1,2,3,4,5]'::vector, 3, 2);
SELECT subvector('[1,2,3,4,5]'::vector, -1, 3);
SELECT subvector('[1,2,3,4,5]'::vector, 3, 9);
SELECT subvector('[1,2,3,4,5]'::vector, 1, 0);
SELECT subvector('[1,2,3,4,5]'::vector, 3, -1);
SELECT subvector('[1,2,3,4,5]'::vector, -1, 2);
SELECT subvector('[1,2,3,4,5]'::vector, 2147483647, 10);
SELECT subvector('[1,2,3,4,5]'::vector, 3, 2147483647);
SELECT subvector('[1,2,3,4,5]'::vector, -2147483644, 2147483647);

SELECT avg(v) FROM unnest(ARRAY['[1,2,3]'::vector, '[3,5,7]']) v;
SELECT avg(v) FROM unnest(ARRAY['[1,2,3]'::vector, '[3,5,7]', NULL]) v;
SELECT avg(v) FROM unnest(ARRAY[]::vector[]) v;
SELECT avg(v) FROM unnest(ARRAY['[1,2]'::vector, '[3]']) v;
SELECT avg(v) FROM unnest(ARRAY['[3e38]'::vector, '[3e38]']) v;
SELECT vector_avg(array_agg(n)) FROM generate_series(1, 16002) n;

SELECT sum(v) FROM unnest(ARRAY['[1,2,3]'::vector, '[3,5,7]']) v;
SELECT sum(v) FROM unnest(ARRAY['[1,2,3]'::vector, '[3,5,7]', NULL]) v;
SELECT sum(v) FROM unnest(ARRAY[]::vector[]) v;
SELECT sum(v) FROM unnest(ARRAY['[1,2]'::vector, '[3]']) v;
SELECT sum(v) FROM unnest(ARRAY['[3e38]'::vector, '[3e38]']) v;
