-- I/O
SELECT '[1,2,3]'::int8vec;
SELECT '[-1,-2,-3]'::int8vec;
SELECT '[-128,0,127]'::int8vec;
SELECT ' [ 1,  2 ,    3  ] '::int8vec;
SELECT '[hello,1]'::int8vec;
SELECT '[1.5,2]'::int8vec;
SELECT '[129,0]'::int8vec;
SELECT '[-129,0]'::int8vec;
SELECT '[1,2,3'::int8vec;
SELECT '[1,2,3]9'::int8vec;
SELECT '1,2,3'::int8vec;
SELECT ''::int8vec;
SELECT '['::int8vec;
SELECT '[ '::int8vec;
SELECT '[,'::int8vec;
SELECT '[]'::int8vec;
SELECT '[ ]'::int8vec;
SELECT '[,]'::int8vec;
SELECT '[1,]'::int8vec;
SELECT '[1a]'::int8vec;
SELECT '[1,,3]'::int8vec;
SELECT '[1, ,3]'::int8vec;

-- typmod
SELECT '[1,2,3]'::int8vec(3);
SELECT '[1,2,3]'::int8vec(2);
SELECT '[1,2,3]'::int8vec(3, 2);
SELECT '[1,2,3]'::int8vec('a');
SELECT '[1,2,3]'::int8vec(0);
SELECT '[1,2,3]'::int8vec(16001);

-- array
SELECT unnest('{"[1,2,3]", "[4,5,6]"}'::int8vec[]);
SELECT '{"[1,2,3]"}'::int8vec(2)[];

-- arithmetic
SELECT '[1,2,3]'::int8vec + '[4,5,6]';
SELECT '[127]'::int8vec + '[1]';
SELECT '[1,2]'::int8vec + '[3]';

SELECT '[1,2,3]'::int8vec - '[4,5,6]';
SELECT '[-128]'::int8vec - '[1]';
SELECT '[1,2]'::int8vec - '[3]';

SELECT '[1,2,3]'::int8vec * '[4,5,6]';
SELECT '[127]'::int8vec * '[2]';
SELECT '[1,2]'::int8vec * '[3]';

SELECT '[1,2,3]'::int8vec || '[4,5]';

-- comparison
SELECT '[1,2,3]'::int8vec < '[1,2,3]';
SELECT '[1,2,3]'::int8vec < '[1,2]';
SELECT '[1,2,3]'::int8vec <= '[1,2,3]';
SELECT '[1,2,3]'::int8vec <= '[1,2]';
SELECT '[1,2,3]'::int8vec = '[1,2,3]';
SELECT '[1,2,3]'::int8vec = '[1,2]';
SELECT '[1,2,3]'::int8vec != '[1,2,3]';
SELECT '[1,2,3]'::int8vec != '[1,2]';
SELECT '[1,2,3]'::int8vec >= '[1,2,3]';
SELECT '[1,2,3]'::int8vec >= '[1,2]';
SELECT '[1,2,3]'::int8vec > '[1,2,3]';
SELECT '[1,2,3]'::int8vec > '[1,2]';

SELECT int8vec_cmp('[1,2,3]', '[1,2,3]');
SELECT int8vec_cmp('[1,2,3]', '[0,0,0]');
SELECT int8vec_cmp('[0,0,0]', '[1,2,3]');
SELECT int8vec_cmp('[1,2]', '[1,2,3]');
SELECT int8vec_cmp('[1,2,3]', '[1,2]');
SELECT int8vec_cmp('[1,2]', '[2,3,4]');
SELECT int8vec_cmp('[2,3]', '[1,2,3]');

-- dims
SELECT vector_dims('[1,2,3]'::int8vec);

-- norm
SELECT l2_norm('[3,4]'::int8vec);
SELECT l2_norm('[0,1]'::int8vec);
SELECT l2_norm('[0,0]'::int8vec);

-- distance
SELECT l2_distance('[0,0]'::int8vec, '[3,4]');
SELECT l2_distance('[0,0]'::int8vec, '[0,1]');
SELECT l2_distance('[1,2]'::int8vec, '[3]');
SELECT '[0,0]'::int8vec <-> '[3,4]';

SELECT inner_product('[1,2]'::int8vec, '[3,4]');
SELECT inner_product('[1,2]'::int8vec, '[3]');
SELECT inner_product('[127]'::int8vec, '[127]');
SELECT '[1,2]'::int8vec <#> '[3,4]';

SELECT cosine_distance('[1,2]'::int8vec, '[2,4]');
SELECT cosine_distance('[1,2]'::int8vec, '[0,0]');
SELECT cosine_distance('[1,1]'::int8vec, '[1,1]');
SELECT cosine_distance('[1,0]'::int8vec, '[0,2]');
SELECT cosine_distance('[1,1]'::int8vec, '[-1,-1]');
SELECT cosine_distance('[1,2]'::int8vec, '[3]');
SELECT '[1,2]'::int8vec <=> '[2,4]';

SELECT l1_distance('[0,0]'::int8vec, '[3,4]');
SELECT l1_distance('[0,0]'::int8vec, '[0,1]');
SELECT l1_distance('[1,2]'::int8vec, '[3]');
SELECT '[0,0]'::int8vec <+> '[3,4]';

-- normalize
SELECT l2_normalize('[3,4,0]'::int8vec);
SELECT l2_normalize('[0,0,0]'::int8vec);

-- binary quantize
SELECT binary_quantize('[1,0,-1]'::int8vec);
SELECT binary_quantize('[0,1,-2,-3,4,5,6,-7,8,-9,1]'::int8vec);

-- subvector
SELECT subvector('[1,2,3,4,5]'::int8vec, 1, 3);
SELECT subvector('[1,2,3,4,5]'::int8vec, 3, 2);
SELECT subvector('[1,2,3,4,5]'::int8vec, -1, 3);
SELECT subvector('[1,2,3,4,5]'::int8vec, 3, 9);
SELECT subvector('[1,2,3,4,5]'::int8vec, 1, 0);
SELECT subvector('[1,2,3,4,5]'::int8vec, 3, -1);
SELECT subvector('[1,2,3,4,5]'::int8vec, -1, 2);
SELECT subvector('[1,2,3,4,5]'::int8vec, 2147483647, 10);
SELECT subvector('[1,2,3,4,5]'::int8vec, 3, 2147483647);
SELECT subvector('[1,2,3,4,5]'::int8vec, -2147483644, 2147483647);

-- aggregates
SELECT avg(v) FROM unnest(ARRAY['[1,2,3]'::int8vec, '[3,6,9]']) v;
SELECT avg(v) FROM unnest(ARRAY['[1,2,3]'::int8vec, '[3,6,9]', NULL]) v;
SELECT avg(v) FROM unnest(ARRAY[]::int8vec[]) v;
SELECT avg(v) FROM unnest(ARRAY['[1,2]'::int8vec, '[3]']) v;

SELECT sum(v) FROM unnest(ARRAY['[1,2,3]'::int8vec, '[3,5,7]']) v;
SELECT sum(v) FROM unnest(ARRAY['[1,2,3]'::int8vec, '[3,5,7]', NULL]) v;
SELECT sum(v) FROM unnest(ARRAY[]::int8vec[]) v;
SELECT sum(v) FROM unnest(ARRAY['[1,2]'::int8vec, '[3]']) v;
SELECT sum(v) FROM unnest(ARRAY['[100,0]'::int8vec, '[100,0]']) v;
