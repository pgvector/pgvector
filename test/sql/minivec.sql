SELECT '[1,2,3]'::minivec;
SELECT '[-1,-2,-3]'::minivec;
SELECT '[1.,2.,3.]'::minivec;
SELECT ' [ 1,  2 ,    3  ] '::minivec;
SELECT '[1.23456]'::minivec;
SELECT '[hello,1]'::minivec;
SELECT '[NaN,1]'::minivec;
SELECT '[Infinity,1]'::minivec;
SELECT '[-Infinity,1]'::minivec;
SELECT '[65519,-65519]'::minivec;
SELECT '[65520,-65520]'::minivec;
SELECT '[1e-8,-1e-8]'::minivec;
SELECT '[4e38,1]'::minivec;
SELECT '[1e-46,1]'::minivec;
SELECT '[1,2,3'::minivec;
SELECT '[1,2,3]9'::minivec;
SELECT '1,2,3'::minivec;
SELECT ''::minivec;
SELECT '['::minivec;
SELECT '[ '::minivec;
SELECT '[,'::minivec;
SELECT '[]'::minivec;
SELECT '[ ]'::minivec;
SELECT '[,]'::minivec;
SELECT '[1,]'::minivec;
SELECT '[1a]'::minivec;
SELECT '[1,,3]'::minivec;
SELECT '[1, ,3]'::minivec;

SELECT '[1,2,3]'::minivec(3);
SELECT '[1,2,3]'::minivec(2);
SELECT '[1,2,3]'::minivec(3, 2);
SELECT '[1,2,3]'::minivec('a');
SELECT '[1,2,3]'::minivec(0);
SELECT '[1,2,3]'::minivec(16001);

SELECT unnest('{"[1,2,3]", "[4,5,6]"}'::minivec[]);
SELECT '{"[1,2,3]"}'::minivec(2)[];

SELECT '[1,2,3]'::minivec + '[4,5,6]';
SELECT '[448]'::minivec + '[448]';
SELECT '[1,2]'::minivec + '[3]';

SELECT '[1,2,3]'::minivec - '[4,5,6]';
SELECT '[-448]'::minivec - '[448]';
SELECT '[1,2]'::minivec - '[3]';

SELECT '[1,2,3]'::minivec * '[4,5,6]';
SELECT '[448]'::minivec * '[448]';
SELECT '[1e-7]'::minivec * '[1e-7]';
SELECT '[1,2]'::minivec * '[3]';

SELECT '[1,2,3]'::minivec || '[4,5]';
SELECT array_fill(0, ARRAY[16000])::minivec || '[1]';

SELECT '[1,2,3]'::minivec < '[1,2,3]';
SELECT '[1,2,3]'::minivec < '[1,2]';
SELECT '[1,2,3]'::minivec <= '[1,2,3]';
SELECT '[1,2,3]'::minivec <= '[1,2]';
SELECT '[1,2,3]'::minivec = '[1,2,3]';
SELECT '[1,2,3]'::minivec = '[1,2]';
SELECT '[1,2,3]'::minivec != '[1,2,3]';
SELECT '[1,2,3]'::minivec != '[1,2]';
SELECT '[1,2,3]'::minivec >= '[1,2,3]';
SELECT '[1,2,3]'::minivec >= '[1,2]';
SELECT '[1,2,3]'::minivec > '[1,2,3]';
SELECT '[1,2,3]'::minivec > '[1,2]';

SELECT minivec_cmp('[1,2,3]', '[1,2,3]');
SELECT minivec_cmp('[1,2,3]', '[0,0,0]');
SELECT minivec_cmp('[0,0,0]', '[1,2,3]');
SELECT minivec_cmp('[1,2]', '[1,2,3]');
SELECT minivec_cmp('[1,2,3]', '[1,2]');
SELECT minivec_cmp('[1,2]', '[2,3,4]');
SELECT minivec_cmp('[2,3]', '[1,2,3]');

SELECT vector_dims('[1,2,3]'::minivec);

SELECT round(l2_norm('[1,1]'::minivec)::numeric, 5);
SELECT l2_norm('[3,4]'::minivec);
SELECT l2_norm('[0,1]'::minivec);
SELECT l2_norm('[0,0]'::minivec);
SELECT l2_norm('[2]'::minivec);

SELECT l2_distance('[0,0]'::minivec, '[3,4]');
SELECT l2_distance('[0,0]'::minivec, '[0,1]');
SELECT l2_distance('[1,2]'::minivec, '[3]');
SELECT l2_distance('[1,1,1,1,1,1,1,1,1]'::minivec, '[1,1,1,1,1,1,1,4,5]');
SELECT '[0,0]'::minivec <-> '[3,4]';

SELECT inner_product('[1,2]'::minivec, '[3,4]');
SELECT inner_product('[1,2]'::minivec, '[3]');
SELECT inner_product('[448]'::minivec, '[448]');
SELECT inner_product('[1,1,1,1,1,1,1,1,1]'::minivec, '[1,2,3,4,5,6,7,8,9]');
SELECT '[1,2]'::minivec <#> '[3,4]';

SELECT cosine_distance('[1,2]'::minivec, '[2,4]');
SELECT cosine_distance('[1,2]'::minivec, '[0,0]');
SELECT cosine_distance('[1,1]'::minivec, '[1,1]');
SELECT cosine_distance('[1,0]'::minivec, '[0,2]');
SELECT cosine_distance('[1,1]'::minivec, '[-1,-1]');
SELECT cosine_distance('[1,2]'::minivec, '[3]');
SELECT cosine_distance('[1,1]'::minivec, '[1.1,1.1]');
SELECT cosine_distance('[1,1]'::minivec, '[-1.1,-1.1]');
SELECT cosine_distance('[1,2,3,4,5,6,7,8,9]'::minivec, '[1,2,3,4,5,6,7,8,9]');
SELECT cosine_distance('[1,2,3,4,5,6,7,8,9]'::minivec, '[-1,-2,-3,-4,-5,-6,-7,-8,-9]');
SELECT '[1,2]'::minivec <=> '[2,4]';

SELECT l1_distance('[0,0]'::minivec, '[3,4]');
SELECT l1_distance('[0,0]'::minivec, '[0,1]');
SELECT l1_distance('[1,2]'::minivec, '[3]');
SELECT l1_distance('[1,2,3,4,5,6,7,8,9]'::minivec, '[1,2,3,4,5,6,7,8,9]');
SELECT l1_distance('[1,2,3,4,5,6,7,8,9]'::minivec, '[0,3,2,5,4,7,6,9,8]');
SELECT '[0,0]'::minivec <+> '[3,4]';

SELECT l2_normalize('[3,4]'::minivec);
SELECT l2_normalize('[3,0]'::minivec);
SELECT l2_normalize('[0,0.1]'::minivec);
SELECT l2_normalize('[0,0]'::minivec);
SELECT l2_normalize('[448]'::minivec);

SELECT binary_quantize('[1,0,-1]'::minivec);
SELECT binary_quantize('[0,0.1,-0.2,-0.3,0.4,0.5,0.6,-0.7,0.8,-0.9,1]'::minivec);

SELECT subvector('[1,2,3,4,5]'::minivec, 1, 3);
SELECT subvector('[1,2,3,4,5]'::minivec, 3, 2);
SELECT subvector('[1,2,3,4,5]'::minivec, -1, 3);
SELECT subvector('[1,2,3,4,5]'::minivec, 3, 9);
SELECT subvector('[1,2,3,4,5]'::minivec, 1, 0);
SELECT subvector('[1,2,3,4,5]'::minivec, 3, -1);
SELECT subvector('[1,2,3,4,5]'::minivec, -1, 2);
SELECT subvector('[1,2,3,4,5]'::minivec, 2147483647, 10);
SELECT subvector('[1,2,3,4,5]'::minivec, 3, 2147483647);
SELECT subvector('[1,2,3,4,5]'::minivec, -2147483644, 2147483647);
