SELECT '[1,2,3]'::halfvec;
SELECT '[-1,-2,-3]'::halfvec;
SELECT '[1.,2.,3.]'::halfvec;
SELECT ' [ 1,  2 ,    3  ] '::halfvec;
SELECT '[1.23456]'::halfvec;
SELECT '[hello,1]'::halfvec;
SELECT '[NaN,1]'::halfvec;
SELECT '[Infinity,1]'::halfvec;
SELECT '[-Infinity,1]'::halfvec;
SELECT '[65519,-65519]'::halfvec;
SELECT '[65520,-65520]'::halfvec;
SELECT '[1e-8,-1e-8]'::halfvec;
SELECT '[4e38,1]'::halfvec;
SELECT '[1e-46,1]'::halfvec;
SELECT '[1,2,3'::halfvec;
SELECT '[1,2,3]9'::halfvec;
SELECT '1,2,3'::halfvec;
SELECT ''::halfvec;
SELECT '['::halfvec;
SELECT '[ '::halfvec;
SELECT '[,'::halfvec;
SELECT '[]'::halfvec;
SELECT '[ ]'::halfvec;
SELECT '[,]'::halfvec;
SELECT '[1,]'::halfvec;
SELECT '[1a]'::halfvec;
SELECT '[1,,3]'::halfvec;
SELECT '[1, ,3]'::halfvec;

SELECT '[1,2,3]'::halfvec(3);
SELECT '[1,2,3]'::halfvec(2);
SELECT '[1,2,3]'::halfvec(3, 2);
SELECT '[1,2,3]'::halfvec('a');
SELECT '[1,2,3]'::halfvec(0);
SELECT '[1,2,3]'::halfvec(16001);

SELECT unnest('{"[1,2,3]", "[4,5,6]"}'::halfvec[]);
SELECT '{"[1,2,3]"}'::halfvec(2)[];

SELECT '[1,2,3]'::halfvec + '[4,5,6]';
SELECT '[65519]'::halfvec + '[65519]';
SELECT '[1,2]'::halfvec + '[3]';

SELECT '[1,2,3]'::halfvec - '[4,5,6]';
SELECT '[-65519]'::halfvec - '[65519]';
SELECT '[1,2]'::halfvec - '[3]';

SELECT '[1,2,3]'::halfvec * '[4,5,6]';
SELECT '[65519]'::halfvec * '[65519]';
SELECT '[1e-7]'::halfvec * '[1e-7]';
SELECT '[1,2]'::halfvec * '[3]';

SELECT '[1,2,3]'::halfvec || '[4,5]';
SELECT array_fill(0, ARRAY[16000])::halfvec || '[1]';

SELECT '[1,2,3]'::halfvec < '[1,2,3]';
SELECT '[1,2,3]'::halfvec < '[1,2]';
SELECT '[1,2,3]'::halfvec <= '[1,2,3]';
SELECT '[1,2,3]'::halfvec <= '[1,2]';
SELECT '[1,2,3]'::halfvec = '[1,2,3]';
SELECT '[1,2,3]'::halfvec = '[1,2]';
SELECT '[1,2,3]'::halfvec != '[1,2,3]';
SELECT '[1,2,3]'::halfvec != '[1,2]';
SELECT '[1,2,3]'::halfvec >= '[1,2,3]';
SELECT '[1,2,3]'::halfvec >= '[1,2]';
SELECT '[1,2,3]'::halfvec > '[1,2,3]';
SELECT '[1,2,3]'::halfvec > '[1,2]';

SELECT halfvec_cmp('[1,2,3]', '[1,2,3]');
SELECT halfvec_cmp('[1,2,3]', '[0,0,0]');
SELECT halfvec_cmp('[0,0,0]', '[1,2,3]');
SELECT halfvec_cmp('[1,2]', '[1,2,3]');
SELECT halfvec_cmp('[1,2,3]', '[1,2]');
SELECT halfvec_cmp('[1,2]', '[2,3,4]');
SELECT halfvec_cmp('[2,3]', '[1,2,3]');

SELECT vector_dims('[1,2,3]'::halfvec);

SELECT round(l2_norm('[1,1]'::halfvec)::numeric, 5);
SELECT l2_norm('[3,4]'::halfvec);
SELECT l2_norm('[0,1]'::halfvec);
SELECT l2_norm('[0,0]'::halfvec);
SELECT l2_norm('[2]'::halfvec);

SELECT l2_distance('[0,0]'::halfvec, '[3,4]');
SELECT l2_distance('[0,0]'::halfvec, '[0,1]');
SELECT l2_distance('[1,2]'::halfvec, '[3]');
SELECT l2_distance('[1,1,1,1,1,1,1,1,1]'::halfvec, '[1,1,1,1,1,1,1,4,5]');
SELECT '[0,0]'::halfvec <-> '[3,4]';

SELECT inner_product('[1,2]'::halfvec, '[3,4]');
SELECT inner_product('[1,2]'::halfvec, '[3]');
SELECT inner_product('[65504]'::halfvec, '[65504]');
SELECT inner_product('[1,1,1,1,1,1,1,1,1]'::halfvec, '[1,2,3,4,5,6,7,8,9]');
SELECT '[1,2]'::halfvec <#> '[3,4]';

SELECT cosine_distance('[1,2]'::halfvec, '[2,4]');
SELECT cosine_distance('[1,2]'::halfvec, '[0,0]');
SELECT cosine_distance('[1,1]'::halfvec, '[1,1]');
SELECT cosine_distance('[1,0]'::halfvec, '[0,2]');
SELECT cosine_distance('[1,1]'::halfvec, '[-1,-1]');
SELECT cosine_distance('[1,2]'::halfvec, '[3]');
SELECT cosine_distance('[1,1]'::halfvec, '[1.1,1.1]');
SELECT cosine_distance('[1,1]'::halfvec, '[-1.1,-1.1]');
SELECT cosine_distance('[1,2,3,4,5,6,7,8,9]'::halfvec, '[1,2,3,4,5,6,7,8,9]');
SELECT cosine_distance('[1,2,3,4,5,6,7,8,9]'::halfvec, '[-1,-2,-3,-4,-5,-6,-7,-8,-9]');
SELECT '[1,2]'::halfvec <=> '[2,4]';

SELECT l1_distance('[0,0]'::halfvec, '[3,4]');
SELECT l1_distance('[0,0]'::halfvec, '[0,1]');
SELECT l1_distance('[1,2]'::halfvec, '[3]');
SELECT l1_distance('[1,2,3,4,5,6,7,8,9]'::halfvec, '[1,2,3,4,5,6,7,8,9]');
SELECT l1_distance('[1,2,3,4,5,6,7,8,9]'::halfvec, '[0,3,2,5,4,7,6,9,8]');
SELECT '[0,0]'::halfvec <+> '[3,4]';

SELECT l2_normalize('[3,4]'::halfvec);
SELECT l2_normalize('[3,0]'::halfvec);
SELECT l2_normalize('[0,0.1]'::halfvec);
SELECT l2_normalize('[0,0]'::halfvec);
SELECT l2_normalize('[65504]'::halfvec);

SELECT binary_quantize('[1,0,-1]'::halfvec);
SELECT binary_quantize('[0,0.1,-0.2,-0.3,0.4,0.5,0.6,-0.7,0.8,-0.9,1]'::halfvec);

SELECT subvector('[1,2,3,4,5]'::halfvec, 1, 3);
SELECT subvector('[1,2,3,4,5]'::halfvec, 3, 2);
SELECT subvector('[1,2,3,4,5]'::halfvec, -1, 3);
SELECT subvector('[1,2,3,4,5]'::halfvec, 3, 9);
SELECT subvector('[1,2,3,4,5]'::halfvec, 1, 0);
SELECT subvector('[1,2,3,4,5]'::halfvec, 3, -1);
SELECT subvector('[1,2,3,4,5]'::halfvec, -1, 2);
SELECT subvector('[1,2,3,4,5]'::halfvec, 2147483647, 10);
SELECT subvector('[1,2,3,4,5]'::halfvec, 3, 2147483647);
SELECT subvector('[1,2,3,4,5]'::halfvec, -2147483644, 2147483647);

SELECT avg(v) FROM unnest(ARRAY['[1,2,3]'::halfvec, '[3,5,7]']) v;
SELECT avg(v) FROM unnest(ARRAY['[1,2,3]'::halfvec, '[3,5,7]', NULL]) v;
SELECT avg(v) FROM unnest(ARRAY[]::halfvec[]) v;
SELECT avg(v) FROM unnest(ARRAY['[1,2]'::halfvec, '[3]']) v;
SELECT avg(v) FROM unnest(ARRAY['[65504]'::halfvec, '[65504]']) v;
SELECT halfvec_avg(array_agg(n)) FROM generate_series(1, 16002) n;

SELECT sum(v) FROM unnest(ARRAY['[1,2,3]'::halfvec, '[3,5,7]']) v;
SELECT sum(v) FROM unnest(ARRAY['[1,2,3]'::halfvec, '[3,5,7]', NULL]) v;
SELECT sum(v) FROM unnest(ARRAY[]::halfvec[]) v;
SELECT sum(v) FROM unnest(ARRAY['[1,2]'::halfvec, '[3]']) v;
SELECT sum(v) FROM unnest(ARRAY['[65504]'::halfvec, '[65504]']) v;
