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
SELECT '[1,2]'::halfvec <=> '[2,4]';

SELECT l1_distance('[0,0]'::halfvec, '[3,4]');
SELECT l1_distance('[0,0]'::halfvec, '[0,1]');
SELECT l1_distance('[1,2]'::halfvec, '[3]');

SELECT quantize_binary('[1,0,-1]'::halfvec);
SELECT quantize_binary('[0,0.1,-0.2,-0.3,0.4,0.5,0.6,-0.7,0.8,-0.9,1]'::halfvec);

SELECT subvector('[1,2,3,4,5]'::halfvec, 1, 3);
SELECT subvector('[1,2,3,4,5]'::halfvec, 3, 2);
SELECT subvector('[1,2,3,4,5]'::halfvec, -1, 3);
SELECT subvector('[1,2,3,4,5]'::halfvec, 3, 9);
SELECT subvector('[1,2,3,4,5]'::halfvec, 1, 0);
SELECT subvector('[1,2,3,4,5]'::halfvec, 3, -1);
SELECT subvector('[1,2,3,4,5]'::halfvec, -1, 2);

SELECT round(halfvec_norm('[1,1]')::numeric, 5);
SELECT halfvec_norm('[3,4]');
SELECT halfvec_norm('[0,1]');
