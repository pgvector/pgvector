SET halfvec.use_fp16_compute = true;
SELECT l2_distance('[0,0]'::halfvec, '[3,4]');
SELECT l2_distance('[0,0]'::halfvec, '[0,1]');
SELECT l2_distance('[1,2]'::halfvec, '[3]');
SELECT l2_distance('[1,1,1,1,1,1,1,1,1]'::halfvec, '[1,1,1,1,1,1,1,4,5]');
SELECT '[0,0]'::halfvec <-> '[3,4]';

SELECT inner_product('[1,2]'::halfvec, '[3,4]');
SELECT inner_product('[1,2]'::halfvec, '[3]');
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

