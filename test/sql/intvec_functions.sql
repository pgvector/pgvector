SELECT l2_distance('[0,0]'::intvec, '[3,4]');
SELECT l2_distance('[0,0]'::intvec, '[0,1]');
SELECT l2_distance('[1,2]'::intvec, '[3]');
SELECT '[0,0]'::intvec <-> '[3,4]';

SELECT inner_product('[1,2]'::intvec, '[3,4]');
SELECT inner_product('[1,2]'::intvec, '[3]');
SELECT inner_product('[127]'::intvec, '[127]');
SELECT '[1,2]'::intvec <#> '[3,4]';

SELECT cosine_distance('[1,2]'::intvec, '[2,4]');
SELECT cosine_distance('[1,2]'::intvec, '[0,0]');
SELECT cosine_distance('[1,1]'::intvec, '[1,1]');
SELECT cosine_distance('[1,0]'::intvec, '[0,2]');
SELECT cosine_distance('[1,1]'::intvec, '[-1,-1]');
SELECT cosine_distance('[1,2]'::intvec, '[3]');
SELECT '[1,2]'::intvec <=> '[2,4]';

SELECT l1_distance('[0,0]'::intvec, '[3,4]');
SELECT l1_distance('[0,0]'::intvec, '[0,1]');
SELECT l1_distance('[1,2]'::intvec, '[3]');
