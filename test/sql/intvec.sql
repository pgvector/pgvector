SELECT '[1,2,3]'::intvec;
SELECT '[-1,-2,-3]'::intvec;
SELECT ' [ 1,  2 ,    3  ] '::intvec;
SELECT '[1.23456]'::intvec;
SELECT '[hello,1]'::intvec;
SELECT '[127,-128]'::intvec;
SELECT '[128,-129]'::intvec;
SELECT '[1,2,3'::intvec;
SELECT '[1,2,3]9'::intvec;
SELECT '1,2,3'::intvec;
SELECT ''::intvec;
SELECT '['::intvec;
SELECT '[,'::intvec;
SELECT '[]'::intvec;
SELECT '[1,]'::intvec;
SELECT '[1a]'::intvec;
SELECT '[1,,3]'::intvec;
SELECT '[1, ,3]'::intvec;

SELECT '[1,2,3]'::intvec(3);
SELECT '[1,2,3]'::intvec(2);
SELECT '[1,2,3]'::intvec(3, 2);
SELECT '[1,2,3]'::intvec('a');
SELECT '[1,2,3]'::intvec(0);
SELECT '[1,2,3]'::intvec(16001);

SELECT unnest('{"[1,2,3]", "[4,5,6]"}'::intvec[]);
SELECT '{"[1,2,3]"}'::intvec(2)[];

SELECT '[1,2,3]'::intvec < '[1,2,3]';
SELECT '[1,2,3]'::intvec < '[1,2]';
SELECT '[1,2,3]'::intvec <= '[1,2,3]';
SELECT '[1,2,3]'::intvec <= '[1,2]';
SELECT '[1,2,3]'::intvec = '[1,2,3]';
SELECT '[1,2,3]'::intvec = '[1,2]';
SELECT '[1,2,3]'::intvec != '[1,2,3]';
SELECT '[1,2,3]'::intvec != '[1,2]';
SELECT '[1,2,3]'::intvec >= '[1,2,3]';
SELECT '[1,2,3]'::intvec >= '[1,2]';
SELECT '[1,2,3]'::intvec > '[1,2,3]';
SELECT '[1,2,3]'::intvec > '[1,2]';

SELECT intvec_cmp('[1,2,3]', '[1,2,3]');
SELECT intvec_cmp('[1,2,3]', '[0,0,0]');
SELECT intvec_cmp('[0,0,0]', '[1,2,3]');
SELECT intvec_cmp('[1,2]', '[1,2,3]');
SELECT intvec_cmp('[1,2,3]', '[1,2]');
SELECT intvec_cmp('[1,2]', '[2,3,4]');
SELECT intvec_cmp('[2,3]', '[1,2,3]');

SELECT vector_dims('[1,2,3]'::intvec);

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
