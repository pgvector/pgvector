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
