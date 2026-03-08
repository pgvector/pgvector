SELECT '{1:1.5,3:3.5}/5'::sparsevec;
SELECT '{1:-2,3:-4}/5'::sparsevec;
SELECT '{1:2.,3:4.}/5'::sparsevec;
SELECT ' { 1 : 1.5 ,  3  :  3.5  } / 5 '::sparsevec;
SELECT '{1:1.23456}/1'::sparsevec;
SELECT '{1:hello,2:1}/2'::sparsevec;
SELECT '{1:NaN,2:1}/2'::sparsevec;
SELECT '{1:Infinity,2:1}/2'::sparsevec;
SELECT '{1:-Infinity,2:1}/2'::sparsevec;
SELECT '{1:1.5e38,2:-1.5e38}/2'::sparsevec;
SELECT '{1:1.5e+38,2:-1.5e+38}/2'::sparsevec;
SELECT '{1:1.5e-38,2:-1.5e-38}/2'::sparsevec;
SELECT '{1:4e38,2:1}/2'::sparsevec;
SELECT '{1:-4e38,2:1}/2'::sparsevec;
SELECT '{1:1e-46,2:1}/2'::sparsevec;
SELECT '{1:-1e-46,2:1}/2'::sparsevec;
SELECT ''::sparsevec;
SELECT '{'::sparsevec;
SELECT '{ '::sparsevec;
SELECT '{:'::sparsevec;
SELECT '{,'::sparsevec;
SELECT '{}'::sparsevec;
SELECT '{}/'::sparsevec;
SELECT '{}/1'::sparsevec;
SELECT '{}/1a'::sparsevec;
SELECT '{ }/1'::sparsevec;
SELECT '{:}/1'::sparsevec;
SELECT '{,}/1'::sparsevec;
SELECT '{1,}/1'::sparsevec;
SELECT '{:1}/1'::sparsevec;
SELECT '{1:}/1'::sparsevec;
SELECT '{1a:1}/1'::sparsevec;
SELECT '{1:1a}/1'::sparsevec;
SELECT '{1:1,}/1'::sparsevec;
SELECT '{1:0,2:1,3:0}/3'::sparsevec;
SELECT '{2:1,1:1}/2'::sparsevec;
SELECT '{1:1,1:1}/2'::sparsevec;
SELECT '{1:1,2:1,1:1}/2'::sparsevec;
SELECT '{}/5'::sparsevec;
SELECT '{}/-1'::sparsevec;
SELECT '{}/1000000001'::sparsevec;
SELECT '{}/2147483648'::sparsevec;
SELECT '{}/-2147483649'::sparsevec;
SELECT '{}/9223372036854775808'::sparsevec;
SELECT '{}/-9223372036854775809'::sparsevec;
SELECT '{2147483647:1}/1'::sparsevec;
SELECT '{2147483648:1}/1'::sparsevec;
SELECT '{-2147483648:1}/1'::sparsevec;
SELECT '{-2147483649:1}/1'::sparsevec;
SELECT '{0:1}/1'::sparsevec;
SELECT '{2:1}/1'::sparsevec;

SELECT '{}/3'::sparsevec(3);
SELECT '{}/3'::sparsevec(2);
SELECT '{}/3'::sparsevec(3, 2);
SELECT '{}/3'::sparsevec('a');
SELECT '{}/3'::sparsevec(0);
SELECT '{}/3'::sparsevec(1000000001);

SELECT '{1:1,2:2,3:3}/3'::sparsevec < '{1:1,2:2,3:3}/3';
SELECT '{1:1,2:2,3:3}/3'::sparsevec < '{1:1,2:2}/2';
SELECT '{1:1,2:2,3:3}/3'::sparsevec <= '{1:1,2:2,3:3}/3';
SELECT '{1:1,2:2,3:3}/3'::sparsevec <= '{1:1,2:2}/2';
SELECT '{1:1,2:2,3:3}/3'::sparsevec = '{1:1,2:2,3:3}/3';
SELECT '{1:1,2:2,3:3}/3'::sparsevec = '{1:1,2:2}/2';
SELECT '{1:1,2:2,3:3}/3'::sparsevec != '{1:1,2:2,3:3}/3';
SELECT '{1:1,2:2,3:3}/3'::sparsevec != '{1:1,2:2}/2';
SELECT '{1:1,2:2,3:3}/3'::sparsevec >= '{1:1,2:2,3:3}/3';
SELECT '{1:1,2:2,3:3}/3'::sparsevec >= '{1:1,2:2}/2';
SELECT '{1:1,2:2,3:3}/3'::sparsevec > '{1:1,2:2,3:3}/3';
SELECT '{1:1,2:2,3:3}/3'::sparsevec > '{1:1,2:2}/2';

SELECT sparsevec_cmp('{1:1,2:2,3:3}/3', '{1:1,2:2,3:3}/3');
SELECT sparsevec_cmp('{1:1,2:2,3:3}/3', '{}/3');
SELECT sparsevec_cmp('{}/3', '{1:1,2:2,3:3}/3');
SELECT sparsevec_cmp('{1:1,2:2}/2', '{1:1,2:2,3:3}/3');
SELECT sparsevec_cmp('{1:1,2:2,3:3}/3', '{1:1,2:2}/2');
SELECT sparsevec_cmp('{1:1,2:2}/2', '{1:2,2:3,3:4}/3');
SELECT sparsevec_cmp('{1:2,2:3}/2', '{1:1,2:2,3:3}/3');

SELECT round(l2_norm('{1:1,2:1}/2'::sparsevec)::numeric, 5);
SELECT l2_norm('{1:3,2:4}/2'::sparsevec);
SELECT l2_norm('{2:1}/2'::sparsevec);
SELECT l2_norm('{1:3e37,2:4e37}/2'::sparsevec)::real;
SELECT l2_norm('{}/2'::sparsevec);
SELECT l2_norm('{1:2}/1'::sparsevec);

SELECT l2_distance('{}/2'::sparsevec, '{1:3,2:4}/2');
SELECT l2_distance('{1:3}/2'::sparsevec, '{2:4}/2');
SELECT l2_distance('{2:4}/2'::sparsevec, '{1:3}/2');
SELECT l2_distance('{1:3,2:4}/2'::sparsevec, '{}/2');
SELECT l2_distance('{}/2'::sparsevec, '{2:1}/2');
SELECT '{}/2'::sparsevec <-> '{1:3,2:4}/2';

SELECT inner_product('{1:1,2:2}/2'::sparsevec, '{1:2,2:4}/2');
SELECT inner_product('{1:1,2:2}/2'::sparsevec, '{1:3}/1');
SELECT inner_product('{1:1,3:3}/4'::sparsevec, '{2:2,4:4}/4');
SELECT inner_product('{2:2,4:4}/4'::sparsevec, '{1:1,3:3}/4');
SELECT inner_product('{1:1,3:3,5:5}/5'::sparsevec, '{2:4,3:6,4:8}/5');
SELECT inner_product('{1:1}/2'::sparsevec, '{}/2');
SELECT inner_product('{}/2'::sparsevec, '{1:1}/2');
SELECT inner_product('{1:3e38}/1'::sparsevec, '{1:3e38}/1');
SELECT inner_product('{1:1,3:3,5:5}/5'::sparsevec, '{2:4,3:6,4:8}/5');
SELECT '{1:1,2:2}/2'::sparsevec <#> '{1:3,2:4}/2';

SELECT cosine_distance('{1:1,2:2}/2'::sparsevec, '{1:2,2:4}/2');
SELECT cosine_distance('{1:1,2:2}/2'::sparsevec, '{}/2');
SELECT cosine_distance('{1:1,2:1}/2'::sparsevec, '{1:1,2:1}/2');
SELECT cosine_distance('{1:1}/2'::sparsevec, '{2:2}/2');
SELECT cosine_distance('{1:1,2:1}/2'::sparsevec, '{1:-1,2:-1}/2');
SELECT cosine_distance('{1:2}/2'::sparsevec, '{2:2}/2');
SELECT cosine_distance('{2:2}/2'::sparsevec, '{1:2}/2');
SELECT cosine_distance('{1:1,2:2}/2'::sparsevec, '{1:3}/1');
SELECT cosine_distance('{1:1,2:1}/2'::sparsevec, '{1:1.1,2:1.1}/2');
SELECT cosine_distance('{1:1,2:1}/2'::sparsevec, '{1:-1.1,2:-1.1}/2');
SELECT cosine_distance('{1:3e38}/1'::sparsevec, '{1:3e38}/1');
SELECT cosine_distance('{}/1'::sparsevec, '{}/1');
SELECT '{1:1,2:2}/2'::sparsevec <=> '{1:2,2:4}/2';

SELECT l1_distance('{}/2'::sparsevec, '{1:3,2:4}/2');
SELECT l1_distance('{}/2'::sparsevec, '{2:1}/2');
SELECT l1_distance('{1:1,2:2}/2'::sparsevec, '{1:3}/1');
SELECT l1_distance('{1:3e38}/1'::sparsevec, '{1:-3e38}/1');
SELECT l1_distance('{1:1,3:3,5:5,7:7}/8'::sparsevec, '{2:2,4:4,6:6,8:8}/8');
SELECT l1_distance('{1:1,3:3,5:5,7:7,9:9}/9'::sparsevec, '{2:2,4:4,6:6,8:8}/9');
SELECT '{}/2'::sparsevec <+> '{1:3,2:4}/2';

SELECT l2_normalize('{1:3,2:4}/2'::sparsevec);
SELECT l2_normalize('{1:3}/2'::sparsevec);
SELECT l2_normalize('{2:0.1}/2'::sparsevec);
SELECT l2_normalize('{}/2'::sparsevec);
SELECT l2_normalize('{1:3e38}/1'::sparsevec);
SELECT l2_normalize('{1:3e38,2:1e-37}/2'::sparsevec);
SELECT l2_normalize('{2:3e37,4:3e-37,6:4e37,8:4e-37}/9'::sparsevec);

-- sparsevec constructor from indices and values arrays

-- basic construction
SELECT sparsevec(ARRAY[1,3], ARRAY[1.5,3.5]::real[], 5);
-- equivalent to text literal
SELECT sparsevec(ARRAY[1,3], ARRAY[1.5,3.5]::real[], 5) = '{1:1.5,3:3.5}/5'::sparsevec;
-- unsorted input is sorted automatically
SELECT sparsevec(ARRAY[3,1], ARRAY[3.5,1.5]::real[], 5);
-- zero values are silently dropped
SELECT sparsevec(ARRAY[1,2,3], ARRAY[1.0,0.0,2.0]::real[], 5);
-- all zeros yields empty sparse vector
SELECT sparsevec(ARRAY[1,2,3], ARRAY[0.0,0.0,0.0]::real[], 3);
-- index equal to dim (upper bound) is valid
SELECT sparsevec(ARRAY[5], ARRAY[1.0]::real[], 5);

-- error: array length mismatch
SELECT sparsevec(ARRAY[1,2], ARRAY[1.0]::real[], 5);
-- error: index 0 is out of bounds (1-based)
SELECT sparsevec(ARRAY[0], ARRAY[1.0]::real[], 5);
-- error: index exceeds dim
SELECT sparsevec(ARRAY[6], ARRAY[1.0]::real[], 5);
-- error: negative index
SELECT sparsevec(ARRAY[-1], ARRAY[1.0]::real[], 5);
-- error: duplicate indices
SELECT sparsevec(ARRAY[1,1], ARRAY[1.0,2.0]::real[], 5);
-- error: NaN not allowed
SELECT sparsevec(ARRAY[1], ARRAY['NaN']::real[], 5);
-- error: infinite value not allowed
SELECT sparsevec(ARRAY[1], ARRAY['Infinity']::real[], 5);
-- error: dim must be at least 1
SELECT sparsevec(ARRAY[1], ARRAY[1.0]::real[], 0);
-- error: dim exceeds maximum
SELECT sparsevec(ARRAY[1], ARRAY[1.0]::real[], 1000000001);
-- error: 2-D arrays not accepted
SELECT sparsevec(ARRAY[[1,2]]::int[], ARRAY[1.0,2.0]::real[], 5);
-- error: null elements not accepted
SELECT sparsevec(ARRAY[1, NULL]::int[], ARRAY[1.0,2.0]::real[], 5);

-- value array types
SELECT sparsevec(ARRAY[1,3], ARRAY[2,4]::integer[], 5);
SELECT sparsevec(ARRAY[1,3], ARRAY[1.5,3.5]::double precision[], 5);
SELECT sparsevec(ARRAY[1,3], ARRAY[1.5,3.5]::numeric[], 5);
