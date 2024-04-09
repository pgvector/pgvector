SELECT ARRAY[1,2,3]::vector;
SELECT ARRAY[1.0,2.0,3.0]::vector;
SELECT ARRAY[1,2,3]::float4[]::vector;
SELECT ARRAY[1,2,3]::float8[]::vector;
SELECT ARRAY[1,2,3]::numeric[]::vector;
SELECT '{NULL}'::real[]::vector;
SELECT '{NaN}'::real[]::vector;
SELECT '{Infinity}'::real[]::vector;
SELECT '{-Infinity}'::real[]::vector;
SELECT '{}'::real[]::vector;
SELECT '{{1}}'::real[]::vector;
SELECT '[1,2,3]'::vector::real[];

SELECT '[1,2,3]'::vector::halfvec;
SELECT '[1,2,3]'::halfvec::vector;
SELECT '[1,2,3]'::vector::halfvec(2);
SELECT '[1,2,3]'::halfvec::vector(2);
SELECT '[65520]'::vector::halfvec;
SELECT '[1e-8]'::vector::halfvec;

SELECT '{2:1.5,4:3.5}/5'::sparsevec::vector;
SELECT '[0,1.5,0,3.5,0]'::vector::sparsevec;
SELECT '{}/16001'::sparsevec::vector;

SELECT array_agg(n)::vector FROM generate_series(1, 16001) n;
SELECT array_to_vector(array_agg(n), 16001, false) FROM generate_series(1, 16001) n;

-- ensure no error
SELECT ARRAY[1,2,3] = ARRAY[1,2,3];
