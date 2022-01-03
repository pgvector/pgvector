SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS vector;

SELECT ARRAY[1,2,3]::vector;
SELECT ARRAY[1.0,2.0,3.0]::vector;
SELECT ARRAY[1,2,3]::float4[]::vector;
SELECT ARRAY[1,2,3]::float8[]::vector;
SELECT '{NULL}'::real[]::vector;
SELECT '{NaN}'::real[]::vector;
SELECT '{Infinity}'::real[]::vector;
SELECT '{-Infinity}'::real[]::vector;
SELECT '{}'::real[]::vector;
SELECT '[1,2,3]'::vector::real[];
SELECT array_agg(n)::vector FROM generate_series(1, 1025) n;

-- ensure no error
SELECT ARRAY[1,2,3] = ARRAY[1,2,3];
