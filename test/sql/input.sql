SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS vector;

SELECT '[1,2,3]'::vector;
SELECT '[-1,2,3]'::vector;
SELECT '[hello,1]'::vector;
SELECT '[NaN,1]'::vector;
SELECT '[Infinity,1]'::vector;
SELECT '[-Infinity,1]'::vector;
SELECT '[1,2,3'::vector;
SELECT '[1,2,3]9'::vector;
SELECT '1,2,3'::vector;
SELECT '[]'::vector;
SELECT '[1,]'::vector;
SELECT '[1,2,3]'::vector(2);
SELECT array_agg(n)::vector FROM generate_series(1, 1025) n;
