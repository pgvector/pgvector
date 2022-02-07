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

SELECT unnest('{"[1,2,3]", "[4,5,6]"}'::vector[]);
SELECT '{"[1,2,3]"}'::vector(2)[];
