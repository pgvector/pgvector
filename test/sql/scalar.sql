SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS vector;

SELECT '[1,2,3]'::vector + 10;
SELECT '[1,2,3]'::vector - 10;
SELECT '[1,2,3]'::vector * 10;
SELECT '[1,2,3]'::vector / 10;
SELECT 10 + '[1,2,3]'::vector;
SELECT 10 - '[1,2,3]'::vector;
SELECT 10 * '[1,2,3]'::vector;
SELECT 10 / '[1,2,3]'::vector;
