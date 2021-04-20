SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS vector;

SELECT '[1,2,3]'::vector + '[4,5,6]';
SELECT '[1,2,3]'::vector - '[4,5,6]';

SELECT vector_dims('[1,2,3]');
SELECT round(vector_norm('[1,1]')::numeric, 5);

SELECT round(l2_distance('[1,2]', '[0,0]')::numeric, 5);
SELECT l2_distance('[1,2]', '[3]');

SELECT inner_product('[1,2]', '[3,4]');
SELECT inner_product('[1,2]', '[3]');

SELECT round(cosine_distance('[1,2]', '[2,4]')::numeric, 5);
SELECT cosine_distance('[1,2]', '[0,0]');
SELECT cosine_distance('[1,2]', '[3]');
