SELECT l2_distance('{}'::sparsevec, '{0:3,1:4}');
SELECT l2_distance('{}'::sparsevec, '{1:1}');
SELECT '{}'::sparsevec <-> '{0:3,1:4}';

SELECT inner_product('{0:1,1:2}'::sparsevec, '{0:2,1:4}');
SELECT sparsevec_negative_inner_product('{0:1,1:2}', '{0:2,1:4}');

SELECT cosine_distance('{0:1,1:2}'::sparsevec, '{0:2,1:4}');
SELECT cosine_distance('{0:1,1:2}'::sparsevec, '{}');
SELECT cosine_distance('{0:1,1:1}'::sparsevec, '{0:-1,1:-1}');
SELECT cosine_distance('{0:1}'::sparsevec, '{1:2}');
SELECT cosine_distance('{}'::sparsevec, '{}');
