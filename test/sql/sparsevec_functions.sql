SELECT l2_distance('{}/2'::sparsevec, '{0:3,1:4}/2');
SELECT l2_distance('{}/2'::sparsevec, '{1:1}/2');
SELECT '{}/2'::sparsevec <-> '{0:3,1:4}/2';

SELECT inner_product('{0:1,1:2}/2'::sparsevec, '{0:2,1:4}/2');
SELECT sparsevec_negative_inner_product('{0:1,1:2}/2', '{0:2,1:4}/2');

SELECT cosine_distance('{0:1,1:2}/2'::sparsevec, '{0:2,1:4}/2');
SELECT cosine_distance('{0:1,1:2}/2'::sparsevec, '{}/2');
SELECT cosine_distance('{0:1,1:1}/2'::sparsevec, '{0:-1,1:-1}/2');
SELECT cosine_distance('{0:1}/2'::sparsevec, '{1:2}/2');
SELECT cosine_distance('{}/1'::sparsevec, '{}/1');
SELECT cosine_distance('{0:1}/2'::sparsevec, '{0:1}/3');

SELECT subvector('{0:1,1:2,2:3,3:4,4:5}/5'::sparsevec, 1, 3);
SELECT subvector('{0:1,1:2,2:3,3:4,4:5}/5'::sparsevec, 3, 2);
SELECT subvector('{0:1,1:2,2:3,3:4,4:5}/5'::sparsevec, -1, 3);
SELECT subvector('{0:1,1:2,2:3,3:4,4:5}/5'::sparsevec, 3, 9);
SELECT subvector('{0:1,1:2,2:3,3:4,4:5}/5'::sparsevec, 1, 0);
SELECT subvector('{0:1,1:2,2:3,3:4,4:5}/5'::sparsevec, 3, -1);
SELECT subvector('{0:1,1:2,2:3,3:4,4:5}/5'::sparsevec, -1, 2);
