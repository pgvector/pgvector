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

SELECT l2_distance('{}/2'::sparsevec, '{1:3,2:4}/2');
SELECT l2_distance('{}/2'::sparsevec, '{2:1}/2');
SELECT '{}/2'::sparsevec <-> '{1:3,2:4}/2';

SELECT inner_product('{1:1,2:2}/2'::sparsevec, '{1:2,2:4}/2');
SELECT sparsevec_negative_inner_product('{1:1,2:2}/2', '{1:2,2:4}/2');

SELECT cosine_distance('{1:1,2:2}/2'::sparsevec, '{1:2,2:4}/2');
SELECT cosine_distance('{1:1,2:2}/2'::sparsevec, '{}/2');
SELECT cosine_distance('{1:1,2:1}/2'::sparsevec, '{1:-1,2:-1}/2');
SELECT cosine_distance('{1:2}/2'::sparsevec, '{2:2}/2');
SELECT cosine_distance('{}/1'::sparsevec, '{}/1');
SELECT cosine_distance('{1:2}/2'::sparsevec, '{1:1}/3');
