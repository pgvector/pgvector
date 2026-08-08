-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.8.7'" to load this file. \quit

COMMENT ON FUNCTION vector_in(cstring, oid, integer) IS 'I/O';

COMMENT ON FUNCTION vector_out(vector) IS 'I/O';

COMMENT ON FUNCTION vector_typmod_in(cstring[]) IS 'I/O typmod';

COMMENT ON FUNCTION vector_recv(internal, oid, integer) IS 'I/O';

COMMENT ON FUNCTION vector_send(vector) IS 'I/O';

COMMENT ON TYPE vector IS 'single-precision vector';

COMMENT ON FUNCTION l2_distance(vector, vector) IS 'Euclidean distance';

COMMENT ON FUNCTION inner_product(vector, vector) IS 'inner product';

COMMENT ON FUNCTION cosine_distance(vector, vector) IS 'cosine distance';

COMMENT ON FUNCTION l1_distance(vector, vector) IS 'taxicab distance';

COMMENT ON FUNCTION vector_dims(vector) IS 'number of dimensions';

COMMENT ON FUNCTION vector_norm(vector) IS 'Euclidean norm';

COMMENT ON FUNCTION l2_normalize(vector) IS 'normalize with Euclidean norm';

COMMENT ON FUNCTION binary_quantize(vector) IS 'binary quantize';

COMMENT ON FUNCTION subvector(vector, int, int) IS 'subvector';

COMMENT ON FUNCTION vector_add(vector, vector) IS 'implementation of + operator';

COMMENT ON FUNCTION vector_sub(vector, vector) IS 'implementation of - operator';

COMMENT ON FUNCTION vector_mul(vector, vector) IS 'implementation of * operator';

COMMENT ON FUNCTION vector_concat(vector, vector) IS 'implementation of || operator';

COMMENT ON FUNCTION vector_lt(vector, vector) IS 'implementation of < operator';

COMMENT ON FUNCTION vector_le(vector, vector) IS 'implementation of <= operator';

COMMENT ON FUNCTION vector_eq(vector, vector) IS 'implementation of = operator';

COMMENT ON FUNCTION vector_ne(vector, vector) IS 'implementation of <> operator';

COMMENT ON FUNCTION vector_ge(vector, vector) IS 'implementation of >= operator';

COMMENT ON FUNCTION vector_gt(vector, vector) IS 'implementation of > operator';

COMMENT ON FUNCTION vector_cmp(vector, vector) IS 'less-equal-greater';

COMMENT ON FUNCTION vector_l2_squared_distance(vector, vector) IS 'squared Euclidean distance';

COMMENT ON FUNCTION vector_negative_inner_product(vector, vector) IS 'negative inner product';

COMMENT ON FUNCTION vector_spherical_distance(vector, vector) IS 'spherical distance';

COMMENT ON FUNCTION vector_accum(double precision[], vector) IS 'aggregate transition function';

COMMENT ON FUNCTION vector_avg(double precision[]) IS 'aggregate final function';

COMMENT ON FUNCTION vector_combine(double precision[], double precision[]) IS 'aggregate combine function';

COMMENT ON AGGREGATE avg(vector) IS 'the average (arithmetic mean) as vector of all vector values';

COMMENT ON AGGREGATE sum(vector) IS 'sum as vector across all vector input values';

COMMENT ON FUNCTION vector(vector, integer, boolean) IS 'adjust vector to typmod';

COMMENT ON FUNCTION array_to_vector(integer[], integer, boolean) IS 'convert int4 array to vector';

COMMENT ON FUNCTION array_to_vector(real[], integer, boolean) IS 'convert float4 array to vector';

COMMENT ON FUNCTION array_to_vector(double precision[], integer, boolean) IS 'convert float8 array to vector';

COMMENT ON FUNCTION array_to_vector(numeric[], integer, boolean) IS 'convert numeric array to vector';

COMMENT ON FUNCTION vector_to_float4(vector, integer, boolean) IS 'convert vector to float4 array';

COMMENT ON OPERATOR + (vector, vector) IS 'add';

COMMENT ON OPERATOR - (vector, vector) IS 'subtract';

COMMENT ON OPERATOR * (vector, vector) IS 'multiply';

COMMENT ON OPERATOR || (vector, vector) IS 'concatenate';

COMMENT ON OPERATOR < (vector, vector) IS 'less than';

COMMENT ON OPERATOR <= (vector, vector) IS 'less than or equal';

COMMENT ON OPERATOR = (vector, vector) IS 'equal';

COMMENT ON OPERATOR <> (vector, vector) IS 'not equal';

COMMENT ON OPERATOR >= (vector, vector) IS 'greater than or equal';

COMMENT ON OPERATOR > (vector, vector) IS 'greater than';

COMMENT ON FUNCTION ivfflathandler(internal) IS 'ivfflat index access method handler';

COMMENT ON FUNCTION hnswhandler(internal) IS 'hnsw index access method handler';

COMMENT ON FUNCTION ivfflat_halfvec_support(internal) IS 'ivfflat halfvec support';

COMMENT ON FUNCTION ivfflat_bit_support(internal) IS 'ivfflat bit support';

COMMENT ON FUNCTION hnsw_halfvec_support(internal) IS 'hnsw halfvec support';

COMMENT ON FUNCTION hnsw_bit_support(internal) IS 'hnsw bit support';

COMMENT ON FUNCTION hnsw_sparsevec_support(internal) IS 'hnsw sparsevec support';

COMMENT ON FUNCTION halfvec_in(cstring, oid, integer) IS 'I/O';

COMMENT ON FUNCTION halfvec_out(halfvec) IS 'I/O';

COMMENT ON FUNCTION halfvec_typmod_in(cstring[]) IS 'I/O typmod';

COMMENT ON FUNCTION halfvec_recv(internal, oid, integer) IS 'I/O';

COMMENT ON FUNCTION halfvec_send(halfvec) IS 'I/O';

COMMENT ON TYPE halfvec IS 'half-precision vector';

COMMENT ON FUNCTION l2_distance(halfvec, halfvec) IS 'Euclidean distance';

COMMENT ON FUNCTION inner_product(halfvec, halfvec) IS 'inner product';

COMMENT ON FUNCTION cosine_distance(halfvec, halfvec) IS 'cosine distance';

COMMENT ON FUNCTION l1_distance(halfvec, halfvec) IS 'taxicab distance';

COMMENT ON FUNCTION vector_dims(halfvec) IS 'number of dimensions';

COMMENT ON FUNCTION l2_norm(halfvec) IS 'Euclidean norm';

COMMENT ON FUNCTION l2_normalize(halfvec) IS 'normalize with Euclidean norm';

COMMENT ON FUNCTION binary_quantize(halfvec) IS 'binary quantize';

COMMENT ON FUNCTION subvector(halfvec, int, int) IS 'subvector';

COMMENT ON FUNCTION halfvec_add(halfvec, halfvec) IS 'implementation of + operator';

COMMENT ON FUNCTION halfvec_sub(halfvec, halfvec) IS 'implementation of - operator';

COMMENT ON FUNCTION halfvec_mul(halfvec, halfvec) IS 'implementation of * operator';

COMMENT ON FUNCTION halfvec_concat(halfvec, halfvec) IS 'implementation of || operator';

COMMENT ON FUNCTION halfvec_lt(halfvec, halfvec) IS 'implementation of < operator';

COMMENT ON FUNCTION halfvec_le(halfvec, halfvec) IS 'implementation of <= operator';

COMMENT ON FUNCTION halfvec_eq(halfvec, halfvec) IS 'implementation of = operator';

COMMENT ON FUNCTION halfvec_ne(halfvec, halfvec) IS 'implementation of <> operator';

COMMENT ON FUNCTION halfvec_ge(halfvec, halfvec) IS 'implementation of >= operator';

COMMENT ON FUNCTION halfvec_gt(halfvec, halfvec) IS 'implementation of > operator';

COMMENT ON FUNCTION halfvec_cmp(halfvec, halfvec) IS 'less-equal-greater';

COMMENT ON FUNCTION halfvec_l2_squared_distance(halfvec, halfvec) IS 'squared Euclidean distance';

COMMENT ON FUNCTION halfvec_negative_inner_product(halfvec, halfvec) IS 'negative inner product';

COMMENT ON FUNCTION halfvec_spherical_distance(halfvec, halfvec) IS 'spherical distance';

COMMENT ON FUNCTION halfvec_accum(double precision[], halfvec) IS 'aggregate transition function';

COMMENT ON FUNCTION halfvec_avg(double precision[]) IS 'aggregate final function';

COMMENT ON FUNCTION halfvec_combine(double precision[], double precision[]) IS 'aggregate combine function';

COMMENT ON AGGREGATE avg(halfvec) IS 'the average (arithmetic mean) as halfvec of all halfvec values';

COMMENT ON AGGREGATE sum(halfvec) IS 'sum as halfvec across all halfvec input values';

COMMENT ON FUNCTION halfvec(halfvec, integer, boolean) IS 'adjust halfvec to typmod';

COMMENT ON FUNCTION halfvec_to_vector(halfvec, integer, boolean) IS 'convert halfvec to vector';

COMMENT ON FUNCTION vector_to_halfvec(vector, integer, boolean) IS 'convert vector to halfvec';

COMMENT ON FUNCTION array_to_halfvec(integer[], integer, boolean) IS 'convert int4 array to halfvec';

COMMENT ON FUNCTION array_to_halfvec(real[], integer, boolean) IS 'convert float4 array to halfvec';

COMMENT ON FUNCTION array_to_halfvec(double precision[], integer, boolean) IS 'convert float8 array to halfvec';

COMMENT ON FUNCTION array_to_halfvec(numeric[], integer, boolean) IS 'convert numeric array to halfvec';

COMMENT ON FUNCTION halfvec_to_float4(halfvec, integer, boolean) IS 'convert halfvec to float4 array';

COMMENT ON OPERATOR + (halfvec, halfvec) IS 'add';

COMMENT ON OPERATOR - (halfvec, halfvec) IS 'subtract';

COMMENT ON OPERATOR * (halfvec, halfvec) IS 'multiply';

COMMENT ON OPERATOR || (halfvec, halfvec) IS 'concatenate';

COMMENT ON OPERATOR < (halfvec, halfvec) IS 'less than';

COMMENT ON OPERATOR <= (halfvec, halfvec) IS 'less than or equal';

COMMENT ON OPERATOR = (halfvec, halfvec) IS 'equal';

COMMENT ON OPERATOR <> (halfvec, halfvec) IS 'not equal';

COMMENT ON OPERATOR >= (halfvec, halfvec) IS 'greater than or equal';

COMMENT ON OPERATOR > (halfvec, halfvec) IS 'greater than';

COMMENT ON FUNCTION hamming_distance(bit, bit) IS 'Hamming distance';

COMMENT ON FUNCTION jaccard_distance(bit, bit) IS 'Jaccard distance';

COMMENT ON FUNCTION sparsevec_in(cstring, oid, integer) IS 'I/O';

COMMENT ON FUNCTION sparsevec_out(sparsevec) IS 'I/O';

COMMENT ON FUNCTION sparsevec_typmod_in(cstring[]) IS 'I/O typmod';

COMMENT ON FUNCTION sparsevec_recv(internal, oid, integer) IS 'I/O';

COMMENT ON FUNCTION sparsevec_send(sparsevec) IS 'I/O';

COMMENT ON TYPE sparsevec IS 'single-precision sparse vector';

COMMENT ON FUNCTION l2_distance(sparsevec, sparsevec) IS 'Euclidean distance';

COMMENT ON FUNCTION inner_product(sparsevec, sparsevec) IS 'inner product';

COMMENT ON FUNCTION cosine_distance(sparsevec, sparsevec) IS 'cosine distance';

COMMENT ON FUNCTION l1_distance(sparsevec, sparsevec) IS 'taxicab distance';

COMMENT ON FUNCTION l2_norm(sparsevec) IS 'Euclidean norm';

COMMENT ON FUNCTION l2_normalize(sparsevec) IS 'normalize with Euclidean norm';

COMMENT ON FUNCTION sparsevec_lt(sparsevec, sparsevec) IS 'implementation of < operator';

COMMENT ON FUNCTION sparsevec_le(sparsevec, sparsevec) IS 'implementation of <= operator';

COMMENT ON FUNCTION sparsevec_eq(sparsevec, sparsevec) IS 'implementation of = operator';

COMMENT ON FUNCTION sparsevec_ne(sparsevec, sparsevec) IS 'implementation of <> operator';

COMMENT ON FUNCTION sparsevec_ge(sparsevec, sparsevec) IS 'implementation of >= operator';

COMMENT ON FUNCTION sparsevec_gt(sparsevec, sparsevec) IS 'implementation of > operator';

COMMENT ON FUNCTION sparsevec_cmp(sparsevec, sparsevec) IS 'less-equal-greater';

COMMENT ON FUNCTION sparsevec_l2_squared_distance(sparsevec, sparsevec) IS 'squared Euclidean distance';

COMMENT ON FUNCTION sparsevec_negative_inner_product(sparsevec, sparsevec) IS 'negative inner product';

COMMENT ON FUNCTION sparsevec(sparsevec, integer, boolean) IS 'adjust sparsevec to typmod';

COMMENT ON FUNCTION vector_to_sparsevec(vector, integer, boolean) IS 'convert vector to sparsevec';

COMMENT ON FUNCTION sparsevec_to_vector(sparsevec, integer, boolean) IS 'convert sparsevec to vector';

COMMENT ON FUNCTION halfvec_to_sparsevec(halfvec, integer, boolean) IS 'convert halfvec to sparsevec';

COMMENT ON FUNCTION sparsevec_to_halfvec(sparsevec, integer, boolean) IS 'convert sparsevec to halfvec';

COMMENT ON FUNCTION array_to_sparsevec(integer[], integer, boolean) IS 'convert int4 array to sparsevec';

COMMENT ON FUNCTION array_to_sparsevec(real[], integer, boolean) IS 'convert float4 array to sparsevec';

COMMENT ON FUNCTION array_to_sparsevec(double precision[], integer, boolean) IS 'convert float8 array to sparsevec';

COMMENT ON FUNCTION array_to_sparsevec(numeric[], integer, boolean) IS 'convert numeric array to sparsevec';

COMMENT ON OPERATOR < (sparsevec, sparsevec) IS 'less than';

COMMENT ON OPERATOR <= (sparsevec, sparsevec) IS 'less than or equal';

COMMENT ON OPERATOR = (sparsevec, sparsevec) IS 'equal';

COMMENT ON OPERATOR <> (sparsevec, sparsevec) IS 'not equal';

COMMENT ON OPERATOR >= (sparsevec, sparsevec) IS 'greater than or equal';

COMMENT ON OPERATOR > (sparsevec, sparsevec) IS 'greater than';
