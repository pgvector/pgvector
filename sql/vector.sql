-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION vector" to load this file. \quit

-- vector type

CREATE TYPE vector;

CREATE FUNCTION vector_in(cstring, oid, integer) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_in(cstring, oid, integer) IS 'I/O';

CREATE FUNCTION vector_out(vector) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_out(vector) IS 'I/O';

CREATE FUNCTION vector_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_typmod_in(cstring[]) IS 'I/O typmod';

CREATE FUNCTION vector_recv(internal, oid, integer) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_recv(internal, oid, integer) IS 'I/O';

CREATE FUNCTION vector_send(vector) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_send(vector) IS 'I/O';

CREATE TYPE vector (
	INPUT     = vector_in,
	OUTPUT    = vector_out,
	TYPMOD_IN = vector_typmod_in,
	RECEIVE   = vector_recv,
	SEND      = vector_send,
	STORAGE   = external
);

COMMENT ON TYPE vector IS 'single-precision vector';

-- vector functions

CREATE FUNCTION l2_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION l2_distance(vector, vector) IS 'Euclidean distance';

CREATE FUNCTION inner_product(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION inner_product(vector, vector) IS 'inner product';

CREATE FUNCTION cosine_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION cosine_distance(vector, vector) IS 'cosine distance';

CREATE FUNCTION l1_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION l1_distance(vector, vector) IS 'taxicab distance';

CREATE FUNCTION vector_dims(vector) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_dims(vector) IS 'number of dimensions';

CREATE FUNCTION vector_norm(vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_norm(vector) IS 'Euclidean norm';

CREATE FUNCTION l2_normalize(vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION l2_normalize(vector) IS 'normalize with Euclidean norm';

CREATE FUNCTION binary_quantize(vector) RETURNS bit
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION binary_quantize(vector) IS 'binary quantize';

CREATE FUNCTION subvector(vector, int, int) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION subvector(vector, int, int) IS 'subvector';

-- vector private functions

CREATE FUNCTION vector_add(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_add(vector, vector) IS 'implementation of + operator';

CREATE FUNCTION vector_sub(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_sub(vector, vector) IS 'implementation of - operator';

CREATE FUNCTION vector_mul(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_mul(vector, vector) IS 'implementation of * operator';

CREATE FUNCTION vector_concat(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_concat(vector, vector) IS 'implementation of || operator';

CREATE FUNCTION vector_lt(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_lt(vector, vector) IS 'implementation of < operator';

CREATE FUNCTION vector_le(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_le(vector, vector) IS 'implementation of <= operator';

CREATE FUNCTION vector_eq(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_eq(vector, vector) IS 'implementation of = operator';

CREATE FUNCTION vector_ne(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_ne(vector, vector) IS 'implementation of <> operator';

CREATE FUNCTION vector_ge(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_ge(vector, vector) IS 'implementation of >= operator';

CREATE FUNCTION vector_gt(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_gt(vector, vector) IS 'implementation of > operator';

CREATE FUNCTION vector_cmp(vector, vector) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_cmp(vector, vector) IS 'less-equal-greater';

CREATE FUNCTION vector_l2_squared_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_l2_squared_distance(vector, vector) IS 'squared Euclidean distance';

CREATE FUNCTION vector_negative_inner_product(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_negative_inner_product(vector, vector) IS 'negative inner product';

CREATE FUNCTION vector_spherical_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_spherical_distance(vector, vector) IS 'spherical distance';

CREATE FUNCTION vector_accum(double precision[], vector) RETURNS double precision[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_accum(double precision[], vector) IS 'aggregate transition function';

CREATE FUNCTION vector_avg(double precision[]) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_avg(double precision[]) IS 'aggregate final function';

CREATE FUNCTION vector_combine(double precision[], double precision[]) RETURNS double precision[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_combine(double precision[], double precision[]) IS 'aggregate combine function';

-- vector aggregates

CREATE AGGREGATE avg(vector) (
	SFUNC = vector_accum,
	STYPE = double precision[],
	FINALFUNC = vector_avg,
	COMBINEFUNC = vector_combine,
	INITCOND = '{0}',
	PARALLEL = SAFE
);

COMMENT ON AGGREGATE avg(vector) IS 'the average (arithmetic mean) as vector of all vector values';

CREATE AGGREGATE sum(vector) (
	SFUNC = vector_add,
	STYPE = vector,
	COMBINEFUNC = vector_add,
	PARALLEL = SAFE
);

COMMENT ON AGGREGATE sum(vector) IS 'sum as vector across all vector input values';

-- vector cast functions

CREATE FUNCTION vector(vector, integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector(vector, integer, boolean) IS 'adjust vector to typmod';

CREATE FUNCTION array_to_vector(integer[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_vector(integer[], integer, boolean) IS 'convert int4 array to vector';

CREATE FUNCTION array_to_vector(real[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_vector(real[], integer, boolean) IS 'convert float4 array to vector';

CREATE FUNCTION array_to_vector(double precision[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_vector(double precision[], integer, boolean) IS 'convert float8 array to vector';

CREATE FUNCTION array_to_vector(numeric[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_vector(numeric[], integer, boolean) IS 'convert numeric array to vector';

CREATE FUNCTION vector_to_float4(vector, integer, boolean) RETURNS real[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_to_float4(vector, integer, boolean) IS 'convert vector to float4 array';

-- vector casts

CREATE CAST (vector AS vector)
	WITH FUNCTION vector(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS real[])
	WITH FUNCTION vector_to_float4(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (integer[] AS vector)
	WITH FUNCTION array_to_vector(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS vector)
	WITH FUNCTION array_to_vector(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS vector)
	WITH FUNCTION array_to_vector(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS vector)
	WITH FUNCTION array_to_vector(numeric[], integer, boolean) AS ASSIGNMENT;

-- vector operators

CREATE OPERATOR <-> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

CREATE OPERATOR <+> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = l1_distance,
	COMMUTATOR = '<+>'
);

CREATE OPERATOR + (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_add,
	COMMUTATOR = +
);

COMMENT ON OPERATOR + (vector, vector) IS 'add';

CREATE OPERATOR - (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_sub
);

COMMENT ON OPERATOR - (vector, vector) IS 'subtract';

CREATE OPERATOR * (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_mul,
	COMMUTATOR = *
);

COMMENT ON OPERATOR * (vector, vector) IS 'multiply';

CREATE OPERATOR || (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_concat
);

COMMENT ON OPERATOR || (vector, vector) IS 'concatenate';

CREATE OPERATOR < (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_lt,
	COMMUTATOR = > , NEGATOR = >= ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

COMMENT ON OPERATOR < (vector, vector) IS 'less than';

CREATE OPERATOR <= (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_le,
	COMMUTATOR = >= , NEGATOR = > ,
	RESTRICT = scalarlesel, JOIN = scalarlejoinsel
);

COMMENT ON OPERATOR <= (vector, vector) IS 'less than or equal';

CREATE OPERATOR = (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_eq,
	COMMUTATOR = = , NEGATOR = <> ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

COMMENT ON OPERATOR = (vector, vector) IS 'equal';

CREATE OPERATOR <> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_ne,
	COMMUTATOR = <> , NEGATOR = = ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

COMMENT ON OPERATOR <> (vector, vector) IS 'not equal';

CREATE OPERATOR >= (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_ge,
	COMMUTATOR = <= , NEGATOR = < ,
	RESTRICT = scalargesel, JOIN = scalargejoinsel
);

COMMENT ON OPERATOR >= (vector, vector) IS 'greater than or equal';

CREATE OPERATOR > (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_gt,
	COMMUTATOR = < , NEGATOR = <= ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

COMMENT ON OPERATOR > (vector, vector) IS 'greater than';

-- access methods

CREATE FUNCTION ivfflathandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION ivfflathandler(internal) IS 'ivfflat index access method handler';

CREATE ACCESS METHOD ivfflat TYPE INDEX HANDLER ivfflathandler;

COMMENT ON ACCESS METHOD ivfflat IS 'ivfflat index access method';

CREATE FUNCTION hnswhandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION hnswhandler(internal) IS 'hnsw index access method handler';

CREATE ACCESS METHOD hnsw TYPE INDEX HANDLER hnswhandler;

COMMENT ON ACCESS METHOD hnsw IS 'hnsw index access method';

-- access method private functions

CREATE FUNCTION ivfflat_halfvec_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION ivfflat_halfvec_support(internal) IS 'ivfflat halfvec support';

CREATE FUNCTION ivfflat_bit_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION ivfflat_bit_support(internal) IS 'ivfflat bit support';

CREATE FUNCTION hnsw_halfvec_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION hnsw_halfvec_support(internal) IS 'hnsw halfvec support';

CREATE FUNCTION hnsw_bit_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION hnsw_bit_support(internal) IS 'hnsw bit support';

CREATE FUNCTION hnsw_sparsevec_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION hnsw_sparsevec_support(internal) IS 'hnsw sparsevec support';

-- vector opclasses

CREATE OPERATOR CLASS vector_ops
	DEFAULT FOR TYPE vector USING btree AS
	OPERATOR 1 < ,
	OPERATOR 2 <= ,
	OPERATOR 3 = ,
	OPERATOR 4 >= ,
	OPERATOR 5 > ,
	FUNCTION 1 vector_cmp(vector, vector);

CREATE OPERATOR CLASS vector_l2_ops
	DEFAULT FOR TYPE vector USING ivfflat AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_l2_squared_distance(vector, vector),
	FUNCTION 3 l2_distance(vector, vector);

CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING ivfflat AS
	OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 3 vector_spherical_distance(vector, vector),
	FUNCTION 4 vector_norm(vector);

CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING ivfflat AS
	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 2 vector_norm(vector),
	FUNCTION 3 vector_spherical_distance(vector, vector),
	FUNCTION 4 vector_norm(vector);

CREATE OPERATOR CLASS vector_l2_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_l2_squared_distance(vector, vector);

CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector);

CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 2 vector_norm(vector);

CREATE OPERATOR CLASS vector_l1_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <+> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 l1_distance(vector, vector);

-- halfvec type

CREATE TYPE halfvec;

CREATE FUNCTION halfvec_in(cstring, oid, integer) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_in(cstring, oid, integer) IS 'I/O';

CREATE FUNCTION halfvec_out(halfvec) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_out(halfvec) IS 'I/O';

CREATE FUNCTION halfvec_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_typmod_in(cstring[]) IS 'I/O typmod';

CREATE FUNCTION halfvec_recv(internal, oid, integer) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_recv(internal, oid, integer) IS 'I/O';

CREATE FUNCTION halfvec_send(halfvec) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_send(halfvec) IS 'I/O';

CREATE TYPE halfvec (
	INPUT     = halfvec_in,
	OUTPUT    = halfvec_out,
	TYPMOD_IN = halfvec_typmod_in,
	RECEIVE   = halfvec_recv,
	SEND      = halfvec_send,
	STORAGE   = external
);

COMMENT ON TYPE halfvec IS 'half-precision vector';

-- halfvec functions

CREATE FUNCTION l2_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_l2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION l2_distance(halfvec, halfvec) IS 'Euclidean distance';

CREATE FUNCTION inner_product(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION inner_product(halfvec, halfvec) IS 'inner product';

CREATE FUNCTION cosine_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_cosine_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION cosine_distance(halfvec, halfvec) IS 'cosine distance';

CREATE FUNCTION l1_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_l1_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION l1_distance(halfvec, halfvec) IS 'taxicab distance';

CREATE FUNCTION vector_dims(halfvec) RETURNS integer
	AS 'MODULE_PATHNAME', 'halfvec_vector_dims' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_dims(halfvec) IS 'number of dimensions';

CREATE FUNCTION l2_norm(halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_l2_norm' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION l2_norm(halfvec) IS 'Euclidean norm';

CREATE FUNCTION l2_normalize(halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME', 'halfvec_l2_normalize' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION l2_normalize(halfvec) IS 'normalize with Euclidean norm';

CREATE FUNCTION binary_quantize(halfvec) RETURNS bit
	AS 'MODULE_PATHNAME', 'halfvec_binary_quantize' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION binary_quantize(halfvec) IS 'binary quantize';

CREATE FUNCTION subvector(halfvec, int, int) RETURNS halfvec
	AS 'MODULE_PATHNAME', 'halfvec_subvector' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION subvector(halfvec, int, int) IS 'subvector';

-- halfvec private functions

CREATE FUNCTION halfvec_add(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_add(halfvec, halfvec) IS 'implementation of + operator';

CREATE FUNCTION halfvec_sub(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_sub(halfvec, halfvec) IS 'implementation of - operator';

CREATE FUNCTION halfvec_mul(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_mul(halfvec, halfvec) IS 'implementation of * operator';

CREATE FUNCTION halfvec_concat(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_concat(halfvec, halfvec) IS 'implementation of || operator';

CREATE FUNCTION halfvec_lt(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_lt(halfvec, halfvec) IS 'implementation of < operator';

CREATE FUNCTION halfvec_le(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_le(halfvec, halfvec) IS 'implementation of <= operator';

CREATE FUNCTION halfvec_eq(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_eq(halfvec, halfvec) IS 'implementation of = operator';

CREATE FUNCTION halfvec_ne(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_ne(halfvec, halfvec) IS 'implementation of <> operator';

CREATE FUNCTION halfvec_ge(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_ge(halfvec, halfvec) IS 'implementation of >= operator';

CREATE FUNCTION halfvec_gt(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_gt(halfvec, halfvec) IS 'implementation of > operator';

CREATE FUNCTION halfvec_cmp(halfvec, halfvec) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_cmp(halfvec, halfvec) IS 'less-equal-greater';

CREATE FUNCTION halfvec_l2_squared_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_l2_squared_distance(halfvec, halfvec) IS 'squared Euclidean distance';

CREATE FUNCTION halfvec_negative_inner_product(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_negative_inner_product(halfvec, halfvec) IS 'negative inner product';

CREATE FUNCTION halfvec_spherical_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_spherical_distance(halfvec, halfvec) IS 'spherical distance';

CREATE FUNCTION halfvec_accum(double precision[], halfvec) RETURNS double precision[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_accum(double precision[], halfvec) IS 'aggregate transition function';

CREATE FUNCTION halfvec_avg(double precision[]) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_avg(double precision[]) IS 'aggregate final function';

CREATE FUNCTION halfvec_combine(double precision[], double precision[]) RETURNS double precision[]
	AS 'MODULE_PATHNAME', 'vector_combine' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_combine(double precision[], double precision[]) IS 'aggregate combine function';

-- halfvec aggregates

CREATE AGGREGATE avg(halfvec) (
	SFUNC = halfvec_accum,
	STYPE = double precision[],
	FINALFUNC = halfvec_avg,
	COMBINEFUNC = halfvec_combine,
	INITCOND = '{0}',
	PARALLEL = SAFE
);

COMMENT ON AGGREGATE avg(halfvec) IS 'the average (arithmetic mean) as halfvec of all halfvec values';

CREATE AGGREGATE sum(halfvec) (
	SFUNC = halfvec_add,
	STYPE = halfvec,
	COMBINEFUNC = halfvec_add,
	PARALLEL = SAFE
);

COMMENT ON AGGREGATE sum(halfvec) IS 'sum as halfvec across all halfvec input values';

-- halfvec cast functions

CREATE FUNCTION halfvec(halfvec, integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec(halfvec, integer, boolean) IS 'adjust halfvec to typmod';

CREATE FUNCTION halfvec_to_vector(halfvec, integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_to_vector(halfvec, integer, boolean) IS 'convert halfvec to vector';

CREATE FUNCTION vector_to_halfvec(vector, integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_to_halfvec(vector, integer, boolean) IS 'convert vector to halfvec';

CREATE FUNCTION array_to_halfvec(integer[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_halfvec(integer[], integer, boolean) IS 'convert int4 array to halfvec';

CREATE FUNCTION array_to_halfvec(real[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_halfvec(real[], integer, boolean) IS 'convert float4 array to halfvec';

CREATE FUNCTION array_to_halfvec(double precision[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_halfvec(double precision[], integer, boolean) IS 'convert float8 array to halfvec';

CREATE FUNCTION array_to_halfvec(numeric[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_halfvec(numeric[], integer, boolean) IS 'convert numeric array to halfvec';

CREATE FUNCTION halfvec_to_float4(halfvec, integer, boolean) RETURNS real[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_to_float4(halfvec, integer, boolean) IS 'convert halfvec to float4 array';

-- halfvec casts

CREATE CAST (halfvec AS halfvec)
	WITH FUNCTION halfvec(halfvec, integer, boolean) AS IMPLICIT;

CREATE CAST (halfvec AS vector)
	WITH FUNCTION halfvec_to_vector(halfvec, integer, boolean) AS ASSIGNMENT;

CREATE CAST (vector AS halfvec)
	WITH FUNCTION vector_to_halfvec(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (halfvec AS real[])
	WITH FUNCTION halfvec_to_float4(halfvec, integer, boolean) AS ASSIGNMENT;

CREATE CAST (integer[] AS halfvec)
	WITH FUNCTION array_to_halfvec(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS halfvec)
	WITH FUNCTION array_to_halfvec(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS halfvec)
	WITH FUNCTION array_to_halfvec(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS halfvec)
	WITH FUNCTION array_to_halfvec(numeric[], integer, boolean) AS ASSIGNMENT;

-- halfvec operators

CREATE OPERATOR <-> (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

CREATE OPERATOR <+> (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = l1_distance,
	COMMUTATOR = '<+>'
);

CREATE OPERATOR + (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_add,
	COMMUTATOR = +
);

COMMENT ON OPERATOR + (halfvec, halfvec) IS 'add';

CREATE OPERATOR - (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_sub
);

COMMENT ON OPERATOR - (halfvec, halfvec) IS 'subtract';

CREATE OPERATOR * (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_mul,
	COMMUTATOR = *
);

COMMENT ON OPERATOR * (halfvec, halfvec) IS 'multiply';

CREATE OPERATOR || (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_concat
);

COMMENT ON OPERATOR || (halfvec, halfvec) IS 'concatenate';

CREATE OPERATOR < (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_lt,
	COMMUTATOR = > , NEGATOR = >= ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

COMMENT ON OPERATOR < (halfvec, halfvec) IS 'less than';

CREATE OPERATOR <= (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_le,
	COMMUTATOR = >= , NEGATOR = > ,
	RESTRICT = scalarlesel, JOIN = scalarlejoinsel
);

COMMENT ON OPERATOR <= (halfvec, halfvec) IS 'less than or equal';

CREATE OPERATOR = (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_eq,
	COMMUTATOR = = , NEGATOR = <> ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

COMMENT ON OPERATOR = (halfvec, halfvec) IS 'equal';

CREATE OPERATOR <> (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_ne,
	COMMUTATOR = <> , NEGATOR = = ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

COMMENT ON OPERATOR <> (halfvec, halfvec) IS 'not equal';

CREATE OPERATOR >= (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_ge,
	COMMUTATOR = <= , NEGATOR = < ,
	RESTRICT = scalargesel, JOIN = scalargejoinsel
);

COMMENT ON OPERATOR >= (halfvec, halfvec) IS 'greater than or equal';

CREATE OPERATOR > (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_gt,
	COMMUTATOR = < , NEGATOR = <= ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

COMMENT ON OPERATOR > (halfvec, halfvec) IS 'greater than';

-- halfvec opclasses

CREATE OPERATOR CLASS halfvec_ops
	DEFAULT FOR TYPE halfvec USING btree AS
	OPERATOR 1 < ,
	OPERATOR 2 <= ,
	OPERATOR 3 = ,
	OPERATOR 4 >= ,
	OPERATOR 5 > ,
	FUNCTION 1 halfvec_cmp(halfvec, halfvec);

CREATE OPERATOR CLASS halfvec_l2_ops
	FOR TYPE halfvec USING ivfflat AS
	OPERATOR 1 <-> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_l2_squared_distance(halfvec, halfvec),
	FUNCTION 3 l2_distance(halfvec, halfvec),
	FUNCTION 5 ivfflat_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_ip_ops
	FOR TYPE halfvec USING ivfflat AS
	OPERATOR 1 <#> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 3 halfvec_spherical_distance(halfvec, halfvec),
	FUNCTION 4 l2_norm(halfvec),
	FUNCTION 5 ivfflat_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_cosine_ops
	FOR TYPE halfvec USING ivfflat AS
	OPERATOR 1 <=> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 2 l2_norm(halfvec),
	FUNCTION 3 halfvec_spherical_distance(halfvec, halfvec),
	FUNCTION 4 l2_norm(halfvec),
	FUNCTION 5 ivfflat_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_l2_ops
	FOR TYPE halfvec USING hnsw AS
	OPERATOR 1 <-> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_l2_squared_distance(halfvec, halfvec),
	FUNCTION 3 hnsw_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_ip_ops
	FOR TYPE halfvec USING hnsw AS
	OPERATOR 1 <#> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 3 hnsw_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_cosine_ops
	FOR TYPE halfvec USING hnsw AS
	OPERATOR 1 <=> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 2 l2_norm(halfvec),
	FUNCTION 3 hnsw_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_l1_ops
	FOR TYPE halfvec USING hnsw AS
	OPERATOR 1 <+> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 l1_distance(halfvec, halfvec),
	FUNCTION 3 hnsw_halfvec_support(internal);

-- bit functions

CREATE FUNCTION hamming_distance(bit, bit) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION hamming_distance(bit, bit) IS 'Hamming distance';

CREATE FUNCTION jaccard_distance(bit, bit) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION jaccard_distance(bit, bit) IS 'Jaccard distance';

-- bit operators

CREATE OPERATOR <~> (
	LEFTARG = bit, RIGHTARG = bit, PROCEDURE = hamming_distance,
	COMMUTATOR = '<~>'
);

CREATE OPERATOR <%> (
	LEFTARG = bit, RIGHTARG = bit, PROCEDURE = jaccard_distance,
	COMMUTATOR = '<%>'
);

-- bit opclasses

CREATE OPERATOR CLASS bit_hamming_ops
	FOR TYPE bit USING ivfflat AS
	OPERATOR 1 <~> (bit, bit) FOR ORDER BY float_ops,
	FUNCTION 1 hamming_distance(bit, bit),
	FUNCTION 3 hamming_distance(bit, bit),
	FUNCTION 5 ivfflat_bit_support(internal);

CREATE OPERATOR CLASS bit_hamming_ops
	FOR TYPE bit USING hnsw AS
	OPERATOR 1 <~> (bit, bit) FOR ORDER BY float_ops,
	FUNCTION 1 hamming_distance(bit, bit),
	FUNCTION 3 hnsw_bit_support(internal);

CREATE OPERATOR CLASS bit_jaccard_ops
	FOR TYPE bit USING hnsw AS
	OPERATOR 1 <%> (bit, bit) FOR ORDER BY float_ops,
	FUNCTION 1 jaccard_distance(bit, bit),
	FUNCTION 3 hnsw_bit_support(internal);

-- sparsevec type

CREATE TYPE sparsevec;

CREATE FUNCTION sparsevec_in(cstring, oid, integer) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_in(cstring, oid, integer) IS 'I/O';

CREATE FUNCTION sparsevec_out(sparsevec) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_out(sparsevec) IS 'I/O';

CREATE FUNCTION sparsevec_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_typmod_in(cstring[]) IS 'I/O typmod';

CREATE FUNCTION sparsevec_recv(internal, oid, integer) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_recv(internal, oid, integer) IS 'I/O';

CREATE FUNCTION sparsevec_send(sparsevec) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_send(sparsevec) IS 'I/O';

CREATE TYPE sparsevec (
	INPUT     = sparsevec_in,
	OUTPUT    = sparsevec_out,
	TYPMOD_IN = sparsevec_typmod_in,
	RECEIVE   = sparsevec_recv,
	SEND      = sparsevec_send,
	STORAGE   = external
);

COMMENT ON TYPE sparsevec IS 'single-precision sparse vector';

-- sparsevec functions

CREATE FUNCTION l2_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_l2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION l2_distance(sparsevec, sparsevec) IS 'Euclidean distance';

CREATE FUNCTION inner_product(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION inner_product(sparsevec, sparsevec) IS 'inner product';

CREATE FUNCTION cosine_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_cosine_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION cosine_distance(sparsevec, sparsevec) IS 'cosine distance';

CREATE FUNCTION l1_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_l1_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION l1_distance(sparsevec, sparsevec) IS 'taxicab distance';

CREATE FUNCTION l2_norm(sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_l2_norm' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION l2_norm(sparsevec) IS 'Euclidean norm';

CREATE FUNCTION l2_normalize(sparsevec) RETURNS sparsevec
	AS 'MODULE_PATHNAME', 'sparsevec_l2_normalize' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION l2_normalize(sparsevec) IS 'normalize with Euclidean norm';

-- sparsevec private functions

CREATE FUNCTION sparsevec_lt(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_lt(sparsevec, sparsevec) IS 'implementation of < operator';

CREATE FUNCTION sparsevec_le(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_le(sparsevec, sparsevec) IS 'implementation of <= operator';

CREATE FUNCTION sparsevec_eq(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_eq(sparsevec, sparsevec) IS 'implementation of = operator';

CREATE FUNCTION sparsevec_ne(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_ne(sparsevec, sparsevec) IS 'implementation of <> operator';

CREATE FUNCTION sparsevec_ge(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_ge(sparsevec, sparsevec) IS 'implementation of >= operator';

CREATE FUNCTION sparsevec_gt(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_gt(sparsevec, sparsevec) IS 'implementation of > operator';

CREATE FUNCTION sparsevec_cmp(sparsevec, sparsevec) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_cmp(sparsevec, sparsevec) IS 'less-equal-greater';

CREATE FUNCTION sparsevec_l2_squared_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_l2_squared_distance(sparsevec, sparsevec) IS 'squared Euclidean distance';

CREATE FUNCTION sparsevec_negative_inner_product(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_negative_inner_product(sparsevec, sparsevec) IS 'negative inner product';

-- sparsevec cast functions

CREATE FUNCTION sparsevec(sparsevec, integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec(sparsevec, integer, boolean) IS 'adjust sparsevec to typmod';

CREATE FUNCTION vector_to_sparsevec(vector, integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_to_sparsevec(vector, integer, boolean) IS 'convert vector to sparsevec';

CREATE FUNCTION sparsevec_to_vector(sparsevec, integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_to_vector(sparsevec, integer, boolean) IS 'convert sparsevec to vector';

CREATE FUNCTION halfvec_to_sparsevec(halfvec, integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_to_sparsevec(halfvec, integer, boolean) IS 'convert halfvec to sparsevec';

CREATE FUNCTION sparsevec_to_halfvec(sparsevec, integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION sparsevec_to_halfvec(sparsevec, integer, boolean) IS 'convert sparsevec to halfvec';

CREATE FUNCTION array_to_sparsevec(integer[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_sparsevec(integer[], integer, boolean) IS 'convert int4 array to sparsevec';

CREATE FUNCTION array_to_sparsevec(real[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_sparsevec(real[], integer, boolean) IS 'convert float4 array to sparsevec';

CREATE FUNCTION array_to_sparsevec(double precision[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_sparsevec(double precision[], integer, boolean) IS 'convert float8 array to sparsevec';

CREATE FUNCTION array_to_sparsevec(numeric[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION array_to_sparsevec(numeric[], integer, boolean) IS 'convert numeric array to sparsevec';

-- sparsevec casts

CREATE CAST (sparsevec AS sparsevec)
	WITH FUNCTION sparsevec(sparsevec, integer, boolean) AS IMPLICIT;

CREATE CAST (sparsevec AS vector)
	WITH FUNCTION sparsevec_to_vector(sparsevec, integer, boolean) AS ASSIGNMENT;

CREATE CAST (vector AS sparsevec)
	WITH FUNCTION vector_to_sparsevec(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (sparsevec AS halfvec)
	WITH FUNCTION sparsevec_to_halfvec(sparsevec, integer, boolean) AS ASSIGNMENT;

CREATE CAST (halfvec AS sparsevec)
	WITH FUNCTION halfvec_to_sparsevec(halfvec, integer, boolean) AS IMPLICIT;

CREATE CAST (integer[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(numeric[], integer, boolean) AS ASSIGNMENT;

-- sparsevec operators

CREATE OPERATOR <-> (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

CREATE OPERATOR <+> (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = l1_distance,
	COMMUTATOR = '<+>'
);

CREATE OPERATOR < (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_lt,
	COMMUTATOR = > , NEGATOR = >= ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

COMMENT ON OPERATOR < (sparsevec, sparsevec) IS 'less than';

CREATE OPERATOR <= (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_le,
	COMMUTATOR = >= , NEGATOR = > ,
	RESTRICT = scalarlesel, JOIN = scalarlejoinsel
);

COMMENT ON OPERATOR <= (sparsevec, sparsevec) IS 'less than or equal';

CREATE OPERATOR = (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_eq,
	COMMUTATOR = = , NEGATOR = <> ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

COMMENT ON OPERATOR = (sparsevec, sparsevec) IS 'equal';

CREATE OPERATOR <> (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_ne,
	COMMUTATOR = <> , NEGATOR = = ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

COMMENT ON OPERATOR <> (sparsevec, sparsevec) IS 'not equal';

CREATE OPERATOR >= (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_ge,
	COMMUTATOR = <= , NEGATOR = < ,
	RESTRICT = scalargesel, JOIN = scalargejoinsel
);

COMMENT ON OPERATOR >= (sparsevec, sparsevec) IS 'greater than or equal';

CREATE OPERATOR > (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_gt,
	COMMUTATOR = < , NEGATOR = <= ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

COMMENT ON OPERATOR > (sparsevec, sparsevec) IS 'greater than';

-- sparsevec opclasses

CREATE OPERATOR CLASS sparsevec_ops
	DEFAULT FOR TYPE sparsevec USING btree AS
	OPERATOR 1 < ,
	OPERATOR 2 <= ,
	OPERATOR 3 = ,
	OPERATOR 4 >= ,
	OPERATOR 5 > ,
	FUNCTION 1 sparsevec_cmp(sparsevec, sparsevec);

CREATE OPERATOR CLASS sparsevec_l2_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 <-> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_l2_squared_distance(sparsevec, sparsevec),
	FUNCTION 3 hnsw_sparsevec_support(internal);

CREATE OPERATOR CLASS sparsevec_ip_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 <#> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_negative_inner_product(sparsevec, sparsevec),
	FUNCTION 3 hnsw_sparsevec_support(internal);

CREATE OPERATOR CLASS sparsevec_cosine_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 <=> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_negative_inner_product(sparsevec, sparsevec),
	FUNCTION 2 l2_norm(sparsevec),
	FUNCTION 3 hnsw_sparsevec_support(internal);

CREATE OPERATOR CLASS sparsevec_l1_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 <+> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 l1_distance(sparsevec, sparsevec),
	FUNCTION 3 hnsw_sparsevec_support(internal);
