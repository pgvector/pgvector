-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.7.0'" to load this file. \quit

CREATE FUNCTION l2_normalize(vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION binary_quantize(vector) RETURNS bit
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION subvector(vector, int, int) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_concat(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR <+> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = l1_distance,
	COMMUTATOR = '<+>'
);

CREATE OPERATOR || (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_concat
);

CREATE FUNCTION ivfflat_halfvec_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION ivfflat_bit_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION hnsw_halfvec_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION hnsw_bit_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION hnsw_sparsevec_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE OPERATOR CLASS vector_l1_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <+> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 l1_distance(vector, vector);

CREATE TYPE halfvec;

CREATE FUNCTION halfvec_in(cstring, oid, integer) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_out(halfvec) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_recv(internal, oid, integer) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_send(halfvec) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE halfvec (
	INPUT     = halfvec_in,
	OUTPUT    = halfvec_out,
	TYPMOD_IN = halfvec_typmod_in,
	RECEIVE   = halfvec_recv,
	SEND      = halfvec_send,
	STORAGE   = external
);

CREATE FUNCTION l2_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_l2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_cosine_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l1_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_l1_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_dims(halfvec) RETURNS integer
	AS 'MODULE_PATHNAME', 'halfvec_vector_dims' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l2_norm(halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_l2_norm' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l2_normalize(halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME', 'halfvec_l2_normalize' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION binary_quantize(halfvec) RETURNS bit
	AS 'MODULE_PATHNAME', 'halfvec_binary_quantize' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION subvector(halfvec, int, int) RETURNS halfvec
	AS 'MODULE_PATHNAME', 'halfvec_subvector' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_add(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_sub(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_mul(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_concat(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_lt(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_le(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_eq(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_ne(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_ge(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_gt(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_cmp(halfvec, halfvec) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_l2_squared_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_negative_inner_product(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_spherical_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_accum(double precision[], halfvec) RETURNS double precision[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_avg(double precision[]) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_combine(double precision[], double precision[]) RETURNS double precision[]
	AS 'MODULE_PATHNAME', 'vector_combine' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE avg(halfvec) (
	SFUNC = halfvec_accum,
	STYPE = double precision[],
	FINALFUNC = halfvec_avg,
	COMBINEFUNC = halfvec_combine,
	INITCOND = '{0}',
	PARALLEL = SAFE
);

CREATE AGGREGATE sum(halfvec) (
	SFUNC = halfvec_add,
	STYPE = halfvec,
	COMBINEFUNC = halfvec_add,
	PARALLEL = SAFE
);

CREATE FUNCTION halfvec(halfvec, integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_to_vector(halfvec, integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_to_halfvec(vector, integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_halfvec(integer[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_halfvec(real[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_halfvec(double precision[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_halfvec(numeric[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_to_float4(halfvec, integer, boolean) RETURNS real[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

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

CREATE OPERATOR - (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_sub
);

CREATE OPERATOR * (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_mul,
	COMMUTATOR = *
);

CREATE OPERATOR || (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_concat
);

CREATE OPERATOR < (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_lt,
	COMMUTATOR = > , NEGATOR = >= ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_le,
	COMMUTATOR = >= , NEGATOR = > ,
	RESTRICT = scalarlesel, JOIN = scalarlejoinsel
);

CREATE OPERATOR = (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_eq,
	COMMUTATOR = = , NEGATOR = <> ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR <> (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_ne,
	COMMUTATOR = <> , NEGATOR = = ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR >= (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_ge,
	COMMUTATOR = <= , NEGATOR = < ,
	RESTRICT = scalargesel, JOIN = scalargejoinsel
);

CREATE OPERATOR > (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_gt,
	COMMUTATOR = < , NEGATOR = <= ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

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

CREATE FUNCTION hamming_distance(bit, bit) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION jaccard_distance(bit, bit) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR <~> (
	LEFTARG = bit, RIGHTARG = bit, PROCEDURE = hamming_distance,
	COMMUTATOR = '<~>'
);

CREATE OPERATOR <%> (
	LEFTARG = bit, RIGHTARG = bit, PROCEDURE = jaccard_distance,
	COMMUTATOR = '<%>'
);

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

CREATE TYPE sparsevec;

CREATE FUNCTION sparsevec_in(cstring, oid, integer) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_out(sparsevec) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_recv(internal, oid, integer) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_send(sparsevec) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE sparsevec (
	INPUT     = sparsevec_in,
	OUTPUT    = sparsevec_out,
	TYPMOD_IN = sparsevec_typmod_in,
	RECEIVE   = sparsevec_recv,
	SEND      = sparsevec_send,
	STORAGE   = external
);

CREATE FUNCTION l2_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_l2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_cosine_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l1_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_l1_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l2_norm(sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_l2_norm' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l2_normalize(sparsevec) RETURNS sparsevec
	AS 'MODULE_PATHNAME', 'sparsevec_l2_normalize' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_lt(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_le(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_eq(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_ne(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_ge(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_gt(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_cmp(sparsevec, sparsevec) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_l2_squared_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_negative_inner_product(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec(sparsevec, integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_to_sparsevec(vector, integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_to_vector(sparsevec, integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_to_sparsevec(halfvec, integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_to_halfvec(sparsevec, integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

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

CREATE OPERATOR <= (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_le,
	COMMUTATOR = >= , NEGATOR = > ,
	RESTRICT = scalarlesel, JOIN = scalarlejoinsel
);

CREATE OPERATOR = (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_eq,
	COMMUTATOR = = , NEGATOR = <> ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR <> (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_ne,
	COMMUTATOR = <> , NEGATOR = = ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR >= (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_ge,
	COMMUTATOR = <= , NEGATOR = < ,
	RESTRICT = scalargesel, JOIN = scalargejoinsel
);

CREATE OPERATOR > (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_gt,
	COMMUTATOR = < , NEGATOR = <= ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

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
