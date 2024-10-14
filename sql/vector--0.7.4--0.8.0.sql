-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.8.0'" to load this file. \quit

CREATE FUNCTION hnsw_intvec_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE TYPE intvec;

CREATE FUNCTION intvec_in(cstring, oid, integer) RETURNS intvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_out(intvec) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_recv(internal, oid, integer) RETURNS intvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_send(intvec) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE intvec (
	INPUT     = intvec_in,
	OUTPUT    = intvec_out,
	TYPMOD_IN = intvec_typmod_in,
	RECEIVE   = intvec_recv,
	SEND      = intvec_send,
	STORAGE   = external
);

CREATE FUNCTION l2_distance(intvec, intvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'intvec_l2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(intvec, intvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'intvec_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(intvec, intvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'intvec_cosine_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l1_distance(intvec, intvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'intvec_l1_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_dims(intvec) RETURNS integer
	AS 'MODULE_PATHNAME', 'intvec_vector_dims' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l2_norm(intvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'intvec_l2_norm' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_lt(intvec, intvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_le(intvec, intvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_eq(intvec, intvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_ne(intvec, intvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_ge(intvec, intvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_gt(intvec, intvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_cmp(intvec, intvec) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_l2_squared_distance(intvec, intvec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec_negative_inner_product(intvec, intvec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION intvec(intvec, integer, boolean) RETURNS intvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_intvec(integer[], integer, boolean) RETURNS intvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE CAST (intvec AS intvec)
	WITH FUNCTION intvec(intvec, integer, boolean) AS IMPLICIT;

CREATE CAST (integer[] AS intvec)
	WITH FUNCTION array_to_intvec(integer[], integer, boolean) AS ASSIGNMENT;

CREATE OPERATOR <-> (
	LEFTARG = intvec, RIGHTARG = intvec, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = intvec, RIGHTARG = intvec, PROCEDURE = intvec_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = intvec, RIGHTARG = intvec, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

CREATE OPERATOR <+> (
	LEFTARG = intvec, RIGHTARG = intvec, PROCEDURE = l1_distance,
	COMMUTATOR = '<+>'
);

CREATE OPERATOR < (
	LEFTARG = intvec, RIGHTARG = intvec, PROCEDURE = intvec_lt,
	COMMUTATOR = > , NEGATOR = >= ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
	LEFTARG = intvec, RIGHTARG = intvec, PROCEDURE = intvec_le,
	COMMUTATOR = >= , NEGATOR = > ,
	RESTRICT = scalarlesel, JOIN = scalarlejoinsel
);

CREATE OPERATOR = (
	LEFTARG = intvec, RIGHTARG = intvec, PROCEDURE = intvec_eq,
	COMMUTATOR = = , NEGATOR = <> ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR <> (
	LEFTARG = intvec, RIGHTARG = intvec, PROCEDURE = intvec_ne,
	COMMUTATOR = <> , NEGATOR = = ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR >= (
	LEFTARG = intvec, RIGHTARG = intvec, PROCEDURE = intvec_ge,
	COMMUTATOR = <= , NEGATOR = < ,
	RESTRICT = scalargesel, JOIN = scalargejoinsel
);

CREATE OPERATOR > (
	LEFTARG = intvec, RIGHTARG = intvec, PROCEDURE = intvec_gt,
	COMMUTATOR = < , NEGATOR = <= ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR CLASS intvec_ops
	DEFAULT FOR TYPE intvec USING btree AS
	OPERATOR 1 < ,
	OPERATOR 2 <= ,
	OPERATOR 3 = ,
	OPERATOR 4 >= ,
	OPERATOR 5 > ,
	FUNCTION 1 intvec_cmp(intvec, intvec);

CREATE OPERATOR CLASS intvec_l2_ops
	FOR TYPE intvec USING hnsw AS
	OPERATOR 1 <-> (intvec, intvec) FOR ORDER BY float_ops,
	FUNCTION 1 intvec_l2_squared_distance(intvec, intvec),
	FUNCTION 3 hnsw_intvec_support(internal);

CREATE OPERATOR CLASS intvec_ip_ops
	FOR TYPE intvec USING hnsw AS
	OPERATOR 1 <#> (intvec, intvec) FOR ORDER BY float_ops,
	FUNCTION 1 intvec_negative_inner_product(intvec, intvec),
	FUNCTION 3 hnsw_intvec_support(internal);

CREATE OPERATOR CLASS intvec_cosine_ops
	FOR TYPE intvec USING hnsw AS
	OPERATOR 1 <=> (intvec, intvec) FOR ORDER BY float_ops,
	FUNCTION 1 cosine_distance(intvec, intvec),
	FUNCTION 2 l2_norm(intvec),
	FUNCTION 3 hnsw_intvec_support(internal);

CREATE OPERATOR CLASS intvec_l1_ops
	FOR TYPE intvec USING hnsw AS
	OPERATOR 1 <+> (intvec, intvec) FOR ORDER BY float_ops,
	FUNCTION 1 l1_distance(intvec, intvec),
	FUNCTION 3 hnsw_intvec_support(internal);

CREATE FUNCTION array_to_sparsevec(integer[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_sparsevec(real[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_sparsevec(double precision[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_sparsevec(numeric[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE CAST (integer[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(numeric[], integer, boolean) AS ASSIGNMENT;
