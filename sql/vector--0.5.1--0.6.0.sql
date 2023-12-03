-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.6.0'" to load this file. \quit

CREATE TYPE half;

CREATE FUNCTION half_in(cstring, oid, integer) RETURNS half
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION half_out(half) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION half_recv(internal, oid, integer) RETURNS half
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION half_send(half) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE half (
	INPUT     = half_in,
	OUTPUT    = half_out,
	RECEIVE   = half_recv,
	SEND      = half_send,
	INTERNALLENGTH = 2,
	PASSEDBYVALUE,
	ALIGNMENT = int2
);

CREATE FUNCTION l2_distance(half[], half[]) RETURNS float8
	AS 'MODULE_PATHNAME', 'half_l2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(half[], half[]) RETURNS float8
	AS 'MODULE_PATHNAME', 'half_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(half[], half[]) RETURNS float8
	AS 'MODULE_PATHNAME', 'half_cosine_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l1_distance(half[], half[]) RETURNS float8
	AS 'MODULE_PATHNAME', 'half_l1_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION half_l2_squared_distance(half[], half[]) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION half_negative_inner_product(half[], half[]) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION float4_to_half(real, integer, boolean) RETURNS half
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION integer_to_half(integer, integer, boolean) RETURNS half
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION numeric_to_half(numeric, integer, boolean) RETURNS half
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE CAST (real AS half)
	WITH FUNCTION float4_to_half(real, integer, boolean) AS IMPLICIT;

CREATE CAST (integer AS half)
	WITH FUNCTION integer_to_half(integer, integer, boolean) AS IMPLICIT;

CREATE CAST (numeric AS half)
	WITH FUNCTION numeric_to_half(numeric, integer, boolean) AS IMPLICIT;

CREATE OPERATOR <-> (
	LEFTARG = half[], RIGHTARG = half[], PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = half[], RIGHTARG = half[], PROCEDURE = half_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = half[], RIGHTARG = half[], PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

CREATE OPERATOR CLASS half_l2_ops
	FOR TYPE half[] USING hnsw AS
	OPERATOR 1 <-> (half[], half[]) FOR ORDER BY float_ops,
	FUNCTION 1 half_l2_squared_distance(half[], half[]);
