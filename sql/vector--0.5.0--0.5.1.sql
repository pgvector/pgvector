-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.5.1'" to load this file. \quit

-- tinyint

CREATE TYPE tinyint;

CREATE FUNCTION tinyint_in(cstring, oid, integer) RETURNS tinyint
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION tinyint_out(tinyint) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION tinyint_recv(internal, oid, integer) RETURNS tinyint
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION tinyint_send(tinyint) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE tinyint (
	INPUT     = tinyint_in,
	OUTPUT    = tinyint_out,
	RECEIVE   = tinyint_recv,
	SEND      = tinyint_send,
	INTERNALLENGTH = 1,
	PASSEDBYVALUE,
	ALIGNMENT = char
);

CREATE FUNCTION integer_to_tinyint(integer, integer, boolean) RETURNS tinyint
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION numeric_to_tinyint(numeric, integer, boolean) RETURNS tinyint
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE CAST (integer AS tinyint)
	WITH FUNCTION integer_to_tinyint(integer, integer, boolean) AS IMPLICIT;

CREATE CAST (numeric AS tinyint)
	WITH FUNCTION numeric_to_tinyint(numeric, integer, boolean) AS IMPLICIT;

CREATE FUNCTION l2_distance(tinyint[], tinyint[]) RETURNS float8
	AS 'MODULE_PATHNAME', 'tinyint_l2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(tinyint[], tinyint[]) RETURNS float8
	AS 'MODULE_PATHNAME', 'tinyint_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(tinyint[], tinyint[]) RETURNS float8
	AS 'MODULE_PATHNAME', 'tinyint_cosine_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION tinyint_negative_inner_product(tinyint[], tinyint[]) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR <-> (
	LEFTARG = tinyint[], RIGHTARG = tinyint[], PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = tinyint[], RIGHTARG = tinyint[], PROCEDURE = tinyint_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = tinyint[], RIGHTARG = tinyint[], PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);
