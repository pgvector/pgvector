-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.5.2'" to load this file. \quit

CREATE TYPE svector;

CREATE FUNCTION svector_in(cstring, oid, integer) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_out(svector) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_recv(internal, oid, integer) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_send(svector) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE svector (
	INPUT     = svector_in,
	OUTPUT    = svector_out,
	TYPMOD_IN = svector_typmod_in,
	RECEIVE   = svector_recv,
	SEND      = svector_send,
	STORAGE   = external
);

CREATE FUNCTION l2_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME', 'svector_l2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME', 'svector_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME', 'svector_cosine_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION jaccard_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME', 'svector_jaccard_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_l2_squared_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_negative_inner_product(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector(svector, integer, boolean) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_to_svector(vector, integer, boolean) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_to_vector(svector, integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE CAST (svector AS svector)
	WITH FUNCTION svector(svector, integer, boolean) AS IMPLICIT;

CREATE CAST (svector AS vector)
	WITH FUNCTION svector_to_vector(svector, integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS svector)
	WITH FUNCTION vector_to_svector(vector, integer, boolean) AS IMPLICIT;

CREATE OPERATOR <-> (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);
