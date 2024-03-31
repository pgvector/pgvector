-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.7.0'" to load this file. \quit

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

CREATE FUNCTION sparsevec_norm(sparsevec) RETURNS float8
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

CREATE CAST (sparsevec AS sparsevec)
	WITH FUNCTION sparsevec(sparsevec, integer, boolean) AS IMPLICIT;

CREATE CAST (sparsevec AS vector)
	WITH FUNCTION sparsevec_to_vector(sparsevec, integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS sparsevec)
	WITH FUNCTION vector_to_sparsevec(vector, integer, boolean) AS IMPLICIT;

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

CREATE OPERATOR CLASS sparsevec_l2_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 <-> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_l2_squared_distance(sparsevec, sparsevec);

CREATE OPERATOR CLASS sparsevec_ip_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 <#> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_negative_inner_product(sparsevec, sparsevec);

CREATE OPERATOR CLASS sparsevec_cosine_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 <=> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_negative_inner_product(sparsevec, sparsevec),
	FUNCTION 2 sparsevec_norm(sparsevec);
