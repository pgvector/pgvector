-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.8.0'" to load this file. \quit

CREATE TYPE minivec;

CREATE FUNCTION minivec_in(cstring, oid, integer) RETURNS minivec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION minivec_out(minivec) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION minivec_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION minivec_recv(internal, oid, integer) RETURNS minivec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION minivec_send(minivec) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE minivec (
	INPUT     = minivec_in,
	OUTPUT    = minivec_out,
	TYPMOD_IN = minivec_typmod_in,
	RECEIVE   = minivec_recv,
	SEND      = minivec_send,
	STORAGE   = external
);

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
