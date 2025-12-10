-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.9.0'" to load this file. \quit

CREATE FUNCTION hnsw_attribute_distance(integer, integer) RETURNS float8
	AS 'MODULE_PATHNAME', 'hnsw_int4_attribute_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS vector_integer_ops
	DEFAULT FOR TYPE integer USING hnsw AS
	OPERATOR 2 = (integer, integer),
	FUNCTION 4 hnsw_attribute_distance(integer, integer);
