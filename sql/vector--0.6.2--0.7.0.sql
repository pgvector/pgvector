-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.7.0'" to load this file. \quit

CREATE FUNCTION vector_subscript(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

ALTER TYPE vector SET (SUBSCRIPT = vector_subscript);
