-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.4.1'" to load this file. \quit

CREATE FUNCTION random_vector(integer) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE STRICT PARALLEL SAFE;
