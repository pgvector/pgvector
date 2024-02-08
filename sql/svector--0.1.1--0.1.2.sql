\echo Use "ALTER EXTENSION svector UPDATE TO '0.1.2'" to load this file. \quit

CREATE FUNCTION array_to_svector(numeric[], integer, boolean) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE CAST (numeric[] AS svector)
	WITH FUNCTION array_to_svector(numeric[], integer, boolean) AS IMPLICIT;
