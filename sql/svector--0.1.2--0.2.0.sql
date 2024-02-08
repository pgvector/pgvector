\echo Use "ALTER EXTENSION svector UPDATE TO '0.2.0'" to load this file. \quit

CREATE FUNCTION svector_to_float4(svector, integer, boolean) RETURNS real[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE CAST (svector AS real[])
	WITH FUNCTION svector_to_float4(svector, integer, boolean) AS IMPLICIT;
