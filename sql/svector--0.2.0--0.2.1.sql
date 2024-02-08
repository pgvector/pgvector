\echo Use "ALTER EXTENSION svector UPDATE TO '0.2.1'" to load this file. \quit

DROP CAST (integer[] AS svector);
DROP CAST (real[] AS svector);
DROP CAST (double precision[] AS svector);
DROP CAST (numeric[] AS svector);

CREATE CAST (integer[] AS svector)
	WITH FUNCTION array_to_svector(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS svector)
	WITH FUNCTION array_to_svector(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS svector)
	WITH FUNCTION array_to_svector(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS svector)
	WITH FUNCTION array_to_svector(numeric[], integer, boolean) AS ASSIGNMENT;
