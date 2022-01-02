-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.2.1'" to load this file. \quit

DROP CAST (integer[] AS vector);
DROP CAST (real[] AS vector);
DROP CAST (double precision[] AS vector);
DROP CAST (numeric[] AS vector);

CREATE CAST (integer[] AS vector)
  WITH FUNCTION array_to_vector(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS vector)
  WITH FUNCTION array_to_vector(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS vector)
  WITH FUNCTION array_to_vector(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS vector)
  WITH FUNCTION array_to_vector(numeric[], integer, boolean) AS ASSIGNMENT;
