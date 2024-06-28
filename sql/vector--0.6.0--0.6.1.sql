-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.6.1'" to load this file. \quit

DROP OPERATOR - (vector, vector);

CREATE OPERATOR - (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_sub
);

ALTER OPERATOR <= (vector, vector) SET (
	RESTRICT = scalarlesel, JOIN = scalarlejoinsel
);

ALTER OPERATOR >= (vector, vector) SET (
	RESTRICT = scalargesel, JOIN = scalargejoinsel
);
