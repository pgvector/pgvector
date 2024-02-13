-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.7.0'" to load this file. \quit

CREATE OPERATOR CLASS vector_integer_ops
	DEFAULT FOR TYPE integer USING hnsw AS
	OPERATOR 2 = (integer, integer);

CREATE OPERATOR CLASS vector_text_ops
	DEFAULT FOR TYPE text USING hnsw AS
	OPERATOR 2 = (text, text);
