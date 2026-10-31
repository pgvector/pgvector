-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.9.0'" to load this file. \quit

CREATE OR REPLACE FUNCTION vector_dims(sparsevec) RETURNS integer
    AS 'MODULE_PATHNAME', 'sparsevec_vector_dims' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_dims(sparsevec) IS 'number of dimensions';

-- Experimental exact CPU staging access method

CREATE FUNCTION annhandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION annhandler(internal) IS 'ann index access method handler';

CREATE ACCESS METHOD ann TYPE INDEX HANDLER annhandler;

COMMENT ON ACCESS METHOD ann IS 'experimental exact vector staging index';

CREATE OPERATOR CLASS vector_l2_ops
	FOR TYPE vector USING ann AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 l2_distance(vector, vector);
