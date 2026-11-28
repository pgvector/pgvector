-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.9.0'" to load this file. \quit

CREATE FUNCTION vector_dims(sparsevec) RETURNS integer
    AS 'MODULE_PATHNAME', 'sparsevec_vector_dims' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION vector_dims(sparsevec) IS 'number of dimensions';

CREATE FUNCTION halfvec_sum(double precision[]) RETURNS halfvec
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION halfvec_sum(double precision[]) IS 'aggregate final function';

DROP AGGREGATE sum(halfvec);

CREATE AGGREGATE sum(halfvec) (
    SFUNC = halfvec_accum,
    STYPE = double precision[],
    FINALFUNC = halfvec_sum,
    COMBINEFUNC = halfvec_combine,
    INITCOND = '{0}',
    PARALLEL = SAFE
);

COMMENT ON AGGREGATE sum(halfvec) IS 'sum as halfvec across all halfvec input values';
