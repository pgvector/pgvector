-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.1.8'" to load this file. \quit

CREATE FUNCTION vector_to_float4(vector, integer, boolean) RETURNS real[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE CAST (vector AS real[])
	WITH FUNCTION vector_to_float4(vector, integer, boolean) AS IMPLICIT;

CREATE FUNCTION vector_float_add(vector, real) RETURNS vector
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION float_vector_add(real, vector) RETURNS vector
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_float_sub(vector, real) RETURNS vector
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION float_vector_sub(real, vector) RETURNS vector
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_float_mul(vector, real) RETURNS vector
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION float_vector_mul(real, vector) RETURNS vector
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_float_div(vector, real) RETURNS vector
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION float_vector_div(real, vector) RETURNS vector
    AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR + (
	LEFTARG = vector, RIGHTARG = real, PROCEDURE = vector_float_add,
	COMMUTATOR = +
);

CREATE OPERATOR + (
    LEFTARG = real, RIGHTARG = vector, PROCEDURE = float_vector_add,
    COMMUTATOR = +
);

CREATE OPERATOR - (
	LEFTARG = vector, RIGHTARG = real, PROCEDURE = vector_float_sub,
	COMMUTATOR = -
);

CREATE OPERATOR - (
    LEFTARG = real, RIGHTARG = vector, PROCEDURE = float_vector_sub,
    COMMUTATOR = -
);

CREATE OPERATOR * (
	LEFTARG = vector, RIGHTARG = real, PROCEDURE = vector_float_mul,
	COMMUTATOR = *
);

CREATE OPERATOR * (
    LEFTARG = real, RIGHTARG = vector, PROCEDURE = float_vector_mul,
    COMMUTATOR = *
);

CREATE OPERATOR / (
	LEFTARG = vector, RIGHTARG = real, PROCEDURE = vector_float_div,
	COMMUTATOR = /
);

CREATE OPERATOR / (
    LEFTARG = real, RIGHTARG = vector, PROCEDURE = float_vector_div,
    COMMUTATOR = /
);

CREATE AGGREGATE sum (vector)
(
    sfunc = vector_add,
    stype = vector,
    combinefunc = vector_add,
    parallel = safe
);

/*
 * avg() aggregate function
 */
CREATE TYPE vector_avg_accum AS (
    sum vector,
    count bigint
);

CREATE FUNCTION vector_avg_accum(a vector_avg_accum, b vector) RETURNS vector_avg_accum AS 
$func$
    SELECT 
        CASE WHEN a IS NULL AND b IS NULL THEN NULL ELSE (
            CASE WHEN b is NULL               THEN a.sum
                 WHEN a IS NULL               THEN b
                 WHEN a.count != 0            THEN a.sum + b
                 ELSE                              b
                 END,
            CASE WHEN a IS NULL               THEN 1
                 WHEN b IS NULL               THEN a.count
                 ELSE                              a.count + 1
                 END
            )::vector_avg_accum END;
$func$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION vector_avg_combine(a vector_avg_accum, b vector_avg_accum) RETURNS vector_avg_accum AS 
$func$
    SELECT
        CASE WHEN a IS NULL AND b IS NULL THEN NULL ELSE (
            CASE WHEN a IS NULL                     THEN b.sum
                 WHEN b is NULL                     THEN a.sum
                 WHEN a.count != 0 AND b.count != 0 THEN a.sum + b.sum
                 WHEN a.count != 0                  THEN a.sum
                 ELSE                                    b.sum
                 END,
            CASE WHEN a IS NULL THEN b.count
                 WHEN b IS NULL THEN a.count
                 ELSE                a.count + b.count
                 END
            )::vector_avg_accum END;
$func$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION vector_avg_final(a vector_avg_accum) RETURNS vector AS 
$func$
    SELECT a.sum / a.count;
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE avg (vector)
(
    sfunc = vector_avg_accum,
    stype = vector_avg_accum,
    finalfunc = vector_avg_final,
    combinefunc = vector_avg_combine,
    parallel = safe
);
