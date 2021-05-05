-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.1.1'" to load this file. \quit

CREATE FUNCTION vector_recv(internal, oid, integer) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION vector_send(vector) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

ALTER TYPE vector SET ( RECEIVE = vector_recv, SEND = vector_send );

-- functions

ALTER FUNCTION vector_in(cstring, oid, integer) PARALLEL SAFE;
ALTER FUNCTION vector_out(vector) PARALLEL SAFE;
ALTER FUNCTION vector_typmod_in(cstring[]) PARALLEL SAFE;
ALTER FUNCTION vector_recv(internal, oid, integer) PARALLEL SAFE;
ALTER FUNCTION vector_send(vector) PARALLEL SAFE;
ALTER FUNCTION l2_distance(vector, vector) PARALLEL SAFE;
ALTER FUNCTION inner_product(vector, vector) PARALLEL SAFE;
ALTER FUNCTION cosine_distance(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_dims(vector) PARALLEL SAFE;
ALTER FUNCTION vector_norm(vector) PARALLEL SAFE;
ALTER FUNCTION vector_add(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_sub(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_lt(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_le(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_eq(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_ne(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_ge(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_gt(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_cmp(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_l2_squared_distance(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_negative_inner_product(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector_spherical_distance(vector, vector) PARALLEL SAFE;
ALTER FUNCTION vector(vector, integer, boolean) PARALLEL SAFE;
ALTER FUNCTION array_to_vector(integer[], integer, boolean) PARALLEL SAFE;
ALTER FUNCTION array_to_vector(real[], integer, boolean) PARALLEL SAFE;
ALTER FUNCTION array_to_vector(double precision[], integer, boolean) PARALLEL SAFE;
