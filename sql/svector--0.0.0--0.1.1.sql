\echo Use "ALTER EXTENSION svector UPDATE TO '0.1.1'" to load this file. \quit

CREATE FUNCTION svector_recv(internal, oid, integer) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION svector_send(svector) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

ALTER TYPE svector SET ( RECEIVE = svector_recv, SEND = svector_send );

-- functions

ALTER FUNCTION svector_in(cstring, oid, integer) PARALLEL SAFE;
ALTER FUNCTION svector_out(svector) PARALLEL SAFE;
ALTER FUNCTION svector_typmod_in(cstring[]) PARALLEL SAFE;
ALTER FUNCTION svector_recv(internal, oid, integer) PARALLEL SAFE;
ALTER FUNCTION svector_send(svector) PARALLEL SAFE;
ALTER FUNCTION l2_distance(svector, svector) PARALLEL SAFE;
ALTER FUNCTION inner_product(svector, svector) PARALLEL SAFE;
ALTER FUNCTION cosine_distance(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_dims(svector) PARALLEL SAFE;
ALTER FUNCTION svectorn_elem(svector) PARALLEL SAFE;
ALTER FUNCTION svector_norm(svector) PARALLEL SAFE;
ALTER FUNCTION svector_add(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_sub(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_lt(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_le(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_eq(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_ne(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_ge(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_gt(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_cmp(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_l2_squared_distance(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_negative_inner_product(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector_spherical_distance(svector, svector) PARALLEL SAFE;
ALTER FUNCTION svector(svector, integer, boolean) PARALLEL SAFE;
ALTER FUNCTION array_to_svector(integer[], integer, boolean) PARALLEL SAFE;
ALTER FUNCTION array_to_svector(real[], integer, boolean) PARALLEL SAFE;
ALTER FUNCTION array_to_svector(double precision[], integer, boolean) PARALLEL SAFE;
