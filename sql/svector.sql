\echo Use "CREATE EXTENSION svector" to load this file. \quit

-- type

CREATE TYPE svector;

CREATE FUNCTION svector_in(cstring, oid, integer) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_out(svector) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_recv(internal, oid, integer) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_send(svector) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE svector (
	INPUT     = svector_in,
	OUTPUT    = svector_out,
	TYPMOD_IN = svector_typmod_in,
	RECEIVE   = svector_recv,
	SEND      = svector_send,
	STORAGE   = extended
);

-- functions

CREATE FUNCTION l2_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l1_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_dims(svector) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svectorn_elem(svector) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_norm(svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_add(svector, svector) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_sub(svector, svector) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_mul(svector, svector) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- private functions

CREATE FUNCTION svector_lt(svector, svector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_le(svector, svector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_eq(svector, svector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_ne(svector, svector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_ge(svector, svector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_gt(svector, svector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_cmp(svector, svector) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_l2_squared_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_negative_inner_product(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_spherical_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE sum(svector) (
	SFUNC = svector_add,
	STYPE = svector,
	COMBINEFUNC = svector_add,
	PARALLEL = SAFE
);

-- cast functions

CREATE FUNCTION svector(svector, integer, boolean) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_svector(integer[], integer, boolean) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_svector(real[], integer, boolean) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_svector(double precision[], integer, boolean) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_svector(numeric[], integer, boolean) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_to_float4(svector, integer, boolean) RETURNS real[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- casts

CREATE CAST (svector AS svector)
	WITH FUNCTION svector(svector, integer, boolean) AS IMPLICIT;

CREATE CAST (svector AS real[])
	WITH FUNCTION svector_to_float4(svector, integer, boolean) AS IMPLICIT;

CREATE CAST (integer[] AS svector)
	WITH FUNCTION array_to_svector(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS svector)
	WITH FUNCTION array_to_svector(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS svector)
	WITH FUNCTION array_to_svector(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS svector)
	WITH FUNCTION array_to_svector(numeric[], integer, boolean) AS ASSIGNMENT;

-- operators

CREATE OPERATOR <-> (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

CREATE OPERATOR + (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_add,
	COMMUTATOR = +
);

CREATE OPERATOR - (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_sub,
	COMMUTATOR = -
);

CREATE OPERATOR * (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_mul,
	COMMUTATOR = *
);

CREATE OPERATOR < (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_lt,
	COMMUTATOR = > , NEGATOR = >= ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

-- should use scalarlesel and scalarlejoinsel, but not supported in Postgres < 11
CREATE OPERATOR <= (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_le,
	COMMUTATOR = >= , NEGATOR = > ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR = (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_eq,
	COMMUTATOR = = , NEGATOR = <> ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR <> (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_ne,
	COMMUTATOR = <> , NEGATOR = = ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

-- should use scalargesel and scalargejoinsel, but not supported in Postgres < 11
CREATE OPERATOR >= (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_ge,
	COMMUTATOR = <= , NEGATOR = < ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR > (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_gt,
	COMMUTATOR = < , NEGATOR = <= ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

-- access methods

CREATE FUNCTION shnswhandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD shnsw TYPE INDEX HANDLER shnswhandler;

COMMENT ON ACCESS METHOD shnsw IS 'shnsw index access method';

-- opclasses

CREATE OPERATOR CLASS svectorops
	DEFAULT FOR TYPE svector USING btree AS
	OPERATOR 1 < ,
	OPERATOR 2 <= ,
	OPERATOR 3 = ,
	OPERATOR 4 >= ,
	OPERATOR 5 > ,
	FUNCTION 1 svector_cmp(svector, svector);


CREATE OPERATOR CLASS svector_l2_ops
	FOR TYPE svector USING shnsw AS
	OPERATOR 1 <-> (svector, svector) FOR ORDER BY float_ops,
	FUNCTION 1 svector_l2_squared_distance(svector, svector);

CREATE OPERATOR CLASS svector_ip_ops
	FOR TYPE svector USING shnsw AS
	OPERATOR 1 <#> (svector, svector) FOR ORDER BY float_ops,
	FUNCTION 1 svector_negative_inner_product(svector, svector);

CREATE OPERATOR CLASS svector_cosine_ops
	FOR TYPE svector USING shnsw AS
	OPERATOR 1 <=> (svector, svector) FOR ORDER BY float_ops,
	FUNCTION 1 svector_negative_inner_product(svector, svector),
	FUNCTION 2 svector_norm(svector);
