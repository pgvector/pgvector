\echo Use "ALTER EXTENSION svector UPDATE TO '0.3.0'" to load this file. \quit

CREATE FUNCTION l1_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_mul(svector, svector) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR * (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_mul,
	COMMUTATOR = *
);

CREATE AGGREGATE sum(svector) (
	SFUNC = svector_add,
	STYPE = svector,
	COMBINEFUNC = svector_add,
	PARALLEL = SAFE
);

CREATE FUNCTION shnswhandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD shnsw TYPE INDEX HANDLER shnswhandler;

COMMENT ON ACCESS METHOD shnsw IS 'shnsw index access method';

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
