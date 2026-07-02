-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.8.6'" to load this file. \quit

-- tqflat access method

CREATE FUNCTION tqhandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD tqflat TYPE INDEX HANDLER tqhandler;

COMMENT ON ACCESS METHOD tqflat IS 'tqflat (TurboQuant flat) index access method';

CREATE FUNCTION tqflat_l2_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqflat_ip_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqflat_cosine_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqflat_halfvec_l2_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqflat_halfvec_ip_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqflat_halfvec_cosine_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqflat_sparsevec_l2_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqflat_sparsevec_ip_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqflat_sparsevec_cosine_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE OPERATOR CLASS vector_l2_ops
	FOR TYPE vector USING tqflat AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_l2_squared_distance(vector, vector),
	FUNCTION 5 tqflat_l2_support(internal);

CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING tqflat AS
	OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 5 tqflat_ip_support(internal);

CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING tqflat AS
	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 2 vector_norm(vector),
	FUNCTION 5 tqflat_cosine_support(internal);

CREATE OPERATOR CLASS halfvec_l2_ops
	FOR TYPE halfvec USING tqflat AS
	OPERATOR 1 <-> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_l2_squared_distance(halfvec, halfvec),
	FUNCTION 5 tqflat_halfvec_l2_support(internal);

CREATE OPERATOR CLASS halfvec_ip_ops
	FOR TYPE halfvec USING tqflat AS
	OPERATOR 1 <#> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 5 tqflat_halfvec_ip_support(internal);

CREATE OPERATOR CLASS halfvec_cosine_ops
	FOR TYPE halfvec USING tqflat AS
	OPERATOR 1 <=> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 2 l2_norm(halfvec),
	FUNCTION 5 tqflat_halfvec_cosine_support(internal);

CREATE OPERATOR CLASS sparsevec_l2_ops
	FOR TYPE sparsevec USING tqflat AS
	OPERATOR 1 <-> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_l2_squared_distance(sparsevec, sparsevec),
	FUNCTION 5 tqflat_sparsevec_l2_support(internal);

CREATE OPERATOR CLASS sparsevec_ip_ops
	FOR TYPE sparsevec USING tqflat AS
	OPERATOR 1 <#> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_negative_inner_product(sparsevec, sparsevec),
	FUNCTION 5 tqflat_sparsevec_ip_support(internal);

CREATE OPERATOR CLASS sparsevec_cosine_ops
	FOR TYPE sparsevec USING tqflat AS
	OPERATOR 1 <=> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_negative_inner_product(sparsevec, sparsevec),
	FUNCTION 2 l2_norm(sparsevec),
	FUNCTION 5 tqflat_sparsevec_cosine_support(internal);

-- tqivf access method

CREATE FUNCTION tqivfhandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD tqivf TYPE INDEX HANDLER tqivfhandler;

COMMENT ON ACCESS METHOD tqivf IS 'tqivf index access method';

CREATE FUNCTION tqivf_l2_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqivf_ip_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqivf_cosine_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqivf_halfvec_l2_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqivf_halfvec_ip_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqivf_halfvec_cosine_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE OPERATOR CLASS vector_l2_ops
	DEFAULT FOR TYPE vector USING tqivf AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_l2_squared_distance(vector, vector),
	FUNCTION 3 l2_distance(vector, vector),
	FUNCTION 6 tqivf_l2_support(internal);

CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING tqivf AS
	OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 3 vector_spherical_distance(vector, vector),
	FUNCTION 4 vector_norm(vector),
	FUNCTION 6 tqivf_ip_support(internal);

CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING tqivf AS
	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 2 vector_norm(vector),
	FUNCTION 3 vector_spherical_distance(vector, vector),
	FUNCTION 4 vector_norm(vector),
	FUNCTION 6 tqivf_cosine_support(internal);

CREATE OPERATOR CLASS halfvec_l2_ops
	FOR TYPE halfvec USING tqivf AS
	OPERATOR 1 <-> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_l2_squared_distance(halfvec, halfvec),
	FUNCTION 3 l2_distance(halfvec, halfvec),
	FUNCTION 5 ivfflat_halfvec_support(internal),
	FUNCTION 6 tqivf_halfvec_l2_support(internal);

CREATE OPERATOR CLASS halfvec_ip_ops
	FOR TYPE halfvec USING tqivf AS
	OPERATOR 1 <#> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 3 halfvec_spherical_distance(halfvec, halfvec),
	FUNCTION 4 l2_norm(halfvec),
	FUNCTION 5 ivfflat_halfvec_support(internal),
	FUNCTION 6 tqivf_halfvec_ip_support(internal);

CREATE OPERATOR CLASS halfvec_cosine_ops
	FOR TYPE halfvec USING tqivf AS
	OPERATOR 1 <=> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 2 l2_norm(halfvec),
	FUNCTION 3 halfvec_spherical_distance(halfvec, halfvec),
	FUNCTION 4 l2_norm(halfvec),
	FUNCTION 5 ivfflat_halfvec_support(internal),
	FUNCTION 6 tqivf_halfvec_cosine_support(internal);

-- tqhnsw access method

CREATE FUNCTION tqhnswhandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD tqhnsw TYPE INDEX HANDLER tqhnswhandler;

COMMENT ON ACCESS METHOD tqhnsw IS 'tqhnsw index access method';

CREATE FUNCTION tqhnsw_l2_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqhnsw_ip_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqhnsw_cosine_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqhnsw_halfvec_l2_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqhnsw_halfvec_ip_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqhnsw_halfvec_cosine_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqhnsw_sparsevec_l2_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqhnsw_sparsevec_ip_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tqhnsw_sparsevec_cosine_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE OPERATOR CLASS vector_l2_ops
	DEFAULT FOR TYPE vector USING tqhnsw AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_l2_squared_distance(vector, vector),
	FUNCTION 3 tqhnsw_l2_support(internal);

CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING tqhnsw AS
	OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 2 vector_norm(vector),
	FUNCTION 3 tqhnsw_ip_support(internal);

CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING tqhnsw AS
	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 2 vector_norm(vector),
	FUNCTION 3 tqhnsw_cosine_support(internal);

CREATE OPERATOR CLASS halfvec_l2_ops
	FOR TYPE halfvec USING tqhnsw AS
	OPERATOR 1 <-> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_l2_squared_distance(halfvec, halfvec),
	FUNCTION 3 tqhnsw_halfvec_l2_support(internal);

CREATE OPERATOR CLASS halfvec_ip_ops
	FOR TYPE halfvec USING tqhnsw AS
	OPERATOR 1 <#> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 3 tqhnsw_halfvec_ip_support(internal);

CREATE OPERATOR CLASS halfvec_cosine_ops
	FOR TYPE halfvec USING tqhnsw AS
	OPERATOR 1 <=> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 2 l2_norm(halfvec),
	FUNCTION 3 tqhnsw_halfvec_cosine_support(internal);

CREATE OPERATOR CLASS sparsevec_l2_ops
	FOR TYPE sparsevec USING tqhnsw AS
	OPERATOR 1 <-> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_l2_squared_distance(sparsevec, sparsevec),
	FUNCTION 3 tqhnsw_sparsevec_l2_support(internal);

CREATE OPERATOR CLASS sparsevec_ip_ops
	FOR TYPE sparsevec USING tqhnsw AS
	OPERATOR 1 <#> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_negative_inner_product(sparsevec, sparsevec),
	FUNCTION 3 tqhnsw_sparsevec_ip_support(internal);

CREATE OPERATOR CLASS sparsevec_cosine_ops
	FOR TYPE sparsevec USING tqhnsw AS
	OPERATOR 1 <=> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_negative_inner_product(sparsevec, sparsevec),
	FUNCTION 2 l2_norm(sparsevec),
	FUNCTION 3 tqhnsw_sparsevec_cosine_support(internal);

