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

-- loaded cuVS index functions

CREATE FUNCTION ann_index_load(name text, path text) RETURNS bigint
	AS 'MODULE_PATHNAME' LANGUAGE C STRICT VOLATILE;

COMMENT ON FUNCTION ann_index_load(text, text) IS 'load an immutable ann index directory into the worker';

REVOKE ALL ON FUNCTION ann_index_load(text, text) FROM PUBLIC;

CREATE FUNCTION ann_index_search(name text, query vector, k integer, filter bigint[] DEFAULT NULL,
	OUT id bigint, OUT distance real) RETURNS SETOF record
	AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE ROWS 10;

COMMENT ON FUNCTION ann_index_search(text, vector, integer, bigint[]) IS 'top-k search of a loaded ann index, optionally restricted to filter keys';

-- where loaded indexes come from on this host (for local exact routing)

CREATE TABLE ann_index_catalog (
	name text PRIMARY KEY,
	source regclass NOT NULL,
	key_column name NOT NULL,
	vector_column name NOT NULL,
	rows bigint NOT NULL,
	loaded_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE ann_index_catalog IS 'source tables of loaded ann indexes, recorded by ann_index_load and ann_index_attach';

REVOKE ALL ON ann_index_catalog FROM PUBLIC;
GRANT SELECT ON ann_index_catalog TO PUBLIC;

CREATE FUNCTION ann_index_attach(name text) RETURNS bigint
	AS 'MODULE_PATHNAME' LANGUAGE C STRICT VOLATILE;

COMMENT ON FUNCTION ann_index_attach(text) IS 'verify and record an ann index already loaded on the GPU nodes';

REVOKE ALL ON FUNCTION ann_index_attach(text) FROM PUBLIC;

CREATE FUNCTION ann_hybrid_search(name text, query vector, k integer, filter text,
	OUT id bigint, OUT distance real) RETURNS SETOF record
	AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE ROWS 10;

COMMENT ON FUNCTION ann_hybrid_search(text, vector, integer, text) IS 'hybrid search: selective SQL filters run exactly here, broad ones on GPU nodes';

CREATE FUNCTION ann_attribute_search(name text, query vector, k integer, attribute text, attribute_values integer[],
	OUT id bigint, OUT distance real) RETURNS SETOF record
	AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE ROWS 10;

COMMENT ON FUNCTION ann_attribute_search(text, vector, integer, text, integer[]) IS 'hybrid search filtered on the GPU node: rows whose index attribute is one of the values';

-- filter conditions precomputed as bitmaps over an index's rows

CREATE TABLE ann_filter_bitmap (
	id bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
	index_name text NOT NULL,
	filter text NOT NULL,
	eligible bigint NOT NULL,
	bitmap bytea NOT NULL,
	built_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (index_name, filter)
);

-- bitmaps are read whole on every search: store them uncompressed
ALTER TABLE ann_filter_bitmap ALTER COLUMN bitmap SET STORAGE EXTERNAL;

COMMENT ON TABLE ann_filter_bitmap IS 'filter conditions as bitmaps over an ann index''s rows in key order; GPU nodes cache them by id for cuVS filtered search';

REVOKE ALL ON ann_filter_bitmap FROM PUBLIC;
GRANT SELECT ON ann_filter_bitmap TO PUBLIC;

CREATE FUNCTION ann_filter_bitmap_build(name text, filter text) RETURNS bigint
	AS 'MODULE_PATHNAME' LANGUAGE C STRICT VOLATILE;

COMMENT ON FUNCTION ann_filter_bitmap_build(text, text) IS 'evaluate a filter once, store it in ann_filter_bitmap and cache it on the GPU nodes; returns its eligible rows';

REVOKE ALL ON FUNCTION ann_filter_bitmap_build(text, text) FROM PUBLIC;

CREATE FUNCTION ann_filter_bitmap_push(name text) RETURNS bigint
	AS 'MODULE_PATHNAME' LANGUAGE C STRICT VOLATILE;

COMMENT ON FUNCTION ann_filter_bitmap_push(text) IS 'cache every stored filter bitmap of an index on the GPU nodes; returns the number pushed';

REVOKE ALL ON FUNCTION ann_filter_bitmap_push(text) FROM PUBLIC;

CREATE FUNCTION ann_bitmap_search(name text, query vector, k integer, filter text,
	OUT id bigint, OUT distance real) RETURNS SETOF record
	AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE ROWS 10;

COMMENT ON FUNCTION ann_bitmap_search(text, vector, integer, text) IS 'filtered search with a filter stored in ann_filter_bitmap and cached on the GPU nodes';
