-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.8.1'" to load this file. \quit

CREATE FUNCTION inner_product(bit, bit) RETURNS float8
	AS 'MODULE_PATHNAME', 'bit_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION bit_negative_inner_product(bit, bit) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR <#> (
	LEFTARG = bit, RIGHTARG = bit, PROCEDURE = bit_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR CLASS bit_ip_ops
	FOR TYPE bit USING ivfflat AS
	OPERATOR 1 <#> (bit, bit) FOR ORDER BY float_ops,
	FUNCTION 1 bit_negative_inner_product(bit, bit),
	FUNCTION 3 bit_negative_inner_product(bit, bit),
	FUNCTION 5 ivfflat_bit_support(internal);

CREATE OPERATOR CLASS bit_ip_ops
	FOR TYPE bit USING hnsw AS
	OPERATOR 1 <#> (bit, bit) FOR ORDER BY float_ops,
	FUNCTION 1 bit_negative_inner_product(bit, bit),
	FUNCTION 3 hnsw_bit_support(internal);
