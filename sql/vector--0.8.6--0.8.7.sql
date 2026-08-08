-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.8.7'" to load this file. \quit

COMMENT ON TYPE vector IS 'single-precision vector';

COMMENT ON TYPE halfvec IS 'half-precision vector';

COMMENT ON TYPE sparsevec IS 'single-precision sparse vector';
