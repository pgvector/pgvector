-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.6.0'" to load this file. \quit

-- remove this single line for Postgres < 13
ALTER TYPE vector SET (STORAGE = external);
