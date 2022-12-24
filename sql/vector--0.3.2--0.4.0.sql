-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.4.0'" to load this file. \quit

ALTER TYPE vector SET (STORAGE = extended);
