\echo Use "ALTER EXTENSION svector UPDATE TO '0.2.2'" to load this file. \quit

-- Remove this single line for Postgres < 13
ALTER TYPE svector SET (STORAGE = extended);
