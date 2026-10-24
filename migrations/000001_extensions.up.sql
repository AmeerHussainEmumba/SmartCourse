-- CITEXT: case-insensitive email comparison at the database level (RFC §5.3.1).
-- pg_trgm: fuzzy/trigram matching for search (RFC §5.3.6, ADR-0016).
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
