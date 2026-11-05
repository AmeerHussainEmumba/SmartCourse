-- CITEXT: case-insensitive email comparison at the database level (RFC §5.3.1).
-- pg_trgm: fuzzy/trigram matching for search (RFC §5.3.6, ADR-0016).
-- Installed into `public`, not a service schema: extension-provided types
-- and operators (citext, gin_trgm_ops) are used by more than one service
-- (identity's users.email, course's course_search_index), so every
-- service's search_path includes `public` precisely so these resolve
-- without each service reinstalling its own copy.
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
