-- Drops identity.outbox and every event row in it. Unpublished events are
-- lost: only roll back on a database whose outbox is drained or disposable.
SET search_path TO identity, public;
DROP TABLE IF EXISTS outbox;
