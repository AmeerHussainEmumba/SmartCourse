SET search_path TO course, public;

-- When the current publish claim was taken. Publish claims a version
-- (draft/publish_failed -> publishing) with one conditional UPDATE BEFORE it
-- starts the Temporal workflow, so a second publish gets a 409 instead of
-- attaching to the running one, and the workflow's own state writes can never
-- be overwritten by a late "publishing" write. A claim older than the lease
-- (see publishClaimStaleAfter in services/course/course) is treated as
-- abandoned, so a crash between claiming and starting the workflow cannot
-- wedge a course in "publishing" forever. NULL on rows from before this
-- migration, which a stale-claim check also treats as reclaimable.
ALTER TABLE course_versions ADD COLUMN publish_started_at TIMESTAMPTZ;
