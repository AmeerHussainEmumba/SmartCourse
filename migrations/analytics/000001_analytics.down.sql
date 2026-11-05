SET search_path TO analytics, public;
DROP TABLE IF EXISTS platform_snapshot;
DROP TABLE IF EXISTS daily_enrollments;
DROP TABLE IF EXISTS course_stats;
DROP TABLE IF EXISTS enrollment_mirror;
DROP TABLE IF EXISTS course_mirror;
DROP TABLE IF EXISTS user_role_mirror;
DROP SCHEMA IF EXISTS analytics CASCADE;
