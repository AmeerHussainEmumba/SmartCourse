#!/bin/sh
# Creates (or re-converges) the per-service Postgres roles and their grants
# (docs/plans/microservices-split.md section 3.2). Idempotent: run it as often
# as you like; it is called by `make migrate-up` (after the migrations) and by
# the compose postgres init (first start of an empty volume).
#
#   svc_identity, svc_course, svc_analytics, svc_notification
#     LOGIN, no superuser/createdb/createrole, own nothing. USAGE + DML on the
#     tables and sequences of their OWN schema only, EXECUTE on public's
#     functions (citext / pg_trgm), a CONNECTION LIMIT, and a role-level
#     search_path of "<schema>, public". Nothing on any other service's schema.
#   Every service schema: REVOKE ALL FROM PUBLIC. Future tables the OWNER
#     creates in a service schema are covered through ALTER DEFAULT PRIVILEGES
#     (they apply to objects created by the role that runs this script, so run
#     it as the same owner that runs the migrations).
#
# Passwords come from the environment, never from SQL files in git:
#   SVC_IDENTITY_PASSWORD SVC_COURSE_PASSWORD SVC_ANALYTICS_PASSWORD
#   SVC_NOTIFICATION_PASSWORD                         (all required)
# Optional connection limits (defaults in brackets):
#   SVC_IDENTITY_CONN_LIMIT [30] SVC_COURSE_CONN_LIMIT [60]
#   SVC_ANALYTICS_CONN_LIMIT [30] SVC_NOTIFICATION_CONN_LIMIT [30]
#
# Connection (the OWNER, never a service role):
#   DB_OWNER_DSN or POSTGRES_DSN   a libpq URL; or, inside the postgres
#                                  container (compose init), POSTGRES_USER and
#                                  POSTGRES_DB over the local socket.
#   Without a host psql 15+ (needed for \getenv), the script falls back to
#   `docker compose exec postgres psql` (dev compose only).
#
# POSIX sh on purpose: the postgres:alpine image has no bash.
set -eu

for v in SVC_IDENTITY_PASSWORD SVC_COURSE_PASSWORD SVC_ANALYTICS_PASSWORD SVC_NOTIFICATION_PASSWORD; do
  eval "val=\${$v:-}"
  if [ -z "$val" ]; then
    echo "db-roles: $v is not set (copy env.example to .env, or export it)" >&2
    exit 1
  fi
done
SVC_IDENTITY_CONN_LIMIT="${SVC_IDENTITY_CONN_LIMIT:-30}"
SVC_COURSE_CONN_LIMIT="${SVC_COURSE_CONN_LIMIT:-60}"
SVC_ANALYTICS_CONN_LIMIT="${SVC_ANALYTICS_CONN_LIMIT:-30}"
SVC_NOTIFICATION_CONN_LIMIT="${SVC_NOTIFICATION_CONN_LIMIT:-30}"
export SVC_IDENTITY_CONN_LIMIT SVC_COURSE_CONN_LIMIT SVC_ANALYTICS_CONN_LIMIT SVC_NOTIFICATION_CONN_LIMIT

passenv="SVC_IDENTITY_PASSWORD SVC_COURSE_PASSWORD SVC_ANALYTICS_PASSWORD SVC_NOTIFICATION_PASSWORD SVC_IDENTITY_CONN_LIMIT SVC_COURSE_CONN_LIMIT SVC_ANALYTICS_CONN_LIMIT SVC_NOTIFICATION_CONN_LIMIT"
export SVC_IDENTITY_PASSWORD SVC_COURSE_PASSWORD SVC_ANALYTICS_PASSWORD SVC_NOTIFICATION_PASSWORD

# The SQL. Values are read with \getenv (psql 15+), so passwords never appear
# on a command line or in process listings. Identifiers are fixed literals;
# values are quoted by format(%L).
sql() {
cat <<'SQL'
\set ON_ERROR_STOP on
\getenv pw_identity SVC_IDENTITY_PASSWORD
\getenv pw_course SVC_COURSE_PASSWORD
\getenv pw_analytics SVC_ANALYTICS_PASSWORD
\getenv pw_notification SVC_NOTIFICATION_PASSWORD
\getenv lim_identity SVC_IDENTITY_CONN_LIMIT
\getenv lim_course SVC_COURSE_CONN_LIMIT
\getenv lim_analytics SVC_ANALYTICS_CONN_LIMIT
\getenv lim_notification SVC_NOTIFICATION_CONN_LIMIT

BEGIN;

-- Schemas exist before the migrations run so default privileges can be set
-- on them (the migrations use CREATE SCHEMA IF NOT EXISTS).
CREATE SCHEMA IF NOT EXISTS identity;
CREATE SCHEMA IF NOT EXISTS course;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS notification;

SELECT format(
  'DO $r$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %1$L) THEN CREATE ROLE %1$I LOGIN; END IF; END $r$',
  r.name) AS stmt
FROM (VALUES ('svc_identity'), ('svc_course'), ('svc_analytics'), ('svc_notification')) AS r(name)
\gexec

SELECT format('ALTER ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L CONNECTION LIMIT %s', r.name, r.pw, r.lim)
FROM (VALUES ('svc_identity', :'pw_identity', :'lim_identity'),
             ('svc_course', :'pw_course', :'lim_course'),
             ('svc_analytics', :'pw_analytics', :'lim_analytics'),
             ('svc_notification', :'pw_notification', :'lim_notification')) AS r(name, pw, lim)
\gexec

SELECT format('ALTER ROLE %I SET search_path = %s, public', 'svc_' || s, s)
FROM unnest(ARRAY['identity','course','analytics','notification']) AS s
\gexec

-- Nothing is reachable through PUBLIC; public stays usable, not writable.
REVOKE ALL ON SCHEMA identity, course, analytics, notification FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- Converge every role on exactly its own schema: strip the others.
SELECT format($f$
  REVOKE ALL ON SCHEMA %1$I FROM %2$I;
  REVOKE ALL ON ALL TABLES IN SCHEMA %1$I FROM %2$I;
  REVOKE ALL ON ALL SEQUENCES IN SCHEMA %1$I FROM %2$I;
  REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %1$I FROM %2$I;
  ALTER DEFAULT PRIVILEGES IN SCHEMA %1$I REVOKE ALL ON TABLES FROM %2$I;
  ALTER DEFAULT PRIVILEGES IN SCHEMA %1$I REVOKE ALL ON SEQUENCES FROM %2$I;
$f$, s.schema, 'svc_' || r.svc)
FROM (VALUES ('identity'), ('course'), ('analytics'), ('notification')) AS r(svc)
CROSS JOIN (VALUES ('identity'), ('course'), ('analytics'), ('notification')) AS s(schema)
WHERE s.schema <> r.svc
\gexec

-- Own schema: USAGE + DML now and for tables created later by this owner.
SELECT format($f$
  GRANT USAGE ON SCHEMA %1$I TO %2$I;
  GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %1$I TO %2$I;
  GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA %1$I TO %2$I;
  ALTER DEFAULT PRIVILEGES IN SCHEMA %1$I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %2$I;
  ALTER DEFAULT PRIVILEGES IN SCHEMA %1$I GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO %2$I;
  GRANT USAGE ON SCHEMA public TO %2$I;
  GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO %2$I;
$f$, s, 'svc_' || s)
FROM unnest(ARRAY['identity','course','analytics','notification']) AS s
\gexec

COMMIT;
SQL
}

DSN="${DB_OWNER_DSN:-${POSTGRES_DSN:-}}"
if command -v psql >/dev/null 2>&1 && { [ -n "$DSN" ] || [ -n "${POSTGRES_USER:-}" ]; }; then
  ver=$(psql --version | sed -E 's/[^0-9]*([0-9]+).*/\1/')
  if [ "${ver:-0}" -ge 15 ]; then
    if [ -n "$DSN" ]; then
      sql | psql -X -q -v ON_ERROR_STOP=1 "$DSN" >/dev/null
    else
      sql | psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "${POSTGRES_DB:-$POSTGRES_USER}" >/dev/null
    fi
    echo "db-roles: roles and grants converged"
    exit 0
  fi
fi

# Fallback: psql inside the dev compose postgres container (owner over the
# local socket). Variables are forwarded by name, so values stay off argv.
if command -v docker >/dev/null 2>&1; then
  eflags=""
  for name in $passenv; do eflags="$eflags -e $name"; done
  # shellcheck disable=SC2086
  sql | docker compose exec -T $eflags postgres sh -c 'psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' >/dev/null
  echo "db-roles: roles and grants converged (via docker compose exec postgres)"
  exit 0
fi
echo "db-roles: need psql 15+ with DB_OWNER_DSN/POSTGRES_DSN, or docker compose with the postgres service up" >&2
exit 1
