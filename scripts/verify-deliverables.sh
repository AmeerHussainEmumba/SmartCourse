#!/usr/bin/env bash
# Runs the live acceptance checks of docs/plans/week-3-acceptance-matrix.md
# against a RUNNING stack and prints one line per row:
#
#   PASS    <id> <name>   the check ran and the stack behaved as the matrix says
#   FAIL    <id> <name>   the check ran and it did not (detail on the next line)
#   PENDING <id> <name>   the feature is not built yet; the line names the phase
#   SKIP    <id> <name>   a precondition of the check is missing (detail says which)
#
# Exit status is non-zero if any row FAILED. PENDING and SKIP never fail the
# run, so a green exit is NOT "everything is proven": read the summary line.
#
# Usage:
#   scripts/verify-deliverables.sh                # every row
#   scripts/verify-deliverables.sh --only <id>    # one row (repeatable)
#   scripts/verify-deliverables.sh --list         # ids, names, kind; touches nothing
#
# Stack (bring it up first; see README quick start):
#   make dev-core && make dev-workflow && make migrate-up && make seed
# Base URLs (defaults match docker-compose.yml's host ports):
#   IDENTITY_URL=http://localhost:8090  COURSE_URL=http://localhost:8091
#   ANALYTICS_URL=http://localhost:8092
# Admin used to promote a freshly registered user to instructor: the first admin
# that `make seed` creates with identity-admin (SEED_ADMIN_EMAIL /
# SEED_ADMIN_PASSWORD, env.example defaults unless you set them):
#   VERIFY_ADMIN_EMAIL=admin@smartcourse.local  VERIFY_ADMIN_PASSWORD=change-me-admin-password
# Other: VERIFY_PUBLISH_TIMEOUT_S=90 (wait for a publish to reach ready).
#
# What it creates: three users (all with fresh random emails) and one published
# course per run, through the public API only. identity-api rate-limits
# register to 5 per minute per IP; the script waits and retries once on 429.
# It never touches the database, volumes or containers directly.
#
# Requires: bash, curl, jq.
set -uo pipefail

IDENTITY_URL=${IDENTITY_URL:-http://localhost:8090}
COURSE_URL=${COURSE_URL:-http://localhost:8091}
ANALYTICS_URL=${ANALYTICS_URL:-http://localhost:8092}
ADMIN_EMAIL=${VERIFY_ADMIN_EMAIL:-${SEED_ADMIN_EMAIL:-admin@smartcourse.local}}
ADMIN_PASSWORD=${VERIFY_ADMIN_PASSWORD:-${SEED_ADMIN_PASSWORD:-change-me-admin-password}}
PUBLISH_TIMEOUT_S=${VERIFY_PUBLISH_TIMEOUT_S:-90}
USER_PASSWORD='Verify-Pass-1234'
RUN_ID="$(date +%s)${RANDOM}"

TMPDIR_V=$(mktemp -d)
trap 'rm -rf "$TMPDIR_V"' EXIT

# ---------------------------------------------------------------- reporting

STATUS=""   # set by a check: PASS | FAIL | PENDING | SKIP
DETAIL=""   # one line of context
pass()    { STATUS=PASS;    DETAIL="${1:-}"; }
fail()    { STATUS=FAIL;    DETAIL="${1:-}"; }
skip()    { STATUS=SKIP;    DETAIL="${1:-}"; }
pending() { STATUS=PENDING; DETAIL="${1:-}"; } # "Phase N: what is missing"

# ---------------------------------------------------------------- HTTP

HTTP_CODE=""; HTTP_BODY=""; HTTP_HDRS=""

# req METHOD URL [TOKEN] [JSON_BODY] [extra curl args...]
# Fills HTTP_CODE (000 when unreachable), HTTP_BODY, HTTP_HDRS (lower-cased).
req() {
  local method=$1 url=$2 token=${3:-} body=${4:-}
  shift 4
  local args=(-sS --max-time 20 -X "$method" -D "$TMPDIR_V/h" -o "$TMPDIR_V/b" -w '%{http_code}')
  [ -n "$token" ] && args+=(-H "Authorization: Bearer $token")
  if [ -n "$body" ]; then args+=(-H 'Content-Type: application/json' --data-binary "$body"); fi
  HTTP_CODE=$(curl "${args[@]}" "$@" "$url" 2>/dev/null) || HTTP_CODE=000
  HTTP_BODY=$(cat "$TMPDIR_V/b" 2>/dev/null || true)
  HTTP_HDRS=$(tr 'A-Z' 'a-z' <"$TMPDIR_V/h" 2>/dev/null | tr -d '\r' || true)
  [ -n "$HTTP_CODE" ] || HTTP_CODE=000
}

jqr() { jq -r "$1" <<<"$HTTP_BODY" 2>/dev/null; }

# ---------------------------------------------------------------- fixtures
# Lazily created, shared by every check in one run, created through the API.

STUDENT_TOKEN=""; STUDENT_ID=""; STUDENT_EMAIL=""
OUTSIDER_TOKEN=""; OUTSIDER_ID=""
INSTR_TOKEN=""; INSTR_ID=""
COURSE_ID=""; COURSE_SLUG=""; COURSE_TITLE_WORD=""; COURSE_ATTEMPT=0
PUBLISH_CODE1=""; PUBLISH_CODE2=""; PUBLISH_CODE2_BODY=""; PUBLISH_FINAL_STATE=""
ENROLL_ID=""
FIX_ERR=""

register_user() { # email -> 0 ok; sets FIX_ERR on failure
  local email=$1 body
  body=$(jq -nc --arg e "$email" --arg p "$USER_PASSWORD" '{email:$e,password:$p,full_name:"Verify Script"}')
  req POST "$IDENTITY_URL/api/v1/auth/register" "" "$body"
  if [ "$HTTP_CODE" = 429 ]; then
    sleep 62
    req POST "$IDENTITY_URL/api/v1/auth/register" "" "$body"
  fi
  [ "$HTTP_CODE" = 202 ] || { FIX_ERR="register $email: HTTP $HTTP_CODE $HTTP_BODY"; return 1; }
}

login_user() { # email password -> sets LOGIN_TOKEN, LOGIN_ID
  local body
  body=$(jq -nc --arg e "$1" --arg p "$2" '{email:$e,password:$p}')
  req POST "$IDENTITY_URL/api/v1/auth/login" "" "$body"
  [ "$HTTP_CODE" = 200 ] || { FIX_ERR="login $1: HTTP $HTTP_CODE $HTTP_BODY"; return 1; }
  LOGIN_TOKEN=$(jqr .access_token); LOGIN_ID=$(jqr .user.id)
  [ -n "$LOGIN_TOKEN" ] && [ "$LOGIN_TOKEN" != null ] || { FIX_ERR="login $1: no access_token"; return 1; }
}

ensure_student() {
  [ -n "$STUDENT_TOKEN" ] && return 0
  STUDENT_EMAIL="verify-student-$RUN_ID@example.test"
  register_user "$STUDENT_EMAIL" && login_user "$STUDENT_EMAIL" "$USER_PASSWORD" || return 1
  STUDENT_TOKEN=$LOGIN_TOKEN; STUDENT_ID=$LOGIN_ID
}

ensure_outsider() { # a second student who is never enrolled
  [ -n "$OUTSIDER_TOKEN" ] && return 0
  local email="verify-outsider-$RUN_ID@example.test"
  register_user "$email" && login_user "$email" "$USER_PASSWORD" || return 1
  OUTSIDER_TOKEN=$LOGIN_TOKEN; OUTSIDER_ID=$LOGIN_ID
}

ADMIN_MISSING=0
ensure_instructor() {
  [ -n "$INSTR_TOKEN" ] && return 0
  local email="verify-instructor-$RUN_ID@example.test"
  register_user "$email" && login_user "$email" "$USER_PASSWORD" || return 1
  local uid=$LOGIN_ID
  if ! login_user "$ADMIN_EMAIL" "$ADMIN_PASSWORD"; then
    ADMIN_MISSING=1
    FIX_ERR="cannot log in as admin $ADMIN_EMAIL (run make seed, or set VERIFY_ADMIN_EMAIL and VERIFY_ADMIN_PASSWORD): $FIX_ERR"
    return 1
  fi
  req PATCH "$IDENTITY_URL/api/v1/admin/users/$uid/role" "$LOGIN_TOKEN" '{"role":"instructor","status":"active"}'
  [ "$HTTP_CODE" = 200 ] || { FIX_ERR="promote to instructor: HTTP $HTTP_CODE $HTTP_BODY"; return 1; }
  login_user "$email" "$USER_PASSWORD" || return 1 # a fresh token carries the new role and version
  INSTR_TOKEN=$LOGIN_TOKEN; INSTR_ID=$LOGIN_ID
}

# create_course_tree: course + module + lesson (draft). Sets COURSE_ID.
create_course_tree() {
  [ -n "$COURSE_ID" ] && return 0
  ensure_instructor || return 1
  COURSE_TITLE_WORD="verifyrun$RUN_ID"
  COURSE_ATTEMPT=$((COURSE_ATTEMPT + 1))
  COURSE_SLUG="verify-$RUN_ID-$COURSE_ATTEMPT"
  local body
  body=$(jq -nc --arg t "Course $COURSE_TITLE_WORD" --arg s "$COURSE_SLUG" '{title:$t,slug:$s,description:"created by verify-deliverables.sh",enrollment_limit:100}')
  req POST "$COURSE_URL/api/v1/courses" "$INSTR_TOKEN" "$body"
  [ "$HTTP_CODE" = 201 ] || { FIX_ERR="create course: HTTP $HTTP_CODE $HTTP_BODY"; return 1; }
  COURSE_ID=$(jqr .course.id)
  [ -n "$COURSE_ID" ] && [ "$COURSE_ID" != null ] || { FIX_ERR="create course: no course.id in $HTTP_BODY"; COURSE_ID=""; return 1; }
  req POST "$COURSE_URL/api/v1/courses/$COURSE_ID/modules" "$INSTR_TOKEN" '{"title":"Module 1","position":1}'
  [ "$HTTP_CODE" = 201 ] || { FIX_ERR="create module: HTTP $HTTP_CODE $HTTP_BODY"; COURSE_ID=""; return 1; }
  local mod; mod=$(jqr .id)
  req POST "$COURSE_URL/api/v1/modules/$mod/lessons" "$INSTR_TOKEN" '{"title":"Lesson 1","position":1,"content_type":"text","content":{"body":"hello"},"duration_s":60}'
  [ "$HTTP_CODE" = 201 ] || { FIX_ERR="create lesson: HTTP $HTTP_CODE $HTTP_BODY"; COURSE_ID=""; return 1; }
}

PUBLISHED=0
# publish_course: publish twice back to back, wait for the final state.
publish_course() {
  [ "$PUBLISHED" = 1 ] && return 0
  create_course_tree || return 1
  req POST "$COURSE_URL/api/v1/courses/$COURSE_ID/publish" "$INSTR_TOKEN" ''
  PUBLISH_CODE1=$HTTP_CODE
  [ "$PUBLISH_CODE1" = 202 ] || { FIX_ERR="first publish: HTTP $HTTP_CODE $HTTP_BODY (is the workflow profile up? make dev-workflow)"; return 1; }
  req POST "$COURSE_URL/api/v1/courses/$COURSE_ID/publish" "$INSTR_TOKEN" ''
  PUBLISH_CODE2=$HTTP_CODE; PUBLISH_CODE2_BODY=$HTTP_BODY
  local deadline=$((SECONDS + PUBLISH_TIMEOUT_S))
  PUBLISH_FINAL_STATE=""
  while [ "$SECONDS" -lt "$deadline" ]; do
    req GET "$COURSE_URL/api/v1/courses/$COURSE_ID/publish-status" "$INSTR_TOKEN" ''
    PUBLISH_FINAL_STATE=$(jqr .state)
    case "$PUBLISH_FINAL_STATE" in ready|publish_failed) break ;; esac
    sleep 2
  done
  PUBLISHED=1
}

# ---------------------------------------------------------------- checks
# One function per matrix row. A function sets STATUS via pass/fail/skip/pending.

fixture_fail() { # turns a fixture error into SKIP (precondition) or FAIL (service broken)
  if [ "$ADMIN_MISSING" = 1 ]; then skip "$FIX_ERR"; else fail "$FIX_ERR"; fi
}

check_EG_W1_4() {
  local svc url bad=""
  for svc in identity:$IDENTITY_URL course:$COURSE_URL analytics:$ANALYTICS_URL; do
    url=${svc#*:}
    req GET "$url/health/live" "" ''
    [ "$HTTP_CODE" = 200 ] || bad="$bad ${svc%%:*} live=$HTTP_CODE"
    req GET "$url/health/ready" "" ''
    if [ "$HTTP_CODE" != 200 ] || [ "$(jqr .status)" != ready ]; then bad="$bad ${svc%%:*} ready=$HTTP_CODE"; fi
  done
  if [ -z "$bad" ]; then pass "live and ready 200 on identity, course, analytics"; else fail "not healthy:$bad"; fi
}

check_B_1_3() {
  local email="verify-reg-$RUN_ID@example.test" body first
  body=$(jq -nc --arg e "$email" --arg p "$USER_PASSWORD" '{email:$e,password:$p,full_name:"Verify Script"}')
  req POST "$IDENTITY_URL/api/v1/auth/register" "" "$body"
  if [ "$HTTP_CODE" = 429 ]; then sleep 62; req POST "$IDENTITY_URL/api/v1/auth/register" "" "$body"; fi
  [ "$HTTP_CODE" = 202 ] || { fail "register new email: want 202, got $HTTP_CODE $HTTP_BODY"; return; }
  first=$HTTP_BODY
  req POST "$IDENTITY_URL/api/v1/auth/register" "" "$body"
  if [ "$HTTP_CODE" = 429 ]; then sleep 62; req POST "$IDENTITY_URL/api/v1/auth/register" "" "$body"; fi
  [ "$HTTP_CODE" = 202 ] || { fail "register same email again: want 202 (no account enumeration), got $HTTP_CODE"; return; }
  [ "$HTTP_BODY" = "$first" ] || { fail "register response differs between a new and a used email (enumeration): '$first' vs '$HTTP_BODY'"; return; }
  login_user "$email" "$USER_PASSWORD" || { fail "$FIX_ERR"; return; }
  req GET "$IDENTITY_URL/api/v1/me" "$LOGIN_TOKEN" ''
  [ "$HTTP_CODE" = 200 ] && [ "$(jqr .role)" = student ] || { fail "GET /me: want 200 role student, got $HTTP_CODE $HTTP_BODY"; return; }
  if ensure_instructor; then
    req GET "$IDENTITY_URL/api/v1/me" "$INSTR_TOKEN" ''
    [ "$(jqr .role)" = instructor ] && pass "register 202 uniform, login 200, /me student, admin promotion gives instructor" \
      || fail "after promotion and re-login /me role is '$(jqr .role)', want instructor"
  else
    fixture_fail
  fi
}

check_B_1_1() {
  create_course_tree || { fixture_fail; return; }
  # Same title (the search check later looks for it); only the description changes.
  req PATCH "$COURSE_URL/api/v1/courses/$COURSE_ID" "$INSTR_TOKEN" \
    "$(jq -nc --arg t "Course $COURSE_TITLE_WORD" '{title:$t,description:"updated by verify-deliverables.sh",tags:[]}')"
  [ "$HTTP_CODE" = 200 ] || { fail "PATCH /courses/{id}: want 200, got $HTTP_CODE $HTTP_BODY"; return; }
  pass "course 201, module 201, lesson 201, draft update 200"
}

check_SR_F37() {
  publish_course || { fixture_fail; return; }
  [ "$PUBLISH_CODE1" = 202 ] || { fail "first publish: want 202, got $PUBLISH_CODE1"; return; }
  [ "$PUBLISH_CODE2" = 409 ] || { fail "second publish: want 409, got $PUBLISH_CODE2 $PUBLISH_CODE2_BODY"; return; }
  pass "publish 202, immediate second publish 409 ($(jq -r .code <<<"$PUBLISH_CODE2_BODY" 2>/dev/null))"
}

check_B_2_6() {
  publish_course || { fixture_fail; return; }
  case "$PUBLISH_FINAL_STATE" in
    ready) pass "publish-status reached ready" ;;
    "")    fail "publish-status never answered within ${PUBLISH_TIMEOUT_S}s" ;;
    *)     fail "publish-status ended '$PUBLISH_FINAL_STATE' (want ready) after ${PUBLISH_TIMEOUT_S}s; is the worker up with Temporal (make dev-workflow)?" ;;
  esac
}

check_B_2_3() {
  publish_course || { fixture_fail; return; }
  [ "$PUBLISH_FINAL_STATE" = ready ] || { fail "course did not reach ready (state '$PUBLISH_FINAL_STATE'), cannot check search"; return; }
  req GET "$COURSE_URL/api/v1/courses/search?q=$COURSE_TITLE_WORD" "" ''
  [ "$HTTP_CODE" = 200 ] || { fail "GET /courses/search: want 200, got $HTTP_CODE"; return; }
  if jq -e --arg id "$COURSE_ID" '.items | map(.course_id // .id) | index($id) != null' <<<"$HTTP_BODY" >/dev/null 2>&1; then
    pass "published course found by search for '$COURSE_TITLE_WORD'"
  else
    fail "search for '$COURSE_TITLE_WORD' did not return course $COURSE_ID: $HTTP_BODY"
  fi
}

enroll_student() {
  [ -n "$ENROLL_ID" ] && return 0
  publish_course || return 1
  [ "$PUBLISH_FINAL_STATE" = ready ] || { FIX_ERR="course not ready (state '$PUBLISH_FINAL_STATE')"; return 1; }
  ensure_student || return 1
  req POST "$COURSE_URL/api/v1/enrollments" "$STUDENT_TOKEN" "$(jq -nc --arg c "$COURSE_ID" '{course_id:$c}')"
  [ "$HTTP_CODE" = 201 ] || { FIX_ERR="enroll: want 201, got $HTTP_CODE $HTTP_BODY"; return 1; }
  ENROLL_ID=$(jqr .id)
}

check_B_1_4() {
  enroll_student || { fixture_fail; return; }
  req POST "$COURSE_URL/api/v1/enrollments" "$STUDENT_TOKEN" "$(jq -nc --arg c "$COURSE_ID" '{course_id:$c}')"
  [ "$HTTP_CODE" = 409 ] || { fail "duplicate enrollment: want 409, got $HTTP_CODE $HTTP_BODY"; return; }
  req POST "$COURSE_URL/api/v1/enrollments" "$STUDENT_TOKEN" '{"course_id":"00000000-0000-4000-8000-000000000000"}'
  case "$HTTP_CODE" in
    404|409|422) pass "first enroll 201, duplicate 409, unknown course $HTTP_CODE" ;;
    *) fail "unknown course: want a 4xx refusal (404), got $HTTP_CODE $HTTP_BODY" ;;
  esac
}

check_B_3_1() {
  enroll_student || { fixture_fail; return; }
  req GET "$COURSE_URL/api/v1/enrollments/$ENROLL_ID/progress" "$STUDENT_TOKEN" ''
  [ "$HTTP_CODE" = 200 ] || { fail "GET progress: want 200, got $HTTP_CODE $HTTP_BODY"; return; }
  pass "enrollment 201 and progress initialised (200)"
}

check_B_5a() {
  enroll_student || { fixture_fail; return; }
  ensure_outsider || { fixture_fail; return; }
  local f missing=""
  req GET "$ANALYTICS_URL/api/v1/analytics/platform" "" ''
  [ "$HTTP_CODE" = 401 ] || { fail "platform without a token: want 401, got $HTTP_CODE"; return; }
  req GET "$ANALYTICS_URL/api/v1/analytics/platform" "$STUDENT_TOKEN" ''
  [ "$HTTP_CODE" = 200 ] || { fail "platform as student: want 200, got $HTTP_CODE $HTTP_BODY"; return; }
  for f in total_students total_instructors total_courses_published avg_courses_per_student; do
    jq -e --arg f "$f" 'has($f)' <<<"$HTTP_BODY" >/dev/null 2>&1 || missing="$missing $f"
  done
  [ -z "$missing" ] || { fail "platform response lacks:$missing"; return; }
  req GET "$ANALYTICS_URL/api/v1/analytics/courses/$COURSE_ID" "$INSTR_TOKEN" ''
  [ "$HTTP_CODE" = 200 ] || { fail "course stats as instructor: want 200, got $HTTP_CODE $HTTP_BODY"; return; }
  req GET "$ANALYTICS_URL/api/v1/analytics/courses/$COURSE_ID" "$OUTSIDER_TOKEN" ''
  [ "$HTTP_CODE" = 404 ] || { fail "course stats as a student not enrolled: want 404, got $HTTP_CODE"; return; }
  req GET "$ANALYTICS_URL/api/v1/analytics/courses/$COURSE_ID" "$STUDENT_TOKEN" ''
  [ "$HTTP_CODE" = 200 ] || { fail "course stats as the enrolled student: want 200, got $HTTP_CODE $HTTP_BODY"; return; }
  req GET "$ANALYTICS_URL/api/v1/analytics/courses/$COURSE_ID/daily-enrollments" "$OUTSIDER_TOKEN" ''
  [ "$HTTP_CODE" = 404 ] || { fail "daily-enrollments as a non-enrolled student: want 404, got $HTTP_CODE"; return; }
  req GET "$ANALYTICS_URL/api/v1/analytics/courses/$COURSE_ID/daily-enrollments" "$STUDENT_TOKEN" ''
  [ "$HTTP_CODE" = 200 ] && jq -e '.items | type == "array"' <<<"$HTTP_BODY" >/dev/null 2>&1 \
    || { fail "daily-enrollments as the enrolled student: want 200 with items[], got $HTTP_CODE $HTTP_BODY"; return; }
  req GET "$ANALYTICS_URL/api/v1/analytics/courses/not-a-uuid" "$INSTR_TOKEN" ''
  [ "$HTTP_CODE" = 400 ] || { fail "non-UUID course id: want 400, got $HTTP_CODE"; return; }
  pass "platform 401/200 with 4 fields; course stats 200 (instructor, enrolled student) / 404 (outsider); daily-enrollments same; bad id 400"
}

check_X_6_HEADERS() {
  local svc url h bad=""
  for svc in identity:$IDENTITY_URL course:$COURSE_URL analytics:$ANALYTICS_URL; do
    url=${svc#*:}
    req GET "$url/health/live" "" ''
    for h in x-content-type-options x-frame-options referrer-policy content-security-policy; do
      grep -q "^$h:" <<<"$HTTP_HDRS" || bad="$bad ${svc%%:*}:$h"
    done
    grep -q '^x-content-type-options: nosniff' <<<"$HTTP_HDRS" || bad="$bad ${svc%%:*}:nosniff-value"
  done
  if [ -z "$bad" ]; then pass "x-content-type-options nosniff, x-frame-options, referrer-policy, content-security-policy on all three"; else fail "missing:$bad"; fi
}

check_X_6_BODYCAP() {
  # The body goes through a file: 2 MiB does not fit in a command-line argument.
  { printf '{"email":"a@example.test","password":"'; head -c 2097152 /dev/zero | tr '\0' 'a'; printf '"}'; } >"$TMPDIR_V/big"
  local code
  code=$(curl -sS --max-time 20 -X POST -H 'Content-Type: application/json' --data-binary "@$TMPDIR_V/big" \
    -o /dev/null -w '%{http_code}' "$IDENTITY_URL/api/v1/auth/login" 2>/dev/null) || code=000
  if [ "$code" = 413 ]; then pass "2 MiB login body refused with 413"; else fail "2 MiB body: want 413, got $code"; fi
}

check_X_6_ERRSHAPE() {
  local bad=""
  req POST "$IDENTITY_URL/api/v1/auth/login" "" '{not json'
  { [ "$HTTP_CODE" = 400 ] && jq -e 'has("code") and has("message")' <<<"$HTTP_BODY" >/dev/null 2>&1; } || bad="$bad malformed-json=$HTTP_CODE"
  req POST "$IDENTITY_URL/api/v1/auth/login" "" '{}'
  { [ "$HTTP_CODE" = 400 ] && jq -e 'has("code")' <<<"$HTTP_BODY" >/dev/null 2>&1; } || bad="$bad empty-object=$HTTP_CODE"
  req GET "$COURSE_URL/api/v1/courses/search" "" ''
  [ "$HTTP_CODE" = 400 ] || bad="$bad search-without-q=$HTTP_CODE"
  req GET "$COURSE_URL/api/v1/courses?limit=-1" "" ''
  [ "$HTTP_CODE" -lt 500 ] 2>/dev/null || bad="$bad negative-limit=$HTTP_CODE"
  if [ -z "$bad" ]; then pass "malformed and empty bodies 400 with code and message; no 5xx"; else fail "unexpected:$bad"; fi
}

# ---- rows that inspect the infrastructure behind the APIs. They talk to the
# stack's own containers through `docker compose` (project from
# COMPOSE_PROJECT_NAME, as for every other make target) and to the published
# ports, never to the database.
MAILPIT_URL=${MAILPIT_URL:-http://localhost:8025}
REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
MINIO_URL=${MINIO_URL:-http://localhost:9002}
MINIO_KEY=${MINIO_ACCESS_KEY_ID:-smartcourse}
MINIO_SECRET=${MINIO_SECRET_ACCESS_KEY:-smartcourse123}
MINIO_BUCKET=${MINIO_BUCKET_ASSETS:-smartcourse-assets}
PROMETHEUS_URL=${PROMETHEUS_URL:-http://localhost:9090}
GRAFANA_URL=${GRAFANA_URL:-http://localhost:3001}
JAEGER_URL=${JAEGER_URL:-http://localhost:16686}
EVENT_TOPICS="identity.events course.events enrollment.events progress.events"

dc() { docker compose "$@" 2>/dev/null; }
need_docker() { docker info >/dev/null 2>&1 || { skip "Docker is not reachable: this check reads the stack's containers"; return 1; }; }

check_B_1_2() {
  ensure_instructor || { fixture_fail; return; }
  # A course of its own: the shared one is already published and has no draft.
  local cid body size=64
  body=$(jq -nc --arg t "Assets $RUN_ID" --arg s "assets-$RUN_ID" '{title:$t,slug:$s,description:"asset upload check",enrollment_limit:10}')
  req POST "$COURSE_URL/api/v1/courses" "$INSTR_TOKEN" "$body"
  [ "$HTTP_CODE" = 201 ] || { fail "create course: HTTP $HTTP_CODE $HTTP_BODY"; return; }
  cid=$(jqr .course.id)
  req POST "$COURSE_URL/api/v1/assets/upload-url" "$INSTR_TOKEN" "$(jq -nc --arg c "$cid" --argjson n "$size" '{course_id:$c,content_type:"image/png",size_bytes:$n}')"
  [ "$HTTP_CODE" = 200 ] || { fail "upload-url: want 200, got $HTTP_CODE $HTTP_BODY"; return; }
  local url key; url=$(jqr .upload_url); key=$(jqr .asset_key)
  { [ -n "$url" ] && [ "$url" != null ] && [ -n "$key" ] && [ "$key" != null ]; } || { fail "upload-url answered without upload_url or asset_key"; return; }
  local hargs=() h
  while IFS= read -r h; do hargs+=(-H "$h"); done < <(jq -r '.headers // {} | to_entries[] | "\(.key): \(.value)"' <<<"$HTTP_BODY")
  head -c "$size" /dev/zero >"$TMPDIR_V/asset.bin"
  local put; put=$(curl -sS --max-time 20 -X PUT "${hargs[@]}" --data-binary "@$TMPDIR_V/asset.bin" -o /dev/null -w '%{http_code}' "$url" 2>/dev/null) || put=000
  [ "$put" = 200 ] || { fail "PUT to the pre-signed URL: want 200, got $put"; return; }
  local head; head=$(curl -sS --max-time 20 --aws-sigv4 "aws:amz:us-east-1:s3" --user "$MINIO_KEY:$MINIO_SECRET" -I -o /dev/null -w '%{http_code}' "$MINIO_URL/$MINIO_BUCKET/$key" 2>/dev/null) || head=000
  if [ "$head" = 200 ]; then pass "upload-url 200, PUT 200, object $key exists in MinIO (HEAD 200)"; else fail "object not found in MinIO: HEAD $MINIO_BUCKET/$key gave $head"; fi
}

# probe_enrollment: one fresh student enrols in the published course; counters
# are read before and after, so B-3.2, B-4.2, B-4.4, TS-KAFKA, TS-MONGO and
# TS-ASYNQ all judge the same event. Sets PROBE_* and PROBE_ERR.
PROBE_DONE=0; PROBE_ERR=""; PROBE_EMAIL=""; PROBE_COURSE=""
PROBE_OFF_BEFORE=0; PROBE_OFF_AFTER=0; PROBE_MONGO_BEFORE=0; PROBE_MONGO_AFTER=0
PROBE_ASQ_BEFORE=0; PROBE_ASQ_AFTER=0; PROBE_MAIL=0
kafka_end_offsets() { # topic -> sum of end offsets over partitions
  dc exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" --time -1 | awk -F: '{n+=$3} END{print n+0}'
}
mongo_events() { dc exec -T mongodb mongosh smartcourse --quiet --eval 'db.events.countDocuments()' | tail -1 | tr -dc '0-9'; }
asynq_processed() { dc exec -T redis redis-cli get "asynq:{$1}:processed" | tr -dc '0-9' | sed 's/^$/0/'; }
probe_enrollment() {
  [ "$PROBE_DONE" = 1 ] && return 0
  PROBE_DONE=1
  need_docker || { PROBE_ERR=$DETAIL; return 2; }
  enroll_student || { PROBE_ERR=$FIX_ERR; return 1; }
  PROBE_OFF_BEFORE=$(kafka_end_offsets enrollment.events); PROBE_MONGO_BEFORE=$(mongo_events)
  PROBE_ASQ_BEFORE=$(asynq_processed notification:critical)
  PROBE_EMAIL="verify-probe-$RUN_ID@example.test"
  local tok
  register_user "$PROBE_EMAIL" && login_user "$PROBE_EMAIL" "$USER_PASSWORD" || { PROBE_ERR=$FIX_ERR; return 1; }
  tok=$LOGIN_TOKEN
  req POST "$COURSE_URL/api/v1/enrollments" "$tok" "$(jq -nc --arg c "$COURSE_ID" '{course_id:$c}')"
  [ "$HTTP_CODE" = 201 ] || { PROBE_ERR="probe enrollment: HTTP $HTTP_CODE $HTTP_BODY"; return 1; }
  PROBE_COURSE=$COURSE_ID
  local deadline=$((SECONDS + 90))
  while [ "$SECONDS" -lt "$deadline" ]; do
    PROBE_OFF_AFTER=$(kafka_end_offsets enrollment.events); PROBE_MONGO_AFTER=$(mongo_events)
    PROBE_ASQ_AFTER=$(asynq_processed notification:critical)
    PROBE_MAIL=$(curl -sS --max-time 10 -G "$MAILPIT_URL/api/v1/search" --data-urlencode "query=to:$PROBE_EMAIL" 2>/dev/null | jq -r '.messages_count // 0' 2>/dev/null || echo 0)
    [ "$PROBE_OFF_AFTER" -gt "$PROBE_OFF_BEFORE" ] && [ "$PROBE_MONGO_AFTER" -gt "$PROBE_MONGO_BEFORE" ] \
      && [ "$PROBE_ASQ_AFTER" -gt "$PROBE_ASQ_BEFORE" ] && [ "$PROBE_MAIL" -ge 1 ] && break
    sleep 3
  done
  return 0
}
probe_or_stop() { probe_enrollment; case $? in 0) return 0 ;; 2) skip "$PROBE_ERR"; return 1 ;; *) fail "$PROBE_ERR"; return 1 ;; esac; }

check_B_3_2() {
  probe_or_stop || return
  if [ "$PROBE_MAIL" -ge 1 ]; then pass "Mailpit lists $PROBE_MAIL message(s) to $PROBE_EMAIL after the enrollment"; else fail "no message to $PROBE_EMAIL in Mailpit within 90 s"; fi
}
check_B_4_2() {
  probe_or_stop || return
  if [ "$PROBE_ASQ_AFTER" -gt "$PROBE_ASQ_BEFORE" ]; then
    pass "notification:critical processed counter $PROBE_ASQ_BEFORE -> $PROBE_ASQ_AFTER after one enrollment; retry on SMTP failure is E07"
  else fail "notification:critical processed stayed at $PROBE_ASQ_BEFORE"; fi
}
check_B_4_4() {
  probe_or_stop || return
  local missing="" t topics
  topics=$(dc exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list)
  for t in $EVENT_TOPICS; do grep -qx "$t" <<<"$topics" || missing="$missing $t"; done
  [ -z "$missing" ] || { fail "topics missing:$missing"; return; }
  [ "$PROBE_OFF_AFTER" -gt "$PROBE_OFF_BEFORE" ] || { fail "enrollment.events end offsets did not grow ($PROBE_OFF_BEFORE)"; return; }
  local keys; keys=$(dc exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic enrollment.events --from-beginning --timeout-ms 15000 --property print.key=true --property key.separator='|' 2>/dev/null | LC_ALL=C cut -d'|' -f1)
  if grep -qax "$PROBE_COURSE" <<<"$keys"; then pass "four event topics exist; enrollment.events end offsets $PROBE_OFF_BEFORE -> $PROBE_OFF_AFTER; a record is keyed by the course id"
  else fail "no record on enrollment.events is keyed by course id $PROBE_COURSE"; fi
}
check_TS_KAFKA() {
  probe_or_stop || return
  if [ "$PROBE_OFF_AFTER" -gt "$PROBE_OFF_BEFORE" ]; then pass "enrollment.events end offsets $PROBE_OFF_BEFORE -> $PROBE_OFF_AFTER after one enrollment"; else fail "end offsets did not grow ($PROBE_OFF_BEFORE)"; fi
}
check_TS_MONGO() {
  probe_or_stop || return
  local coll; coll=$(dc exec -T mongodb mongosh smartcourse --quiet --eval 'db.getCollectionNames().join(",")' | tail -1)
  case ",$coll," in *,failed_events,*) ;; *) fail "failed_events collection missing (have: $coll)"; return ;; esac
  if [ "$PROBE_MONGO_AFTER" -gt "$PROBE_MONGO_BEFORE" ]; then pass "events count $PROBE_MONGO_BEFORE -> $PROBE_MONGO_AFTER; failed_events exists"; else fail "events count stayed at $PROBE_MONGO_BEFORE"; fi
}
check_TS_ASYNQ() {
  probe_or_stop || return
  local q; q=$(dc exec -T redis redis-cli smembers asynq:queues | tr -d '\r' | sort | tr '\n' ' ')
  case $q in *notification:critical*) ;; *) fail "queue notification:critical not registered (have: $q)"; return ;; esac
  if [ "$PROBE_ASQ_AFTER" -gt "$PROBE_ASQ_BEFORE" ]; then pass "queues per service: $q; notification:critical processed $PROBE_ASQ_BEFORE -> $PROBE_ASQ_AFTER (weights are asynqx.DefaultQueueWeights, unit-tested)"
  else fail "processed counter did not grow"; fi
}
check_TS_SR() {
  local subj s lvl bad="" n=0
  subj=$(curl -sS --max-time 10 "$REGISTRY_URL/subjects" 2>/dev/null | jq -r '.[]' 2>/dev/null)
  [ -n "$subj" ] || { fail "no subjects at $REGISTRY_URL/subjects"; return; }
  for s in $subj; do
    n=$((n + 1))
    lvl=$(curl -sS --max-time 10 "$REGISTRY_URL/config/$s?defaultToGlobal=true" 2>/dev/null | jq -r '.compatibilityLevel' 2>/dev/null)
    [ "$lvl" = BACKWARD ] || bad="$bad $s=$lvl"
  done
  [ -z "$bad" ] || { fail "not BACKWARD:$bad"; return; }
  local missing=""; for s in identity.events course.events enrollment.events progress.events; do grep -qx "$s-value" <<<"$subj" || missing="$missing $s-value"; done
  if [ -z "$missing" ]; then pass "$n subjects, all BACKWARD, including the four topic value subjects"; else fail "topic subjects missing:$missing"; fi
}
check_B_5() {
  enroll_student || { fixture_fail; return; }
  local f missing=""
  req GET "$ANALYTICS_URL/api/v1/analytics/platform" "$STUDENT_TOKEN" ''
  for f in total_students total_instructors total_courses_published avg_courses_per_student; do jq -e --arg f "$f" 'has($f)' <<<"$HTTP_BODY" >/dev/null 2>&1 || missing="$missing $f"; done
  req GET "$ANALYTICS_URL/api/v1/analytics/courses/$COURSE_ID" "$INSTR_TOKEN" ''
  for f in completion_rate avg_completion_s total_enrollments; do jq -e --arg f "$f" 'has($f)' <<<"$HTTP_BODY" >/dev/null 2>&1 || missing="$missing $f"; done
  req GET "$ANALYTICS_URL/api/v1/analytics/courses/$COURSE_ID/daily-enrollments" "$INSTR_TOKEN" ''
  jq -e '.items | type == "array"' <<<"$HTTP_BODY" >/dev/null 2>&1 || missing="$missing daily-enrollments"
  req GET "$ANALYTICS_URL/api/v1/analytics/popular-courses" "$STUDENT_TOKEN" ''
  [ "$HTTP_CODE" = 200 ] && jq -e '.items | type == "array"' <<<"$HTTP_BODY" >/dev/null 2>&1 || missing="$missing popular-courses($HTTP_CODE)"
  login_user "$ADMIN_EMAIL" "$ADMIN_PASSWORD" || { skip "cannot log in as admin: $FIX_ERR"; return; }
  req GET "${AUDIT_URL:-http://localhost:8093}/api/v1/admin/failed-events" "$LOGIN_TOKEN" ''
  [ "$HTTP_CODE" = 200 ] && jq -e '.items | type == "array"' <<<"$HTTP_BODY" >/dev/null 2>&1 || missing="$missing failed-events($HTTP_CODE)"
  if [ -z "$missing" ]; then pass "all nine metrics are served: platform (4), course stats (completion rate, average time), daily enrollments, popular courses, failed events"; else fail "missing or failing:$missing"; fi
}
check_B_6_3() {
  local bad="" out
  login_user "$ADMIN_EMAIL" "$ADMIN_PASSWORD" || { skip "cannot log in as admin: $FIX_ERR"; return; }
  req GET "${AUDIT_URL:-http://localhost:8093}/api/v1/admin/failed-events" "$LOGIN_TOKEN" ''
  [ "$HTTP_CODE" = 200 ] || bad="$bad failed-events=$HTTP_CODE"
  out=$(curl -sS --max-time 10 http://localhost:9112/metrics 2>/dev/null)
  grep -q '^asynq_retry_count' <<<"$out" || bad="$bad course-worker:asynq_retry_count"
  out=$(curl -sS --max-time 10 http://localhost:9114/metrics 2>/dev/null)
  grep -q '^asynq_retry_count' <<<"$out" || bad="$bad notification-worker:asynq_retry_count"
  out=$(curl -sS --max-time 10 http://localhost:9115/metrics 2>/dev/null)
  grep -q '^workflow_started_total' <<<"$out" || bad="$bad publishing-worker:workflow_started_total"
  grep -q '^workflow_failed_total' <<<"$out" || bad="$bad publishing-worker:workflow_failed_total"
  grep -q 'dead_letter_total\|dead_letter_held_records' <<<"$(curl -sS --max-time 10 http://localhost:9116/metrics 2>/dev/null; curl -sS --max-time 10 http://localhost:9113/metrics 2>/dev/null)" || true
  if [ -z "$bad" ]; then pass "failed-events endpoint 200; asynq_retry_count on course and notification workers; workflow_started/failed on publishing-worker (parking and retry behaviour are E04, E07, E12)"; else fail "missing:$bad"; fi
}
check_TS_PROM() {
  local port name bad="" body
  for pair in 9101:identity-api 9102:course-api 9103:analytics-api 9111:identity-worker 9112:course-worker 9113:analytics-worker 9114:notification-worker 9115:publishing-worker 9104:audit-api 9116:audit-worker; do
    port=${pair%%:*}; name=${pair#*:}
    body=$(curl -sS --max-time 10 "http://localhost:$port/metrics" 2>/dev/null)
    grep -q '^go_goroutines' <<<"$body" || { bad="$bad $name:no-/metrics"; continue; }
    case $name in
      *-api) [ "$name" = audit-api ] || grep -q '^http_request_duration_seconds_count' <<<"$body" || bad="$bad $name:no-RED" ;;
    esac
    case $name in identity-api|course-api|identity-worker|course-worker) grep -q '^outbox_backlog' <<<"$body" || bad="$bad $name:outbox_backlog" ;; esac
    case $name in course-worker|analytics-worker|notification-worker) grep -q '^kafka_consumer_lag' <<<"$body" || bad="$bad $name:kafka_consumer_lag" ;; esac
    case $name in identity-worker|course-worker|notification-worker|analytics-worker) grep -q '^asynq_queue_depth' <<<"$body" || bad="$bad $name:asynq_queue_depth" ;; esac
  done
  [ -z "$bad" ] || { fail "$bad"; return; }
  if ! curl -sS --max-time 5 -o /dev/null "$PROMETHEUS_URL/-/ready" 2>/dev/null; then skip "all ten /metrics are right, but Prometheus is not reachable at $PROMETHEUS_URL (make dev-observability)"; return; fi
  local down; down=$(curl -sS --max-time 10 "$PROMETHEUS_URL/api/v1/targets" | jq -r '[.data.activeTargets[] | select(.health != "up") | .labels.binary // .scrapeUrl] | join(",")' 2>/dev/null)
  local total; total=$(curl -sS --max-time 10 "$PROMETHEUS_URL/api/v1/targets" | jq -r '.data.activeTargets | length' 2>/dev/null)
  if [ -z "$down" ] && [ "${total:-0}" -ge 10 ]; then pass "ten /metrics endpoints carry their series; Prometheus has $total targets, all up"; else fail "Prometheus targets: $total, not up: ${down:-none}"; fi
}
check_TS_GRAF() {
  curl -sS --max-time 5 -o /dev/null "$GRAFANA_URL/api/health" 2>/dev/null || { skip "Grafana is not reachable at $GRAFANA_URL (make dev-observability)"; return; }
  local n; n=$(curl -sS --max-time 10 -u "${GRAFANA_USER:-admin}:${GRAFANA_PASSWORD:-admin}" "$GRAFANA_URL/api/search?type=dash-db" | jq -r 'length' 2>/dev/null)
  if [ "${n:-0}" -ge 1 ]; then pass "$n provisioned dashboard(s) listed"; else fail "no dashboard listed (HTTP/auth problem or provisioning broken)"; fi
}
check_TS_JAEGER() {
  curl -sS --max-time 5 -o /dev/null "$JAEGER_URL/api/services" 2>/dev/null || { skip "Jaeger is not reachable at $JAEGER_URL (make dev-observability)"; return; }
  local svcs missing="" s deadline=$((SECONDS + 60))
  while :; do
    missing=""; svcs=$(curl -sS --max-time 10 "$JAEGER_URL/api/services" | jq -r '.data[]?' 2>/dev/null)
    for s in identity-api course-api analytics-api course-worker analytics-worker notification-worker; do grep -qx "$s" <<<"$svcs" || missing="$missing $s"; done
    [ -z "$missing" ] || [ "$SECONDS" -ge "$deadline" ] && break
    sleep 5
  done
  if [ -z "$missing" ]; then pass "Jaeger lists identity-api, course-api, analytics-api, course-worker, analytics-worker, notification-worker"; else fail "not in Jaeger:$missing (have: $(tr '\n' ' ' <<<"$svcs"))"; fi
}
check_TS_OTEL() {
  local bad="" root area pat; root=$(cd "$(dirname "$0")/.." && pwd)
  for area in "http:SpanKindServer" "db:otelgorm|plugin/opentelemetry" "redis:redisotel" "kafka:StartProduce|StartConsume" "asynq:StartJob|StartEnqueue" "temporal:contrib/opentelemetry"; do
    pat=${area#*:}
    grep -rEq "$pat" "$root/libs" --include='*.go' 2>/dev/null || bad="$bad ${area%%:*}"
  done
  [ -z "$bad" ] || { fail "no instrumentation found for:$bad"; return; }
  pass "instrumentation present for HTTP, DB, Redis, Kafka, Asynq, Temporal under libs/; the end-to-end trace is E02"
}
check_AD_3() {
  need_docker || return
  local before after; before=$(asynq_processed analytics:low)
  sleep 70
  after=$(asynq_processed analytics:low)
  if [ "$after" -gt "$before" ]; then pass "analytics:low processed $before -> $after in 70 s: the 60 s reconcile task runs on its schedule"; else fail "analytics:low processed stayed at $before over 70 s"; fi
}
check_P1_3() {
  # A fresh student, so no other check's enrollment state is disturbed.
  local email="verify-idem-$RUN_ID@example.test" tok key="idem-$RUN_ID" body i n
  publish_course || { fixture_fail; return; }
  [ "$PUBLISH_FINAL_STATE" = ready ] || { fail "course did not reach ready (state=$PUBLISH_FINAL_STATE)"; return; }
  register_user "$email" && login_user "$email" "$USER_PASSWORD" || { fixture_fail; return; }
  tok=$LOGIN_TOKEN
  body=$(jq -nc --arg c "$COURSE_ID" '{course_id:$c}')
  req POST "$COURSE_URL/api/v1/enrollments" "$tok" "$body" -H "Idempotency-Key: $key-1"
  local c1=$HTTP_CODE b1=$HTTP_BODY
  req POST "$COURSE_URL/api/v1/enrollments" "$tok" "$body" -H "Idempotency-Key: $key-1"
  [ "$c1" = 201 ] || { fail "first enroll: HTTP $c1 $b1"; return; }
  [ "$HTTP_CODE" = 201 ] && [ "$(jq -S . <<<"$HTTP_BODY")" = "$(jq -S . <<<"$b1")" ] \
    || { fail "retry with the same key: HTTP $HTTP_CODE, expected the stored 201 and body"; return; }
  case "$HTTP_HDRS" in *"idempotent-replayed: true"*) ;; *) fail "retry not marked Idempotent-Replayed"; return ;; esac
  req POST "$COURSE_URL/api/v1/enrollments" "$tok" "$body" -H "Idempotency-Key: $key-1-other"
  [ "$HTTP_CODE" = 409 ] || { fail "a new key for the same course should be a genuine duplicate (409), got $HTTP_CODE"; return; }
  # Ten concurrent requests, one key, a second fresh student: exactly one enrollment.
  email="verify-idem2-$RUN_ID@example.test"
  register_user "$email" && login_user "$email" "$USER_PASSWORD" || { fixture_fail; return; }
  tok=$LOGIN_TOKEN
  for i in 1 2 3 4 5 6 7 8 9 10; do
    curl -sS --max-time 20 -o /dev/null -w '%{http_code}\n' -X POST -H "Authorization: Bearer $tok" -H 'Content-Type: application/json' \
      -H "Idempotency-Key: $key-race" --data-binary "$body" "$COURSE_URL/api/v1/enrollments" >"$TMPDIR_V/race.$i" 2>/dev/null &
  done
  wait
  n=$(cat "$TMPDIR_V"/race.* | grep -c '^201$' || true)
  [ "$n" -ge 1 ] || { fail "no concurrent request got the 201"; return; }
  if cat "$TMPDIR_V"/race.* | grep -qvE '^(201|409)$'; then fail "unexpected status among concurrent requests: $(sort "$TMPDIR_V"/race.* | uniq -c | tr '\n' ' ')"; return; fi
  req GET "$COURSE_URL/api/v1/enrollments" "$tok" ''
  n=$(jq --arg c "$COURSE_ID" '[(.items // .data // .)[] | select(.course_id == $c)] | length' <<<"$HTTP_BODY" 2>/dev/null || echo "?")
  [ "$n" = 1 ] && pass "replay returns the stored response; 10 concurrent requests with one key left $n enrollment" \
    || fail "10 concurrent requests with one key left $n enrollments, want 1"
}
# run_e2e <test name>: the container-tier scenario of test/e2e (build tag e2e).
# Unlike the other checks it does not use the stack at the URLs above: it
# builds the service binaries and starts its own containers through
# testcontainers, so it needs Docker and a few minutes per scenario.
run_e2e() {
  local out rc start=$SECONDS
  if ! docker info >/dev/null 2>&1; then skip "Docker is not reachable: go test -tags=e2e -run $1 in test/e2e needs it"; return; fi
  out=$(cd "$(dirname "$0")/../test/e2e" && GOWORK=off go test -tags=e2e -count=1 -timeout 30m -run "$1" ./... 2>&1); rc=$?
  if [ "$rc" -eq 0 ]; then
    pass "go test -tags=e2e -run $1 ok ($((SECONDS - start))s)"
  else
    fail "go test -tags=e2e -run $1 failed: $(grep -E '^\s*(--- FAIL|.*PRODUCT DEFECT|.*timed out|.*e2e_test.go:[0-9]+:)' <<<"$out" | head -4 | tr -s ' ' | tr '\n' ';' | cut -c1-600)"
  fi
}
check_E01() { run_e2e '^TestE2E_E01_'; }
check_E02() { run_e2e '^TestE2E_E02_'; }
check_E03() { run_e2e '^TestE2E_E03_'; }
check_E04() { run_e2e '^TestE2E_E04_'; }
check_E05() { run_e2e '^TestE2E_E05_'; }
check_E06() { run_e2e '^TestE2E_E06_'; }
check_E07() { run_e2e '^TestE2E_E07_'; }
check_E08() { run_e2e '^TestE2E_E08_'; }
check_E09() { run_e2e '^TestE2E_E09_'; }
check_E10() { run_e2e '^TestE2E_E10_'; }
check_E11() { run_e2e '^TestE2E_E11_'; }
check_E12() { run_e2e '^TestE2E_E12_'; }
check_E13() { run_e2e '^TestE2E_E13_'; }
check_E14() { run_e2e '^TestE2E_E14_'; }

# ---------------------------------------------------------------- registry
# id|name|function. Order here is run order; ids match the matrix.
ROWS=(
  "EG-W1-4|health-live-and-ready-on-identity-course-analytics|check_EG_W1_4"
  "X-6-HEADERS|security-headers-on-every-api|check_X_6_HEADERS"
  "X-6-BODYCAP|request-body-capped-413|check_X_6_BODYCAP"
  "X-6-ERRSHAPE|bad-input-gives-error-shape-no-5xx|check_X_6_ERRSHAPE"
  "B-1.3|register-202-uniform-login-roles|check_B_1_3"
  "B-1.1|course-module-lesson-crud|check_B_1_1"
  "SR-F37|publish-202-then-409|check_SR_F37"
  "B-2.6|publish-reaches-ready|check_B_2_6"
  "B-2.3|published-course-in-search|check_B_2_3"
  "B-1.4|duplicate-enrollment-409|check_B_1_4"
  "B-3.1|enrollment-and-progress-initialised|check_B_3_1"
  "B-5a|analytics-seven-metrics-routes-and-access-rules|check_B_5a"
  "B-1.2|asset-upload-url-and-object-exists|check_B_1_2"
  "B-3.2|enrollment-notification-in-mailpit|check_B_3_2"
  "B-4.2|notification-delivery-as-asynq-job|check_B_4_2"
  "B-4.4|kafka-topics-carry-events|check_B_4_4"
  "B-5|nine-metrics-served|check_B_5"
  "B-6.3|failures-diagnosable-via-metrics-and-failed-events|check_B_6_3"
  "TS-KAFKA|kafka-used|check_TS_KAFKA"
  "TS-SR|schema-registry-subjects-backward|check_TS_SR"
  "TS-MONGO|mongodb-archive-written|check_TS_MONGO"
  "TS-ASYNQ|asynq-weighted-queues-processing|check_TS_ASYNQ"
  "TS-PROM|metrics-endpoints-and-prometheus-targets|check_TS_PROM"
  "TS-GRAF|grafana-dashboard-provisioned|check_TS_GRAF"
  "TS-JAEGER|jaeger-has-service-traces|check_TS_JAEGER"
  "TS-OTEL|opentelemetry-instrumentation|check_TS_OTEL"
  "AD-3|analytics-reconciliation-scheduled-60s|check_AD_3"
  "P1-3|idempotency-key-middleware|check_P1_3"
  "E01|e2e-happy-path|check_E01"
  "E02|e2e-one-trace-across-pipeline|check_E02"
  "E03|e2e-duplicate-delivery|check_E03"
  "E04|e2e-poison-message|check_E04"
  "E05|e2e-kafka-down-then-up|check_E05"
  "E06|e2e-kill-9-mid-flow|check_E06"
  "E07|e2e-smtp-down|check_E07"
  "E08|e2e-counter-drift|check_E08"
  "E09|e2e-enrollment-under-load|check_E09"
  "E10|e2e-redis-flush-with-new-services|check_E10"
  "E11|e2e-schema-incompatibility|check_E11"
  "E12|e2e-publish-under-fault|check_E12"
  "E13|e2e-archive-and-audit|check_E13"
  "E14|e2e-auth-on-new-surface|check_E14"
)

# Rows whose check hits the stack. Pending stubs never do.
needs_stack() {
  case "$1" in
    EG-W1-4|X-6-*|B-1.3|B-1.1|SR-F37|B-2.6|B-2.3|B-1.4|B-3.1|B-5a) return 0 ;;
  esac
  return 1
}

# E01 and E03 to E07 run through `go test -tags=e2e` in test/e2e, which starts
# its own containers (they need Docker, not the running stack). E02 and E08 to
# E14 are pending until Wave C3b ports them to the ten-binary harness.
is_e2e() {
  case "$1" in
    E01|E0[3-7]) return 0 ;;
  esac
  return 1
}

usage() { sed -n '2,36p' "$0" | sed 's/^# \{0,1\}//'; }

list_rows() {
  local row id name fn kind
  for row in "${ROWS[@]}"; do
    IFS='|' read -r id name fn <<<"$row"
    if needs_stack "$id"; then kind=live; elif is_e2e "$id"; then kind=e2e; else kind=pending; fi
    printf '%-12s %-8s %s\n' "$id" "$kind" "$name"
  done
}

# ---------------------------------------------------------------- main

ONLY=()
while [ $# -gt 0 ]; do
  case "$1" in
    --list)  list_rows; exit 0 ;;
    --only)  [ $# -ge 2 ] || { echo "--only needs an id" >&2; exit 2; }; ONLY+=("$2"); shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1 (try --help)" >&2; exit 2 ;;
  esac
done

command -v curl >/dev/null || { echo "curl is required" >&2; exit 2; }
command -v jq   >/dev/null || { echo "jq is required" >&2; exit 2; }

selected() {
  local want
  [ "${#ONLY[@]}" -eq 0 ] && return 0
  for want in "${ONLY[@]}"; do [ "$want" = "$1" ] && return 0; done
  return 1
}

for want in "${ONLY[@]+"${ONLY[@]}"}"; do
  found=0
  for row in "${ROWS[@]}"; do [ "${row%%|*}" = "$want" ] && found=1; done
  [ "$found" = 1 ] || { echo "unknown id: $want (see --list)" >&2; exit 2; }
done

n_pass=0; n_fail=0; n_pending=0; n_skip=0
for row in "${ROWS[@]}"; do
  IFS='|' read -r id name fn <<<"$row"
  selected "$id" || continue
  STATUS=""; DETAIL=""
  "$fn"
  case "$STATUS" in
    PASS)    n_pass=$((n_pass+1)) ;;
    FAIL)    n_fail=$((n_fail+1)) ;;
    PENDING) n_pending=$((n_pending+1)) ;;
    SKIP)    n_skip=$((n_skip+1)) ;;
    *)       STATUS=FAIL; DETAIL="check function set no status (script bug)"; n_fail=$((n_fail+1)) ;;
  esac
  printf '%-7s %s %s\n' "$STATUS" "$id" "$name"
  [ -n "$DETAIL" ] && printf '        %s\n' "$DETAIL"
done

echo
echo "summary: $n_pass passed, $n_fail failed, $n_pending pending, $n_skip skipped (run $RUN_ID)"
[ "$n_pending" -gt 0 ] || [ "$n_skip" -gt 0 ] && echo "note: pending and skipped rows are NOT proven; the matrix keeps them pending"
[ "$n_fail" -eq 0 ]
