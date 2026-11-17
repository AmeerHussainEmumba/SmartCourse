#!/usr/bin/env bash
# Static check for HTTP-server resource-abuse hardening (slowloris, oversized
# bodies, spoofable client IP). Looks at the source rather than at a running
# server so it can run in CI without infrastructure.
#
# For every `http.Server{` literal in non-test Go code it requires:
#   ReadHeaderTimeout  (slowloris on headers)
#   ReadTimeout        (slow request BODY: ReadHeaderTimeout alone does not cover it)
#   WriteTimeout       (slow readers holding a handler/connection open)
#   IdleTimeout        (keep-alive connection hoarding)
# and across the HTTP libraries/services it requires:
#   a request-body cap (http.MaxBytesReader / http.MaxBytesHandler / a MaxBodyBytes middleware)
#   a trusted-proxy policy (SetTrustedProxies / TrustedPlatform), because gin trusts every
#     X-Forwarded-For / X-Real-IP by default and ratelimit.Middleware keys on c.ClientIP()
#
# Usage: scripts/check-http-hardening.sh     (exit 1 on any missing control)
set -uo pipefail
cd "$(dirname "$0")/.."

fails=0
fail() { echo "FAIL $*"; fails=$((fails+1)); }
ok()   { echo "ok   $*"; }

srcs=$(grep -rl --include='*.go' 'http\.Server{' . | grep -v '_test\.go' || true)
for f in $srcs; do
  # take the struct literal body: from "http.Server{" to the first line that is just "}"
  body=$(awk '/http\.Server\{/ {p=1} p {print} p && /^[[:space:]]*\}[[:space:]]*$/ {exit}' "$f")
  for field in ReadHeaderTimeout ReadTimeout WriteTimeout IdleTimeout; do
    if grep -qE "^[[:space:]]*$field:" <<<"$body"; then ok "$f sets $field"; else fail "$f: http.Server has no $field"; fi
  done
done
[ -n "$srcs" ] || fail "no http.Server literal found (script out of date?)"

if grep -rlE --include='*.go' 'MaxBytesReader|MaxBytesHandler|MaxBodyBytes|LimitRequestBody' libs services tools 2>/dev/null | grep -qv '_test.go'; then
  ok "a request body size cap exists"
else
  fail "no request body cap anywhere (no http.MaxBytesReader / MaxBytesHandler): every JSON endpoint reads unbounded bodies (reproducer: TestBug_NoRequestBodyLimit_LoginAcceptsHugeBody in services/identity)"
fi

if grep -rlE --include='*.go' 'SetTrustedProxies|TrustedPlatform' libs services 2>/dev/null | grep -qv '_test.go'; then
  ok "a trusted-proxy policy is configured"
else
  fail "no SetTrustedProxies/TrustedPlatform call: c.ClientIP() trusts any X-Forwarded-For, so per-IP rate limits are bypassable (reproducer: TestBug_ClientIPIsSpoofableViaForwardedHeaders in libs/router)"
fi

echo
[ "$fails" -eq 0 ] && echo "http hardening: all controls present" || echo "http hardening: $fails missing control(s)"
[ "$fails" -eq 0 ]
