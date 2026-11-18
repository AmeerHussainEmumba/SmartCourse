#!/usr/bin/env bash
# Regenerates libs/events/gen from libs/events/proto (RFC §6.8: generated code
# is committed, so only a contributor who edits a .proto needs this).
#
# The toolchain is pure Go and pinned below. It is installed into a throwaway
# directory (override with EVENTS_TOOLS_DIR), never system-wide, and the
# result is the same on every machine: same buf + same protoc-gen-go, same
# output. CI/people can verify with `./generate.sh && git diff --exit-code gen`.
set -euo pipefail

BUF_VERSION=v1.75.0
PROTOC_GEN_GO_VERSION=v1.36.11 # keep equal to google.golang.org/protobuf in go.mod

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tools="${EVENTS_TOOLS_DIR:-${TMPDIR:-/tmp}/smartcourse-events-tools}"
mkdir -p "$tools"

# GOWORK=off: install from the module proxy, not from this workspace.
GOWORK=off GOBIN="$tools" go install "github.com/bufbuild/buf/cmd/buf@${BUF_VERSION}"
GOWORK=off GOBIN="$tools" go install "google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}"

export PATH="$tools:$PATH"
cd "$here/proto"
buf lint
buf generate
echo "generated into $here/gen"
