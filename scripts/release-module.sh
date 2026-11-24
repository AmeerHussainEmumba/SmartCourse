#!/usr/bin/env bash
# Tags a release of one Go module in this monorepo. Nested modules are tagged
# with their directory as a prefix, which is how the Go toolchain finds them:
#   libs/auth/v1.2.0   services/course/v0.3.0
# See docs/guides/versioning.md for when to tag and how services pin versions.
#
# Usage: scripts/release-module.sh <module-dir> <vX.Y.Z> [--dry-run] [--push]
#
# By default this only creates the tag LOCALLY, after checking the module is
# ready. Pushing a tag publishes it (the Go module proxy caches it and it cannot
# be cleanly retracted), so it needs the explicit --push flag; the
# release-module GitHub workflow passes it, and only a human can trigger that.
set -euo pipefail

cd "$(dirname "$0")/.."

dir=${1:-}; version=${2:-}; shift 2 || true
dry=0; push=0
for a in "$@"; do
  case "$a" in
    --dry-run) dry=1 ;;
    --push) push=1 ;;
    *) echo "unknown flag: $a" >&2; exit 2 ;;
  esac
done

[ -n "$dir" ] && [ -n "$version" ] || { sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
dir=${dir#./}; dir=${dir%/}

fail() { echo "release-module: $*" >&2; exit 1; }

[[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "version must look like v1.2.3, got '$version' (pre-releases are not used here)"
grep -qx "	./$dir" go.work || fail "$dir is not listed in go.work"
[ -f "$dir/go.mod" ] || fail "$dir has no go.mod"
[ -f "$dir/CHANGELOG.md" ] || fail "$dir has no CHANGELOG.md"
grep -q "^## \[${version#v}\]" "$dir/CHANGELOG.md" || fail "$dir/CHANGELOG.md has no '## [${version#v}]' section — write the release notes first"

tag="$dir/$version"
git rev-parse -q --verify "refs/tags/$tag" >/dev/null && fail "tag $tag already exists"
[ -z "$(git status --porcelain -- "$dir")" ] || fail "$dir has uncommitted changes"

# A module being released must be consumable on its own: no replace pointing
# at a sibling that isn't itself released is a policy decision for the owner,
# but the module must at least build and test standalone.
echo "== verifying $dir standalone (GOWORK=off)"
(cd "$dir" && GOWORK=off go build ./... && GOWORK=off go vet ./... && GOWORK=off go test ./...)

scripts/check-module-graph.sh >/dev/null || fail "module graph check failed"

if [ "$dry" -eq 1 ]; then echo "dry run: would create tag $tag at $(git rev-parse --short HEAD)"; exit 0; fi

git tag -a "$tag" -m "$dir $version"
echo "created local tag $tag at $(git rev-parse --short HEAD)"
if [ "$push" -eq 1 ]; then
  git push origin "refs/tags/$tag"
  echo "pushed $tag"
else
  echo "not pushed. To publish: git push origin refs/tags/$tag   (or run the release-module workflow)"
fi
