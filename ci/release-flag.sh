#!/usr/bin/env bash
# How `gh release create` marks a release (.github/workflows/release.yml): --latest for the highest release of
# all, --latest=false for any other (a patch on an older line is never Latest), --prerelease for vX.Y.Z-pre.
#
# Usage:  ci/release-flag.sh vX.Y.Z
#
# Run in the checkout with its tags fetched. Tested by ci/release_test.sh.
set -euo pipefail

ref="$1"
if [[ "$ref" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-[0-9A-Za-z.-]+$ ]]; then
  echo "--prerelease"
  exit 0
fi
if [[ ! "$ref" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "release-flag.sh: $ref is not vMAJOR.MINOR.PATCH" >&2
  exit 1
fi

# The highest release, by version, not lexically.
highest="$(git tag -l 'v*' | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -t. -k1.2,1n -k2,2n -k3,3n | tail -1)"
if [[ "$highest" == "$ref" ]]; then
  echo "--latest"
else
  echo "--latest=false"
fi
