#!/usr/bin/env bash
# Installs the trivy release CI scans the jar with (just audit-jar) into DIR, checked against the sha256 of
# its release archive pinned here; any other archive is refused. Move the version and both sums together.
#
# Usage:  ci/trivy.sh DIR      (prints DIR/trivy)
set -euo pipefail

version=0.76.2
case "$(uname -m)" in
  x86_64) asset="trivy_${version}_Linux-64bit.tar.gz" sum=bc0fc726e017b8927a62b2d3531d7a84ee01769daf5c99ed866dced54089fdbf ;;
  aarch64 | arm64) asset="trivy_${version}_Linux-ARM64.tar.gz" sum=ab9d51f86e6a5fb936f605faff6b593ac51bc378fff4d3e8b9d58358b9c367c3 ;;
  *) echo "trivy.sh: unsupported machine $(uname -m)" >&2; exit 1 ;;
esac

dir="$1"
mkdir -p "$dir"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
curl -fsSL --retry 3 -o "$tmp/$asset" "https://github.com/aquasecurity/trivy/releases/download/v$version/$asset"
echo "$sum  $tmp/$asset" | sha256sum -c --quiet - >&2
tar -xzf "$tmp/$asset" -C "$dir" trivy
echo "$dir/trivy"
