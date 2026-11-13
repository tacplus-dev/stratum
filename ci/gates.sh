#!/usr/bin/env bash
# `just gates` as CI runs them (.github/workflows/release.yml): in the build image the checkout's tacplus.lock
# pins, by digest, as the build pipeline runs them (tacplus-dev/roadmap decisions/deterministic-builds-2026-10-27.md).
# Generation clones only public tacplus-dev repositories, so it needs no credential. audit-jar's trivy is the
# host's (ci/trivy.sh), mounted in.
#
# Usage:  TRIVY_BIN=/path/to/trivy ci/gates.sh [CHECKOUT]
set -euo pipefail

src="$(cd "${1:-.}" && pwd)"
lock="$src/tacplus.lock"
[[ -f "$lock" ]] || { echo "gates.sh: $src has no tacplus.lock" >&2; exit 1; }
image="$(sed -n -E 's/^build_image:[[:space:]]*"?([^"[:space:]]+)"?.*$/\1/p' "$lock" | head -1)"
[[ "$image" == *@sha256:* ]] || { echo "gates.sh: $lock names no build_image by digest" >&2; exit 1; }
trivy="${TRIVY_BIN:-$(command -v trivy || true)}"
[[ -x "$trivy" ]] || { echo "gates.sh: no trivy (TRIVY_BIN, or ci/trivy.sh)" >&2; exit 1; }

docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -v "$src:$src" -w "$src" \
  -v "$trivy:/opt/ci/trivy:ro" -e TRIVY_BIN=/opt/ci/trivy \
  "$image" just gates
