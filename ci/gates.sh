#!/usr/bin/env bash
# `just gates` as CI runs them (.github/workflows/release.yml): in the build image the checkout's tacplus.lock
# pins, by digest, as the build pipeline runs them (tacplus-dev/roadmap decisions/deterministic-builds-2026-10-27.md).
# Generation clones only public tacplus-dev repositories, so it needs no credential. main's gates no longer scan
# the jar (the build's security job does, #36), but a tag cut before that (v0.1.0) runs audit-jar: its trivy is
# the host's (TRIVY_BIN, ci/trivy.sh), mounted in when there is one.
#
# Usage:  [TRIVY_BIN=/path/to/trivy] ci/gates.sh [CHECKOUT]
set -euo pipefail

src="$(cd "${1:-.}" && pwd)"
lock="$src/tacplus.lock"
[[ -f "$lock" ]] || { echo "gates.sh: $src has no tacplus.lock" >&2; exit 1; }
image="$(sed -n -E 's/^build_image:[[:space:]]*"?([^"[:space:]]+)"?.*$/\1/p' "$lock" | head -1)"
[[ "$image" == *@sha256:* ]] || { echo "gates.sh: $lock names no build_image by digest" >&2; exit 1; }
trivy="${TRIVY_BIN:-$(command -v trivy || true)}"
mount=()
[[ -x "$trivy" ]] && mount=(-v "$trivy:/opt/ci/trivy:ro" -e TRIVY_BIN=/opt/ci/trivy)

docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -v "$src:$src" -w "$src" "${mount[@]}" \
  "$image" just gates
