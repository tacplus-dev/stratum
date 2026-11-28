#!/usr/bin/env bash
# Builds the release image (.github/workflows/release.yml) from the Dockerfile of CHECKOUT, a checkout of the
# release's tag whose gates have run: server/target/stratum.jar, and launcher-linux-<arch> beside the
# Dockerfile for each platform. One build for PLATFORMS (default linux/arm64; "linux/arm64,linux/amd64" builds
# both), labelled with VERSION, the tag's commit and its date, and this repository as the source.
#
# Usage:  ci/release-image.sh CHECKOUT VERSION [TAG...]
#
# With TAGs, $IMAGE:<TAG> (each TAG) is pushed, one multi-architecture image. Without, nothing is pushed: the
# dry run.
set -euo pipefail

src="$(cd "$1" && pwd)"
version="$2"
shift 2
image="${IMAGE:-ghcr.io/tacplus-dev/stratum}"
platforms="${PLATFORMS:-linux/arm64}"

[[ -f "$src/server/target/stratum.jar" ]] || { echo "release-image.sh: no server/target/stratum.jar: run the gates first" >&2; exit 1; }
for platform in ${platforms//,/ }; do
  [[ -f "$src/launcher-linux-${platform#linux/}" ]] || { echo "release-image.sh: no launcher-linux-${platform#linux/} beside the Dockerfile" >&2; exit 1; }
done

args=(--platform "$platforms"
  --build-arg "VERSION=$version"
  --build-arg "VCS_REF=$(git -C "$src" rev-parse HEAD)"
  --build-arg "BUILD_DATE=$(git -C "$src" log -1 --format=%cI HEAD)"
  --label "org.opencontainers.image.source=https://github.com/tacplus-dev/stratum")
if (($#)); then
  for tag in "$@"; do args+=(-t "$image:$tag"); done
  args+=(--push)
else
  args+=(--output type=cacheonly)
fi
docker buildx build "${args[@]}" "$src"
