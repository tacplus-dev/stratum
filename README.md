# stratum

Stratum is the platform's record store: one per tenant, it holds that tenant's tacplus-declared records
(`option (spec.record)`) in memory, serves the generated record plane over them (writes, Get, queries,
Watch), and makes every write durable through the redo service before applying it. See [design.md](design.md),
[RECORD_EVENT_ARCHITECTURE.md](RECORD_EVENT_ARCHITECTURE.md) and [docs/DURABILITY.md](docs/DURABILITY.md).

## Build

```
just generate                          # tacplus build: generated model under model/target/generated-sources
mvn -B -Dtacplus.skip=true install     # model, codegen (stratum-codegen Maven plugin), server and their tests
```

`tacplus.yaml` declares the compiler floor (`tacplus:`), the Java package (`com.github.tacplusdev.stratum`) and
the proto sources this build depends on, all fetched from github.com/tacplus-dev.

Where this code came from: [PROVENANCE.md](PROVENANCE.md).

## Releasing

Stratum is released by version, so fleets pin a release line (`X.Y`), not a commit (tacplus-dev/tacplus#180).

`release.yml` is not on main yet: a looper's token can't push `.github/workflows/` until tacplus-dev/looper#115,
so the workflow waits on #3, which carries it. The scripts it runs (`ci/`) are here and tested.

- **Versions:** `vMAJOR.MINOR.PATCH` tags, annotated: on main for a minor, and on `release/MAJOR.MINOR` for a
  patch to an older line. Stratum stays on `0.x` until SeaBeam's launch (2026-12-01), when `1.0.0` is cut. On
  `0.x` a minor may break; a patch never does. A tag is never moved or deleted.
- **Released by the change:** a looper change merged to main is released by the same run: a fix as a patch, a
  feature as the next minor (from main only, no backport).
- **Cutting one:** tag the commit and push the tag:

  ```
  git tag -a v0.2.0 -m v0.2.0 <commit on main>
  git push origin v0.2.0
  ```

  The tag runs [`.github/workflows/release.yml`](.github/workflows/release.yml): it runs the gates on the tag in
  the build image `tacplus.lock` pins (`ci/gates.sh`), builds the launcher at a pinned commit, and publishes
  `ghcr.io/tacplus-dev/stratum:X.Y.Z` with the jar the gates built, labelled with the version, the commit and
  this repository. The line tag `X.Y` moves to it only when it is that line's highest release, so a patch on an
  older line never moves a newer one and a line never moves backwards (`ci/release-tags.sh`). Then it creates the
  GitHub release with the commits since the previous release (`ci/release-notes.sh`), marked Latest only when it
  is the highest release of all (`ci/release-flag.sh`). A pre-release (`vX.Y.Z-rc.N`) publishes only its own tag.
  The image is built for linux/arm64, what the fleets run on; amd64 is off by default, as in the fleets, and
  `PLATFORMS: linux/arm64,linux/amd64` in the workflow builds both into one image. The package is public: pulls
  need no credential.
- **A tag without the workflow** (v0.1.0, or a patch on a `release/` branch cut before it): run the workflow from
  main with that version and `publish` set (`gh workflow run release.yml -f version=v0.1.1 -f publish=true`). It
  builds the tag as tagged, with main's release scripts; a tag that predates `tacplus.lock` is built with the
  tacplus main pins.
- **A dry run:** the same without `publish` (`gh workflow run release.yml -f version=v0.2.1`) tags main's commit
  in the run only, runs the gates, builds the image and prints the tags and notes the release would get. It
  publishes nothing.

The release scripts' tests (`ci/release_test.sh`) run in `just gates`.
