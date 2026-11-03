# Local development builds, and `just gates`: the checks the looper and the
# nightly scanner run before and after every change to main.

TACPLUS := env_var_or_default("TACPLUS_BIN", "tacplus")
TRIVY := env_var_or_default("TRIVY_BIN", "trivy")

default: generate

# Regenerate model/target/generated-sources (not committed) with the compiler
# line tacplus.yaml pins. Generated code is never committed or edited by hand,
# so generation must leave every tracked file as it found it.
generate:
  #!/usr/bin/env bash
  set -euo pipefail
  before="$(git diff HEAD | sha256sum)"
  {{TACPLUS}} build --tag latest
  if [[ "$(git diff HEAD | sha256sum)" != "$before" ]]; then
    echo "generate: tacplus build changed tracked files:" >&2; git status --short >&2; exit 1
  fi

# Nothing under a build output may be tracked.
check-generated: generate
  #!/usr/bin/env bash
  set -euo pipefail
  if [[ -n "$(git ls-files model/target codegen/target server/target target)" ]]; then
    echo "check-generated: target/ is build output and must not be committed" >&2; exit 1
  fi

# model's antrun step skips tacplus when the generated sources exist, so this
# compiles exactly what generate just wrote.
build: generate
  mvn -B -q install -DskipTests

test: build
  mvn -B test

# Advisories in the release jar itself, shaded copies included: a library
# another jar relocates into its own packages (jackson inside Hazelcast, #28) is
# no dependency, so no check of the resolved tree sees it. HIGH and above fail,
# except those .trivyignore.yaml accepts, each until its expiry.
audit-jar: build
  {{TRIVY}} rootfs --scanners vuln --severity HIGH,CRITICAL --exit-code 1 --ignorefile .trivyignore.yaml server/target/stratum.jar

# The gates every change passes before it is pushed.
gates: check-generated test audit-jar

clean:
  rm -rf model/target codegen/target server/target target
