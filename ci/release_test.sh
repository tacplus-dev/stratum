#!/usr/bin/env bash
# Tests of the release scripts (ci/release-tags.sh, ci/release-flag.sh, ci/release-notes.sh) on a fixture
# repository: a patch moves only its own line, a line tag never moves backwards, versions sort by number, a
# pre-release is no release, and an older line is never marked Latest. Run by `just gates` (test-release).
#
# Usage:  ci/release_test.sh
set -euo pipefail

ci="$(cd "$(dirname "$0")" && pwd)"
repo="$(mktemp -d)"
trap 'rm -rf "$repo"' EXIT

failures=0
check() { # name got want
  if [[ "$2" == "$3" ]]; then
    echo "ok   $1"
  else
    echo "FAIL $1: got '$2', want '$3'"
    failures=$((failures + 1))
  fi
}
fails() { # name command...: the command must exit non-zero
  if (cd "$repo" && "${@:2}") >/dev/null 2>&1; then
    echo "FAIL $1: succeeded"
    failures=$((failures + 1))
  else
    echo "ok   $1"
  fi
}
in_repo() { (cd "$repo" && "$@"); }
commit() { in_repo git commit -q --allow-empty -m "$1"; }
tag() { in_repo git tag -a -m "$1" "$1"; }
tags() { in_repo "$ci/release-tags.sh" "$1"; }
flag() { in_repo "$ci/release-flag.sh" "$1"; }
notes() { in_repo "$ci/release-notes.sh" "$1"; }

in_repo git init -q -b main
in_repo git config user.email test@example.com
in_repo git config user.name test
in_repo git config tag.gpgSign false
in_repo git config commit.gpgSign false

# The first release: its line tag, Latest, every commit in its notes.
commit "first"
commit "second"
tag v0.1.0
check "first release: its own tag and its line" "$(tags v0.1.0)" "0.1.0 0.1"
check "first release: Latest" "$(flag v0.1.0)" "--latest"
check "first release: notes list every commit" "$(notes v0.1.0 | sed -n '1p;3,$p' | sed 's/ (.*//')" \
  "$(printf 'Changes:\n- second\n- first')"

# A patch moves its line; re-releasing the older patch (a publish of an existing tag) never moves it back.
commit "a fix"
tag v0.1.1
check "patch: its line moves" "$(tags v0.1.1)" "0.1.1 0.1"
check "older patch of the line: the line does not move back" "$(tags v0.1.0)" "0.1.0"
check "patch: notes since the previous release" "$(notes v0.1.1 | sed 's/ (.*//')" \
  "$(printf 'Changes since v0.1.0:\n\n- a fix')"

# A new minor on main, then a patch on the older line's release branch.
commit "a feature"
tag v0.2.0
in_repo git checkout -q -b release/0.1 v0.1.1
commit "a backported fix"
tag v0.1.2
in_repo git checkout -q main
check "minor: its own line" "$(tags v0.2.0)" "0.2.0 0.2"
check "patch on an older line: only that line moves" "$(tags v0.1.2)" "0.1.2 0.1"
check "patch on an older line: not Latest" "$(flag v0.1.2)" "--latest=false"
check "the highest release: Latest" "$(flag v0.2.0)" "--latest"
check "patch on an older line: notes since that line's previous release" "$(notes v0.1.2 | sed 's/ (.*//')" \
  "$(printf 'Changes since v0.1.1:\n\n- a backported fix')"

# Versions sort by number, not as text: 0.1.10 is above 0.1.9, 0.10 above 0.9.
for p in 3 4 5 6 7 8 9; do
  in_repo git checkout -q release/0.1
  commit "fix $p"
  tag "v0.1.$p"
done
commit "fix 10"
tag v0.1.10
in_repo git checkout -q main
check "0.1.10 is above 0.1.9: it moves the line" "$(tags v0.1.10)" "0.1.10 0.1"
check "0.1.9 is below 0.1.10: it does not" "$(tags v0.1.9)" "0.1.9"
check "notes of 0.1.10: since 0.1.9" "$(notes v0.1.10 | head -1)" "Changes since v0.1.9:"
commit "nine"
tag v0.9.0
commit "ten"
tag v0.10.0
check "0.10.0 is above 0.9.0: Latest" "$(flag v0.10.0)" "--latest"
check "0.9.0 is below 0.10.0: not Latest" "$(flag v0.9.0)" "--latest=false"
check "notes of 0.10.0: since 0.9.0" "$(notes v0.10.0 | sed 's/ (.*//')" \
  "$(printf 'Changes since v0.9.0:\n\n- ten')"

# A pre-release gets only its own tag, is never Latest, and is no previous release.
commit "candidate"
tag v0.11.0-rc.1
check "pre-release: only its own tag" "$(tags v0.11.0-rc.1)" "0.11.0-rc.1"
check "pre-release: marked a pre-release" "$(flag v0.11.0-rc.1)" "--prerelease"
check "pre-release: the line's highest release still moves it" "$(tags v0.10.0)" "0.10.0 0.10"
check "pre-release: the highest release is still Latest" "$(flag v0.10.0)" "--latest"
commit "final"
tag v0.11.0
check "release after its pre-release: notes since the previous release" "$(notes v0.11.0 | head -1)" \
  "Changes since v0.10.0:"

# Anything else is refused.
fails "tags: not vMAJOR.MINOR.PATCH" "$ci/release-tags.sh" v0.1
fails "flag: not vMAJOR.MINOR.PATCH" "$ci/release-flag.sh" 0.1.x
fails "notes: not vMAJOR.MINOR.PATCH" "$ci/release-notes.sh" latest

if ((failures)); then
  echo "release_test.sh: $failures failed" >&2
  exit 1
fi
echo "release_test.sh: all passed"
