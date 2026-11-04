#!/usr/bin/env bash
set -euo pipefail
tag="${1:?Provide a new version tag}"
repository="${GITHUB_REPOSITORY:?Provide the release repository}"
if response="$(gh api --include --silent "repos/$repository/git/ref/tags/$tag" 2>/dev/null)"; then
  echo 'Git version tag already exists; choose a new package version.' >&2
  exit 1
fi
# Only a confirmed missing ref permits automatic creation from the verified commit.
if [[ ! "$response" =~ ^HTTP/[0-9.]+\ 404([[:space:]]|$) ]]; then
  echo 'Unable to confirm that the Git tag is absent; publication stopped.' >&2
  exit 1
fi
