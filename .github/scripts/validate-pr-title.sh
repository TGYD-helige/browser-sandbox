#!/usr/bin/env bash
set -euo pipefail
pattern='^(feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(\([^)]+\))?(!)?: [^[:space:]].*$'
if [[ ! "${PR_TITLE:-}" =~ $pattern ]]; then
  echo '::error::Use a Conventional Commit title, for example: feat(browser): support persistent profiles'
  exit 1
fi
