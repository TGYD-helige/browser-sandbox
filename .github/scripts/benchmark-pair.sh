#!/usr/bin/env bash
set -euo pipefail
base_ref="${1:?Provide the baseline commit}"
output="${2:?Provide an output directory}"
context="$(mktemp -d)"
baseline_image="browser-sandbox:baseline-${RANDOM}-${RANDOM}"
cleanup() {
  docker image rm "$baseline_image" >/dev/null 2>&1 || true
  rm -rf "$context"
}
trap cleanup EXIT
mkdir -p "$output"
git fetch --depth=1 origin "$base_ref"
git archive FETCH_HEAD | tar -x -C "$context"
test -f "$context/Dockerfile"
# Apply the identical measurement client, leaving the baseline runtime unchanged.
cp .github/scripts/benchmark-*.mjs "$context/.github/scripts/"
printf '\nCOPY .github/scripts/benchmark-*.mjs /opt/browser-sandbox/.github/scripts/\n' >> "$context/Dockerfile"
docker build -t "$baseline_image" "$context"
# Historical baselines need not satisfy the current browser-layout contract.
VERIFY_IMAGE_LAYOUT=0 BENCHMARK_OUTPUT="$output/baseline.json" bash .github/scripts/verify-image.sh "$baseline_image"
BENCHMARK_OUTPUT="$output/current.json" bash .github/scripts/verify-image.sh "${BROWSER_IMAGE:-browser-sandbox:local}"
node .github/scripts/compare-performance.mjs "$output/baseline.json" "$output/current.json" > "$output/comparison.json"
