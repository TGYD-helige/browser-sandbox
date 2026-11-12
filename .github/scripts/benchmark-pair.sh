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
cp .github/scripts/benchmark-*.mjs .github/scripts/image-smoke.mjs "$context/.github/scripts/"
printf '\nCOPY .github/scripts/benchmark-*.mjs .github/scripts/image-smoke.mjs /opt/browser-sandbox/.github/scripts/\n' >> "$context/Dockerfile"
docker build -t "$baseline_image" "$context"
samples="${BENCHMARK_SAMPLES:-20}"
if ((samples < 20 || samples % 2)); then echo 'BENCHMARK_SAMPLES must be an even number of at least 20' >&2; exit 1; fi
run() {
  # Historical baselines need not satisfy the current browser-layout contract.
  local layout=1
  if [[ "$1" == baseline-* ]]; then layout=0; fi
  VERIFY_IMAGE_LAYOUT="$layout" BENCHMARK_SAMPLES="$((samples / 2))" BENCHMARK_OUTPUT="$output/$1.json" bash .github/scripts/verify-image.sh "$2"
}
current_image="${BROWSER_IMAGE:-browser-sandbox:local}"
# ABBA order cancels runner drift that otherwise penalizes whichever runtime runs second.
run baseline-1 "$baseline_image"
run current-1 "$current_image"
run current-2 "$current_image"
run baseline-2 "$baseline_image"
node .github/scripts/compare-performance.mjs "$output"/baseline-{1,2}.json -- "$output"/current-{1,2}.json > "$output/comparison.json"
