#!/usr/bin/env bash
set -euo pipefail
read -ra names <<< "$IMAGE_NAMES"
base_image="$REGISTRY/$NAMESPACE/${names[0]}"
index_tag="$base_image:build-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-index"
crane index append --docker-empty-base \
  --manifest "$(cat amd64.txt)" --manifest "$(cat arm64.txt)" --tag "$index_tag"
digest="$(crane digest "$index_tag")"
if [[ ! "$digest" =~ ^sha256:[a-f0-9]{64}$ ]]; then
  echo "Invalid published image digest" >&2
  exit 1
fi
for name in "${names[@]}"; do
  image="$REGISTRY/$NAMESPACE/$name"
  crane copy "$base_image@$digest" "$image:sha-$SOURCE_SHA"
  crane tag "$image@$digest" "$IMAGE_TAG"
  if [[ "$PUBLISH_LATEST" == true ]]; then
    crane tag "$image@$digest" latest
  fi
  printf 'Published %s:%s (%s, Linux AMD64/ARM64).\n' \
    "$image" "$IMAGE_TAG" "$digest" >> "$GITHUB_STEP_SUMMARY"
done
