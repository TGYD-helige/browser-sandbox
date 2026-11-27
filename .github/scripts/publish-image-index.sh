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
sha_tag="sha-$SOURCE_SHA"
check_tag() {
  local name="$1" tag="$2" existing
  local image="$REGISTRY/$NAMESPACE/$name"
  if existing="$(crane digest "$image:$tag" 2>&1)"; then
    if [[ "${existing##*$'\n'}" != "$digest" ]]; then
      echo "Refusing to move $image:$tag to a different manifest." >&2
      return 1
    fi
    printf '%s\n' "$digest"
  elif ! [[ "${existing##*$'\n'}" =~ ^Error:\ GET\ https?://"$REGISTRY/v2/$NAMESPACE/$name/manifests/$tag":\ (MANIFEST_UNKNOWN|NAME_UNKNOWN): ]]; then
    echo "Unable to confirm that $image:$tag is absent; publication stopped." >&2
    return 1
  fi
}

for i in "${!names[@]}"; do
  version_digests[i]="$(check_tag "${names[i]}" "$IMAGE_TAG")"
  sha_digests[i]="$(check_tag "${names[i]}" "$sha_tag")"
done
for i in "${!names[@]}"; do
  name="${names[i]}"
  image="$REGISTRY/$NAMESPACE/$name"
  if [[ -z "${sha_digests[i]}" ]]; then
    crane copy "$base_image@$digest" "$image:$sha_tag"
  fi
  if [[ -z "${version_digests[i]}" ]]; then
    crane tag "$image@$digest" "$IMAGE_TAG"
  fi
  if [[ "$PUBLISH_LATEST" == true ]]; then
    crane tag "$image@$digest" latest
  fi
  printf 'Published %s:%s (%s, Linux AMD64/ARM64).\n' \
    "$image" "$IMAGE_TAG" "$digest" >> "$GITHUB_STEP_SUMMARY"
done
