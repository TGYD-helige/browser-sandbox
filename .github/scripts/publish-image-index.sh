#!/usr/bin/env bash
set -euo pipefail
read -ra names <<< "$IMAGE_NAMES"
base_image="$REGISTRY/$NAMESPACE/${names[0]}"
read_digest() {
  local name="$1" tag="$2" output tags
  local image="$REGISTRY/$NAMESPACE/$name"
  if output="$(crane digest "$image:$tag" 2>&1)"; then
    output="${output##*$'\n'}"
    if [[ ! "$output" =~ ^sha256:[a-f0-9]{64}$ ]]; then
      echo "Invalid digest for $image:$tag; publication stopped." >&2
      return 1
    fi
    printf '%s\n' "$output"
  elif ! [[ "${output##*$'\n'}" =~ ^Error:\ GET\ https?://"$REGISTRY/v2/$NAMESPACE/$name/manifests/$tag":\ (MANIFEST_UNKNOWN|NAME_UNKNOWN): ]]; then
    # Some registries return a different error for an absent manifest.
    if tags="$(crane ls "$image" 2>/dev/null)" && ! grep -Fxq -- "$tag" <<< "$tags"; then
      return 0
    fi
    echo "Unable to confirm that $image:$tag is absent; publication stopped." >&2
    return 1
  fi
}

# Keep the verified index stable across attempts of the same run.
index_tag="build-$GITHUB_RUN_ID-$SOURCE_SHA-index"
digest="$(read_digest "${names[0]}" "$index_tag")"
if [[ -z "$digest" ]]; then
  crane index append --docker-empty-base \
    --manifest "$(cat amd64.txt)" --manifest "$(cat arm64.txt)" --tag "$base_image:$index_tag"
  digest="$(read_digest "${names[0]}" "$index_tag")"
  if [[ -z "$digest" ]]; then
    echo "Published image index is missing." >&2
    exit 1
  fi
fi

sha_tag="sha-$SOURCE_SHA"
check_tag() {
  local name="$1" tag="$2" existing
  existing="$(read_digest "$name" "$tag")" || return 1
  if [[ -n "$existing" && "$existing" != "$digest" ]]; then
    echo "Refusing to move $REGISTRY/$NAMESPACE/$name:$tag to a different manifest." >&2
    return 1
  fi
  printf '%s' "$existing"
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
