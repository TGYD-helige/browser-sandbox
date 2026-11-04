#!/usr/bin/env bash
set -euo pipefail
image="${1:?Provide the image repository}"
tag="${2:?Provide the version tag}"
notes="$(mktemp)"
trap 'rm -f "$notes"' EXIT
bash "$(dirname "$0")/check-image-tag.sh" "$image:$tag"
# Reserve the exact source atomically; a competing tag creator must stop publication.
gh api --method POST "repos/${GITHUB_REPOSITORY:?}/git/refs" \
  -f "ref=refs/tags/$tag" -f "sha=${GITHUB_SHA:?}" >/dev/null
docker buildx imagetools create --tag "$image:$tag" "$(cat amd64.txt)" "$(cat arm64.txt)"
docker buildx imagetools inspect "$image:$tag" > image-manifest.txt
printf 'Verified Linux AMD64/ARM64 image: %s:%s.\n\nSee docs/usage.md for deployment conditions. Upstream licenses apply separately.\n' "$image" "$tag" > "$notes"
gh release create "$tag" --verify-tag --title "$tag" --draft --notes-file "$notes" image-manifest.txt
