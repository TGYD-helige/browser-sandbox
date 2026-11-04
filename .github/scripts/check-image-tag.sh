#!/usr/bin/env bash
set -euo pipefail
image_tag="${1:?Provide a registry image and tag}"
if output="$(docker manifest inspect "$image_tag" 2>&1)"; then
  echo 'Version already exists; choose a new package version.' >&2
  exit 1
fi
# Unknown/authentication/network failures are not evidence that a tag is absent.
if [[ "$output" != "no such manifest: $image_tag" ]]; then
  echo 'Unable to confirm that the version is absent; publication stopped.' >&2
  exit 1
fi
