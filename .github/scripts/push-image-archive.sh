#!/usr/bin/env bash
set -euo pipefail
sha256sum --check image.tar.sha256
skopeo inspect --config docker-archive:image.tar > config.json
jq --exit-status --arg arch "$ARCH" '.architecture == $arch and .os == "linux"' config.json >/dev/null
tag="$IMAGE:build-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-$ARCH"
timeout --signal=TERM --kill-after=30s 15m \
  skopeo copy --authfile "$DOCKER_CONFIG/config.json" \
    --format v2s2 --digestfile "$ARCH.digest" \
    docker-archive:image.tar "docker://$tag"
digest="$(cat "$ARCH.digest")"
if [[ ! "$digest" =~ ^sha256:[a-f0-9]{64}$ ]]; then
  echo "Invalid published image digest" >&2
  exit 1
fi
printf '%s@%s\n' "$IMAGE" "$digest" > "$ARCH.txt"
