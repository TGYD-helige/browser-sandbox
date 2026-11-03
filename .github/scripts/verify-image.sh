#!/usr/bin/env bash
set -euo pipefail
image="${1:-browser-sandbox:local}"
profile_dir="$(mktemp -d)"
container_id=""
cleanup() {
  if [[ -n "$container_id" ]]; then docker rm -f "$container_id" >/dev/null; fi
  rm -rf "$profile_dir"
}
trap cleanup EXIT
extract_id="$(docker create "$image")"
docker cp "$extract_id:/opt/browser-sandbox/seccomp.json" "$profile_dir/seccomp.json"
docker rm "$extract_id" >/dev/null
container_id="$(docker run -d --init --cap-drop ALL \
  --cap-add SETUID --cap-add SETGID --cap-add CHOWN --cap-add KILL \
  --cap-add DAC_OVERRIDE --cap-add SYS_CHROOT --cap-add NET_ADMIN \
  --security-opt no-new-privileges --security-opt "seccomp=$profile_dir/seccomp.json" \
  --shm-size 512m --memory 2g --cpus 2 "$image")"
for ((attempt=0;attempt<90;attempt++)); do
  if docker exec "$container_id" node /opt/browser-sandbox/dist/healthcheck.js >/dev/null 2>&1; then break; fi
  if ((attempt==89)); then docker logs "$container_id"; exit 1; fi
  sleep 1
done
docker exec "$container_id" node /opt/browser-sandbox/examples/host.mjs
docker exec -e PROFILE_ID=example/persistent "$container_id" node /opt/browser-sandbox/examples/host.mjs
docker exec -e PROFILE_ID=example/persistent "$container_id" node /opt/browser-sandbox/examples/host.mjs

docker exec "$container_id" node /opt/browser-sandbox/.github/scripts/image-smoke.mjs
if [[ -n "${BENCHMARK_OUTPUT:-}" ]]; then
  docker exec -e SAMPLES="${BENCHMARK_SAMPLES:-20}" "$container_id" \
    node /opt/browser-sandbox/.github/scripts/benchmark-image.mjs > "$BENCHMARK_OUTPUT"
fi
