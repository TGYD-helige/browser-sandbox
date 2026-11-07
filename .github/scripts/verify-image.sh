#!/usr/bin/env bash
set -euo pipefail
image="${1:-browser-sandbox:local}"
docker image inspect "$image" --format 'Image: {{.Architecture}} {{.Size}} bytes'
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
extension_args=()
if [[ "${VERIFY_EXTENSIONS:-0}" == "1" ]]; then
  mkdir -p "$profile_dir/extensions/fixture"
  cp .github/scripts/fixtures/extension/* "$profile_dir/extensions/fixture/"
  chmod 0755 "$profile_dir/extensions" "$profile_dir/extensions/fixture"
  chmod 0644 "$profile_dir/extensions/fixture/"*
  extension_args=(--mount "type=bind,src=$profile_dir/extensions,dst=/browser-extensions,readonly"
    -e BROWSER_EXTENSIONS_DIR=/browser-extensions -e BROWSER_EXTENSION_SMOKE=1)
fi
runtime_args=()
command_args=()
if [[ -n "${BENCHMARK_OUTPUT:-}" ]]; then
  runtime_args=(--entrypoint node)
  command_args=(--import /opt/browser-sandbox/.github/scripts/benchmark-observer.mjs /opt/browser-sandbox/dist/main.js)
fi
# Bash 3.2 treats an empty array as unset under nounset.
container_id="$(docker run -d --init --cap-drop ALL \
  --sysctl net.ipv4.ip_local_reserved_ports=3001,42000-42999 \
  --cap-add SETUID --cap-add SETGID --cap-add CHOWN --cap-add KILL \
  --cap-add DAC_OVERRIDE --cap-add SYS_CHROOT --cap-add NET_ADMIN \
  --security-opt no-new-privileges --security-opt "seccomp=$profile_dir/seccomp.json" \
  ${extension_args[@]+"${extension_args[@]}"} \
  --shm-size 512m --memory 2g --cpus 2 ${runtime_args[@]+"${runtime_args[@]}"} "$image" ${command_args[@]+"${command_args[@]}"})"
for ((attempt=0;attempt<90;attempt++)); do
  if docker exec "$container_id" node /opt/browser-sandbox/dist/healthcheck.js >/dev/null 2>&1; then break; fi
  if ((attempt==89)); then docker logs "$container_id"; exit 1; fi
  sleep 1
done
docker exec "$container_id" node /opt/browser-sandbox/.github/scripts/image-layout.mjs
docker exec "$container_id" node /opt/browser-sandbox/examples/host.mjs
docker exec -e PROFILE_ID=example/persistent "$container_id" node /opt/browser-sandbox/examples/host.mjs
docker exec -e PROFILE_ID=example/persistent "$container_id" node /opt/browser-sandbox/examples/host.mjs

docker exec "$container_id" node /opt/browser-sandbox/.github/scripts/image-smoke.mjs
if [[ -n "${BENCHMARK_OUTPUT:-}" ]]; then
  docker exec -e SAMPLES="${BENCHMARK_SAMPLES:-20}" -e REQUIRE_PREPARATION_TIMINGS="${REQUIRE_PREPARATION_TIMINGS:-1}" "$container_id" \
    node /opt/browser-sandbox/.github/scripts/benchmark-image.mjs > "$BENCHMARK_OUTPUT"
fi
