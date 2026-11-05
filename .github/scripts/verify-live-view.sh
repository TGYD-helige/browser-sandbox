#!/usr/bin/env bash
set -euo pipefail
image="${1:-browser-sandbox:live-viewer}"
: "${CHROME_EXECUTABLE:?Set CHROME_EXECUTABLE to the local viewer browser}"
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
# An isolated local test listener; change the port if it is occupied.
port="${VIEWER_TEST_PORT:-18089}"
set -- "$image"
if [[ "$(uname -s)" == Linux ]]; then
  set -- --add-host host.docker.internal:host-gateway "$@"
fi
container_id="$(docker run -d --init --cap-drop ALL \
  --sysctl net.ipv4.ip_local_reserved_ports=3001,42000-42999 \
  --cap-add SETUID --cap-add SETGID --cap-add CHOWN --cap-add KILL \
  --cap-add DAC_OVERRIDE --cap-add SYS_CHROOT --cap-add NET_ADMIN \
  --security-opt no-new-privileges --security-opt "seccomp=$profile_dir/seccomp.json" \
  --shm-size 512m --memory 2g --cpus 2 \
  -p "127.0.0.1:$port:8080" \
  -e BROWSER_ACCESS_PORT=8080 -e BROWSER_ACCESS_HOST=0.0.0.0 \
  -e "BROWSER_ACCESS_ORIGIN=http://127.0.0.1:$port" "$@")"
failure() {
  echo 'Browser image readiness failed' >&2
  docker logs "$container_id" 2>&1 | node --input-type=module -e 'let input=""; for await (const chunk of process.stdin) input+=chunk; console.error([...new Set(input.match(/\bbrowser_[a-z_]+\b|\b(?:ENOSPC|EACCES|ENOENT|EROFS|EPERM|ERR_[A-Z_]+)\b/g) ?? [])].join("\n"));'
  exit 1
}
for ((attempt=0;attempt<90;attempt++)); do
  if [[ "$(docker inspect -f '{{.State.Running}}' "$container_id")" != true ]]; then failure; fi
  if docker exec "$container_id" node /opt/browser-sandbox/dist/healthcheck.js >/dev/null 2>&1; then break; fi
  if ((attempt==89)); then failure; fi
  sleep 1
done
BROWSER_TEST_CONTAINER="$container_id" node .github/scripts/live-view-e2e.mjs
