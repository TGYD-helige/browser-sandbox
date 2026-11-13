#!/usr/bin/env bash
set -euo pipefail
case "$(uname -m)" in
  x86_64) arch=amd64; checksum=06d8f25bc3a971c4eb29e0ff08429b180402db0f4dec838c9eac427e296800a0 ;;
  aarch64|arm64) arch=arm64; checksum=03cde5cf23e6e8e67de5a039ecf26e5b85aca82fba3e5d13dadf904cd218a250 ;;
  *) echo 'Unsupported k3d architecture' >&2; exit 1 ;;
esac
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
base=https://github.com/k3d-io/k3d/releases/download/v5.9.0
curl --fail --location --retry 3 --connect-timeout 10 --max-time 120 "$base/k3d-linux-$arch" -o "$scratch/k3d-linux-$arch"
printf '%s  %s\n' "$checksum" "$scratch/k3d-linux-$arch" | sha256sum --check --strict
mkdir -p "$HOME/.local/bin"
install -m 0755 "$scratch/k3d-linux-$arch" "$HOME/.local/bin/k3d"
