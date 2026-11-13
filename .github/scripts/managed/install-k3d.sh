#!/usr/bin/env bash
set -euo pipefail
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo 'Unsupported k3d architecture' >&2; exit 1 ;;
esac
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
base=https://github.com/k3d-io/k3d/releases/download/v5.9.0
curl --fail --location --retry 3 --connect-timeout 10 --max-time 120 "$base/k3d-linux-$arch" -o "$scratch/k3d-linux-$arch"
curl --fail --location --retry 3 --connect-timeout 10 --max-time 120 "$base/checksums.txt" -o "$scratch/checksums.txt"
(cd "$scratch" && grep -E " k3d-linux-$arch$" checksums.txt | sha256sum --check --strict)
mkdir -p "$HOME/.local/bin"
install -m 0755 "$scratch/k3d-linux-$arch" "$HOME/.local/bin/k3d"
