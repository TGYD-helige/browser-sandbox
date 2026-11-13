#!/usr/bin/env bash
set -euo pipefail
# This file never changes the user's default kubeconfig or an existing cluster.
action=${1:?Usage: k3d-cluster.sh create|delete NAME KUBECONFIG}
cluster=${2:?Supply a task-owned cluster name}
config=${3:?Supply an isolated kubeconfig path}
case "$action" in
  create)
    k3d cluster create "$cluster" --servers 1 --agents 2 --no-lb --api-port 127.0.0.1:0 \
      --image 'rancher/k3s:v1.36.4-k3s1@sha256:edad48e12bf81c3a09ac1c05c0c0ffaaa22145980b989d6fae84543a76b83657' \
      --kubeconfig-update-default=false --kubeconfig-switch-context=false \
      --k3s-arg '--disable=traefik,servicelb,metrics-server@server:*' --wait --timeout 180s
    umask 077
    k3d kubeconfig get "$cluster" > "$config"
    # k3d records literal port zero; Docker chooses the actual published port.
    address=$(docker port "k3d-$cluster-server-0" 6443/tcp)
    [[ "$address" =~ ^127\.0\.0\.1:[1-9][0-9]*$ ]]
    kubectl --kubeconfig "$config" config set-cluster "k3d-$cluster" --server="https://$address"
    kubectl --kubeconfig "$config" wait --for=condition=Ready nodes --all --timeout=120s
    ;;
  delete) k3d cluster delete "$cluster" ;;
  *) echo "Unknown action: $action" >&2; exit 2 ;;
esac
