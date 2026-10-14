#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Deleting sample clusters (this triggers GC of child resources)"
kubectl delete -f "$HERE/05-samples/" --ignore-not-found=true

echo "==> Deleting operator"
kubectl delete -f "$HERE/04-operator/" --ignore-not-found=true

echo "==> Deleting monitoring resources"
kubectl delete -f "$HERE/06-monitoring/" --ignore-not-found=true

echo "==> Deleting CRD"
kubectl delete -f "$HERE/03-crd/" --ignore-not-found=true

echo "==> Deleting RBAC"
kubectl delete -f "$HERE/01-rbac/" --ignore-not-found=true

echo "==> Deleting namespaces (this deletes everything remaining inside them)"
kubectl delete -f "$HERE/00-namespaces.yaml" --ignore-not-found=true

echo "Uninstall complete."