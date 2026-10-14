#!/usr/bin/env bash
set -euo pipefail

NAMESPACE_OPERATOR="graphstream-system"
NAMESPACE_CLUSTERS="graphstream-clusters"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Applying namespaces"
kubectl apply -f "$HERE/00-namespaces.yaml"

echo "==> Applying RBAC"
kubectl apply -f "$HERE/01-rbac/"

echo "==> Applying CRD"
kubectl apply -f "$HERE/03-crd/"

echo "==> Waiting for CRD to be established"
kubectl wait --for=condition=Established \
    crd/graphstreamclusters.graphstream.io \
    --timeout=60s

echo "==> Applying operator"
kubectl apply -f "$HERE/04-operator/"

echo "==> Waiting for operator to be available"
kubectl -n "$NAMESPACE_OPERATOR" rollout status \
    deployment/graphstream-operator --timeout=120s

echo "==> Applying monitoring stack (Prometheus Adapter config, ServiceMonitor, HPA)"
kubectl apply -f "$HERE/06-monitoring/"

if [[ "${INSTALL_SAMPLE:-0}" == "1" ]]; then
    echo "==> Applying minimal sample cluster"
    kubectl apply -f "$HERE/05-samples/graphstreamcluster-minimal.yaml"

    echo "==> Waiting for cluster to be Ready"
    for i in {1..60}; do
        phase=$(kubectl -n "$NAMESPACE_CLUSTERS" get gsc minimal \
            -o jsonpath='{.status.phase}' 2>/dev/null || true)
        if [[ "$phase" == "Ready" ]]; then
            echo "    cluster is Ready"
            break
        fi
        sleep 2
    done
fi

echo
echo "Install complete."
echo "  kubectl -n $NAMESPACE_CLUSTERS get gsc"
echo "  kubectl -n $NAMESPACE_CLUSTERS get pods"