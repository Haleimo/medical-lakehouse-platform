#!/usr/bin/env bash
# Script to launch port-forwarding for ArgoCD Web UI and print login credentials

set -euo pipefail

echo "========================================================"
echo "      Medical Lakehouse Platform - ArgoCD UI Launcher   "
echo "========================================================"

# Check if kubectl can talk to the cluster
if ! kubectl cluster-info &>/dev/null; then
    echo "[ERROR] Cannot connect to Kubernetes cluster. Make sure minikube is running ('minikube start')."
    exit 1
fi

# Fetch initial admin password if available
if kubectl get secret argocd-initial-admin-secret -n argocd &>/dev/null; then
    PASS=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d)
    echo "🔑 ArgoCD Admin Password: $PASS"
else
    echo "🔑 Secret argocd-initial-admin-secret not found (password may have been changed)."
fi

echo "🌐 Access URL:            https://localhost:8080"
echo "👤 Admin Username:        admin"
echo "========================================================"
echo "Starting port-forward on port 8080... (Press Ctrl+C to stop)"
echo ""

kubectl port-forward svc/argocd-server -n argocd 8080:443
