#!/bin/bash

set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-}"
etcd_tarball="$(mktemp)"

cleanup() {
    rm -f "${etcd_tarball}"
}
trap cleanup EXIT

kubectl_cmd() {
    if [[ -n "${KUBE_CONTEXT}" ]]; then
        kubectl --context "${KUBE_CONTEXT}" "$@"
    else
        kubectl "$@"
    fi
}

# Create the temporary pod
kubectl_cmd apply -f temp-pod.yaml

# Wait for the pod to be in the 'Running' state
echo "Waiting for temp-pod to be Running..."
kubectl_cmd wait --for=condition=Ready pod/temp-pod --timeout=300s -n core
kubectl_cmd wait --for=condition=Ready pod/temp-pod-orch --timeout=300s -n orchestrator

# Copy local files to the PVC
kubectl_cmd cp ./k8s_service_files/definitions.json temp-pod:/mnt/ -n core
kubectl_cmd cp ./k8s_service_files/rabbitmq.conf temp-pod:/mnt/ -n core

# Create a tarball of the files
tar -czvf "${etcd_tarball}" -C ./etcd_launch_files/ .

# Copy the tarball to the pod
kubectl_cmd cp "${etcd_tarball}" temp-pod-orch:/mnt/etcd_files.tar.gz -n orchestrator

# Untar the files inside the pod (optional, if you want to unpack the files inside the pod)
kubectl_cmd exec -n orchestrator temp-pod-orch -- tar -xzvf /mnt/etcd_files.tar.gz -C /mnt

# Delete the temporary pod
kubectl_cmd delete -f temp-pod.yaml
