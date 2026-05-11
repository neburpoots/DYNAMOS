#!/bin/bash

set -euo pipefail

echo "Setting up paths..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DYNAMOS_ROOT="${DYNAMOS_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
DOCKER_ARTIFACT_ACCOUNT="${DOCKER_ARTIFACT_ACCOUNT:-dynamos1}"
BRANCH_NAME_TAG="${BRANCH_NAME_TAG:-main}"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-Always}"
SUDO="${SUDO:-}"

kubectl_cmd() {
    if [[ -n "${KUBE_CONTEXT}" ]]; then
        kubectl --context "${KUBE_CONTEXT}" "$@"
    else
        kubectl "$@"
    fi
}

helm_cmd() {
    if [[ -n "${KUBE_CONTEXT}" ]]; then
        helm --kube-context "${KUBE_CONTEXT}" "$@"
    else
        helm "$@"
    fi
}

image_values_args=(
    --set "dockerArtifactAccount=${DOCKER_ARTIFACT_ACCOUNT}"
    --set "branchNameTag=${BRANCH_NAME_TAG}"
    --set "imagePullPolicy=${IMAGE_PULL_POLICY}"
)

# Charts
charts_path="${DYNAMOS_ROOT}/charts"
core_chart="${charts_path}/core"
namespace_chart="${charts_path}/namespaces"
orchestrator_chart="${charts_path}/orchestrator"
agents_chart="${charts_path}/agents"
ttp_chart="${charts_path}/thirdparty"
api_gw_chart="${charts_path}/api-gateway"

# Config
config_path="${DYNAMOS_ROOT}/configuration"
k8s_service_files="${config_path}/k8s_service_files"
etcd_launch_files="${config_path}/etcd_launch_files"

rabbit_definitions_file="${k8s_service_files}/definitions.json"
example_definitions_file="${k8s_service_files}/definitions_example.json"

cp "$example_definitions_file" "$rabbit_definitions_file"
echo "definitions_example.json copied over definitions.json to ensure a clean file"

echo "Generating RabbitMQ password..."
# Create a password for a rabbit user
rabbit_pw=$(openssl rand -hex 16)

# Use the RabbitCtl to make a special hash of that password:
hashed_pw=$($SUDO docker run --rm rabbitmq:3-management rabbitmqctl hash_password $rabbit_pw)
actual_hash=$(echo "$hashed_pw" | cut -d $'\n' -f2)

echo "Replacing tokens..."
cp ${k8s_service_files}/definitions_example.json ${rabbit_definitions_file}


# The Rabbit Hashed password needs to be in definitions.json file, that is the configuration for RabbitMQ
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS sed
    sed -i '' "s|%PASSWORD%|${actual_hash}|g" ${rabbit_definitions_file}
else
    # GNU sed
    sed -i "s|%PASSWORD%|${actual_hash}|g" ${rabbit_definitions_file}
fi

echo "Installing namespaces..."

# Install namespaces
helm_cmd upgrade -i -f "${namespace_chart}/values.yaml" namespaces "${namespace_chart}" --set "secret.password=${rabbit_pw}"

echo "Preparing PVC"

{
    cd ${DYNAMOS_ROOT}/configuration
    KUBE_CONTEXT="${KUBE_CONTEXT}" ./fill-rabbit-pvc.sh
}

#Install prometheus
echo "Installing Prometheus..."

helm_cmd repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update
helm_cmd repo update
helm_cmd upgrade -i -f "${core_chart}/prometheus-values.yaml" prometheus prometheus-community/prometheus

echo "Installing NGINX..."
helm_cmd upgrade -i -f "${core_chart}/ingress-values.yaml" nginx oci://ghcr.io/nginxinc/charts/nginx-ingress -n ingress --version 0.18.0

echo "Installing Grafana dashboards..."
kubectl_cmd -n core delete configmap grafana-dashboards --ignore-not-found
kubectl_cmd -n core create configmap grafana-dashboards --from-file="${config_path}/grafana"

echo "Installing DYNAMOS core..."
helm_cmd upgrade -i -f "${core_chart}/values.yaml" core "${core_chart}" --set "hostPath=${DYNAMOS_ROOT}"

sleep 3
# Install orchestrator layer
helm_cmd upgrade -i -f "${orchestrator_chart}/values.yaml" orchestrator "${orchestrator_chart}" "${image_values_args[@]}"

sleep 1

echo "Installing agents layer"
helm_cmd upgrade -i -f "${agents_chart}/values.yaml" agents "${agents_chart}" "${image_values_args[@]}"

sleep 1

echo "Installing thirdparty layer..."
helm_cmd upgrade -i -f "${ttp_chart}/values.yaml" surf "${ttp_chart}" "${image_values_args[@]}"

sleep 1

echo "Installing api gateway"
helm_cmd upgrade -i -f "${api_gw_chart}/values.yaml" api-gateway "${api_gw_chart}" "${image_values_args[@]}"

echo "Finished setting up DYNAMOS"

exit 0
