#!/usr/bin/env bash

set -euo pipefail

CLASSIC_ROOT="${CLASSIC_ROOT:-/home/nebur/src/DYNAMOS-clean-20260511}"
STREAM_ROOT="${STREAM_ROOT:-/home/nebur/src/DYNAMOS-streaming-concept-20260524}"
RESULT_ROOT="${RESULT_ROOT:-${CLASSIC_ROOT}/benchmark-results/rq3-extra10-20260710}"
CLUSTER_NAME="${CLUSTER_NAME:-dynamos-rq3-extra10}"
KIND_BIN="${KIND_BIN:-${HOME}/.cache/dynamos/kind/v0.29.0/kind}"
IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-dynamos-local}"
REPETITIONS="${REPETITIONS:-10}"
BENCHMARK_TIMEOUT="${BENCHMARK_TIMEOUT:-2700}"
CLASSIC_BENCHMARK_TIMEOUT="${CLASSIC_BENCHMARK_TIMEOUT:-600}"
STREAMING_BENCHMARK_TIMEOUT="${STREAMING_BENCHMARK_TIMEOUT:-600}"
RESOURCE_INTERVAL="${RESOURCE_INTERVAL:-1}"
PORT_FORWARD_PORT="${PORT_FORWARD_PORT:-18080}"
SQL_STREAM_BATCH_ROWS="${SQL_STREAM_BATCH_ROWS:-5000}"
RABBITMQ_STREAM_CHUNK_ROWS="${RABBITMQ_STREAM_CHUNK_ROWS:-100}"
RABBITMQ_STREAM_CHUNK_BYTES="${RABBITMQ_STREAM_CHUNK_BYTES:-65536}"
HTTP_TIMEOUT="${HTTP_TIMEOUT:-30m}"
HTTP_STREAM_TIMEOUT="${HTTP_STREAM_TIMEOUT:-45m}"
SKIP_CLASSIC="${SKIP_CLASSIC:-0}"
SKIP_STREAMING="${SKIP_STREAMING:-0}"
RESUME_CLASSIC_DEPLOYMENT="${RESUME_CLASSIC_DEPLOYMENT:-0}"
RESUME_STREAMING_DEPLOYMENT="${RESUME_STREAMING_DEPLOYMENT:-0}"
LINKERD_VERSION="${LINKERD_VERSION:-stable-2.13.5}"
LINKERD_BIN="${LINKERD_BIN:-${HOME}/.cache/dynamos/linkerd/${LINKERD_VERSION}/linkerd}"

PORT_FORWARD_PID=""

log() {
  printf '[rq3-extra10] %s\n' "$*" >&2
}

stop_port_forward() {
  if [[ -n "${PORT_FORWARD_PID}" ]]; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
    wait "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
    PORT_FORWARD_PID=""
  fi
}

cleanup() {
  stop_port_forward
}
trap cleanup EXIT

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required tool: $1" >&2
    exit 1
  fi
}

record_provenance() {
  local root="$1"
  local output_dir="$2"
  mkdir -p "${output_dir}"
  git -C "${root}" rev-parse HEAD >"${output_dir}/git-commit.txt"
  git -C "${root}" branch --show-current >"${output_dir}/git-branch.txt"
  git -C "${root}" status --short >"${output_dir}/git-status.txt"
  git -C "${root}" diff >"${output_dir}/working-tree.patch"
}

create_cluster() {
  log "Recreating kind cluster ${CLUSTER_NAME}"
  "${KIND_BIN}" delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
  "${KIND_BIN}" create cluster --name "${CLUSTER_NAME}"
  kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null

  log "Installing metrics-server"
  kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml >/dev/null
  kubectl -n kube-system patch deployment metrics-server --type=json -p='[
    {"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}
  ]' >/dev/null
  kubectl -n kube-system rollout status deployment/metrics-server --timeout=180s >/dev/null
  for _ in $(seq 1 60); do
    if kubectl top nodes >/dev/null 2>&1; then
      install_linkerd
      return 0
    fi
    sleep 2
  done
  echo "metrics-server did not become ready" >&2
  return 1
}

install_linkerd() {
  if [[ ! -x "${LINKERD_BIN}" ]]; then
    log "Downloading Linkerd CLI ${LINKERD_VERSION}"
    mkdir -p "$(dirname "${LINKERD_BIN}")"
    curl -fsSL \
      "https://github.com/linkerd/linkerd2/releases/download/${LINKERD_VERSION}/linkerd2-cli-${LINKERD_VERSION}-linux-amd64" \
      -o "${LINKERD_BIN}"
    chmod +x "${LINKERD_BIN}"
  fi

  if kubectl -n linkerd get deployment linkerd-destination >/dev/null 2>&1; then
    log "Existing Linkerd control plane detected"
  else
    log "Installing Linkerd CRDs and control plane"
    "${LINKERD_BIN}" install --crds | kubectl apply -f - >/dev/null
    "${LINKERD_BIN}" install --set proxyInit.runAsRoot=true | kubectl apply -f - >/dev/null
  fi
  kubectl -n linkerd rollout status deployment --timeout=300s >/dev/null
}

build_and_load_images() {
  local root="$1"
  local tag="$2"
  log "Building ${root} as ${IMAGE_REPOSITORY}/*:${tag}"
  IMAGE_REPOSITORY="${IMAGE_REPOSITORY}" \
    IMAGE_TAG="${tag}" \
    SQL_QUERY_BENCH_ROWS=250000 \
    SQL_QUERY_BENCH_PROVIDERS=UVA,VU \
    "${root}/scripts/build-local-images.sh"

  local services=(
    sidecar policy-enforcer orchestrator agent api-gateway sql-algorithm
    sql-anonymize sql-aggregate sql-test sql-query
  )
  for service in "${services[@]}"; do
    "${KIND_BIN}" load docker-image "${IMAGE_REPOSITORY}/${service}:${tag}" --name "${CLUSTER_NAME}"
  done
}

seed_etcd_files() {
  local root="$1"
  kubectl -n orchestrator delete job init-etcd-pvc-local --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n orchestrator create configmap etcd-launch-files \
    --from-file="${root}/configuration/etcd_launch_files" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl apply -f - >/dev/null <<'YAML'
apiVersion: batch/v1
kind: Job
metadata:
  name: init-etcd-pvc-local
  namespace: orchestrator
spec:
  template:
    metadata:
      annotations:
        linkerd.io/inject: disabled
    spec:
      restartPolicy: OnFailure
      containers:
      - name: init
        image: busybox:1.36
        command: ["/bin/sh", "-c", "cp /config/* /etcd/"]
        volumeMounts:
        - {name: config, mountPath: /config}
        - {name: pvc-volume, mountPath: /etcd}
      volumes:
      - name: config
        configMap: {name: etcd-launch-files}
      - name: pvc-volume
        persistentVolumeClaim: {claimName: etcd-pvc}
YAML
  kubectl -n orchestrator wait --for=condition=complete job/init-etcd-pvc-local --timeout=180s >/dev/null
}

seed_rabbit_files() {
  local root="$1"
  local rabbit_tmp
  local rabbit_password
  rabbit_tmp="$(mktemp -d)"
  rabbit_password="$(kubectl -n api-gateway get secret rabbit -o jsonpath='{.data.password}' | base64 -d)"
  python3 - "${root}/configuration/k8s_service_files/definitions.json" "${rabbit_tmp}/definitions.json" "${rabbit_password}" <<'PY'
import json
import sys

source, target, password = sys.argv[1:]
with open(source, "r", encoding="utf-8") as handle:
    definitions = json.load(handle)

for user in definitions.get("users", []):
    if user.get("name") == "normal_user":
        user.pop("password_hash", None)
        user.pop("hashing_algorithm", None)
        user["password"] = password

with open(target, "w", encoding="utf-8") as handle:
    json.dump(definitions, handle, indent=2)
    handle.write("\n")
PY
  kubectl -n core delete job init-rabbit-pvc-local --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n core create configmap rabbitmq-config-files \
    --from-file=definitions.json="${rabbit_tmp}/definitions.json" \
    --from-file=rabbitmq.conf="${root}/configuration/k8s_service_files/rabbitmq.conf" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  rm -rf "${rabbit_tmp}"
  kubectl apply -f - >/dev/null <<'YAML'
apiVersion: batch/v1
kind: Job
metadata:
  name: init-rabbit-pvc-local
  namespace: core
spec:
  template:
    metadata:
      annotations:
        linkerd.io/inject: disabled
    spec:
      restartPolicy: OnFailure
      containers:
      - name: init
        image: busybox:1.36
        command: ["/bin/sh", "-c", "cp /config/* /mnt/"]
        volumeMounts:
        - {name: config, mountPath: /config}
        - {name: rabbit-config, mountPath: /mnt}
      volumes:
      - name: config
        configMap: {name: rabbitmq-config-files}
      - name: rabbit-config
        persistentVolumeClaim: {claimName: rabbit-pvc}
YAML
  kubectl -n core wait --for=condition=complete job/init-rabbit-pvc-local --timeout=180s >/dev/null
}

reconcile_rabbitmq_credentials() {
  local password
  password="$(kubectl -n api-gateway get secret rabbit -o jsonpath='{.data.password}' | base64 -d)"
  for _ in $(seq 1 90); do
    if kubectl -n core exec deploy/rabbitmq -c rabbitmq -- \
      rabbitmqctl authenticate_user normal_user "${password}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "RabbitMQ normal_user did not authenticate with namespace secret" >&2
  return 1
}

restart_application_deployments() {
  kubectl -n orchestrator rollout restart deployment/orchestrator deployment/policy-enforcer >/dev/null 2>&1 || true
  kubectl -n api-gateway rollout restart deployment/api-gateway >/dev/null 2>&1 || true
  kubectl -n uva rollout restart deployment/uva >/dev/null 2>&1 || true
  kubectl -n vu rollout restart deployment/vu >/dev/null 2>&1 || true
  kubectl -n surf rollout restart deployment/surf >/dev/null 2>&1 || true
}

scale_application_deployments() {
  local replicas="$1"
  kubectl -n orchestrator scale deployment/orchestrator deployment/policy-enforcer --replicas="${replicas}" >/dev/null 2>&1 || true
  kubectl -n api-gateway scale deployment/api-gateway --replicas="${replicas}" >/dev/null 2>&1 || true
  kubectl -n uva scale deployment/uva --replicas="${replicas}" >/dev/null 2>&1 || true
  kubectl -n vu scale deployment/vu --replicas="${replicas}" >/dev/null 2>&1 || true
  kubectl -n surf scale deployment/surf --replicas="${replicas}" >/dev/null 2>&1 || true
}

wait_application_pods_deleted() {
  kubectl -n orchestrator wait --for=delete pod -l app=orchestrator --timeout=180s >/dev/null 2>&1 || true
  kubectl -n orchestrator wait --for=delete pod -l app=policy-enforcer --timeout=180s >/dev/null 2>&1 || true
  kubectl -n api-gateway wait --for=delete pod -l app=api-gateway --timeout=180s >/dev/null 2>&1 || true
  kubectl -n uva wait --for=delete pod -l app=uva --timeout=180s >/dev/null 2>&1 || true
  kubectl -n vu wait --for=delete pod -l app=vu --timeout=180s >/dev/null 2>&1 || true
  kubectl -n surf wait --for=delete pod -l app=surf --timeout=180s >/dev/null 2>&1 || true
}

wait_application_rollouts() {
  kubectl -n orchestrator rollout status deployment/orchestrator --timeout=300s >/dev/null
  kubectl -n orchestrator rollout status deployment/policy-enforcer --timeout=300s >/dev/null
  kubectl -n api-gateway rollout status deployment/api-gateway --timeout=300s >/dev/null
  kubectl -n uva rollout status deployment/uva --timeout=300s >/dev/null
  kubectl -n vu rollout status deployment/vu --timeout=300s >/dev/null
  kubectl -n surf rollout status deployment/surf --timeout=300s >/dev/null
}

clear_agent_online_registry() {
  kubectl -n core exec etcd-0 -c etcd -- \
    etcdctl --endpoints=http://127.0.0.1:2379 del /agents/online --prefix >/dev/null 2>&1 || true
}

deploy_stack() {
  local root="$1"
  local tag="$2"
  local restart_existing="${3:-0}"
  log "Deploying DYNAMOS from ${root}"
  install_linkerd
  kubectl create namespace linkerd-jaeger --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  helm upgrade --install dynamos-namespaces "${root}/charts/namespaces" --timeout 5m >/dev/null
  seed_etcd_files "${root}"
  seed_rabbit_files "${root}"
  # The core chart keeps the Grafana deployment when observability is disabled,
  # but conditionally omits the ConfigMap mounted by that deployment.
  kubectl -n core create configmap grafana-dashboards \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  helm upgrade --install dynamos-core "${root}/charts/core" -n core \
    --set observability.enabled=false \
    --set linkerdJaegerNodePort.enabled=false \
    --wait --timeout 10m >/dev/null
  reconcile_rabbitmq_credentials
  clear_agent_online_registry

  local common_args=(
    --set "dockerArtifactAccount=${IMAGE_REPOSITORY}"
    --set "branchNameTag=${tag}"
    --set imagePullPolicy=IfNotPresent
    --set "sqlStreamBatchRows=${SQL_STREAM_BATCH_ROWS}"
    --set "rabbitmqStreamChunkRows=${RABBITMQ_STREAM_CHUNK_ROWS}"
    --set "rabbitmqStreamChunkBytes=${RABBITMQ_STREAM_CHUNK_BYTES}"
    --set "httpRequestTimeout=${HTTP_TIMEOUT}"
    --set "httpTimeout=${HTTP_TIMEOUT}"
    --set "httpStreamTimeout=${HTTP_STREAM_TIMEOUT}"
  )
  helm upgrade --install dynamos-orchestrator "${root}/charts/orchestrator" -n orchestrator "${common_args[@]}" --wait --timeout 10m >/dev/null
  helm upgrade --install dynamos-api-gateway "${root}/charts/api-gateway" -n api-gateway "${common_args[@]}" --wait --timeout 10m >/dev/null
  helm upgrade --install dynamos-agents "${root}/charts/agents" -n uva "${common_args[@]}" --wait --timeout 10m >/dev/null
  helm upgrade --install dynamos-thirdparty "${root}/charts/thirdparty" -n surf "${common_args[@]}" --wait --timeout 10m >/dev/null

  kubectl -n core rollout status statefulset/etcd --timeout=300s >/dev/null
  kubectl -n core rollout status deployment/rabbitmq --timeout=300s >/dev/null
  if [[ "${restart_existing}" == "1" ]]; then
    restart_application_deployments
  fi
  wait_application_rollouts
  wait_for_agents UVA VU SURF
}

agent_online() {
  local value
  value="$(kubectl -n core exec etcd-0 -c etcd -- \
    etcdctl --endpoints=http://127.0.0.1:2379 get "/agents/online/$1" \
    --print-value-only 2>/dev/null)" || return 1
  [[ -n "${value}" ]]
}

wait_for_agents() {
  local agent
  for agent in "$@"; do
    for _ in $(seq 1 180); do
      if agent_online "${agent}"; then
        break
      fi
      sleep 2
    done
    if ! agent_online "${agent}"; then
      echo "Agent ${agent} did not register" >&2
      return 1
    fi
  done
}

start_port_forward() {
  local output_dir="$1"
  stop_port_forward
  mkdir -p "${output_dir}"
  kubectl -n api-gateway port-forward --address 127.0.0.1 svc/api-gateway "${PORT_FORWARD_PORT}:8080" \
    >"${output_dir}/port-forward.log" 2>&1 &
  PORT_FORWARD_PID="$!"
  for _ in $(seq 1 60); do
    if timeout 1 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/${PORT_FORWARD_PORT}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "api-gateway port-forward did not become ready" >&2
  return 1
}

reset_runtime() {
  local root="$1"
  log "Resetting broker and application deployments between cells"
  scale_application_deployments 0
  wait_application_pods_deleted
  clear_agent_online_registry
  kubectl -n core scale deployment/rabbitmq --replicas=0 >/dev/null
  kubectl -n core wait --for=delete pod -l app=rabbitmq --timeout=90s >/dev/null 2>&1 || \
    kubectl -n core delete pod -l app=rabbitmq --force --grace-period=0 --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n core delete pvc rabbitmq-data-pvc rabbitmq-log-pvc --ignore-not-found >/dev/null
  helm upgrade --install dynamos-core "${root}/charts/core" -n core \
    --set observability.enabled=false \
    --set linkerdJaegerNodePort.enabled=false \
    --wait --timeout 10m >/dev/null
  kubectl -n core rollout status deployment/rabbitmq --timeout=300s >/dev/null
  reconcile_rabbitmq_credentials

  scale_application_deployments 1
  wait_application_rollouts
  wait_for_agents UVA VU SURF
}

result_cell_complete() {
  local result_json="$1"
  local expected_runs="$2"
  [[ -f "${result_json}" ]] || return 1
  python3 - "${result_json}" "${expected_runs}" <<'PY'
import json
import sys

path, expected = sys.argv[1], int(sys.argv[2])
try:
    with open(path, "r", encoding="utf-8") as handle:
        summaries = json.load(handle).get("summaries", [])
except (OSError, ValueError):
    raise SystemExit(1)

complete = bool(summaries) and all(
    summary.get("runs") == expected
    and summary.get("okRuns") == expected
    and summary.get("failedRuns") == 0
    for summary in summaries
)
raise SystemExit(0 if complete else 1)
PY
}

preserve_incomplete_cell() {
  local output_dir="$1"
  local name="$2"
  local archive_dir="${output_dir}/previous-attempt-$(date +%Y%m%d-%H%M%S)"
  local files=(
    "${output_dir}/${name}.json"
    "${output_dir}/${name}.csv"
    "${output_dir}/${name}.md"
    "${output_dir}/resources-${name}.json"
    "${output_dir}/run.log"
    "${output_dir}/run.err"
    "${output_dir}/port-forward.log"
  )
  local file
  for file in "${files[@]}"; do
    if [[ -f "${file}" ]]; then
      mkdir -p "${archive_dir}"
      cp -p "${file}" "${archive_dir}/"
    fi
  done
}

run_classic_cell() {
  local suite="$1"
  local archetype="$2"
  local providers="$3"
  local limit="$4"
  local output_dir="${RESULT_ROOT}/classic/${suite}/${archetype}-${providers//,/-}-${limit}"
  local name="classic-${suite}-${archetype}-${providers//,/-}-${limit}-extra${REPETITIONS}"
  local result_json="${output_dir}/${name}.json"
  mkdir -p "${output_dir}"
  if result_cell_complete "${result_json}" "${REPETITIONS}"; then
    log "Skipping completed classic cell ${suite} ${archetype} ${providers} ${limit}"
    return 0
  fi
  preserve_incomplete_cell "${output_dir}" "${name}"
  reset_runtime "${CLASSIC_ROOT}"
  start_port_forward "${output_dir}"
  log "Classic cell ${suite} ${archetype} ${providers} ${limit}"
  python3 "${CLASSIC_ROOT}/scripts/benchmark_with_resources.py" \
    --output "${output_dir}/resources-${name}.json" \
    --interval "${RESOURCE_INTERVAL}" \
    --namespaces api-gateway,orchestrator,uva,surf,vu,core \
    --exclude-containers POD \
    --keep-samples \
    -- \
    python3 "${CLASSIC_ROOT}/scripts/benchmark_main_classic_matrix.py" \
      --url "http://127.0.0.1:${PORT_FORWARD_PORT}/api/v1/requestApproval" \
      --limits "${limit}" \
      --providers "${providers}" \
      --archetypes "${archetype}" \
      --query-shapes default \
      --repetitions "${REPETITIONS}" \
      --timeout "${CLASSIC_BENCHMARK_TIMEOUT}" \
      --temperature warm \
      --strict \
      --cleanup-generated-jobs \
      --retry-failed-runs 2 \
      --restart-deployments-on-failure uva/uva,vu/vu,surf/surf \
      --recovery-agent-ids UVA,VU,SURF \
      --output-dir "${output_dir}" \
      --name "${name}" \
      >"${output_dir}/run.log" 2>"${output_dir}/run.err"
  stop_port_forward
}

run_stream_cell() {
  local suite="$1"
  local transport="$2"
  local archetype="$3"
  local providers="$4"
  local limit="$5"
  local output_dir="${RESULT_ROOT}/streaming/${suite}/${transport}-${archetype}-${providers//,/-}-${limit}"
  local name="${transport}-${suite}-${archetype}-${providers//,/-}-${limit}-extra${REPETITIONS}"
  local result_json="${output_dir}/${name}.json"
  mkdir -p "${output_dir}"
  if result_cell_complete "${result_json}" "${REPETITIONS}"; then
    log "Skipping completed streaming cell ${suite} ${transport} ${archetype} ${providers} ${limit}"
    return 0
  fi
  preserve_incomplete_cell "${output_dir}" "${name}"
  reset_runtime "${STREAM_ROOT}"
  start_port_forward "${output_dir}"
  log "Streaming cell ${suite} ${transport} ${archetype} ${providers} ${limit}"
  python3 "${CLASSIC_ROOT}/scripts/benchmark_with_resources.py" \
    --output "${output_dir}/resources-${name}.json" \
    --interval "${RESOURCE_INTERVAL}" \
    --namespaces api-gateway,orchestrator,uva,surf,vu,core \
    --exclude-containers POD \
    --keep-samples \
    -- \
    python3 "${STREAM_ROOT}/scripts/benchmark_matrix.py" \
      --url "http://127.0.0.1:${PORT_FORWARD_PORT}/api/v1/requestApproval" \
      --transports "${transport}" \
      --response-modes batched \
      --workloads bulk \
      --datasets large \
      --large-limits "${limit}" \
      --archetypes "${archetype}" \
      --provider-sets "${providers}" \
      --query-shapes default \
      --temperature warm \
      --repetitions "${REPETITIONS}" \
      --timeout "${STREAMING_BENCHMARK_TIMEOUT}" \
      --sql-batch-rows "${SQL_STREAM_BATCH_ROWS}" \
      --rabbitmq-chunk-rows "${RABBITMQ_STREAM_CHUNK_ROWS}" \
      --strict \
      --require-partial \
      --cleanup-generated-jobs \
      --retry-failed-runs 2 \
      --restart-deployments-on-failure uva/uva,vu/vu,surf/surf \
      --recovery-agent-ids UVA,VU,SURF \
      --output-dir "${output_dir}" \
      --name "${name}" \
      >"${output_dir}/run.log" 2>"${output_dir}/run.err"
  stop_port_forward
}

smoke_classic() {
  local output_dir="${RESULT_ROOT}/classic/smoke"
  mkdir -p "${output_dir}"
  start_port_forward "${output_dir}"
  for attempt in 1 2 3; do
    if python3 "${CLASSIC_ROOT}/scripts/benchmark_main_classic_matrix.py" \
      --url "http://127.0.0.1:${PORT_FORWARD_PORT}/api/v1/requestApproval" \
      --limits 5000 --providers UVA --archetypes dataThroughTtp \
      --repetitions 1 --timeout 600 --strict --cleanup-generated-jobs \
      --output-dir "${output_dir}" --name classic-primary-smoke; then
      break
    fi
    [[ "${attempt}" == "3" ]] && return 1
    log "Classic primary smoke failed on attempt ${attempt}; waiting before retry"
    sleep 30
  done
  for attempt in 1 2 3; do
    if python3 "${CLASSIC_ROOT}/scripts/benchmark_main_classic_matrix.py" \
      --url "http://127.0.0.1:${PORT_FORWARD_PORT}/api/v1/requestApproval" \
      --limits 5000 --providers UVA,VU --archetypes dataThroughTtp,computeToData \
      --repetitions 1 --timeout 600 --strict --cleanup-generated-jobs \
      --output-dir "${output_dir}" --name classic-compatibility-smoke; then
      break
    fi
    [[ "${attempt}" == "3" ]] && return 1
    log "Classic compatibility smoke failed on attempt ${attempt}; waiting before retry"
    sleep 30
  done
  stop_port_forward
}

smoke_streaming() {
  local output_dir="${RESULT_ROOT}/streaming/smoke"
  mkdir -p "${output_dir}"
  start_port_forward "${output_dir}"
  for attempt in 1 2 3; do
    if python3 "${STREAM_ROOT}/scripts/benchmark_matrix.py" \
      --url "http://127.0.0.1:${PORT_FORWARD_PORT}/api/v1/requestApproval" \
      --transports unary,streaming,rabbitmq-streams --response-modes batched \
      --workloads bulk --datasets large --large-limits 5000 \
      --archetypes dataThroughTtp --provider-sets UVA --query-shapes default \
      --repetitions 1 --timeout 600 --strict --cleanup-generated-jobs \
      --retry-failed-runs 2 \
      --restart-deployments-on-failure uva/uva,vu/vu,surf/surf \
      --recovery-agent-ids UVA,VU,SURF \
      --sql-batch-rows "${SQL_STREAM_BATCH_ROWS}" --rabbitmq-chunk-rows "${RABBITMQ_STREAM_CHUNK_ROWS}" \
      --output-dir "${output_dir}" --name streaming-primary-smoke; then
      break
    fi
    [[ "${attempt}" == "3" ]] && return 1
    log "Streaming primary smoke failed on attempt ${attempt}; waiting before retry"
    sleep 30
  done
  for attempt in 1 2 3; do
    if python3 "${STREAM_ROOT}/scripts/benchmark_matrix.py" \
      --url "http://127.0.0.1:${PORT_FORWARD_PORT}/api/v1/requestApproval" \
      --transports unary,streaming,rabbitmq-streams --response-modes batched \
      --workloads bulk --datasets large --large-limits 10000 \
      --archetypes dataThroughTtp,computeToData --provider-sets UVA,VU --query-shapes default \
      --repetitions 1 --timeout 600 --strict --require-partial --cleanup-generated-jobs \
      --retry-failed-runs 2 \
      --restart-deployments-on-failure uva/uva,vu/vu,surf/surf \
      --recovery-agent-ids UVA,VU,SURF \
      --sql-batch-rows "${SQL_STREAM_BATCH_ROWS}" --rabbitmq-chunk-rows "${RABBITMQ_STREAM_CHUNK_ROWS}" \
      --output-dir "${output_dir}" --name streaming-compatibility-smoke; then
      break
    fi
    [[ "${attempt}" == "3" ]] && return 1
    log "Streaming compatibility smoke failed on attempt ${attempt}; waiting before retry"
    sleep 30
  done
  stop_port_forward
}

run_classic_campaign() {
  local tag="rq3-classic-extra10"
  record_provenance "${CLASSIC_ROOT}" "${RESULT_ROOT}/classic/provenance"
  case "${RESUME_CLASSIC_DEPLOYMENT}" in
    0)
      create_cluster
      build_and_load_images "${CLASSIC_ROOT}" "${tag}"
      deploy_stack "${CLASSIC_ROOT}" "${tag}" 0
      ;;
    1)
      log "Resuming classic deployment with images already loaded"
      kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null
      deploy_stack "${CLASSIC_ROOT}" "${tag}" 1
      ;;
    2)
      log "Reusing ready classic deployment"
      kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null
      wait_application_rollouts
      wait_for_agents UVA VU SURF
      ;;
    *)
      echo "RESUME_CLASSIC_DEPLOYMENT must be 0, 1, or 2" >&2
      return 2
      ;;
  esac
  smoke_classic
  for limit in 50000 100000 250000; do
    run_classic_cell primary dataThroughTtp UVA "${limit}"
  done
  for archetype in computeToData dataThroughTtp; do
    for limit in 50000 250000; do
      run_classic_cell compatibility "${archetype}" UVA,VU "${limit}"
    done
  done
}

run_streaming_campaign() {
  local tag="rq3-streaming-extra10"
  record_provenance "${STREAM_ROOT}" "${RESULT_ROOT}/streaming/provenance"
  case "${RESUME_STREAMING_DEPLOYMENT}" in
    0)
      create_cluster
      build_and_load_images "${STREAM_ROOT}" "${tag}"
      deploy_stack "${STREAM_ROOT}" "${tag}" 0
      ;;
    2)
      log "Reusing ready streaming deployment"
      kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null
      wait_application_rollouts
      wait_for_agents UVA VU SURF
      ;;
    *)
      echo "RESUME_STREAMING_DEPLOYMENT must be 0 or 2" >&2
      return 2
      ;;
  esac
  smoke_streaming
  for transport in unary streaming rabbitmq-streams; do
    for limit in 50000 100000 250000; do
      run_stream_cell primary "${transport}" dataThroughTtp UVA "${limit}"
    done
  done
  for transport in unary streaming rabbitmq-streams; do
    for archetype in computeToData dataThroughTtp; do
      for limit in 50000 250000; do
        run_stream_cell compatibility "${transport}" "${archetype}" UVA,VU "${limit}"
      done
    done
  done
}

main() {
  require_tool docker
  require_tool kubectl
  require_tool helm
  require_tool python3
  [[ -x "${KIND_BIN}" ]] || { echo "kind binary not found: ${KIND_BIN}" >&2; exit 1; }
  mkdir -p "${RESULT_ROOT}"
  printf '%s\n' "$(date --iso-8601=seconds)" >"${RESULT_ROOT}/started-at.txt"
  if [[ "${SKIP_CLASSIC}" != "1" ]]; then
    run_classic_campaign
    printf '%s\n' "$(date --iso-8601=seconds)" >"${RESULT_ROOT}/classic-completed-at.txt"
  fi
  if [[ "${SKIP_STREAMING}" != "1" ]]; then
    run_streaming_campaign
    printf '%s\n' "$(date --iso-8601=seconds)" >"${RESULT_ROOT}/streaming-completed-at.txt"
  fi
  printf '%s\n' "$(date --iso-8601=seconds)" >"${RESULT_ROOT}/completed-at.txt"
  log "Campaign complete: ${RESULT_ROOT}"
}

main "$@"
