#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-dynamos-rmqs-bench}"
IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-dynamos-local}"
IMAGE_TAG="${IMAGE_TAG:-rmqs-bench}"
SQL_QUERY_BENCH_ROWS="${SQL_QUERY_BENCH_ROWS:-250000}"
SQL_STREAM_BATCH_ROWS_LIST="${SQL_STREAM_BATCH_ROWS_LIST:-5000}"
RABBITMQ_STREAM_CHUNK_ROWS_LIST="${RABBITMQ_STREAM_CHUNK_ROWS_LIST:-100}"
RABBITMQ_STREAM_CHUNK_BYTES="${RABBITMQ_STREAM_CHUNK_BYTES:-65536}"
PORT_FORWARD_PORT="${PORT_FORWARD_PORT:-18080}"
REPETITIONS="${REPETITIONS:-3}"
TRANSPORTS="${TRANSPORTS:-unary,streaming,rabbitmq-streams}"
DATASETS="${DATASETS:-large,original}"
LARGE_LIMITS="${LARGE_LIMITS:-50000,250000}"
ARCHETYPES="${ARCHETYPES:-dataThroughTtp,computeToData}"
ORIGINAL_LIMIT="${ORIGINAL_LIMIT:-1000000}"
RESULT_DIR="${RESULT_DIR:-${ROOT}/benchmark-results/controlled-$(date +%Y%m%d-%H%M%S)}"
RESET_CLUSTER="${RESET_CLUSTER:-1}"
RESET_CLUSTER_PER_SWEEP="${RESET_CLUSTER_PER_SWEEP:-0}"
RESET_RABBITMQ_BETWEEN_MATRICES="${RESET_RABBITMQ_BETWEEN_MATRICES:-1}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"
LOAD_IMAGES="${LOAD_IMAGES:-1}"
RUN_MATRIX="${RUN_MATRIX:-1}"
WARM_ENDPOINT="${WARM_ENDPOINT:-1}"
STRICT_BENCHMARKS="${STRICT_BENCHMARKS:-1}"
REQUIRE_PARTIAL="${REQUIRE_PARTIAL:-0}"
INSTALL_KIND_IF_MISSING="${INSTALL_KIND_IF_MISSING:-0}"
KIND_VERSION="${KIND_VERSION:-v0.29.0}"
KIND_DOWNLOAD_DIR="${KIND_DOWNLOAD_DIR:-${XDG_CACHE_HOME:-${HOME}/.cache}/dynamos/kind/${KIND_VERSION}}"

KIND_BIN="${KIND_BIN:-}"
PORT_FORWARD_PID=""

log() {
  printf '[controlled-bench] %s\n' "$*" >&2
}

need_tool() {
  local tool="$1"
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "Missing required tool: ${tool}" >&2
    return 1
  fi
}

ensure_kind() {
  if [[ -n "${KIND_BIN}" && -x "${KIND_BIN}" ]]; then
    return 0
  fi

  if command -v kind >/dev/null 2>&1; then
    KIND_BIN="$(command -v kind)"
    return 0
  fi

  if [[ "${INSTALL_KIND_IF_MISSING}" != "1" ]]; then
    echo "kind is not on PATH. Set INSTALL_KIND_IF_MISSING=1 to download a pinned local kind binary." >&2
    return 1
  fi

  local tools_dir="${KIND_DOWNLOAD_DIR}"
  mkdir -p "${tools_dir}"
  KIND_BIN="${tools_dir}/kind"
  if [[ ! -x "${KIND_BIN}" ]]; then
    log "Downloading kind ${KIND_VERSION} to ${KIND_BIN}"
    curl -fsSL "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64" -o "${KIND_BIN}"
    chmod +x "${KIND_BIN}"
  fi
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

create_or_reset_cluster() {
  if [[ "${RESET_CLUSTER}" == "1" ]]; then
    log "Deleting old kind cluster ${CLUSTER_NAME}, if present"
    "${KIND_BIN}" delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
  fi

  if ! "${KIND_BIN}" get clusters | grep -qx "${CLUSTER_NAME}"; then
    log "Creating kind cluster ${CLUSTER_NAME}"
    "${KIND_BIN}" create cluster --name "${CLUSTER_NAME}"
  fi

  kubectl cluster-info --context "kind-${CLUSTER_NAME}" >/dev/null
  kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null
}

build_images() {
  log "Building local images ${IMAGE_REPOSITORY}/*:${IMAGE_TAG}"
  IMAGE_REPOSITORY="${IMAGE_REPOSITORY}" \
    IMAGE_TAG="${IMAGE_TAG}" \
    SQL_QUERY_BENCH_ROWS="${SQL_QUERY_BENCH_ROWS}" \
    "${ROOT}/scripts/build-local-images.sh"
}

load_images() {
  local services=(
    sidecar
    policy-enforcer
    orchestrator
    agent
    api-gateway
    sql-algorithm
    sql-anonymize
    sql-aggregate
    sql-test
    sql-query
  )

  for service in "${services[@]}"; do
    log "Loading ${IMAGE_REPOSITORY}/${service}:${IMAGE_TAG} into kind"
    "${KIND_BIN}" load docker-image "${IMAGE_REPOSITORY}/${service}:${IMAGE_TAG}" --name "${CLUSTER_NAME}"
  done
}

seed_local_etcd_files() {
  log "Seeding etcd launch files from this checkout"
  kubectl -n orchestrator delete job init-etcd-pvc --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n orchestrator delete job init-etcd-pvc-local --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n orchestrator create configmap etcd-launch-files \
    --from-file="${ROOT}/configuration/etcd_launch_files" \
    --dry-run=client -o yaml | kubectl apply -f -

  cat <<'YAML' | kubectl apply -f -
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
        command:
        - /bin/sh
        - -c
        - cp /config/* /etcd/
        volumeMounts:
        - name: config
          mountPath: /config
        - name: pvc-volume
          mountPath: /etcd
      volumes:
      - name: config
        configMap:
          name: etcd-launch-files
      - name: pvc-volume
        persistentVolumeClaim:
          claimName: etcd-pvc
YAML
  kubectl -n orchestrator wait --for=condition=complete job/init-etcd-pvc-local --timeout=180s
}

seed_local_rabbit_files() {
  log "Seeding RabbitMQ config files from this checkout"
  kubectl -n core delete job init-rabbit-pvc-local --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n core create configmap rabbitmq-config-files \
    --from-file=definitions.json="${ROOT}/configuration/k8s_service_files/definitions.json" \
    --from-file=rabbitmq.conf="${ROOT}/configuration/k8s_service_files/rabbitmq.conf" \
    --dry-run=client -o yaml | kubectl apply -f -

  cat <<'YAML' | kubectl apply -f -
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
        command:
        - /bin/sh
        - -c
        - cp /config/* /mnt/
        volumeMounts:
        - name: config
          mountPath: /config
        - name: rabbit-config
          mountPath: /mnt
      volumes:
      - name: config
        configMap:
          name: rabbitmq-config-files
      - name: rabbit-config
        persistentVolumeClaim:
          claimName: rabbit-pvc
YAML
  kubectl -n core wait --for=condition=complete job/init-rabbit-pvc-local --timeout=180s
}

helm_common_args() {
  printf '%s\n' \
    --set "dockerArtifactAccount=${IMAGE_REPOSITORY}" \
    --set "branchNameTag=${IMAGE_TAG}" \
    --set "imagePullPolicy=IfNotPresent" \
    --set "sqlStreamBatchRows=${1}" \
    --set "rabbitmqStreamChunkRows=${2}" \
    --set "rabbitmqStreamChunkBytes=${RABBITMQ_STREAM_CHUNK_BYTES}"
}

deploy_core_once() {
  log "Installing namespaces and local etcd seed files"
  helm upgrade --install dynamos-namespaces "${ROOT}/charts/namespaces" --timeout 5m
  kubectl create namespace linkerd-jaeger --dry-run=client -o yaml | kubectl apply -f -
  kubectl wait --for=condition=Established crd --all --timeout=5s >/dev/null 2>&1 || true
  seed_local_etcd_files
  seed_local_rabbit_files

  log "Deploying core services"
  helm upgrade --install dynamos-core "${ROOT}/charts/core" -n core --set observability.enabled=false --set linkerdJaegerNodePort.enabled=false --wait --timeout 10m
  kubectl -n core rollout status statefulset/etcd --timeout=300s
  kubectl -n core rollout status deployment/rabbitmq --timeout=300s
}

reset_rabbitmq() {
  log "Resetting RabbitMQ storage for a clean matrix"
  kubectl -n core scale deployment/rabbitmq --replicas=0 >/dev/null 2>&1 || true
  kubectl -n core wait --for=delete pod -l app=rabbitmq --timeout=180s >/dev/null 2>&1 || true
  kubectl -n core delete pvc rabbitmq-data-pvc rabbitmq-log-pvc --ignore-not-found >/dev/null 2>&1 || true
  helm upgrade --install dynamos-core "${ROOT}/charts/core" -n core --set observability.enabled=false --set linkerdJaegerNodePort.enabled=false --wait --timeout 10m
  kubectl -n core rollout status deployment/rabbitmq --timeout=300s
}

deploy_application() {
  local sql_batch_rows="$1"
  local rmq_chunk_rows="$2"
  mapfile -t common_args < <(helm_common_args "${sql_batch_rows}" "${rmq_chunk_rows}")

  log "Deploying application batch=${sql_batch_rows} rmq_chunk=${rmq_chunk_rows}"
  helm upgrade --install dynamos-orchestrator "${ROOT}/charts/orchestrator" -n orchestrator "${common_args[@]}" --wait --timeout 10m
  helm upgrade --install dynamos-api-gateway "${ROOT}/charts/api-gateway" -n api-gateway "${common_args[@]}" --wait --timeout 10m
  helm upgrade --install dynamos-agents "${ROOT}/charts/agents" -n uva "${common_args[@]}" --wait --timeout 10m
  helm upgrade --install dynamos-thirdparty "${ROOT}/charts/thirdparty" -n surf "${common_args[@]}" --wait --timeout 10m

  kubectl -n orchestrator rollout status deployment/orchestrator --timeout=300s
  kubectl -n orchestrator rollout status deployment/policy-enforcer --timeout=300s
  kubectl -n api-gateway rollout status deployment/api-gateway --timeout=300s
  kubectl -n uva rollout status deployment/uva --timeout=300s
  kubectl -n vu rollout status deployment/vu --timeout=300s
  kubectl -n surf rollout status deployment/surf --timeout=300s
}

start_port_forward() {
  mkdir -p "${RESULT_DIR}"
  stop_port_forward
  log "Starting api-gateway port-forward on 127.0.0.1:${PORT_FORWARD_PORT}"
  kubectl -n api-gateway port-forward --address 127.0.0.1 svc/api-gateway "${PORT_FORWARD_PORT}:8080" \
    >"${RESULT_DIR}/port-forward.log" 2>&1 &
  PORT_FORWARD_PID="$!"

  for _ in $(seq 1 60); do
    if timeout 1 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/${PORT_FORWARD_PORT}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "api-gateway port-forward did not become ready; see ${RESULT_DIR}/port-forward.log" >&2
  return 1
}

warm_endpoint() {
  if [[ "${WARM_ENDPOINT}" != "1" ]]; then
    return 0
  fi

  log "Warming endpoint with one small unary request"
  python3 "${ROOT}/scripts/benchmark_ndjson.py" \
    --url "http://127.0.0.1:${PORT_FORWARD_PORT}/api/v1/requestApproval" \
    --transport unary \
    --dataset original \
    --limit "${ORIGINAL_LIMIT}" \
    --archetype dataThroughTtp \
    --timeout 1200 \
    >"${RESULT_DIR}/warmup.json" || true
}

run_matrix() {
  local sql_batch_rows="$1"
  local rmq_chunk_rows="$2"
  local name="matrix-batch${sql_batch_rows}-chunk${rmq_chunk_rows}"
  local strict_args=()
  if [[ "${STRICT_BENCHMARKS}" == "1" ]]; then
    strict_args+=(--strict)
  fi
  if [[ "${REQUIRE_PARTIAL}" == "1" ]]; then
    strict_args+=(--require-partial)
  fi

  log "Running benchmark matrix ${name}"
  python3 "${ROOT}/scripts/benchmark_matrix.py" \
    --url "http://127.0.0.1:${PORT_FORWARD_PORT}/api/v1/requestApproval" \
    --transports "${TRANSPORTS}" \
    --datasets "${DATASETS}" \
    --large-limits "${LARGE_LIMITS}" \
    --original-limit "${ORIGINAL_LIMIT}" \
    --archetypes "${ARCHETYPES}" \
    --repetitions "${REPETITIONS}" \
    --timeout 1800 \
    --sql-batch-rows "${sql_batch_rows}" \
    --rabbitmq-chunk-rows "${rmq_chunk_rows}" \
    --output-dir "${RESULT_DIR}" \
    --name "${name}" \
    "${strict_args[@]}"
}

main() {
  need_tool docker
  need_tool kubectl
  need_tool helm
  need_tool python3
  ensure_kind

  mkdir -p "${RESULT_DIR}"
  log "Writing results to ${RESULT_DIR}"

  create_or_reset_cluster
  if [[ "${BUILD_IMAGES}" == "1" ]]; then
    build_images
  fi
  if [[ "${LOAD_IMAGES}" == "1" ]]; then
    load_images
  fi
  deploy_core_once

  local first_matrix=1
  IFS=',' read -r -a sql_batch_values <<< "${SQL_STREAM_BATCH_ROWS_LIST}"
  IFS=',' read -r -a rmq_chunk_values <<< "${RABBITMQ_STREAM_CHUNK_ROWS_LIST}"

  for sql_batch_rows in "${sql_batch_values[@]}"; do
    sql_batch_rows="${sql_batch_rows//[[:space:]]/}"
    for rmq_chunk_rows in "${rmq_chunk_values[@]}"; do
      rmq_chunk_rows="${rmq_chunk_rows//[[:space:]]/}"

      if [[ "${RESET_CLUSTER_PER_SWEEP}" == "1" && "${first_matrix}" != "1" ]]; then
        RESET_CLUSTER=1 create_or_reset_cluster
        load_images
        deploy_core_once
      elif [[ "${RESET_RABBITMQ_BETWEEN_MATRICES}" == "1" && "${first_matrix}" != "1" ]]; then
        reset_rabbitmq
      fi

      deploy_application "${sql_batch_rows}" "${rmq_chunk_rows}"
      start_port_forward
      warm_endpoint
      if [[ "${RUN_MATRIX}" == "1" ]]; then
        run_matrix "${sql_batch_rows}" "${rmq_chunk_rows}"
      fi
      stop_port_forward
      first_matrix=0
    done
  done

  log "Controlled benchmark path finished"
}

main "$@"
