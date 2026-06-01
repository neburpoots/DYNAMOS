#!/usr/bin/env bash

set -euo pipefail

STREAM_ROOT="${STREAM_ROOT:-/home/nebur/src/DYNAMOS-streaming-concept-20260524}"
RESULT_ROOT="${RESULT_ROOT:-/home/nebur/src/DYNAMOS-clean-20260511/benchmark-results/concept-20260524-streaming-clean-focused}"
RESOURCE_WRAPPER="${RESOURCE_WRAPPER:-/home/nebur/src/DYNAMOS-clean-20260511/scripts/benchmark_with_resources.py}"
PORT_FORWARD_PORT="${PORT_FORWARD_PORT:-18080}"
REPETITIONS="${REPETITIONS:-3}"
BENCHMARK_TIMEOUT="${BENCHMARK_TIMEOUT:-1800}"
RESET_BETWEEN_CELLS="${RESET_BETWEEN_CELLS:-1}"
RUN_CELLS="${RUN_CELLS:-rabbitmq-streams:250000}"
RESOURCE_INTERVAL="${RESOURCE_INTERVAL:-1}"
APP_SETTLE_SECONDS="${APP_SETTLE_SECONDS:-20}"

PORT_FORWARD_PID=""

log() {
  printf '[clean-streaming-cells] %s\n' "$*" >&2
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

reset_rabbitmq_and_restart_app() {
  log "Resetting RabbitMQ storage"
  kubectl -n core scale deployment/rabbitmq --replicas=0 >/dev/null
  kubectl -n core wait --for=delete pod -l app=rabbitmq --timeout=180s >/dev/null 2>&1 || true
  kubectl -n core delete pvc rabbitmq-data-pvc rabbitmq-log-pvc --ignore-not-found >/dev/null
  helm upgrade core "${STREAM_ROOT}/charts/core" -f "${STREAM_ROOT}/charts/core/values.yaml" --wait --timeout 10m >/dev/null
  kubectl -n core rollout status deployment/rabbitmq --timeout=300s >/dev/null

  log "Restarting application deployments"
  kubectl -n orchestrator rollout restart deployment/orchestrator deployment/policy-enforcer >/dev/null
  kubectl -n api-gateway rollout restart deployment/api-gateway >/dev/null
  kubectl -n uva rollout restart deployment/uva >/dev/null
  kubectl -n vu rollout restart deployment/vu >/dev/null
  kubectl -n surf rollout restart deployment/surf >/dev/null

  kubectl -n orchestrator rollout status deployment/orchestrator --timeout=300s >/dev/null
  kubectl -n orchestrator rollout status deployment/policy-enforcer --timeout=300s >/dev/null
  kubectl -n api-gateway rollout status deployment/api-gateway --timeout=300s >/dev/null
  kubectl -n uva rollout status deployment/uva --timeout=300s >/dev/null
  kubectl -n vu rollout status deployment/vu --timeout=300s >/dev/null
  kubectl -n surf rollout status deployment/surf --timeout=300s >/dev/null
  sleep "${APP_SETTLE_SECONDS}"
  wait_for_agents_online UVA SURF VU
}

agent_is_online() {
  local agent="$1"
  kubectl -n core exec etcd-0 -c etcd -- \
    etcdctl --endpoints=http://127.0.0.1:2379 get "/agents/online/${agent}" 2>/dev/null |
    grep -q "/agents/online/${agent}"
}

restart_agent() {
  local agent="$1"
  local namespace
  namespace="$(printf '%s' "${agent}" | tr '[:upper:]' '[:lower:]')"
  log "Restarting missing agent registration for ${agent}"
  kubectl -n "${namespace}" rollout restart "deployment/${namespace}" >/dev/null
  kubectl -n "${namespace}" rollout status "deployment/${namespace}" --timeout=300s >/dev/null
}

wait_for_agents_online() {
  local agent
  local attempt
  for agent in "$@"; do
    for attempt in 1 2; do
      for _ in $(seq 1 60); do
        if agent_is_online "${agent}"; then
          log "Agent ${agent} is online in etcd"
          break 2
        fi
        sleep 2
      done
      restart_agent "${agent}"
    done

    if ! agent_is_online "${agent}"; then
      echo "Agent ${agent} did not register in etcd" >&2
      return 1
    fi
  done
}

start_port_forward() {
  local name="$1"
  stop_port_forward
  kubectl -n api-gateway port-forward --address 127.0.0.1 svc/api-gateway "${PORT_FORWARD_PORT}:8080" \
    >"${RESULT_ROOT}/port-forward-${name}.log" 2>&1 &
  PORT_FORWARD_PID="$!"

  for _ in $(seq 1 60); do
    status="$(curl -sS --max-time 2 -o /dev/null -w "%{http_code}" "http://127.0.0.1:${PORT_FORWARD_PORT}/healthz" 2>/dev/null || true)"
    if [[ "${status}" != "000" && -n "${status}" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "api-gateway port-forward did not become ready; see ${RESULT_ROOT}/port-forward-${name}.log" >&2
  return 1
}

run_cell() {
  local transport="$1"
  local limit="$2"
  local name="${transport}-${limit}-clean${REPETITIONS}"

  if [[ "${RESET_BETWEEN_CELLS}" == "1" ]]; then
    reset_rabbitmq_and_restart_app
  fi

  start_port_forward "${name}"
  log "Running transport=${transport} limit=${limit} repetitions=${REPETITIONS}"
  python3 "${RESOURCE_WRAPPER}" \
    --output "${RESULT_ROOT}/resources-${name}.json" \
    --interval "${RESOURCE_INTERVAL}" \
    --namespaces api-gateway,orchestrator,uva,surf,vu,core \
    --exclude-containers POD \
    -- \
    python3 "${STREAM_ROOT}/scripts/benchmark_matrix.py" \
      --url "http://127.0.0.1:${PORT_FORWARD_PORT}/api/v1/requestApproval" \
      --transports "${transport}" \
      --response-modes batched \
      --workloads bulk \
      --datasets large \
      --large-limits "${limit}" \
      --archetypes dataThroughTtp \
      --provider-sets UVA \
      --temperature warm \
      --repetitions "${REPETITIONS}" \
      --timeout "${BENCHMARK_TIMEOUT}" \
      --sql-batch-rows 5000 \
      --rabbitmq-chunk-rows 100 \
      --strict \
      --require-partial \
      --output-dir "${RESULT_ROOT}" \
      --name "${name}"
  stop_port_forward
}

main() {
  mkdir -p "${RESULT_ROOT}"
  kubectl config use-context docker-desktop >/dev/null

  IFS=',' read -r -a cells <<< "${RUN_CELLS}"
  for cell in "${cells[@]}"; do
    cell="${cell//[[:space:]]/}"
    [[ -z "${cell}" ]] && continue
    transport="${cell%%:*}"
    limit="${cell##*:}"
    run_cell "${transport}" "${limit}"
  done
}

main "$@"
