#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-dynamos-local}"
IMAGE_TAG="${IMAGE_TAG:-main}"

GO_SERVICES=(
  sidecar
  policy-enforcer
  orchestrator
  agent
  api-gateway
  sql-algorithm
  sql-anonymize
  sql-aggregate
  sql-test
)

PYTHON_SERVICES=(
  sql-query
)

cleanup_dirs=()
cleanup() {
  for dir in "${cleanup_dirs[@]}"; do
    rm -rf "${dir}"
  done
}
trap cleanup EXIT

build_go_service() {
  local service="$1"
  local context_dir
  context_dir="$(mktemp -d)"
  cleanup_dirs+=("${context_dir}")

  cp "${ROOT}/go/Dockerfile" "${context_dir}/Dockerfile"
  cp "${ROOT}/go/go.mod" "${ROOT}/go/go.sum" "${context_dir}/"
  cp -a "${ROOT}/go/pkg" "${context_dir}/pkg"
  cp "${ROOT}/go/cmd/${service}"/*.go "${context_dir}/"

  echo "Building ${IMAGE_REPOSITORY}/${service}:${IMAGE_TAG}"
  docker build \
    --build-arg "NAME=${service}" \
    -t "${IMAGE_REPOSITORY}/${service}:${IMAGE_TAG}" \
    -t "${IMAGE_REPOSITORY}/${service}:latest" \
    "${context_dir}"
}

build_python_service() {
  local service="$1"
  local tmp_dir context_dir lib_dir wheel_path wheel_name
  tmp_dir="$(mktemp -d)"
  cleanup_dirs+=("${tmp_dir}")
  context_dir="${tmp_dir}/${service}"
  lib_dir="${tmp_dir}/dynamos-python-lib"

  mkdir -p "${context_dir}"
  cp "${ROOT}/python/Dockerfile" "${context_dir}/Dockerfile"
  cp "${ROOT}/python/${service}"/*.py "${context_dir}/"
  cp "${ROOT}/python/${service}"/*.csv "${context_dir}/"
  cp "${ROOT}/python/${service}/requirements.txt" "${context_dir}/"
  cp -a "${ROOT}/python/dynamos-python-lib/protofiles" "${context_dir}/protofiles"
  cp -a "${ROOT}/python/dynamos-python-lib" "${lib_dir}"

  echo "Building Python wheel for ${service}"
  docker run --rm \
    --user "$(id -u):$(id -g)" \
    -e HOME=/tmp \
    -v "${lib_dir}:/src" \
    -v "${context_dir}:/dist" \
    -w /src \
    python:3.12-slim \
    sh -c 'python -m pip install --user --no-cache-dir wheel setuptools >/dev/null && python setup.py bdist_wheel --dist-dir /dist'

  wheel_path="$(find "${context_dir}" -maxdepth 1 -name '*.whl' -print -quit)"
  if [[ -z "${wheel_path}" ]]; then
    echo "No wheel produced for ${service}" >&2
    return 1
  fi
  wheel_name="$(basename "${wheel_path}")"

  echo "Building ${IMAGE_REPOSITORY}/${service}:${IMAGE_TAG}"
  docker build \
    --build-arg "NAME=${service}" \
    --build-arg "WHEEL_NAME=${wheel_name}" \
    -t "${IMAGE_REPOSITORY}/${service}:${IMAGE_TAG}" \
    -t "${IMAGE_REPOSITORY}/${service}:latest" \
    "${context_dir}"
}

should_build() {
  local service="$1"
  shift

  if [[ "$#" -eq 0 ]]; then
    return 0
  fi

  local requested
  for requested in "$@"; do
    if [[ "${requested}" == "${service}" ]]; then
      return 0
    fi
  done

  return 1
}

main() {
  for service in "${GO_SERVICES[@]}"; do
    if should_build "${service}" "$@"; then
      build_go_service "${service}"
    fi
  done

  for service in "${PYTHON_SERVICES[@]}"; do
    if should_build "${service}" "$@"; then
      build_python_service "${service}"
    fi
  done

  echo "Built local DYNAMOS images with repository ${IMAGE_REPOSITORY} and tag ${IMAGE_TAG}"
}

main "$@"
