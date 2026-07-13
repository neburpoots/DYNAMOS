#!/usr/bin/env bash

set +e

ROOT="/home/nebur/src/DYNAMOS-clean-20260511"
RESULT_ROOT="${ROOT}/benchmark-results/rq3-compat-chunked-extra10-20260711"
RESUME_MODE="${1:-${RESUME_STREAMING_DEPLOYMENT:-2}}"

cd "${ROOT}" || exit 1
mkdir -p "${RESULT_ROOT}"
printf '%s\n' "$(date --iso-8601=seconds)" >"${RESULT_ROOT}/launched-at.txt"

RESULT_ROOT="${RESULT_ROOT}" \
SKIP_CLASSIC=1 \
RESUME_STREAMING_DEPLOYMENT="${RESUME_MODE}" \
STREAMING_SCOPE=compatibility \
REPETITIONS=10 \
./scripts/run_rq3_extra10_campaign.sh \
  >"${RESULT_ROOT}/campaign.log" \
  2>"${RESULT_ROOT}/campaign.err"

rc="$?"
printf '%s\n' "${rc}" >"${RESULT_ROOT}/campaign-exit.txt"
printf '%s\n' "$(date --iso-8601=seconds)" >"${RESULT_ROOT}/launcher-finished-at.txt"
exit "${rc}"
