#!/usr/bin/env bash

set +e

ROOT="/home/nebur/src/DYNAMOS-clean-20260511"
RESULT_ROOT="${ROOT}/benchmark-results/rq3-extra10-20260710"

cd "${ROOT}" || exit 1
mkdir -p "${RESULT_ROOT}"
RESUME_CLASSIC_DEPLOYMENT="${RESUME_CLASSIC_DEPLOYMENT:-1}" ./scripts/run_rq3_extra10_campaign.sh \
  >"${RESULT_ROOT}/campaign.log" \
  2>"${RESULT_ROOT}/campaign.err"
rc="$?"
printf '%s\n' "${rc}" >"${RESULT_ROOT}/campaign-exit.txt"
exit "${rc}"
