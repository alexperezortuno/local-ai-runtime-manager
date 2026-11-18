#!/usr/bin/env bash
# LARM-007.3.7a.3 E1 — Start the experimental LARM process.
#
# Starts LARM with the isolated experimental configuration, records the PID and
# the command line for later identity verification, and verifies the endpoints
# required by E1. It never touches an operational configuration and never
# stops a process it does not own.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/e1-common.sh
source "${SCRIPT_DIR}/e1-common.sh"

LARM_LOG="${EXP_LOGS}/larm-e1.log"
LARM_BIN="${REPO_ROOT}/bin/larm"

# Prefer the freshly built binary when present, else the repository binary.
if [ ! -x "$LARM_BIN" ]; then LARM_BIN="${REPO_ROOT}/larm"; fi
[ -x "$LARM_BIN" ] || fail "no LARM binary available"

# --- Preflight gate -------------------------------------------------------
"${SCRIPT_DIR}/preflight.sh" >/dev/null 2>&1 || fail "preflight did not pass; run scripts/preflight.sh for details"

# --- Start ----------------------------------------------------------------
log "starting LARM (${LARM_BIN} -config ${EXP_CONFIG})"
: > "$LARM_LOG"
nohup "$LARM_BIN" -config "$EXP_CONFIG" >>"$LARM_LOG" 2>&1 &
LARM_PID=$!
echo "$LARM_PID" > "$LARM_PID_FILE"
echo "$LARM_BIN -config $EXP_CONFIG" > "$LARM_CMD_FILE"
log "LARM pid=${LARM_PID} log=${LARM_LOG}"

# --- Wait for the listener ------------------------------------------------
READY=0
for _ in $(seq 1 60); do
  if ! ps -p "$LARM_PID" >/dev/null 2>&1; then
    log "LARM exited during startup"
    sed -n '1,80p' "$LARM_LOG" | tee "${EXP_LOGS}/larm-startup-failure.log"
    exit 3
  fi
  if curl --fail-with-body --silent --show-error --max-time 5 "${BASE_URL}/health" >"${EXP_HTTP}/health.json" 2>/dev/null; then
    READY=1; break
  fi
  sleep 1
done
[ "$READY" = "1" ] || { log "LARM did not become healthy"; exit 3; }

# --- Identity verification -------------------------------------------------
if ! pid_matches "$LARM_PID" "larm" "-config" "$EXP_CONFIG"; then
  fail "LARM identity could not be confirmed for pid ${LARM_PID}"
fi
log "LARM identity verified: $(pid_cmdline "$LARM_PID")"

# --- Endpoint checks ------------------------------------------------------
curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/models"  > "${EXP_HTTP}/models.json"
curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/status"   > "${EXP_HTTP}/status-start.json"
curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/processes"> "${EXP_HTTP}/processes-start.json"
curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/recovery/status" > "${EXP_HTTP}/recovery-status-start.json"

if ! grep -q "\"$MODEL_ID\"" "${EXP_HTTP}/models.json"; then
  log "model ${MODEL_ID} is not registered"; exit 4
fi
log "model ${MODEL_ID} registered"

# Admission must be disabled. The endpoint always answers with a zeroed
# recorder when no evaluator is installed, so the check is on the counter
# values, not on the presence of the "evaluations" key.
curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/admission/metrics" > "${EXP_HTTP}/admission-metrics-start.json" 2>/dev/null || echo "{}" > "${EXP_HTTP}/admission-metrics-start.json"
NONZERO=$(grep -oE '"(evaluation_errors|admitted|waiting|reevaluations|unknown_allowed|timeouts|cancellations)":[0-9]+' "${EXP_HTTP}/admission-metrics-start.json" | grep -vE ':0$' || true)
if [ -n "$NONZERO" ]; then
  log "admission appears to be active: ${NONZERO}"; exit 5
fi
log "admission disabled (all admission counters zero)"

# No llama-server may exist before the lease is created.
if port_busy "$LLAMA_PORT"; then
  log "llama-server already present on ${LLAMA_PORT} before lease creation"; exit 6
fi
log "no llama-server on ${LLAMA_PORT} before lease creation"

log "LARM started and verified"
exit 0
