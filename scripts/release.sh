#!/usr/bin/env bash
# LARM-007.3.7a.3 E1 — Release the registered lease and observe the unload.
#
# Acts only on the lease registered by register-lease.sh. The unload is
# asynchronous: the script observes the terminal lease state, the termination
# of the llama-server process and the release of the experimental port until a
# timeout expires.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/e1-common.sh
source "${SCRIPT_DIR}/e1-common.sh"

OUT="${EXP_DIR}/release.txt"
UNLOAD_TIMEOUT=60
: > "$OUT"
emit() { printf '%s\n' "$*" | tee -a "$OUT"; }

[ -f "$LEASE_ID_FILE" ] || fail "no registered lease; nothing to release"
LEASE_ID=$(cat "$LEASE_ID_FILE")

emit "=== LARM-007.3.7a.3 E1 RELEASE ==="
emit "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
emit "lease_id=${LEASE_ID}"

# --- Release --------------------------------------------------------------
HTTP_CODE=$(curl --silent --show-error --max-time 30 -o "${EXP_HTTP}/lease-release-response.json" -w '%{http_code}' \
  -X DELETE "${BASE_URL}/api/v1/leases/${LEASE_ID}" || echo "000")
emit "release_http_code=${HTTP_CODE}"
cat "${EXP_HTTP}/lease-release-response.json" 2>/dev/null | tee -a "$OUT"
emit ""

# --- Observe the asynchronous unload --------------------------------------
emit "--- UNLOAD OBSERVATION (timeout ${UNLOAD_TIMEOUT}s) ---"
DEADLINE=$(( $(date +%s) + UNLOAD_TIMEOUT ))
TERMINAL_STATE=""
PORT_RELEASED=0
PROCESS_GONE=0

while [ "$(date +%s)" -le "$DEADLINE" ]; do
  STATE=$(curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/leases/${LEASE_ID}" 2>/dev/null || echo "")
  if [ -n "$STATE" ]; then
    s=$(echo "$STATE" | sed -n 's/.*"state":"\([A-Z_]*\)".*/\1/p')
    [ -n "$s" ] && TERMINAL_STATE="$s"
    case "$s" in
      COMPLETED|CANCELLED|FAILED|EXPIRED|RECOVERY_REQUIRED) ;;
      *) ;;
    esac
  fi

  # llama-server process and port state.
  LLAMA_PID=""
  [ -f "$LLAMA_PID_FILE" ] && LLAMA_PID=$(cat "$LLAMA_PID_FILE")
  if [ -n "$LLAMA_PID" ] && ! ps -p "$LLAMA_PID" >/dev/null 2>&1; then PROCESS_GONE=1; fi
  port_free "$LLAMA_PORT" && PORT_RELEASED=1

  if [ "$PORT_RELEASED" = "1" ] && [ "$PROCESS_GONE" = "1" ]; then break; fi
  sleep 2
done

emit "terminal_lease_state=${TERMINAL_STATE:-UNKNOWN}"
emit "llama_process_gone=${PROCESS_GONE}"
emit "port_${LLAMA_PORT}_released=${PORT_RELEASED}"

# --- Final evidence ------------------------------------------------------
curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/leases/${LEASE_ID}" > "${EXP_HTTP}/lease-final.json" 2>/dev/null || true
curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/queue"        > "${EXP_HTTP}/queue-final.json" 2>/dev/null || true
curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/processes"    > "${EXP_HTTP}/processes-final.json" 2>/dev/null || true
curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/status"       > "${EXP_HTTP}/status-final.json" 2>/dev/null || true

emit ""
emit "--- FINAL STATE ---"
emit "lease: $(cat "${EXP_HTTP}/lease-final.json" 2>/dev/null)"
emit "processes: $(cat "${EXP_HTTP}/processes-final.json" 2>/dev/null)"

if [ "$TERMINAL_STATE" != "COMPLETED" ] && [ "$TERMINAL_STATE" != "CANCELLED" ]; then
  emit "release=INCOMPLETE terminal_state=${TERMINAL_STATE}"
  exit 2
fi
if [ "$PORT_RELEASED" != "1" ]; then
  emit "release=INCOMPLETE port ${LLAMA_PORT} still busy"
  exit 2
fi
emit "release=OK"
exit 0
