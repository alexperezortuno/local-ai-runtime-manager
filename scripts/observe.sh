#!/usr/bin/env bash
# LARM-007.3.7a.3 E1 — Memory observation with the lease ACTIVE.
#
# Records memory and swap before and after, the RSS footprint of the real
# llama-server process, the Planner estimation and the snapshot status. It
# compares estimation and observation without assuming that RSS equals the
# total physical consumption of the model. It never changes context or
# parameters and never performs an inference.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/e1-common.sh
source "${SCRIPT_DIR}/e1-common.sh"

OUT="${EXP_DIR}/observe.txt"
: > "$OUT"
emit() { printf '%s\n' "$*" | tee -a "$OUT"; }

[ -f "$LEASE_ID_FILE" ] || fail "no registered lease; run register-lease.sh first"
LEASE_ID=$(cat "$LEASE_ID_FILE")

emit "=== LARM-007.3.7a.3 E1 MEMORY OBSERVATION ==="
emit "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
emit "lease_id=${LEASE_ID}"

# --- BEFORE ---------------------------------------------------------------
emit ""
emit "--- BEFORE ---"
B_AVAIL=$(mem_available_bytes); B_USED=$(mem_used_bytes)
B_SWAP=$(swap_used_bytes); B_PRESSURE=$(mem_pressure_percent)
emit "available_bytes=${B_AVAIL}"
emit "used_bytes=${B_USED}"
emit "swap_used_bytes=${B_SWAP}"
emit "swap_total_bytes=$(swap_total_bytes)"
emit "pressure_percent=${B_PRESSURE}"

# --- Planner estimation (same instant as the observation) ------------------
curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/capacity/${MODEL_ID}" > "${EXP_HTTP}/capacity-active.json"
emit ""
emit "--- PLANNER ESTIMATION ---"
cat "${EXP_HTTP}/capacity-active.json" | tee -a "$OUT"

# --- Process identity and RSS --------------------------------------------
emit ""
emit "--- PROCESS ---"
LLAMA_PID=""
if [ -f "$LLAMA_PID_FILE" ]; then
  cand=$(cat "$LLAMA_PID_FILE")
  if ps -p "$cand" >/dev/null 2>&1; then LLAMA_PID="$cand"; fi
fi
if [ -z "$LLAMA_PID" ] && port_busy "$LLAMA_PORT"; then
  # Fall back to the recorded owner of the port, verified against the model.
  owner=$(lsof -nP -iTCP:"$LLAMA_PORT" -sTCP:LISTEN -t 2>/dev/null | head -1)
  if [ -n "$owner" ] && pid_matches "$owner" "llama-server" "$GGUF_PATH"; then LLAMA_PID="$owner"; fi
fi

if [ -n "$LLAMA_PID" ]; then
  echo "$LLAMA_PID" > "$LLAMA_PID_FILE"
  emit "llama_pid=${LLAMA_PID}"
  emit "llama_cmdline=$(pid_cmdline "$LLAMA_PID")"
  RSS=$(ps -p "$LLAMA_PID" -o rss= 2>/dev/null | tr -d ' ')
  if [ -n "$RSS" ]; then emit "llama_rss_bytes=$((RSS * 1024))"; fi
  VSZ=$(ps -p "$LLAMA_PID" -o vsz= 2>/dev/null | tr -d ' ')
  if [ -n "$VSZ" ]; then emit "llama_vsz_bytes=$((VSZ * 1024))"; fi
else
  emit "llama_pid=NOT_IDENTIFIED"
fi

# --- Snapshot from LARM ---------------------------------------------------
curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/status" > "${EXP_HTTP}/status-active.json"
emit ""
emit "--- LARM STATUS (snapshot) ---"
cat "${EXP_HTTP}/status-active.json" | tee -a "$OUT"

# --- AFTER ----------------------------------------------------------------
sleep 2
emit ""
emit "--- AFTER ---"
A_AVAIL=$(mem_available_bytes); A_USED=$(mem_used_bytes)
A_SWAP=$(swap_used_bytes); A_PRESSURE=$(mem_pressure_percent)
emit "available_bytes=${A_AVAIL}"
emit "used_bytes=${A_USED}"
emit "swap_used_bytes=${A_SWAP}"
emit "swap_total_bytes=$(swap_total_bytes)"
emit "pressure_percent=${A_PRESSURE}"

emit ""
emit "--- DELTA ---"
emit "available_delta_bytes=$((A_AVAIL - B_AVAIL))"
emit "used_delta_bytes=$((A_USED - B_USED))"
emit "swap_delta_bytes=$((A_SWAP - B_SWAP))"

emit ""
emit "note: RSS of llama-server and total physical consumption of the model are reported separately; they are not equivalent."
exit 0
