#!/usr/bin/env bash
# LARM-007.3.7a.3 E1 — Preflight.
#
# Records the pre-execution baseline and aborts when a precondition fails.
# It never repairs a failing condition: it reports and stops.
#
# Exit codes: 0 = preflight satisfactory, 2 = aborted.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/e1-common.sh
source "${SCRIPT_DIR}/e1-common.sh"

REPORT="${EXP_DIR}/preflight.txt"
START_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

: > "$REPORT"
emit() { printf '%s\n' "$*" | tee -a "$REPORT"; }

emit "=== LARM-007.3.7a.3 E1 PREFLIGHT ==="
emit "timestamp_utc=${START_TS}"
emit "host=$(hostname -f 2>/dev/null || hostname)"
emit "arch=$(uname -m)"
emit "os=$(sw_vers -productVersion 2>/dev/null || uname -sr)"

# --- Configuration fingerprints -------------------------------------------
emit ""
emit "--- CONFIG HASHES ---"
emit "experimental_config_path=${EXP_CONFIG}"
if [ -f "$EXP_CONFIG" ]; then
  emit "experimental_config_sha256=$(file_sha256 "$EXP_CONFIG")"
else
  emit "experimental_config_sha256=MISSING"
fi

# Operational configurations are read-only inputs: only their hashes are read.
for op in "${REPO_ROOT}/configs/models.yaml" \
           "${REPO_ROOT}/configs/models-commit.yaml" \
           "${REPO_ROOT}/configs/models.coder.yaml" \
           "${REPO_ROOT}/configs/models.example.yaml"; do
  if [ -f "$op" ]; then
    emit "operational_config $(basename "$op") sha256=$(file_sha256 "$op")"
  else
    emit "operational_config $(basename "$op") sha256=ABSENT"
  fi
done

if [ -f "$GGUF_PATH" ]; then
  emit "gguf_path=${GGUF_PATH}"
  emit "gguf_sha256=$(file_sha256 "$GGUF_PATH")"
  emit "gguf_size_bytes=$(stat -f %z "$GGUF_PATH")"
else
  emit "gguf_path=${GGUF_PATH}"
  emit "gguf_sha256=MISSING"
fi

# --- Memory and swap ------------------------------------------------------
emit ""
emit "--- MEMORY ---"
TOTAL=$(mem_total_bytes); AVAILABLE=$(mem_available_bytes); USED=$(mem_used_bytes)
SWAP_USED=$(swap_used_bytes); SWAP_TOTAL=$(swap_total_bytes); PRESSURE=$(mem_pressure_percent)
emit "total_bytes=${TOTAL}"
emit "available_bytes=${AVAILABLE}"
emit "used_bytes=${USED}"
emit "swap_total_bytes=${SWAP_TOTAL}"
emit "swap_used_bytes=${SWAP_USED}"
emit "pressure_percent=${PRESSURE}"

# --- Ports ----------------------------------------------------------------
emit ""
emit "--- PORTS ---"
PORT_PROBLEM=0
for p in "$LARM_PORT" "$LLAMA_PORT"; do
  if port_busy "$p"; then
    emit "port_${p}=BUSY"
    lsof -nP -iTCP:"$p" -sTCP:LISTEN 2>/dev/null | tee -a "$REPORT"
    PORT_PROBLEM=1
  else
    emit "port_${p}=FREE"
  fi
done
# Port 8090 must not be used by this experiment; it is only observed.
if port_busy 8090; then emit "port_8090=BUSY (not used by E1)"; else emit "port_8090=FREE (not used by E1)"; fi

# --- Residual experimental processes --------------------------------------
emit ""
emit "--- PROCESS IDENTITY ---"
RESIDUAL=0
if [ -f "$LARM_PID_FILE" ]; then
  pid=$(cat "$LARM_PID_FILE")
  if [ -n "$pid" ] && ps -p "$pid" >/dev/null 2>&1; then
    emit "residual_larm_pid=${pid} ALIVE cmd=$(pid_cmdline "$pid")"
    RESIDUAL=1
  else
    emit "residual_larm_pid=${pid} not_alive"
  fi
else
  emit "residual_larm_pid=none_recorded"
fi
if [ -f "$LLAMA_PID_FILE" ]; then
  pid=$(cat "$LLAMA_PID_FILE")
  if [ -n "$pid" ] && ps -p "$pid" >/dev/null 2>&1; then
    emit "residual_llama_pid=${pid} ALIVE cmd=$(pid_cmdline "$pid")"
    RESIDUAL=1
  else
    emit "residual_llama_pid=${pid} not_alive"
  fi
else
  emit "residual_llama_pid=none_recorded"
fi

# Any llama-server on the experimental port is residual regardless of who owns it.
if port_busy "$LLAMA_PORT"; then
  emit "residual_llama_on_${LLAMA_PORT}=DETECTED"
  RESIDUAL=1
fi

# Ambiguity check: the LARM binary must not be running under a different config
# or an unknown identity, and no stray llama-server may hold the model.
STRAY=0
for pid in $(pgrep -f "larm" 2>/dev/null || true); do
  [ "$pid" = "$$" ] && continue
  cmd=$(pid_cmdline "$pid")
  case "$cmd" in
    *"$EXP_CONFIG"*|*"-config ${EXP_CONFIG}"*) ;;
    *) emit "ambiguous_larm_process pid=${pid} cmd=${cmd}"; STRAY=1 ;;
  esac
done

# --- Abort conditions -----------------------------------------------------
emit ""
emit "--- VERDICT ---"
ABORT=0
if [ ! -f "$EXP_CONFIG" ]; then emit "ABORT: experimental configuration missing"; ABORT=1; fi
if [ ! -f "$GGUF_PATH" ]; then emit "ABORT: GGUF missing"; ABORT=1; fi
if [ "$PORT_PROBLEM" = "1" ]; then emit "ABORT: experimental port occupied"; ABORT=1; fi
if [ "$RESIDUAL" = "1" ]; then emit "ABORT: residual experimental process detected"; ABORT=1; fi
if [ "$STRAY" = "1" ]; then emit "ABORT: ambiguous LARM process identity"; ABORT=1; fi
# Significant memory pressure: 15% of RAM in compressor pages is the abort line.
if [ "$PRESSURE" -ge 15 ]; then emit "ABORT: significant memory pressure (${PRESSURE}%)"; ABORT=1; fi
# Available memory must exceed the model file plus a margin to have any chance.
if [ "$AVAILABLE" -lt $((634553760 * 4)) ]; then emit "ABORT: available memory too low for the experiment"; ABORT=1; fi

if [ "$ABORT" = "1" ]; then
  emit "preflight=ABORTED"
  exit 2
fi

emit "preflight=PASS"
exit 0
