#!/usr/bin/env bash
# LARM-007.3.7a.3 E1 — Cleanup.
#
# Stops only the processes owned by this experiment and only after verifying
# their identity (PID plus command line). It never uses pkill, killall or
# process-name patterns. If an identity cannot be verified, no signal is sent
# and the run is declared BLOCKED_CLEANUP.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/e1-common.sh
source "${SCRIPT_DIR}/e1-common.sh"

OUT="${EXP_DIR}/cleanup.txt"
: > "$OUT"
emit() { printf '%s\n' "$*" | tee -a "$OUT"; }
BLOCKED=0

emit "=== LARM-007.3.7a.3 E1 CLEANUP ==="
emit "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# --- llama-server ---------------------------------------------------------
emit ""
emit "--- LLAMA-SERVER ---"
LLAMA_PID=""
[ -f "$LLAMA_PID_FILE" ] && LLAMA_PID=$(cat "$LLAMA_PID_FILE")

if [ -n "$LLAMA_PID" ] && ps -p "$LLAMA_PID" >/dev/null 2>&1; then
  CMD=$(pid_cmdline "$LLAMA_PID")
  emit "llama_pid=${LLAMA_PID}"
  emit "llama_cmdline=${CMD}"
  if pid_matches "$LLAMA_PID" "llama-server" "$GGUF_PATH"; then
    emit "identity=VERIFIED (llama-server + experimental GGUF)"
    kill -TERM "$LLAMA_PID" 2>/dev/null || true
    for _ in $(seq 1 15); do
      ps -p "$LLAMA_PID" >/dev/null 2>&1 || break
      sleep 1
    done
    if ps -p "$LLAMA_PID" >/dev/null 2>&1; then
      # Re-verify identity before escalating to SIGKILL on this PID only.
      if pid_matches "$LLAMA_PID" "llama-server" "$GGUF_PATH"; then
        emit "escalating to SIGKILL on verified pid ${LLAMA_PID}"
        kill -KILL "$LLAMA_PID" 2>/dev/null || true
        sleep 2
      else
        emit "identity lost before SIGKILL; not signalling"
        BLOCKED=1
      fi
    fi
    if ps -p "$LLAMA_PID" >/dev/null 2>&1; then
      emit "llama_process=STILL_ALIVE"; BLOCKED=1
    else
      emit "llama_process=TERMINATED"
    fi
  else
    emit "identity=UNVERIFIED; no signal sent"
    BLOCKED=1
  fi
elif [ -n "$LLAMA_PID" ]; then
  emit "llama_pid=${LLAMA_PID} already_not_running"
else
  emit "llama_pid=none_recorded"
fi

if port_busy "$LLAMA_PORT"; then
  emit "port_${LLAMA_PORT}=BUSY"
  lsof -nP -iTCP:"$LLAMA_PORT" -sTCP:LISTEN 2>/dev/null | tee -a "$OUT"
  BLOCKED=1
else
  emit "port_${LLAMA_PORT}=FREE"
fi

# --- LARM -----------------------------------------------------------------
emit ""
emit "--- LARM ---"
LARM_PID=""
[ -f "$LARM_PID_FILE" ] && LARM_PID=$(cat "$LARM_PID_FILE")

if [ -n "$LARM_PID" ] && ps -p "$LARM_PID" >/dev/null 2>&1; then
  CMD=$(pid_cmdline "$LARM_PID")
  emit "larm_pid=${LARM_PID}"
  emit "larm_cmdline=${CMD}"
  if pid_matches "$LARM_PID" "larm" "-config" "$EXP_CONFIG"; then
    emit "identity=VERIFIED (larm + experimental config)"
    kill -TERM "$LARM_PID" 2>/dev/null || true
    for _ in $(seq 1 30); do
      ps -p "$LARM_PID" >/dev/null 2>&1 || break
      sleep 1
    done
    if ps -p "$LARM_PID" >/dev/null 2>&1; then
      if pid_matches "$LARM_PID" "larm" "-config" "$EXP_CONFIG"; then
        emit "escalating to SIGKILL on verified pid ${LARM_PID}"
        kill -KILL "$LARM_PID" 2>/dev/null || true
        sleep 2
      else
        emit "identity lost before SIGKILL; not signalling"
        BLOCKED=1
      fi
    fi
    if ps -p "$LARM_PID" >/dev/null 2>&1; then
      emit "larm_process=STILL_ALIVE"; BLOCKED=1
    else
      emit "larm_process=TERMINATED"
    fi
  else
    emit "identity=UNVERIFIED; no signal sent"
    BLOCKED=1
  fi
elif [ -n "$LARM_PID" ]; then
  emit "larm_pid=${LARM_PID} already_not_running"
else
  emit "larm_pid=none_recorded"
fi

if port_busy "$LARM_PORT"; then
  emit "port_${LARM_PORT}=BUSY"
  lsof -nP -iTCP:"$LARM_PORT" -sTCP:LISTEN 2>/dev/null | tee -a "$OUT"
  BLOCKED=1
else
  emit "port_${LARM_PORT}=FREE"
fi

# --- Preserved evidence and operational configuration ---------------------
emit ""
emit "--- EVIDENCE ---"
emit "journal=${EXP_STATE}/larm-recovery-e1.json present=$([ -f "${EXP_STATE}/larm-recovery-e1.json" ] && echo yes || echo no)"
emit "evidence_dir=${EXP_DIR}"
emit "operational_config_hashes_after:"
for op in "${REPO_ROOT}/configs/models.yaml" \
           "${REPO_ROOT}/configs/models-commit.yaml" \
           "${REPO_ROOT}/configs/models.coder.yaml"; do
  [ -f "$op" ] && emit "  $(basename "$op") sha256=$(file_sha256 "$op")"
done

emit ""
if [ "$BLOCKED" = "1" ]; then
  emit "cleanup=BLOCKED_CLEANUP"
  exit 3
fi
emit "cleanup=OK"
exit 0
