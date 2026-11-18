#!/usr/bin/env bash
# Shared state and helpers for LARM-007.3.7a.3 E1 physical execution.
# Sourced by preflight/start/observe/release/cleanup. Not executable on its own.
#
# All values are experiment-scoped and never touch operational configuration.

set -euo pipefail

REPO_ROOT="/Users/odin/Projects/AI/local-ai-runtime-manager"
EXP_DIR="${REPO_ROOT}/evidence/larm-007.3.7a.3-e1"
EXP_CONFIG="${EXP_DIR}/config/larm-e1.yaml"
EXP_STATE="${EXP_DIR}/state"
EXP_LOGS="${EXP_DIR}/logs"
EXP_HTTP="${EXP_DIR}/http"

LARM_PORT=19080
LLAMA_PORT=19081
MODEL_ID="bge-m3-e1"
GGUF_PATH="/Users/odin/AI/runtime/models/bge-m3-q8_0.gguf"
BASE_URL="http://127.0.0.1:${LARM_PORT}"

# Identifiers of the experimental processes, filled in by start.sh.
LARM_PID_FILE="${EXP_STATE}/larm.pid"
LARM_CMD_FILE="${EXP_STATE}/larm.cmdline"
LLAMA_PID_FILE="${EXP_STATE}/llama.pid"
LLAMA_CMD_FILE="${EXP_STATE}/llama.cmdline"
LLAMA_PORTAL_FILE="${EXP_STATE}/llama.port-owner"
LEASE_ID_FILE="${EXP_STATE}/lease.id"
IDEMPOTENCY_FILE="${EXP_STATE}/idempotency.key"
PROCESS_INSTANCE_FILE="${EXP_STATE}/process-instance.id"

mkdir -p "$EXP_STATE" "$EXP_LOGS" "$EXP_HTTP"

log() { printf '%s [LARM-007.3.7a.3-e1] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
fail() { log "ABORT: $*"; exit 1; }

# Port busy check that never binds the port itself.
port_busy() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | grep -q . && return 0
  fi
  return 1
}

port_free() { ! port_busy "$1"; }

# macOS memory facts used by preflight/observe.
mem_total_bytes()      { sysctl -n hw.memsize; }
mem_available_bytes()  { awk '/Pages free/ {f=$3} /Pages inactive/ {i=$3} END {printf "%d\n", (f+i)*16384}' < <(vm_stat); }
mem_used_bytes()       { local t a; t=$(mem_total_bytes); a=$(mem_available_bytes); echo $((t - a)); }
swap_used_bytes()      { sysctl -n vm.swapusage | sed -n 's/.*used = \([0-9.]*\)\([KMG]\).*/\1 \2/p' | awk '{u=$1; u=($2=="G")?u*1024*1024*1024:($2=="M")?u*1024*1024:u*1024; printf "%d\n", u}'; }
swap_total_bytes()     { sysctl -n vm.swapusage | sed -n 's/.*total = \([0-9.]*\)\([KMG]\).*/\1 \2/p' | awk '{u=$1; u=($2=="G")?u*1024*1024*1024:($2=="M")?u*1024*1024:u*1024; printf "%d\n", u}'; }

# macOS reports memory pressure in the vm_stat "Pages occupied by compressor" and
# "Memory pressure" sysctl. A high percentage of compressed pages indicates
# real pressure rather than ordinary caching.
mem_pressure_percent() {
  local total compressed
  total=$(mem_total_bytes)
  compressed=$(awk '/Pages occupied by compressor/ {c=$5} END {printf "%d\n", c*16384}' < <(vm_stat))
  echo $(( compressed * 100 / total ))
}

# SHA256 of a file, used for configuration fingerprints.
file_sha256() { shasum -a 256 "$1" | awk '{print $1}'; }

# Read the live command line of a PID without signalling it.
pid_cmdline() { ps -p "$1" -o command= 2>/dev/null; }

# True when the PID is alive and its command line contains every needle.
pid_matches() {
  local pid="$1"; shift
  local cmd
  cmd=$(pid_cmdline "$pid") || return 1
  [ -n "$cmd" ] || return 1
  local needle
  for needle in "$@"; do
    case "$cmd" in *"$needle"*) ;; *) return 1 ;; esac
  done
  return 0
}
