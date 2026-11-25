#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
RUN_DIR=${1:-$(mktemp -d "${TMPDIR:-/tmp}/larm-lifecycle-witness.XXXXXX")}
BIN="$RUN_DIR/witness"
SUPERVISOR_LOG="$RUN_DIR/supervisor.jsonl"
CHILD_LOG="$RUN_DIR/child.jsonl"
RUNTIME_LOG="$RUN_DIR/runtime.jsonl"
READY="$RUN_DIR/ready.json"

if [ "$(uname -s)" != "Darwin" ]; then
  echo "BLOCKED: this prototype requires Darwin" >&2
  exit 2
fi
if [ "$(id -u)" -eq 0 ]; then
  echo "BLOCKED: do not run this prototype as root" >&2
  exit 2
fi

mkdir -p "$RUN_DIR"
cd "$ROOT"
go build -o "$BIN" ./experiments/lifecycle-witness/macos

"$BIN" -role=requester -log="$SUPERVISOR_LOG" -ready="$READY" \
  -child-log="$CHILD_LOG" -descendants=true

SUPERVISOR_PID=$(sed -n 's/.*"supervisor_pid":[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$READY")
if [ -z "$SUPERVISOR_PID" ]; then
  echo "missing supervisor PID in $READY" >&2
  exit 1
fi

deadline=$(( $(date +%s) + 10 ))
while ! grep -q '"event":"supervisor_signal_attempt"' "$SUPERVISOR_LOG"; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "supervisor did not complete its safe signal probe" >&2
    exit 1
  fi
  sleep 0.05
done

"$BIN" -role=runtime -log="$RUNTIME_LOG" -ready="$READY"

deadline=$(( $(date +%s) + 10 ))
while ! grep -q '"event":"wait4"' "$SUPERVISOR_LOG"; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "supervisor did not record wait4" >&2
    exit 1
  fi
  sleep 0.05
done

echo "run_dir=$RUN_DIR"
echo "supervisor_pid=$SUPERVISOR_PID"
echo "--- supervisor.jsonl"
cat "$SUPERVISOR_LOG"
echo "--- child.jsonl"
cat "$CHILD_LOG"
echo "--- runtime.jsonl"
cat "$RUNTIME_LOG"
