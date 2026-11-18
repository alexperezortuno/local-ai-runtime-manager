#!/usr/bin/env bash
# LARM-007.3.7a.3 E1 — Register the lease created by this experiment.
#
# Persists the lease ID and the idempotency key so that release.sh and
# cleanup.sh act only on the lease created by E1. It never creates a lease and
# never releases anything.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/e1-common.sh
source "${SCRIPT_DIR}/e1-common.sh"

LEASE_ID="${1:-}"
IDEMPOTENCY_KEY="${2:-}"

[ -n "$LEASE_ID" ] || fail "usage: register-lease.sh <lease_id> [idempotency_key]"

# The lease must exist and belong to this experiment's model.
STATE=$(curl --fail-with-body --silent --show-error --max-time 10 "${BASE_URL}/api/v1/leases/${LEASE_ID}" 2>/dev/null || echo "")
[ -n "$STATE" ] || fail "lease ${LEASE_ID} not found on ${BASE_URL}"
echo "$STATE" | grep -q "\"model\":\"${MODEL_ID}\"" || fail "lease ${LEASE_ID} does not belong to ${MODEL_ID}"

echo "$LEASE_ID" > "$LEASE_ID_FILE"
if [ -n "$IDEMPOTENCY_KEY" ]; then echo "$IDEMPOTENCY_KEY" > "$IDEMPOTENCY_FILE"; fi
chmod 600 "$LEASE_ID_FILE" 2>/dev/null || true

# Persist the full lease state for the evidence record.
echo "$STATE" > "${EXP_HTTP}/lease-registered.json"

log "registered lease ${LEASE_ID}"
exit 0
