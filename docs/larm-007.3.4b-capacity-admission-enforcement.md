# LARM-007.3.4b — Capacity Admission Enforcement Integration

## Status

Enforcement is implemented, connected to the Scheduler and proven with
deterministic fakes. It is **not** enabled in operational configurations and
the HTTP/OpenAI/SSE compatibility layer is still pending (LARM-007.3.4c).

The four states are kept strictly separate:

| State | This delivery |
|---|---|
| Enforcement implemented and proven with fakes | **Yes** |
| Enforcement connected to the Scheduler | **Yes** |
| Enforcement enabled in real configurations | **No** (`capacity.admission.enabled` stays `false`; no evaluator is installed by default) |
| HTTP/SSE compatibility | **Pending (LARM-007.3.4c)** |

No HTTP handler, OpenAI schema, SSE contract, administrative API, lease state,
TTL, heartbeat, `max_lease_duration`, Shadow Mode behavior, persistence record
or operational configuration was changed.

## 1. Architecture

```
internal/capacity                internal/admission              internal/scheduler
─────────────────                ────────────────              ──────────────────
Planner (read-only)  ──▶  Evaluator (capacity)  ──▶  AdmissionEvaluator
Policy (wait/allow)          (adapter)                   (consumer interface)
                                                                  │
                                                       ┌──────────▼──────────┐
                                                       │ (*Scheduler).step   │
                                                       │  admitHeadLocked    │
                                                       │  WAITING_CAPACITY   │
                                                       └─────────────────────┘
```

`internal/capacity` does not import `internal/scheduler` and never can observe
a lease, the FIFO, Runtime or Recovery. The bridge lives in the new
`internal/admission` package, which imports both sides and holds no authority
of its own. The Scheduler remains the only owner of queue and lease
transitions.

### Why the consumer interface changed

LARM-007.3.4a defined `Evaluate(ctx, AdmissionRequest) error`. That signature
cannot express a verdict, so it was extended compatibly to
`Evaluate(ctx, AdmissionRequest) (AdmissionEvidence, error)`. The new
`AdmissionEvidence` preserves the verdict, the evaluation ID, the cause, the
justification, the source and the evaluation timestamp. A verdict is a
decision and an error is the absence of a decision; they are never collapsed
into one generic error.

The public `Request*` signatures are untouched. `RequestWithContext` and
`RequestIdempotentWithContext` were added as compatible adapters that carry
the caller context and its deadline.

## 2. Files modified

| File | Change |
|---|---|
| `internal/scheduler/admission.go` | Consumer interface, `AdmissionVerdict`, `AdmissionOptions`, `AdmissionError` + sentinels, internal `WAITING_CAPACITY` phase type. |
| `internal/scheduler/admission_enforce.go` | **New.** Enforcement: head capture, evaluation outside the lock, final check, commit, wait state, deadline. |
| `internal/scheduler/scheduler.go` | `step` integration, admission state, context adapters, `admissionClassificationLocked`, cleanup of retained contexts. |
| `internal/scheduler/idempotency.go` | Context/deadline registration on the idempotent path. |
| `internal/admission/adapter.go` | **New.** Capacity → Scheduler adapter and the context provider. |
| `internal/scheduler/admission_test.go` | Updated the 007.3.4a fixture to the new interface. |
| `internal/scheduler/admission_enforce_test.go` | **New.** Enforcement tests with fakes. |
| `internal/admission/adapter_test.go` | **New.** Adapter tests. |
| `docs/capacity-admission-test-matrix.md` | Matrix updated with the real states. |

## 3. Capacity → Scheduler adapter

`internal/admission.Evaluator` implements `scheduler.AdmissionEvaluator` on top
of `capacity.AdmissionEvaluator`:

- the verdict is preserved (`FIT`, `INSUFFICIENT`, `UNKNOWN`);
- the evidence, the cause and the timestamps are preserved;
- `RequestID`, `LeaseID`, `ModelID` and the operation are correlated;
- `QUEUED` is never forwarded: the adapter maps only `NEW_LOAD` and
  `RESIDENT_REUSE`;
- the resident identity is forwarded with `ProcessInstanceID`, start identity,
  runtime state and ownership;
- the caller context is propagated.

The evaluator never mutates the Scheduler: the adapter only reads the request
and returns evidence.

## 4. `step` and FIFO handling

`(*Scheduler).step` now calls `admitHeadLocked` before the former
`queue.pop`/`PREPARING` transition:

1. observe the FIFO head **without removing it** (`admissionHeadLocked`);
2. capture an immutable copy of the request and the live resident identity;
3. check Recovery, the Admission Gate and the effective deadline;
4. release `s.mu`;
5. run the evaluation with a bounded context (a panic is contained and becomes
   `UNKNOWN` evidence);
6. re-lock and revalidate the generation, the lease state, the FIFO position
   and the cancellation;
7. commit the head **only** when the final check passes;
8. continue with `PREPARING → activate → finishActivation`.

The destructive `queue.pop` remains only for the `admissionEvaluator == nil`
path. When an evaluator is installed, the head is removed by
`commitHeadLocked` at commit time.

Invariants held by the implementation:

- the evaluation never runs under `s.mu`;
- the head is never removed before the admission is authorized;
- a later request can never overtake the head;
- two activations never overlap: the single worker commits one head at a time;
- there is exactly one queue and one worker;
- no fictitious reservation exists: nothing is added to the memory accounting;
- `internal/capacity` never starts a process.

With `admission.enabled=false` (the default) no evaluator is installed, the
enforcement branch is skipped and the previous flow is byte-for-byte the same.

## 5. `WAITING_CAPACITY`

`WAITING_CAPACITY` is an internal phase of the Scheduler
(`admissionPhaseWaiting`), not a `LeaseState`. While the head waits:

- the lease stays `QUEUED` and keeps `queue_position = 1`;
- no `PREPARING` is published;
- no `llama-server` is started and `manager.Start` is never called;
- the TTL is not started, no heartbeat is sent and `max_lease_duration` is
  untouched;
- no Scheduler lock is held across the wait;
- later requests do not overtake the head.

The worker wakes on a periodic reevaluation through a **single** reusable
`time.Timer` (`admissionTimer`), never one timer per tick and never one
goroutine per request. Cancellation, timeout, Recovery and shutdown interrupt
the wait through the existing cleanup path. No additional persistence was
introduced.

## 6. Contexts and deadlines

`RequestWithContext` and `RequestIdempotentWithContext` register the caller
context atomically with the lease creation, so the worker can never observe a
lease without its context. The effective deadline is computed **at
acceptance** and never restarted:

```
D_admission = acceptance + capacity.admission.wait_timeout
D_proxy     = acceptance + proxy.queue_wait_timeout   (when configured)
D_context   = the caller context deadline              (when present)
D_effective = min(D_admission, D_proxy, D_context)
```

A replay reuses the original lease and the original deadline. The retained
context is dropped when the acquisition reaches a terminal state
(`release`, `failAdmissionHeadLocked`, `finishActivation`, shutdown).

**Known limitation — the proxy deadline is not yet connected.** The proxy
handler still creates its own `context.WithTimeout(r.Context(),
proxy.queue_wait_timeout)` and calls `scheduler.RequestWithRequestID`, which
carries no context. Changing that would modify the HTTP contract, which is
out of scope for this task. The internal path is complete and tested
(`TestADMISSION09bCallerContextDeadlineWins`), but the **proxy deadline is not
yet connected** and `D_effective` is therefore not fully covered end to end.
This is the first item of LARM-007.3.4c.

## 7. Final serialized check

`admissionCommitAllowedLocked` runs under `s.mu` immediately before
`PREPARING` and requires all of:

- the lease is still `QUEUED`;
- the lease is still the FIFO head;
- the admission generation still matches;
- the Scheduler is not closing and is not recovery-blocked;
- Runtime does not require Recovery;
- the Admission Gate is not blocking;
- the effective deadline has not expired;
- for `RESIDENT_REUSE`, the live resident identity still matches the observed
  identity (`ProcessInstanceID`, start identity, runtime state, ownership).

If any check fails, the evaluation is discarded and the head is reevaluated
with a fresh classification. An old `FIT` is never reused, a terminal lease is
never revived and a late result can never authorize `manager.Start`.

The runtime residency evidence is read through
`manager.CurrentProcessForModel`, which returns the verified identity. No
Scheduler lock is held across a Runtime call other than this read-only one,
and the generation and lease state are revalidated under the lock before the
commit.

## 8. `RESIDENT_REUSE`

The reuse path is preserved. The evaluation classifies the operation as
`RESIDENT_REUSE` only when the model is `READY` and the live process identity
is available, and the final check revalidates it. A changed identity discards
the evaluation and reclassifies the head.

The incremental inference memory is **not** assumed to be zero: the engine
does not expose that evidence, so the estimation remains as it is today and
no value is invented.

## 9. Cancellation, shutdown and Recovery

| Event | Behavior |
|---|---|
| Explicit cancellation during the wait | The lease goes `CANCELLED` through `release`; the wait and its timer are cancelled; no process starts. |
| Concurrent cancellation and `FIT` | The commit is serialized: a confirmed cancellation wins and no `Start` happens. |
| Timeout during reevaluation | The effective deadline terminates the wait with a typed timeout cause. |
| Shutdown during the wait | The queue is cancelled, the retained contexts are dropped and the timer is stopped. |
| Recovery during the wait | Recovery precedes the verdict: the head is cancelled with a typed recovery cause and no evaluation is made. |
| Generation change | The result is discarded. |
| Terminal lease before the commit | The result is discarded; the lease is not revived. |
| Late evaluator result | Discarded by the serialized revalidation. |

A lease protected by an active or ambiguous inference is never released, and
Recovery and Execution Gate keep their authority.

## 10. Test matrix

See `docs/capacity-admission-test-matrix.md`. Summary:

- `PASS (fake)`: ADMISSION-02, 04, 06, 07, 08, 09 (proxy + context
  precedence), 10, 13, 14, 15, 16, 17, 19, 22.
- `PASS` (characterization and gates): 01, 03, 05, 11, 12, 18, 20, 21, 25.a.
- `NOT IMPLEMENTED`: ADMISSION-23 and ADMISSION-24.b — they depend on the
  pending HTTP integration.

Additional coverage: `QUEUED` observable during the wait, `queue_position=1`,
absence of `PREPARING` while capacity is missing, zero `manager.Start` calls
before authorization, a single commit per acquisition, idempotent replay
without duplication, cancellation without orphan timers, a single reevaluation
timer, no TTL extension while waiting, context propagation and non-retention,
and Shadow Mode unchanged.

## 11. Validation

```
go test ./... -count=1          → ok (all packages)
go test -race ./... -count=1     → ok (all packages)
go vet ./...                     → clean
gofmt -l .                       → clean
git diff --check                 → clean
```

The tests use fixtures, simulated models and ephemeral ports. No external
service was stopped and no real model was started.

Two pre-existing flakes were observed during validation and were confirmed
unrelated to this delivery by re-running them on the unmodified tree:

- `internal/scheduler`: `TestManualStopDrainsResidentBeforeQueuedLease`
  (fails intermittently under `-race` on the original code as well);
- `internal/recovery`: `TestPositiveRecoveryValidation` (fails intermittently
  under the full-repo parallel load on the original code as well).

## 12. Risks and pending work for LARM-007.3.4c

1. **The proxy deadline is not connected.** The proxy still uses its own
   `queue_wait_timeout` and does not propagate `r.Context()` into the
   Scheduler. Until the handler is adapted, `D_effective` is not covered end to
   end and the proxy `504` path is unchanged.
2. **No HTTP mapping of the typed admission errors.** `ErrAdmissionCapacity`,
   `ErrAdmissionTimeout`, `ErrAdmissionUnavailable`, `ErrAdmissionCancelled`
   and `ErrAdmissionRecovery` exist in the Scheduler but are not yet mapped to
   the existing administrative/OpenAI/SSE responses.
3. **The evaluator is not installed by default.** Nothing constructs
   `capacity.NewAdmissionEvaluator` and installs it into the Scheduler, so
   operational behavior is unchanged. That wiring, plus the decision to enable
   `capacity.admission.enabled`, requires explicit approval.
4. **The proxy lease path is untouched.** A client-provided
   `X-LARM-Lease-ID` bypasses the automatic acquisition, so admission applies
   only to the automatic path until the HTTP layer is designed.
5. **The inference memory model is still missing.** `RESIDENT_REUSE` does not
   estimate the incremental KV-cache/workspace memory; the evidence is not
   available yet.
6. **The pool and queue bounds live in `internal/capacity`.** The Scheduler
   owns a single head, so the evaluator concurrency limit is the remaining
   bound to validate under load.
7. **Metrics and alerts for `UNKNOWN` with `on_unknown=allow` are still
   missing**, as required by the contract before enabling enforcement.
8. **Multiple LARM instances on the same hardware are not coordinated.** The
   local contract does not resolve that.

LARM-007.3.4c must not start without explicit approval.
