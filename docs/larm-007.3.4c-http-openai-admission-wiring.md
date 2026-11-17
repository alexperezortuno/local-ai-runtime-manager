# LARM-007.3.4c — HTTP/OpenAI Compatibility & Operational Wiring

## Status

The capacity admission engine is now wired to the HTTP layer, the effective
deadline is propagated end to end, and the typed admission errors are mapped to
the existing HTTP contract.

**Enforcement is NOT enabled in any real configuration.** The default remains:

```yaml
capacity:
  admission:
    enabled: false
```

The four states are kept strictly separate:

| State | This delivery |
|---|---|
| Enforcement implemented and connected to the Scheduler | **Yes** (007.3.4b) |
| HTTP/OpenAI/SSE compatibility and context propagation | **Yes** (this delivery) |
| Evaluator installed by default | **No** — only when `capacity.admission.enabled=true` |
| Enforcement enabled in real configurations | **No** — `enabled: false` |

No new endpoint, no new HTTP status code, no new OpenAI schema, no new SSE
event, no new lease state, no TTL/heartbeat change and no new persistence were
introduced.

---

## 1. Final wiring architecture

```
cmd/larm/main.go
    │
    │  installAdmission(...)  ← only when capacity.admission.enabled == true
    ▼
internal/admission.InstallFromConfig
    │  ┌───────────────────────────────────────────────────────────┐
    │  │ capacity.Evaluator  (bounded pool: MaxConcurrent workers) │
    │  │   └─ capacity.Planner → manager.MemorySnapshot / ModelConfig│
    │  │ admission.Evaluator   (scheduler.AdmissionEvaluator)      │
    │  │ admission.ContextProvider (scheduler.AdmissionContextProvider)│
    │  └───────────────────────────────────────────────────────────┘
    ▼
scheduler.SetAdmissionOptions / SetAdmissionContextProvider / SetAdmissionEvaluator
    │
    ▼
internal/scheduler  step → admitHeadLocked → evaluate → commitHeadLocked
    │
    ▼
internal/api  proxy.go (OpenAI) / handler.go (administrative)
```

`internal/capacity` never imports `internal/scheduler`. The bridge lives in
`internal/admission`, which imports both sides and holds no authority of its
own. The Scheduler remains the only owner of queue and lease transitions.

### One planner, one snapshot source

The API handler already owned the only `capacity.Planner`. The wiring reuses
it through the new `Handler.Planner()` accessor instead of building a second
planner or a second snapshot source. `NewHandlerWithCapacity` and
`NewHandlerWithJournalAndRecoveryAndCapacity` keep their signatures.

### Disabled vs enabled

| | `enabled: false` (default) | `enabled: true` |
|---|---|---|
| Evaluator installed | No | Yes |
| Admission workers | None started | Bounded pool from `capacity.EvaluatorOptions` |
| Extra memory snapshots | None | Only the ones the planner already needs |
| HTTP responses | Byte-for-byte unchanged | Existing contract, typed admission errors |
| Lease acquisition/reuse | Unchanged | Unchanged apart from the admission wait |
| Shadow Mode | Unchanged, independent | Unchanged, independent |

The wiring is **not duplicated**: `Install` closes the previously installed
evaluator before replacing it, so the Scheduler never runs two evaluators and
never leaks a worker pool.

`scheduler.SetAdmissionEvaluator` now returns the previous evaluator so the
caller can release it. This is an additive, source-compatible change.

---

## 2. Files modified

| File | Change |
|---|---|
| `cmd/larm/main.go` | Calls `installAdmission` before the server accepts requests; closes the wiring on shutdown. |
| `internal/admission/adapter.go` | New `Wiring`, `Install`, `InstallFromConfig`, `Evaluator.Close`. |
| `internal/api/proxy.go` | Context propagation for the OpenAI path, explicit `QUEUED` lease handling, admission error mapping, `admissionSafeMessage`. |
| `internal/api/handler.go` | Context propagation for the administrative lease path, admission error mapping, `Handler.Planner()`. |
| `internal/scheduler/admission.go` | `AdmissionFailure` decoder for the persisted typed admission cause. |
| `internal/scheduler/admission_enforce.go` | The persisted lease error now records the admission kind in a parseable form. |
| `internal/scheduler/scheduler.go` | `AdmissionFailure` accessor, `WaitActive` returns the typed cause, `SetAdmissionEvaluator` returns the previous evaluator. |
| `configs/models.example.yaml` | Documents `capacity.admission` with `enabled: false`. |
| `internal/api/admission_http_test.go` | **New.** HTTP-01–20 and the HTTP error-mapping tests. |
| `internal/admission/wiring_test.go` | **New.** Wiring, duplicate-installation and shutdown tests. |

No public interface was removed. The two additive changes are:

- `scheduler.SetAdmissionEvaluator` now returns the previous evaluator
  (callers that ignore the result keep compiling).
- `scheduler.AdmissionFailure(string) *AdmissionError` and
  `(*Scheduler).AdmissionFailure(string)` are new read-only accessors.

---

## 3. HTTP → Scheduler → Admission flow

### Automatic acquisition (`/v1/chat/completions`, `/v1/embeddings`)

```go
queueCtx, cancelQueue := context.WithTimeout(r.Context(), h.proxy.QueueWaitDuration())
lease, err := h.scheduler.RequestWithContext(queueCtx, requestID, request.Model)
...
err = h.scheduler.WaitActive(queueCtx, leaseID)
```

The queue context is created **before** the acquisition, so the same absolute
deadline covers the admission wait and the activation wait. It is derived from
`r.Context()`, so a client disconnect cancels the wait immediately.

### Administrative acquisition (`POST /api/v1/leases`)

```go
lease, err := h.scheduler.RequestWithContext(r.Context(), "", request.Model)
status, replay, err := h.scheduler.RequestIdempotentWithContext(r.Context(), keys[0], request.Model)
```

The administrative path has no proxy queue deadline, so it is bounded by the
admission timeout alone, as the contract requires.

### Explicit lease (`X-LARM-Lease-ID`)

Three distinct paths, with no second acquisition in any of them:

| Lease state | Behavior |
|---|---|
| `ACTIVE` | Reused. No new acquisition, no new evaluation. Ownership/model/state checks unchanged. |
| `QUEUED` | The request waits for **that** lease's own admission wait and its own deadlines. No second lease, no second evaluation of another lease. The model must match. |
| anything else | Existing `409` ("lease is not active" / "lease has expired") / `404`. Unchanged. |

An explicit lease is never released by the proxy (`!automatic` guard).

---

## 4. Effective deadline

```
D_admission = acceptance + capacity.admission.wait_timeout
D_proxy     = acceptance + proxy.queue_wait_timeout   (proxy path only)
D_context   = the caller context deadline              (when present)

D_effective = min(D_admission, D_proxy, D_context)
```

Implementation: the proxy creates `queueCtx` with the proxy timeout and passes
it to `RequestWithContext`. The Scheduler computes the deadline **once, at
acceptance** (`trackAdmissionDeadlineLocked`) and stores it in
`admissionDeadline[leaseID]`. `admissionDeadlineLocked` returns the stored
value, so a reevaluation or an idempotent replay never restarts it.

Guarantees:

- The time consumed in the queue counts toward the applicable limit.
- The timeout cancels the pending wait through the existing cleanup route
  (the lease becomes `CANCELLED`).
- A reevaluation never resets the deadline.
- An idempotent replay reuses the original lease and the original deadline.
- The proxy deadline never limits an inference that already started:
  `inferCtx` derives from `r.Context()`, not from `queueCtx`.
- With admission disabled the previous timeout semantics are preserved.

---

## 5. HTTP error mapping

No new status code was introduced. Each typed admission error reuses the
branch the contract assigns to it.

| Internal error | Administrative API | OpenAI-compatible API | Body |
|---|---|---|---|
| `ErrAdmissionCapacity` | `409` (same branch as `scheduler.ErrCapacity`) | `409` | `"model capacity is exhausted"` |
| `ErrAdmissionTimeout` | `409` (existing terminal lease body) | `504` (existing `WaitActive` queue-timeout branch) | `"timed out waiting for model capacity"` |
| `ErrAdmissionUnavailable` | `503` | `503` | `"capacity evaluation is unavailable"` |
| `ErrAdmissionCancelled` | `503` | `503` | `"request was cancelled"` |
| `ErrAdmissionRecovery` | `503` + `RECOVERY_REQUIRED` (existing recovery body) | `503` | existing recovery body |
| `scheduler.ErrIdempotencyRecovery` | `503` + `RECOVERY_REQUIRED` | `503` | existing recovery body |
| Unexpected internal error | `500` | `500` | unchanged |

Rules observed:

- `UNKNOWN` is **never** mapped to `ErrAdmissionCapacity`: `on_unknown=allow`
  continues silently, `on_unknown=reject` produces `ErrAdmissionUnavailable`
  (`503`), never the `409` capacity branch.
- The OpenAI error body never exposes process, path or memory details:
  `admissionSafeMessage` replaces the typed admission message with a fixed
  sentence and leaves every other error untouched.
- Errors unrelated to admission keep their existing status and body.
- Equivalent routes use the same code: the administrative and the OpenAI
  paths share the same sentinel mapping.

The typed cause survives the round trip through the lease: the persisted lease
error records the admission kind (`admission: <kind>`), and
`scheduler.AdmissionFailure` reconstructs a typed `*AdmissionError` from it, so
the HTTP layer never parses free-form text.

---

## 6. SSE behavior

**Before SSE starts.** The admission decision happens before the upstream
request is created. A rejection, a timeout or a cancellation therefore
produces a normal HTTP error response: no `text/event-stream` header, no
partial body, no SSE event. HTTP-10 asserts the status, the absence of
`data:` in the body and the absence of the SSE content type.

**After SSE starts.** Once the upstream headers are committed, the status code
cannot change. A later error keeps the existing SSE protocol: the stream ends
through the existing path (`MarkDownstreamDisconnected`,
`RequestCancellation` + `MarkCancellationUnknown`, `SSEReadError`) and no new
SSE event was introduced. HTTP-11 asserts the status stays `200` after the
stream began.

No inference data is emitted before the admission completes: the upstream
request is only issued after `WaitActive` returned successfully, which requires
the lease to be `ACTIVE`, which requires the admission commit.

---

## 7. `X-LARM-Lease-ID` validation

| Rule | Implementation |
|---|---|
| A valid lease does not trigger a second automatic acquisition | The `automatic` branch is skipped entirely; `RequestWith*` is never called. |
| Reuse does not create another request generation | The inference record is created once for the request ID. |
| `NEW_LOAD` is not applied to a valid resident model | The Scheduler classifies the operation from the live Runtime identity; the proxy never classifies it. |
| Ownership, model, state and existing restrictions are verified | `scheduler.Get`, the `ACTIVE`/`QUEUED` state check and the model equality check are unchanged. |
| Recovery and the Execution Gate are not skipped | `BeginInference` still applies the Execution Gate; the Recovery gate still rejects the acquisition before any evaluation. |
| Expired leases are not revived | An `EXPIRED` lease is still rejected with `409`. |
| A foreign lease is not released by an HTTP error | The `!automatic` deferred guard never releases an explicit lease. |

A `QUEUED` explicit lease respects its original wait and deadlines: the proxy
waits for that lease instead of acquiring another one.

---

## 8. Shadow Mode

Preserved completely and independent of `capacity.admission.enabled`:

- `capacity.shadow_mode` keeps its semantics;
- `AdmissionObserver` is unchanged;
- `GET /api/v1/capacity/evaluations` is unchanged;
- the CLI `larm capacity-evaluations` is unchanged;
- the historical `QUEUED` operation is unchanged.

Shadow Mode never authorizes nor blocks a load. The operational wiring does
not duplicate evaluation records: the observer is installed once by the
handler, and the enforcement evaluator is a separate dependency that does not
write to the shadow history.

---

## 9. HTTP-01–20 results

All in `internal/api/admission_http_test.go`. Fakes and ephemeral ports only;
no external service was stopped and no real model was started.

| Case | Validation | Result |
|---|---|---|
| HTTP-01 | Admission disabled: baseline behavior | PASS |
| HTTP-02 | Admission enabled: `FIT` allows continuation | PASS |
| HTTP-03 | `INSUFFICIENT` keeps the request waiting | PASS |
| HTTP-04 | Timeout returns the correct typed error | PASS |
| HTTP-05 | HTTP cancellation interrupts the wait | PASS |
| HTTP-06 | Proxy deadline prevails when it is the earliest | PASS |
| HTTP-07 | Admission deadline prevails when it is the earliest | PASS |
| HTTP-08 | Context deadline prevails when it is the earliest | PASS |
| HTTP-09 | Idempotent replay does not restart the deadline | PASS |
| HTTP-10 | Error before SSE preserves the HTTP response | PASS |
| HTTP-11 | Error after SSE preserves the protocol | PASS |
| HTTP-12 | Explicit `ACTIVE` lease is not reacquired | PASS |
| HTTP-13 | Explicit `QUEUED` lease respects its wait | PASS |
| HTTP-14 | Invalid or expired lease is rejected | PASS |
| HTTP-15 | Recovery prevails over `FIT` | PASS |
| HTTP-16 | Shutdown cancels the wait | PASS |
| HTTP-17 | Shadow Mode keeps its compatibility | PASS |
| HTTP-18 | Concurrency without duplicate activations | PASS |
| HTTP-19 | The evaluator is installed only when enabled | PASS |
| HTTP-20 | Shutdown releases the workers and the resources | PASS |

Additional tests: `TestAdmissionCancellationIsMappedToTypedError`,
`TestSchedulerAdmissionFailureIsTyped`, `TestAdmissionErrorStatusMapping`,
`TestAdmissionRecoveryUsesRecoveryBranch`, plus the `internal/admission`
wiring tests (`TestInstallInstallsEvaluatorAndPolicy`,
`TestInstallFromConfigRespectsEnabled`, `TestInstallIsNotDuplicated`,
`TestWiringCloseReleasesWorkers`).

---

## 10. Validation

| Command | Exit code | Result |
|---|---|---|
| `go test ./... -count=1` | 1 | One pre-existing environmental failure (see below). All other packages `ok`. |
| `go test -race ./... -count=1` | 1 | Same single pre-existing environmental failure. No race reported. |
| `go vet ./...` | 0 | Clean. |
| `gofmt -l .` | 0 | Clean (no output). |
| `git diff --check` | 0 | Clean. |

### Pre-existing failure (not a regression)

`internal/admincli`: `TestDoctorIsReadOnlyAndClassifiesStoppedServiceAsWarning`

```
PORT_8090  FAIL  configured API port is occupied and health did not respond
```

A real `larm` process (PID 38527) is listening on `127.0.0.1:8090` on this
machine. The failure was reproduced on the **unmodified tree** (`git stash`),
so it is environmental and not caused by this delivery. The test was not
modified.

### Pre-existing flakes re-checked

| Test | Result |
|---|---|
| `internal/scheduler` `TestManualStopDrainsResidentBeforeQueuedLease` | PASS (`-count=3`) |
| `internal/recovery` `TestPositiveRecoveryValidation` | PASS (`-count=3`) |

Neither test was modified.

---

## 11. Pending risks before enabling enforcement

1. **Enforcement is still off.** Nothing installs the evaluator unless
   `capacity.admission.enabled=true` is set explicitly. Enabling it requires
   explicit approval.
2. **Metrics and alerts for `UNKNOWN` with `on_unknown=allow` are still
   missing** (required by the contract before enabling enforcement).
3. **The inference memory model is still missing.** `RESIDENT_REUSE` does not
   estimate the incremental KV-cache/workspace memory; the evidence does not
   exist yet.
4. **The evaluator pool bound** (`MaxConcurrent`, `MaxPending`) is the
   remaining bound to validate under load.
5. **Multiple LARM instances on the same hardware are not coordinated.**
6. **Hardware validation on the real target platform** was not performed:
   the tests use fakes, fixtures and ephemeral ports.
7. **The `AdmissionOptions.ProxyQueueWaitTimeout` field is now unused by the
   operational wiring** (the proxy contributes its deadline through the
   context). It remains for compatibility and is still exercised by the
   Scheduler unit tests.

---

## 12. Restrictions observed

- Admission is **not** enabled in any real configuration.
- The FIFO policy was not modified.
- No public lease state was changed.
- Runtime and Recovery were not modified.
- The Execution Gate was not modified.
- No additional persistence was introduced.
- TTL, heartbeat and `max_lease_duration` are untouched.
- No new endpoints were created.
- The OpenAI API was not changed beyond the necessary compatibility and
  error mapping.
- No unbounded goroutines were added: the admission evaluator uses the
  existing bounded pool and the Scheduler keeps a single reusable timer.
- No unrelated files were modified. `__pycache__` was not touched.
- No real NVIDIA test was performed.
- **No commit was made.**
