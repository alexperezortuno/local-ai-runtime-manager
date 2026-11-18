# LARM-007.3.5 — Controlled Admission Validation

## Status

Capacity Admission enforcement was validated in an isolated integration
environment with `enabled=true`. No operational configuration was changed and
the default remains `capacity.admission.enabled: false`.

This delivery validates software behavior only. It does **not** validate physical
RAM/VRAM, NVIDIA behavior, llama.cpp consumption, or production capacity.

## 1. Architecture of the controlled environment

```text
controlled memory snapshot / planner fixture
              │
              ▼
 capacity.Evaluator (bounded workers)
              │  FIT | INSUFFICIENT | UNKNOWN + cause
              ▼
 admission.Evaluator adapter
              │
              ▼
 Scheduler enforcement ──► single FIFO head ──► PREPARING ──► Runtime fake
              │                                      │
              ├── Recovery / Admission Gate          └── ACTIVE lease
              ├── effective deadline
              └── cancellation / shutdown
```

The integration fixtures provide:

- simulated capacity results and controlled memory snapshots;
- the existing fake Runtime process (the test binary), never llama-server or a
  GGUF model;
- temporary stores and model files under `t.TempDir()`;
- ephemeral HTTP ports through `httptest`;
- explicit admission installation with `Enabled: true`.

The HTTP layer uses the existing OpenAI-compatible proxy and its fake backend.
No fixed port, including 8090, is required.

## 2. CONTROL-01 to CONTROL-10

| Control | Result | Evidence |
|---|---|---|
| CONTROL-01 — FIT | PASS (fake) | `TestCONTROL01FitAdmitsFIFOHeadOnce`: one evaluation, one runtime start, lease reaches ACTIVE. |
| CONTROL-02 — INSUFFICIENT | PASS (fake) | `TestCONTROL02InsufficientKeepsHeadQueued`: head remains QUEUED at position 1, follower remains position 2, no PREPARING and no process. |
| CONTROL-03 — capacity recovery | PASS (fake) | `TestCONTROL03CapacityRecoveryReevaluatesOriginalLease`: same lease and generation are reevaluated; one runtime start. |
| CONTROL-04 — UNKNOWN | PASS (fake) | `TestCONTROL04UnknownPoliciesPreserveVerdictAndCause`: `allow` continues, `wait` remains QUEUED, and the verdict/cause are not rewritten as FIT. |
| CONTROL-05 — deadlines | PASS (fake) | Scheduler: `TestCONTROL05EffectiveDeadlineAndIdempotentReplay`; HTTP: `TestCONTROL05HTTPProxyDeadlineDoesNotCancelActiveInference`, `TestCONTROL05HTTPContextCancellationInterruptsWait`, `TestCONTROL05HTTPIdempotentReplayKeepsDeadline`. The queue context is bounded before activation and is not reused as the inference context. |
| CONTROL-06 — cancellation | PASS (fake) | `TestCONTROL06CancellationStopsWaitingLease`: no process, no PREPARE/activation and no armed admission timer. |
| CONTROL-07 — Recovery | PASS (fake) | `TestCONTROL07RecoveryPreventsLateCommit` and `TestCONTROL07HTTPRecoveryDuringEvaluationPreventsStart`: a gate activated during evaluation prevents the late FIT from committing. |
| CONTROL-08 — Execution Gate | PASS (fake) | `TestCONTROL08ExecutionGateBlocksCapacityFit`: a blocked Scheduler Admission Gate prevents process start even after FIT. The independent `internal/executiongate.Coordinator` remains unchanged. |
| CONTROL-09 — concurrency | PASS (fake) | `TestCONTROL09ConcurrencyPreservesFIFOAndSingleCommit`: unique leases, one evaluation/commit per lease and bounded serial activation. Race validation covers the concurrency path. |
| CONTROL-10 — shutdown | PASS (fake) | `TestCONTROL10ShutdownCancelsEvaluation` and `TestCONTROL10HTTPShutdownCancelsWaitingRequest`: waiting work is cancelled, no new work is admitted and no process survives. |

The controlled scenarios were also executed repeatedly:

```text
go test ./internal/scheduler ./internal/api ./internal/admission \
  -run 'CONTROL|Control|AdmissionDecisionObservability' -count=10
```

Result: PASS for all packages.

## 3. Evidence that enforcement was enabled

`internal/admission/control_integration_test.go` installs the real bounded
capacity evaluator and adapter with:

```go
config.AdmissionConfig{
    Enabled: true,
    OnInsufficient: "wait",
    OnUnknown: "allow",
    WaitTimeout: "2s",
    ReevaluationInterval: "5ms",
    MaxSnapshotAge: "1m",
}
```

`TestControlEnvironmentEnforcesExplicitly` proves that the planner is consulted
and its verdict reaches the Scheduler-facing adapter.

The HTTP and Scheduler control fixtures also install an evaluator explicitly.
The absence of a real model or physical memory reading does not weaken this
evidence: the tests prove enforcement wiring and decisions, not hardware
capacity.

## 4. Evidence that the default remains disabled

- `config.AdmissionConfig.Enabled` is a Go `bool` and therefore defaults to
  `false` when omitted.
- `internal/config/loader_test.go:TestLoadAdmissionDefaultsDisabled` verifies
  the loaded default.
- `internal/admission/wiring_test.go:TestInstallFromConfigRespectsEnabled`
  verifies that disabled admission installs no evaluator.
- `internal/admission/control_integration_test.go` repeats that assertion in
  the controlled environment.
- `configs/models.example.yaml` remains `enabled: false`.
- `cmd/larm/installAdmission` returns without installing a wiring while
  disabled.

No real model configuration was changed to enable enforcement.

## 5. FIT, INSUFFICIENT and UNKNOWN

- FIT authorizes only the final Scheduler commit; it is not a reservation and
  does not bypass Recovery or the Admission Gate.
- INSUFFICIENT keeps the lease QUEUED and at the FIFO head. Reevaluation uses
  the original lease and generation.
- UNKNOWN remains UNKNOWN with its cause. Under `on_unknown=allow` the
  Scheduler continues to the final checks; under `on_unknown=wait` it remains
  queued. It is never converted internally to FIT.

## 6. FIFO, deadlines and cancellation

The Scheduler evaluates only the logical FIFO head and removes it only at the
serialized commit. The effective deadline is computed at acceptance as the
minimum of admission wait, proxy queue wait, and caller context deadline.
Reevaluation and idempotent replay do not restart it. Cancellation of a queued
request prevents PREPARING and process startup and removes the single
reevaluation timer.

## 7. Recovery and Execution Gate

Recovery is checked before evaluation and again at the serialized commit. The
controlled tests activate the gate while a FIT is in flight; the late result
cannot commit and no process starts.

`internal/executiongate` is an independent durable execution coordinator and
was not modified. The Scheduler Admission Gate remains the authority that
blocks capacity admission in the tested path.

## 8. Concurrency and shutdown

Concurrent requests are serialized by the existing Scheduler worker. The
control test verifies unique leases, one evaluation/commit per lease and no
overtaking. Shutdown cancels queued work, interrupts the in-flight evaluation
context, clears the admission timer and leaves no active process or inference.

## 9. Observability review

Existing mechanisms:

- the evaluator exposes verdict, evaluation ID, cause, justification, source
  and evaluation timestamp;
- terminal capacity, timeout, unavailable, cancellation and recovery failures
  are typed in the lease error and exposed through `Scheduler.AdmissionFailure`;
- HTTP maps typed failures to the existing status contract;
- reevaluation count is observable through the evaluator in controlled tests.

`internal/scheduler/admission_observability_test.go` characterizes the current
surface for FIT, INSUFFICIENT, UNKNOWN, timeout, cancellation and reevaluation.

Remaining gaps:

1. There are no dedicated counters or histograms for verdict, cause, wait time,
   reevaluation count, timeout or cancellation.
2. There are no alerts for repeated UNKNOWN decisions, prolonged INSUFFICIENT
   waits or timeout/cancellation spikes.
3. Shadow history does not contain enforcement decisions; enforcement causes
   are only visible in evaluator evidence and terminal lease errors.
4. A non-terminal UNKNOWN with `on_unknown=allow` has no durable, queryable
   admission event after the evaluator call.

No new metrics system was implemented in this milestone.

## 10. Files modified by this milestone

- `internal/scheduler/admission_control_test.go` — new CONTROL-01..10 Scheduler
  integration controls.
- `internal/scheduler/admission_observability_test.go` — new observability
  characterization.
- `internal/api/admission_control_http_test.go` — new HTTP/proxy deadline,
  cancellation, Recovery and shutdown controls.
- `internal/admission/control_integration_test.go` — new explicit-enabled and
  default-disabled integration evidence.
- `internal/capacity/admission_test.go` — removed a timing-dependent sleep from
  the existing cancellation characterization so the full race run is
  deterministic.
- `docs/larm-007.3.5-controlled-admission-validation.md` — this delivery
  report.

Production Planner, Admission policy, Recovery, Execution Gate and operational
configuration were not changed.

## 11. Validation

The complete validation set is:

```bash
make build
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
gofmt -l .
git diff --check
```

The final command results are recorded with this delivery.

Observed final results:

```text
make build                    PASS
go test ./... -count=1        PASS (all packages)
go test -race ./... -count=1  PASS (all packages)
go vet ./...                  PASS (no output)
gofmt -l .                    PASS (no output)
git diff --check              PASS (no output)
```

## 12. Residual risks

1. The capacity values are simulated. Physical RAM/VRAM and NVIDIA behavior
   remain unvalidated.
2. `RESIDENT_REUSE` does not estimate incremental inference KV-cache/workspace
   memory; the reuse verdict remains based on live process identity.
3. There is no enforcement-decision metrics/alerting system.
4. Administrative manual start and background preload are outside the FIFO
   capacity-admission path; this milestone does not change those authorities.
5. Multiple LARM instances on one host are not coordinated.
6. A non-cooperative evaluator can retain its bounded worker until it returns;
   shutdown does not wait indefinitely for that provider.
7. This delivery validates software behavior only and does not authorize
   enabling enforcement in production.
