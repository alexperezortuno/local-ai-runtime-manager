# LARM-007.3.6 — Capacity Admission Observability

## Status

Capacity Admission enforcement gained minimal operational observability. No
decision, algorithm, priority or execution contract changed.

The default remains `capacity.admission.enabled: false`. No operational
configuration was modified and enforcement was not activated.

## 1. Architecture of the instrumentation

The instrumentation is a leaf package plus a thin Scheduler hook. It observes
decisions that were already made; it never participates in them.

```text
 Scheduler enforcement (internal/scheduler/admission_enforce.go)
   │  admitHeadLocked: evaluation, verdict, wait, reevaluation
   │  waitForAdmissionLocked: wait start / reevaluation
   │  failAdmissionHeadLocked: timeout, rejection
   │  cancelAdmissionWaitLocked: cancellation
   │  commitHeadLocked: continuation
   ▼
 internal/scheduler/admission_observe.go   (copy decisions into events)
   ▼
 internal/admission/observe.Recorder       (bounded events + bounded counters)
   │
   ├── internal/scheduler.AdmissionRecorder()  ──► read-only diagnostics
   ▼
 internal/api  GET /api/v1/admission/metrics   (counters + durations)
              GET /api/v1/admission/events    (recent event ring buffer)
   ▼
 internal/admincli  larm admission-metrics | admission-events
```

Design constraints honoured:

- `internal/admission/observe` imports nothing from the project, so both
  `internal/scheduler` and `internal/api` can depend on it without a cycle.
- The Recorder owns no goroutine, no timer and no file handle. It is a
  mutex-guarded map plus a fixed-size ring buffer.
- `internal/capacity` is untouched. Shadow Mode keeps its own history and
  never mixes Shadow observations with Enforcement events.
- The Recorder is strictly non-authoritative: it receives copies of decisions
  and its result is never read by the Scheduler.

## 2. Events and metrics available

### 2.1 Events

| Event type | Emitted when | Key fields |
|---|---|---|
| `admission.evaluation` | One capacity evaluation completed | `verdict`, `cause`, `outcome`, `duration_ms`, `lease_id`, `request_id`, `model_id`, `generation`, `operation` |
| `admission.evaluation.error` | The evaluator returned an error | `verdict` (preserved), `cause`, `duration_ms` |
| `admission.wait.start` | The head entered `WAITING_CAPACITY` | `verdict`, `cause`, `lease_id`, `generation` |
| `admission.reevaluation` | A waiting head was evaluated again | `verdict`, `cause`, `lease_id`, `generation` |
| `admission.timeout` | The effective deadline expired | `outcome=timeout`, `verdict` (last known), `cause` |
| `admission.cancellation` | The wait was cancelled | `outcome=cancelled`, `verdict` (last known), `cause` |
| `admission.continuation` | The head was committed to `PREPARING` | `verdict`, `policy_action`, `policy_authorized` |

Correlation identifiers (`lease_id`, `request_id`, `model_id`, `generation`)
are present on every event. `verdict`, `cause`, `duration_ms` and `outcome` are
present whenever they apply.

No event carries prompts, responses, tokens, credentials or model output. The
`cause` is the sanitized cause already produced by the capacity engine.

### 2.1.1 Exact meaning of `verdict` on a terminal event

On `admission.timeout` and `admission.cancellation` the `verdict` field is
the **last known evaluation result** of the lease. It is not a fresh verdict
and it is not produced by the terminal event:

- a terminal event is not an evaluation. It never consults the Planner and
  it never changes the policy;
- `outcome=TIMEOUT` means the **termination of the wait**, not a new
  decision;
- a timeout does not imply that the Planner ran again. The number of
  evaluations the Planner performed is exactly the number counted in
  `evaluations`, and the timeout is not part of it;
- a timeout carries no `duration_ms`: an evaluation duration is the duration
  of an evaluation, and the timeout is not one.

`TestObserveTimeoutDoesNotReevaluateThePlanner` asserts these properties.

### 2.1.2 Exact meaning of `admitted`

`admitted` counts **continuation authorizations**, not activations:

- `admitted` does **not** mean that the lease reached `ACTIVE`. The
  continuation is recorded after the head was committed to `PREPARING`. The
  activation happens later, outside Admission, in the activation path;
- an authorization can be followed by a **Runtime failure** (the process
  dies during the activation), a **Recovery block** or a **late
  cancellation**. The lease can end `Failed` or `Cancelled` after it was
  counted;
- the counter does **not** measure models started successfully. It does not
  measure activations at all.

There is deliberately **no activation counter** in Admission. Admission does
not observe the Runtime outcome, and adding such a counter would make the
observability layer part of the activation path.

`TestObserveAdmittedCountsAuthorizationNotActivation` asserts the
authorization semantics with a lease that is admitted and then fails its
activation. `TestObserveAdmittedIsNotIncrementedForRejectedOrWaitingLeases`
asserts that a rejected lease and a waiting lease are never counted.


### 2.2 Metrics

| Metric | Meaning |
|---|---|
| `evaluations` | Evaluation count per verdict (`FIT`, `INSUFFICIENT`, `UNKNOWN`, `UNSPECIFIED`) |
| `evaluation_errors` | Failed evaluations |
| `timeouts` | Waits terminated by the effective deadline |
| `cancellations` | Waits terminated by cancellation |
| `admitted` | Continuations authorized to proceed (see §2.1.2: authorizations, not activations) |
| `waiting` | Requests that entered a capacity wait |
| `reevaluations` | Repeated evaluations of a waiting head |
| `unknown_allowed` | Continuations authorized by `on_unknown=allow`, not by a FIT |
| `waiting_now` | Leases currently waiting for capacity |
| `evaluation_duration` | `count`, `min_ms`, `max_ms`, `sum_ms`, `avg_ms` |
| `wait_duration` | `count`, `min_ms`, `max_ms`, `sum_ms`, `avg_ms` |

### 2.3 UNKNOWN with allow

The scenario `verdict=UNKNOWN + on_unknown=allow` is instrumented explicitly:

- the `admission.evaluation` event keeps `verdict=UNKNOWN`; it is never
  rewritten as `FIT`;
- the `admission.continuation` event carries `verdict=UNKNOWN`,
  `policy_action="ALLOW_UNKNOWN"` and `policy_authorized=true`;
- the `unknown_allowed` counter is incremented;
- the UNKNOWN `cause` is preserved on both events when it is available;
- `evaluations.FIT` is **not** incremented by this scenario.

The policy behavior itself is unchanged: `UNKNOWN` + `allow` continues exactly
as before.

## 3. Query mechanism

The diagnostics reuse the existing read-only administrative surface. No new
public endpoint was created and no existing contract changed.

```bash
larm admission-metrics     # read-only CLI against the HTTP API
larm admission-events      # read-only CLI against the HTTP API

curl -s http://127.0.0.1:8090/api/v1/admission/metrics
curl -s http://127.0.0.1:8090/api/v1/admission/events
```

Both endpoints are `GET`-only. A mutating request against them is rejected by
the router like any other unknown method.

With admission disabled the recorder is absent. The endpoints then answer
`200` with zeroed counters and an empty event list rather than an error: the
diagnostic surface stays available and never invents a decision.

## 4. UNKNOWN with allow tests

| Test | Assertion |
|---|---|
| `TestObserveUnknownAllowIsIdentified` | The continuation keeps `verdict=UNKNOWN`, carries `policy_authorized=true` and `policy_action=ALLOW_UNKNOWN`, preserves the cause, and increments `unknown_allowed` without incrementing `evaluations.FIT` |
| `TestObserveFitIsNotPolicyAuthorized` | A FIT continuation is never marked as policy authorized |
| `TestUnknownAllowProducesPolicyAuthorizedEvent` | The same scenario through the real evaluator and adapter |

### 4.1 Semantics and guarantee tests

| Test | Assertion |
|---|---|
| `TestObserveAdmittedCountsAuthorizationNotActivation` | A lease that is admitted and whose activation then fails is still counted as admitted: the counter measures authorizations, not activations (§2.1.2) |
| `TestObserveAdmittedIsNotIncrementedForRejectedOrWaitingLeases` | A rejected lease and a waiting lease are never counted as admitted |
| `TestObserveTimeoutDoesNotReevaluateThePlanner` | The verdict of a timeout is the last known evaluation result, the outcome marks the termination of the wait, and the number of counted evaluations equals the number of Planner calls: the timeout never ran the Planner again (§2.1.1) |
| `TestObserveCancellationDuringEvaluationIsRecorded` | A cancellation that arrives while the evaluation runs is recorded exactly once, with the same correlation identifiers (§8.1) |
| `TestObserveDoesNotBlockScheduler` | The instrumentation does not prolong the Scheduler worker |
| `TestObserveShutdownLeavesNoGoroutine` | A Scheduler shutdown leaves no instrumentation goroutine behind |
| `TestRecorderEventOrderIsNewestFirst` | The event buffer has a defined order: newest first (§5.3) |
| `TestRecorderCountersSurviveEventEviction` | The counters are accumulated independently of the event retention (§5.3) |
| `TestRecorderMetricsAndEventsAreCopies` | The read accessors return copies, so a caller can read them without the lock (§5.2) |
| `TestObserveConcurrentUse` | The Recorder is safe under concurrent access |

## 5. Cardinality, concurrency, retention and limitations

### 5.1 Cardinality

The counter keys are a closed vocabulary: the verdict set
(`FIT`, `INSUFFICIENT`, `UNKNOWN`, `UNSPECIFIED`) and the fixed metric names.
No lease ID, request ID, model ID, generation or evaluation ID is ever used
as a metric label. The event ring buffer is bounded at 200 entries by default
and drops the oldest entry when full.

### 5.2 Concurrency guarantees of the Recorder

The audit of `internal/admission/observe/observe.go` and its consumers
confirmed the following. Each point is a property of the shipped code, not an
intention.

| Property | Status | Evidence |
|---|---|---|
| No I/O inside a critical section | Confirmed | `Record` only updates counters and shifts a bounded slice. The package imports only `sync` and `time`; it has no file, socket or logger handle. |
| No network or disk call from the Recorder | Confirmed | Same import set. The Recorder never opens a file and never dials an address. |
| JSON serialization outside the mutex | Confirmed | `Metrics()` and `Events()` return copies of Go values and release the lock. The encoding happens in `internal/api.writeJSON`, in the HTTP handler, after the snapshot was taken. A slow encoder never holds the Recorder lock. |
| Defined order of the 200-event buffer | Confirmed | `Events()` returns the events newest first: the first element is the most recently recorded event and the last is the oldest retained one. `TestRecorderEventOrderIsNewestFirst` asserts it. |
| Counters accumulated independently of event retention | Confirmed | The counters are separate fields, not derived from the buffer. An eviction never reduces a counter. `TestRecorderCountersSurviveEventEviction` asserts it. |
| Concurrent reads are safe | Confirmed | Every method is safe for concurrent use and the read accessors return copies. `TestRecorderMetricsAndEventsAreCopies` asserts that a caller can mutate the returned values without affecting the Recorder. |
| No prolonged Scheduler blocking | Confirmed | The lock is held for a bounded, in-memory operation only: a constant-time counter update and a shift of at most 200 entries. There is no unbounded work and no blocking call inside the critical section. |

**Lock order.** The Recorder acquires its own lock and then the histogram
lock, always in that order. The Scheduler acquires the Scheduler lock and
then the Recorder lock. No path acquires them in the opposite order, so the
instrumentation cannot introduce a lock cycle. The Scheduler lock is never
held while an unbounded operation runs: the Recorder does not perform one.

**The Recorder starts nothing.** It owns no goroutine, no timer and no file
handle. `TestObserveShutdownLeavesNoGoroutine` asserts that a Scheduler
shutdown leaves no instrumentation goroutine behind.

### 5.3 Event retention policy

- The buffer keeps the **200 most recent events** by default
  (`observe.DefaultEventLimit`).
- When the buffer is full, the **oldest event is discarded** to make room
  for the new one. The buffer never grows.
- The **counters are not affected** by the retention. They accumulate for the
  whole lifetime of the Recorder and are never reduced by an eviction.
- The order of the retained events is defined: `Events()` returns them newest
  first.
- The counters and the events are independent: a burst can lose individual
  events without losing a single count.

### 5.4 Limits of the in-memory metrics

1. The counters are **in-memory only**. They are lost on restart and are not
   persisted, by design: the admission contract forbids new persistence.
2. There is no time-series store and no alerting backend. The metrics are a
   point-in-time snapshot for an operator or an external scraper.
3. The event ring buffer keeps the 200 most recent events. Older events are
   dropped without an aggregate, so a burst can lose individual events. The
   counters are not affected.
4. The duration summaries are aggregate statistics (count/min/max/sum/avg),
   not bucketed histograms. A percentile would need a bucket layout that does
   not exist yet.
5. No Prometheus exposition, OpenTelemetry span or trace context was added.
   The project has no metrics dependency, and adding one was not justified by
   this milestone.
6. The events carry the correlation identifiers of the lease, so the ring
   buffer holds lease IDs. They are never used as metric labels.
7. Manual administrative model start and background preload stay outside the
   FIFO capacity-admission path and produce no admission events.
8. Multiple LARM instances on one host are not coordinated, so the counters
   are per instance.
9. The instrumentation observes the Scheduler enforcement path only. The
   `internal/capacity.Policy` type is not used by the operational wiring and
   is not instrumented.
10. `admitted` measures authorizations, not activations. See §2.1.2.

### 5.5 No double counting

The terminal paths clear the wait state before returning, so a timeout or a
cancellation is recorded exactly once per wait. A cancellation is recorded
in both the `WAITING_CAPACITY` phase and the `EVALUATING` phase: the wait is
a real wait in either phase, and the late evaluation that may still be
running is discarded by the revalidation without recording a terminal event
of its own. `TestObserveTimeoutCountedOnce`, `TestObserveCancellationCountedOnce`
and `TestObserveCancellationDuringEvaluationIsRecorded` assert this.

### 5.6 No behavioral change

The instrumentation never alters FIFO order, the effective deadline, the
reevaluation cadence or the policy.
`TestObserveDoesNotAlterFIFO` and `TestObserveDoesNotAlterDeadline` assert it.


## 6. Modified and added files

Added:

- `internal/admission/observe/observe.go` — the Recorder: events, counters,
  duration summaries.
- `internal/admission/observe/observe_test.go`
- `internal/scheduler/admission_observe.go` — the Scheduler-side event helpers.
- `internal/scheduler/admission_observe_test.go`
- `internal/admission/observability_test.go`
- `internal/api/admission_observability_http_test.go`

Modified:

- `internal/scheduler/scheduler.go` — the recorder field, the setter and the
  accessor.
- `internal/scheduler/admission.go` — the `waitingSince` field of the wait
  state.
- `internal/scheduler/admission_enforce.go` — the emission points, plus the
  clarification fix described in §8.1.
- `internal/admission/adapter.go` — the wiring builds and installs the recorder.
- `internal/api/handler.go` — the two read-only endpoints.
- `internal/admincli/cli.go` — the two read-only commands.
- `cmd/larm/main.go` — the read-only command list.

Unchanged: `internal/capacity` (Planner, Shadow, Admission contract),
`internal/recovery`, `internal/executiongate`, `internal/config`,
`configs/`.

## 7. Build, test and race results

```text
make build                    exit 0
go test ./... -count=1        exit 0 (all packages)
go test -race ./... -count=1  exit 0 (all packages)
go vet ./...                  exit 0 (no output)
gofmt -l .                    exit 0 (no output)
git diff --check              exit 0 (no output)
```

The new tests are covered by:

```text
go test ./internal/admission/observe ./internal/admission ./internal/scheduler ./internal/api \
  -run 'Observe|Admission|UNKNOWN|Unknown' -count=1
```

### 7.1 Flake stress runs

`TestManualStopDrainsResidentBeforeQueuedLease` was stressed after the
correction described in §8.2:

```text
go test      ./internal/scheduler -run TestManualStopDrainsResidentBeforeQueuedLease -count=300  exit 0
go test -race ./internal/scheduler -run TestManualStopDrainsResidentBeforeQueuedLease -count=100  exit 0
go test -race ./internal/scheduler -run 'TestObserveCancellation|TestObserveTimeout|TestObserveAdmitted' -count=50  exit 0
```

## 8. Defects found and corrected during the clarification

### 8.1 A cancellation was lost when it arrived during an evaluation

**Symptom.** `TestObserveCancellationCountedOnce` failed intermittently
under `-race` (3 failures in 30 runs). The `cancellations` counter stayed
at zero and no `admission.cancellation` event was recorded.

**Cause.** The capacity evaluation runs outside the Scheduler lock. While it
runs, the wait state is in the `EVALUATING` phase. `Release` reaches
`cancelAdmissionWaitLocked` in that window, and the terminal event was only
recorded for a wait in the `WAITING_CAPACITY` phase. The cancellation was
therefore confirmed by the Scheduler but never observable.

The late evaluation was discarded correctly by the revalidation
(`admissionStillValidLocked`), so no activation happened and no second
terminal event was recorded. Only the observation was missing.

**Correction.** The cancellation terminal event is now recorded in both the
`WAITING_CAPACITY` and the `EVALUATING` phase, and the evaluating wait
carries the same correlation identifiers and the same wait start as the
waiting wait, so the event is identical in both phases. The productive
semantics of the Scheduler are unchanged: the cancellation still prevents any
later evaluation from authorizing an activation.

`TestObserveCancellationDuringEvaluationIsRecorded` is the regression. It
blocks the evaluator so the `EVALUATING` phase is deterministic, and it
fails without the correction.

### 8.2 `TestManualStopDrainsResidentBeforeQueuedLease` was flaky

**Symptom.** The test failed intermittently with
`operation conflicts with scheduler state`.

**Investigation.** The flake was reproduced on the **pristine tree**, with
the whole LARM-007.3.6 work stashed (`git stash push -u`):

```text
pristine tree, no -race,  -count=300  → FAIL (scheduler_test.go:276)
pristine tree, with -race, -count=60  → FAIL (3 failures)
```

The flake is therefore **pre-existing and unrelated to the observability
work**. It is already documented as such in
`docs/larm-007.3.4b-capacity-admission-enforcement.md` §11. There is no
LARM-007.3.4d in this repository: no branch, no commit and no document.
The reference to that milestone could not be corroborated.

**Cause.** The administrative stop and the queued acquisition race for the
same resident process. `ManualStop` rejects a stop with `ErrConflict` when
a lease is already `ACTIVE` (`ErrConflict: ErrLeaseActive`) or in
`PREPARING` (plain `ErrConflict`). Both are documented outcomes of the
existing manual-operation contract, and both were produced by the test run
itself. The test asserted `err != nil → t.Fatal(err)`, which treated a
legitimate conflict as a failure.

**Correction.** The test now accepts the documented `ErrConflict` and
asserts the behavior that its name describes, for both outcomes:

- when the administrative stop reaches the worker first (`err == nil`), the
  resident must be drained: the queued lease must be running on a **different**
  process instance;
- when the queued acquisition wins the resident (`ErrConflict`), the queued
  lease still reaches `ACTIVE`.

Any other error still fails the test. The productive semantics of the
Scheduler were not changed to make the test pass.

## 9. Residual limitations

1. The counters are in-memory only. They are lost on restart and are not
   persisted, by design: the admission contract forbids new persistence.
2. There is no time-series store and no alerting backend. The metrics are a
   point-in-time snapshot for an operator or an external scraper.
3. The event ring buffer keeps the 200 most recent events. Older events are
   dropped without an aggregate, so a burst can lose individual events. The
   counters are not affected.
4. The duration summaries are aggregate statistics (count/min/max/sum/avg),
   not bucketed histograms. A percentile would need a bucket layout that does
   not exist yet.
5. No Prometheus exposition, OpenTelemetry span or trace context was added.
   The project has no metrics dependency, and adding one was not justified by
   this milestone.
6. The events carry the correlation identifiers of the lease, so the ring
   buffer holds lease IDs. They are never used as metric labels.
7. Manual administrative model start and background preload stay outside the
   FIFO capacity-admission path and produce no admission events.
8. Multiple LARM instances on one host are not coordinated, so the counters
   are per instance.
9. The instrumentation observes the Scheduler enforcement path only. The
   `internal/capacity.Policy` type is not used by the operational wiring and
   is not instrumented.
10. `admitted` measures authorizations, not activations. There is no
    activation counter in Admission, by design. See §2.1.2.
11. This delivery does not authorize enabling enforcement in production.
12. `TestManualStopDrainsResidentBeforeQueuedLease` was flaky on the pristine
    tree, independently of this milestone. It was corrected in this
    clarification (§8.2) and is now stable under `-count=300` and under
    `-race -count=100`. The correction is confined to the test; no
    productive Scheduler semantics changed.
