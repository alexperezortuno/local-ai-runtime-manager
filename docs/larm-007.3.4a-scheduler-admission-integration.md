# LARM-007.3.4a — Scheduler Admission Integration (Structural)

## Status

This delivery is structural only. It does not enforce capacity admission and does
not change Scheduler decisions, lease transitions, FIFO order, TTL, heartbeat,
Recovery, Execution Gate, or Shadow Mode.

## Architecture

`internal/scheduler` owns a small consumer-side `AdmissionEvaluator` interface and
an optional dependency installed with `SetAdmissionEvaluator`. The dependency is
independent from `AdmissionObserver`: Shadow remains observation-only and is not
an authority.

The Scheduler keeps one FIFO and one lease map. `internal/capacity` is not
imported by `internal/scheduler`, so there is no dependency cycle. Capacity
remains read-only and has no access to leases or processes.

## Correlation and generation

Accepted requests are correlated with:

- model ID;
- request ID;
- lease ID;
- admission operation;
- monotonically increasing scheduler generation.

`RequestWithRequestID` retains its current public signature. `RequestIdempotent`
uses the lease ID as its correlation ID and records the request only when it
creates a lease; replay does not create another correlation or acquisition.

Current request entry points do not receive `context.Context`. Therefore this
stage does not retain client contexts or deadlines. LARM-007.3.4b must propagate
a context/deadline through a compatible request adapter before calculating the
effective admission deadline. Replay must reuse the original deadline.

## Exact Scheduler integration point

`(*Scheduler).step` calls `admissionHeadLocked` immediately before the existing
`queue.pop`/`PREPARING` transition. This helper captures the logical FIFO head
without removing it and without changing its state or position.

The evaluator is deliberately not called. Future enforcement must:

1. capture the head, generation and Recovery state under `s.mu`;
2. release `s.mu`;
3. evaluate outside the lock;
4. re-lock and validate generation, lease state, cancellation, Recovery,
   residence identity and deadline;
5. commit the existing head transition and activate.

The existing `queue.pop` order and `QUEUED -> PREPARING -> ACTIVE` flow remain
unchanged in this delivery.

## Compatibility evidence

- No admission dependency: the Scheduler starts and operates normally.
- Optional evaluator configured: no evaluator call occurs on the operational
  path.
- `capacity.admission.enabled=false`: no admission decision is made and the
  existing observable Scheduler behavior is retained.
- No new lease state, queue, worker, persistence record, HTTP contract, SSE
  contract, or process lifecycle behavior was added.

## Tests

Structural fixtures cover:

- Scheduler without admission dependency;
- optional evaluator configured but not called;
- FIFO head correlation without queue mutation;
- idempotent replay without a second correlated request.

The existing Scheduler suite continues to characterize FIFO, lease states,
idempotency, Recovery and lifecycle behavior. The full repository validation is
recorded in the delivery report.

## Risks and pending work for LARM-007.3.4b

- Public `Request*` signatures do not carry a request context or deadline.
  LARM-007.3.4b needs a compatible context-propagating adapter.
- The existing destructive `queue.pop` must be replaced by a logical head
  view/commit before enforcement can preserve `QUEUED` while waiting.
- Recovery must be checked in the final commit, not inferred from an evaluation.
- `capacity.Policy` can wait/re-evaluate, so enforcement will need a bounded
  Scheduler-owned wait state without exposing `WAITING_CAPACITY` as a lease
  state or creating per-request goroutines.
- Runtime resident identity and Execution Gate authority must be revalidated
  before `manager.Start`.
- Enforcement remains `NOT IMPLEMENTED`; operational admission cases in the test
  matrix must not be marked PASS until explicitly approved and implemented.
