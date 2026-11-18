package observe

import (
	"testing"
	"time"
)

// TestRecorderBoundedEventBuffer verifies that the event buffer never grows
// past its limit.
func TestRecorderBoundedEventBuffer(t *testing.T) {
	recorder := NewRecorder(4)
	for index := 0; index < 50; index++ {
		recorder.Record(Event{Type: EventEvaluation, Verdict: "FIT"})
	}
	if events := recorder.Events(); len(events) != 4 {
		t.Fatalf("events = %d, want 4", len(events))
	}
}

// TestRecorderBoundedMetricCardinality verifies that the counters never grow
// with the number of leases, models or requests.
func TestRecorderBoundedMetricCardinality(t *testing.T) {
	recorder := NewRecorder(16)
	for index := 0; index < 500; index++ {
		recorder.Record(Event{
			Type:    EventEvaluation,
			Verdict: "FIT",
			LeaseID: "lease-" + string(rune(index)),
			ModelID: "model-" + string(rune(index)),
		})
	}
	metrics := recorder.Metrics()
	if len(metrics.Evaluations) != 1 {
		t.Fatalf("evaluation keys = %+v, want 1", metrics.Evaluations)
	}
	if metrics.Evaluations["FIT"] != 500 {
		t.Fatalf("evaluations = %+v", metrics.Evaluations)
	}
}

// TestRecorderNilIsNoop verifies that a nil recorder accepts every call.
func TestRecorderNilIsNoop(t *testing.T) {
	var recorder *Recorder
	recorder.Record(Event{Type: EventEvaluation})
	recorder.ObserveWaitDuration(time.Second)
	recorder.ObserveEvaluationDuration(time.Second)
	if metrics := recorder.Metrics(); metrics.Admitted != 0 {
		t.Fatalf("metrics = %+v", metrics)
	}
	if events := recorder.Events(); len(events) != 0 {
		t.Fatalf("events = %+v", events)
	}
}

// TestRecorderTerminalEventsCountedOnce verifies that the terminal counters
// are incremented once per terminal event.
func TestRecorderTerminalEventsCountedOnce(t *testing.T) {
	recorder := NewRecorder(16)
	recorder.Record(Event{Type: EventWaitStart})
	recorder.Record(Event{Type: EventTimeout})
	recorder.Record(Event{Type: EventTimeout})
	metrics := recorder.Metrics()
	if metrics.Timeouts != 2 {
		t.Fatalf("timeouts = %d, want 2", metrics.Timeouts)
	}
	if metrics.WaitingNow != 0 {
		t.Fatalf("waiting now = %d, want 0: the wait is released once", metrics.WaitingNow)
	}
}

// TestRecorderUnknownAllowedOnlyForPolicy verifies that only a policy
// authorized UNKNOWN continuation increments the unknown counter.
func TestRecorderUnknownAllowedOnlyForPolicy(t *testing.T) {
	recorder := NewRecorder(16)
	recorder.Record(Event{Type: EventContinuation, Verdict: "FIT"})
	if recorder.Metrics().UnknownAllowed != 0 {
		t.Fatal("a FIT continuation was counted as unknown allowed")
	}
	recorder.Record(Event{Type: EventContinuation, Verdict: "UNKNOWN", PolicyAuthorized: true})
	if recorder.Metrics().UnknownAllowed != 1 {
		t.Fatal("a policy authorized UNKNOWN continuation was not counted")
	}
}

// TestRecorderDurationSummary verifies that the duration summary reports the
// observed values.
func TestRecorderDurationSummary(t *testing.T) {
	recorder := NewRecorder(16)
	recorder.ObserveEvaluationDuration(10 * time.Millisecond)
	recorder.ObserveEvaluationDuration(30 * time.Millisecond)
	summary := recorder.Metrics().EvaluationDuration
	if summary.Count != 2 {
		t.Fatalf("count = %d, want 2", summary.Count)
	}
	if summary.MinMS != 10 || summary.MaxMS != 30 {
		t.Fatalf("summary = %+v", summary)
	}
	if summary.AvgMS != 20 {
		t.Fatalf("average = %v, want 20", summary.AvgMS)
	}
}

// TestRecorderEventOrderIsNewestFirst verifies the defined order of the
// event buffer: the newest event comes first and the oldest retained event
// comes last.
func TestRecorderEventOrderIsNewestFirst(t *testing.T) {
	recorder := NewRecorder(4)
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		recorder.Record(Event{Type: EventEvaluation, LeaseID: id})
	}
	events := recorder.Events()
	if len(events) != 4 {
		t.Fatalf("events = %d, want 4", len(events))
	}
	// The two oldest events were evicted; the remaining ones are ordered
	// newest first.
	want := []string{"f", "e", "d", "c"}
	for index, event := range events {
		if event.LeaseID != want[index] {
			t.Fatalf("event %d = %q, want %q", index, event.LeaseID, want[index])
		}
	}
}

// TestRecorderCountersSurviveEventEviction verifies that the counters are
// accumulated independently of the event retention: evicting events never
// reduces a counter.
func TestRecorderCountersSurviveEventEviction(t *testing.T) {
	recorder := NewRecorder(4)
	const total = 500
	for index := 0; index < total; index++ {
		recorder.Record(Event{Type: EventContinuation, Verdict: "FIT"})
	}
	if events := recorder.Events(); len(events) != 4 {
		t.Fatalf("events = %d, want 4", len(events))
	}
	metrics := recorder.Metrics()
	if metrics.Admitted != total {
		t.Fatalf("admitted = %d, want %d: the counters depend on the event retention", metrics.Admitted, total)
	}
	if metrics.Evaluations["FIT"] != 0 {
		t.Fatalf("evaluations = %+v, want no evaluation", metrics.Evaluations)
	}
}

// TestRecorderMetricsAndEventsAreCopies verifies that the read accessors
// return copies: a caller can read them without the lock and without
// observing a later mutation.
func TestRecorderMetricsAndEventsAreCopies(t *testing.T) {
	recorder := NewRecorder(4)
	recorder.Record(Event{Type: EventEvaluation, Verdict: "FIT"})
	metrics := recorder.Metrics()
	events := recorder.Events()
	// Mutating the returned values must not affect the recorder.
	metrics.Evaluations["FIT"] = 999
	events[0].Verdict = "MUTATED"
	again := recorder.Metrics()
	if again.Evaluations["FIT"] != 1 {
		t.Fatalf("evaluations = %+v, want FIT=1", again.Evaluations)
	}
	if got := recorder.Events()[0].Verdict; got != "FIT" {
		t.Fatalf("verdict = %q, want FIT", got)
	}
}
