package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"local-ai-runtime/internal/admission/observe"
)

// TestAdmissionMetricsAPIWithoutInstrumentation verifies that the admission
// diagnostics endpoint answers with zeroed counters when the admission
// instrumentation is not installed.
func TestAdmissionMetricsAPIWithoutInstrumentation(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response, err := server.Client().Get(server.URL + "/api/v1/admission/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var metrics observe.Metrics
	if err := json.NewDecoder(response.Body).Decode(&metrics); err != nil {
		t.Fatal(err)
	}
	if metrics.Admitted != 0 || metrics.Waiting != 0 || metrics.Timeouts != 0 {
		t.Fatalf("metrics = %+v, want zeroed counters", metrics)
	}
	if metrics.Evaluations == nil {
		t.Fatal("the evaluations map is missing")
	}
}

// TestAdmissionEventsAPIWithoutInstrumentation verifies that the events
// endpoint answers with an empty list when the instrumentation is absent.
func TestAdmissionEventsAPIWithoutInstrumentation(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response, err := server.Client().Get(server.URL + "/api/v1/admission/events")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var events []observe.Event
	if err := json.NewDecoder(response.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("events = %+v, want none", events)
	}
}

// TestAdmissionDiagnosticsAPIExposeRecorder verifies that the read-only
// diagnostics endpoints expose the installed recorder.
func TestAdmissionDiagnosticsAPIExposeRecorder(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	recorder := observe.NewRecorder(observe.DefaultEventLimit)
	sched.SetAdmissionRecorder(recorder)
	recorder.Record(observe.Event{
		Type:             observe.EventContinuation,
		LeaseID:          "lease-1",
		RequestID:        "request-1",
		ModelID:          "model-1",
		Verdict:          "UNKNOWN",
		Outcome:          observe.OutcomeAdmitted,
		PolicyAction:     "ALLOW_UNKNOWN",
		PolicyAuthorized: true,
	})
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	response, err := server.Client().Get(server.URL + "/api/v1/admission/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var metrics observe.Metrics
	if err := json.NewDecoder(response.Body).Decode(&metrics); err != nil {
		t.Fatal(err)
	}
	if metrics.Admitted != 1 || metrics.UnknownAllowed != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}

	response2, err := server.Client().Get(server.URL + "/api/v1/admission/events")
	if err != nil {
		t.Fatal(err)
	}
	defer response2.Body.Close()
	var events []observe.Event
	if err := json.NewDecoder(response2.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %+v, want 1", events)
	}
	if !events[0].PolicyAuthorized || events[0].Verdict != "UNKNOWN" {
		t.Fatalf("event = %+v", events[0])
	}
}

// TestAdmissionDiagnosticsAPIIsReadOnly verifies that the diagnostics endpoints
// never mutate the Scheduler.
func TestAdmissionDiagnosticsAPIIsReadOnly(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()

	for _, path := range []string{"/api/v1/admission/metrics", "/api/v1/admission/events"} {
		response, err := server.Client().Post(server.URL+path, "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode == http.StatusOK {
			t.Fatalf("%s accepted a mutation", path)
		}
	}
}
