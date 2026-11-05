package admincli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReadOnlyAdministrativeCommands(t *testing.T) {
	paths := make([]string, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		if r.Method != http.MethodGet {
			t.Errorf("administrative CLI used %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/status":
			_, _ = io.WriteString(w, `{"state":"READY","active_model":"model-1","recovery_required":false}`)
		case "/api/v1/models":
			_, _ = io.WriteString(w, `[{"id":"model-1"}]`)
		case "/api/v1/recovery/status":
			_, _ = io.WriteString(w, `{"state":"NORMAL","admission_allowed":true}`)
		case "/api/v1/inferences":
			_, _ = io.WriteString(w, `[{"request_id":"request-1","transport_state":"COMPLETED"}]`)
		case "/api/v1/recovery/leases":
			_, _ = io.WriteString(w, `[{"lease_id":"lease-1","protected":false}]`)
		case "/api/v1/processes":
			_, _ = io.WriteString(w, `[{"process_instance_id":"process-1","ownership_status":"OWNED_VERIFIED"}]`)
		case "/api/v1/recovery/incidents":
			_, _ = io.WriteString(w, `[{"incident_id":"incident-1","status":"BLOCKED"}]`)
		case "/api/v1/recovery/incidents/incident-1":
			_, _ = io.WriteString(w, `{"incident_id":"incident-1","blocking_reasons":["protected"]}`)
		case "/api/v1/recovery/incidents/incident-1/plan":
			_, _ = io.WriteString(w, `{"plan_id":"plan-1","safety_classification":"BLOCKED_PROCESS_IDENTITY"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{"normal status", []string{"-url", server.URL, "status"}, `"state": "READY"`},
		{"models", []string{"-url", server.URL, "models"}, `"id": "model-1"`},
		{"incidents", []string{"-url", server.URL, "incidents"}, `"incident_id": "incident-1"`},
		{"incident", []string{"-url", server.URL, "incident", "incident-1"}, `"blocking_reasons"`},
		{"plan", []string{"-url", server.URL, "plan", "incident-1"}, `"BLOCKED_PROCESS_IDENTITY"`},
		{"processes", []string{"-url", server.URL, "processes"}, `"process_instance_id": "process-1"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output, errors strings.Builder
			if code := Run(test.args, &output, &errors); code != 0 {
				t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
			}
			if !strings.Contains(output.String(), test.want) {
				t.Fatalf("output = %s, want %q", output.String(), test.want)
			}
			var decoded any
			if err := json.Unmarshal([]byte(output.String()), &decoded); err != nil {
				t.Fatal(err)
			}
		})
	}
	if len(paths) == 0 {
		t.Fatal("no authoritative endpoint was queried")
	}
}

func TestModelsRejectsArgumentsWithoutContactingLARM(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer server.Close()
	var output, errors strings.Builder
	if code := Run([]string{"-url", server.URL, "models", "extra"}, &output, &errors); code != 2 {
		t.Fatalf("exit code = %d", code)
	}
	if called {
		t.Fatal("invalid models arguments contacted LARM")
	}
}

func TestRequestFailureCategories(t *testing.T) {
	if err := classifyRequestError("/api/v1/models", &timeoutError{connection: true}); !strings.Contains(err.Error(), "LARM connection timeout") {
		t.Fatalf("connection error = %v", err)
	}
	if err := classifyRequestError("/api/v1/models", &timeoutError{}); !strings.Contains(err.Error(), "LARM response timeout") {
		t.Fatalf("response error = %v", err)
	}
}

func TestModelsPropagatesUnavailableTimeoutAndHTTPError(t *testing.T) {
	t.Run("unavailable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		url := server.URL
		server.Close()
		var output, errors strings.Builder
		if code := Run([]string{"-url", url, "-timeout", "100ms", "models"}, &output, &errors); code != 1 {
			t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
		}
		if !strings.Contains(errors.String(), "LARM unavailable") {
			t.Fatalf("stderr = %s", errors.String())
		}
	})

	t.Run("response timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(100 * time.Millisecond)
			_, _ = io.WriteString(w, "[]")
		}))
		defer server.Close()
		var output, errors strings.Builder
		if code := Run([]string{"-url", server.URL, "-timeout", "10ms", "models"}, &output, &errors); code != 1 {
			t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
		}
		if !strings.Contains(errors.String(), "LARM response timeout") {
			t.Fatalf("stderr = %s", errors.String())
		}
	})

	t.Run("HTTP error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "server secret", http.StatusServiceUnavailable)
		}))
		defer server.Close()
		var output, errors strings.Builder
		if code := Run([]string{"-url", server.URL, "models"}, &output, &errors); code != 1 {
			t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
		}
		if !strings.Contains(errors.String(), "LARM HTTP error") || !strings.Contains(errors.String(), "HTTP 503") || strings.Contains(errors.String(), "server secret") {
			t.Fatalf("stderr = %s", errors.String())
		}
	})
}

type timeoutError struct{ connection bool }

func (e *timeoutError) Error() string {
	if e.connection {
		return "dial timeout"
	}
	return "Client.Timeout exceeded while awaiting headers"
}

func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

func TestStatusRecoveryRequiredAndNoSecrets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/status":
			_, _ = io.WriteString(w, `{"state":"RECOVERY_REQUIRED","recovery_reason":"pending incident","prompt":"do not print","secret":"do not print"}`)
		default:
			_, _ = io.WriteString(w, `{}`)
		}
	}))
	defer server.Close()
	var output, errors strings.Builder
	if code := Run([]string{"-url", server.URL, "status"}, &output, &errors); code != 0 {
		t.Fatalf("status failed: %d %s", code, errors.String())
	}
	if !strings.Contains(output.String(), "RECOVERY_REQUIRED") {
		t.Fatalf("recovery state missing from output: %s", output.String())
	}
	if strings.Contains(output.String(), "do not print") || strings.Contains(errors.String(), "do not print") {
		t.Fatalf("secret leaked: stdout=%q stderr=%q", output.String(), errors.String())
	}
}

func TestInvalidCommandHasNoSideEffects(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer server.Close()
	var output, errors strings.Builder
	if code := Run([]string{"-url", server.URL, "execute"}, &output, &errors); code != 2 {
		t.Fatalf("exit code = %d", code)
	}
	if called {
		t.Fatal("invalid command contacted LARM")
	}
}

func TestMissingIncidentIsExplicitError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "incident secret", http.StatusNotFound)
	}))
	defer server.Close()
	var output, errors strings.Builder
	if code := Run([]string{"-url", server.URL, "incident", "missing"}, &output, &errors); code != 1 {
		t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
	}
	if !strings.Contains(errors.String(), "HTTP 404") || strings.Contains(errors.String(), "incident secret") {
		t.Fatalf("unexpected error output: %s", errors.String())
	}
}
