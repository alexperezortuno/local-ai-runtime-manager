package admincli

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
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
		case "/api/v1/capacity/model-1":
			_, _ = io.WriteString(w, `{"verdict":"FIT","model_id":"model-1","justification":["fixture"]}`)
		case "/api/v1/capacity/evaluations":
			_, _ = io.WriteString(w, `[{"model_id":"model-1","operation":"NEW_LOAD","verdict":"FIT","observation_state":"EVALUATED"}]`)
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
		{"capacity", []string{"-url", server.URL, "capacity", "model-1"}, `"verdict": "FIT"`},
		{"capacity evaluations", []string{"-url", server.URL, "capacity-evaluations"}, `"operation": "NEW_LOAD"`},
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

func TestReadOnlyOperationalCommandsRenderExistingSnapshots(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/models":
			_, _ = io.WriteString(w, `[{"id":"commit","state":"READY","residency":"READY_PERSISTENT","keep_alive":true,"pid":42,"process_instance_id":"pi-1","active_leases":0,"capacity_occupied":true}]`)
		case "/api/v1/status":
			_, _ = io.WriteString(w, `{"runtime":"llamacpp","state":"READY","queued_requests":1,"recovery_required":false,"scheduler":{"capacity_used":1,"capacity_limit":2}}`)
		case "/api/v1/queue":
			_, _ = io.WriteString(w, `{"pending":[{"model":"code-fast","state":"QUEUED","lease_id":"lease-1","created_at":"2026-11-05T00:00:00Z"}],"capacity_used":1,"capacity_limit":2}`)
		case "/api/v1/recovery/status":
			_, _ = io.WriteString(w, `{}`)
		case "/api/v1/inferences", "/api/v1/recovery/leases", "/api/v1/processes":
			_, _ = io.WriteString(w, `[]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var output, errors strings.Builder
	if code := RunReadOnly([]string{"-url", server.URL, "models"}, &output, &errors); code != 0 || !strings.Contains(output.String(), "READY_PERSISTENT") || !strings.Contains(output.String(), "pi-1") {
		t.Fatalf("models: code=%d output=%q stderr=%q", code, output.String(), errors.String())
	}
	output.Reset()
	if code := RunReadOnly([]string{"-url", server.URL, "status"}, &output, &errors); code != 0 || !strings.Contains(output.String(), "CAPACITY\t1/2") || !strings.Contains(output.String(), "QUEUED\t1") {
		t.Fatalf("status: code=%d output=%q stderr=%q", code, output.String(), errors.String())
	}
	output.Reset()
	if code := RunReadOnly([]string{"-url", server.URL, "queue"}, &output, &errors); code != 0 || !strings.Contains(output.String(), "1\tcode-fast\tQUEUED\tlease-1") {
		t.Fatalf("queue: code=%d output=%q stderr=%q", code, output.String(), errors.String())
	}
}

func TestDoctorIsReadOnlyAndClassifiesStoppedServiceAsWarning(t *testing.T) {
	dir := t.TempDir()
	model := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(model, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "models.yaml")
	config := `runtime:
  binary: "` + os.Args[0] + `"
models:
  model:
    path: "` + model + `"
    port: 39101
    context: 1024
    batch: 128
    ubatch: 32
    gpu_layers: "0"
    parallel: 1
    temperature: 0.2
    top_p: 0.9
    top_k: 40
`
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	var output, errors strings.Builder
	if code := RunReadOnly([]string{"-config", configPath, "-url", "http://127.0.0.1:1", "doctor"}, &output, &errors); code != 0 {
		t.Fatalf("doctor code=%d output=%q stderr=%q", code, output.String(), errors.String())
	}
	if !strings.Contains(output.String(), "CONFIG\tPASS") || !strings.Contains(output.String(), "SERVICE\tWARN") || !strings.Contains(output.String(), "API\tWARN") {
		t.Fatalf("doctor output=%q", output.String())
	}
	if _, err := os.Stat(configPath); err != nil {
		t.Fatalf("doctor changed config: %v", err)
	}
}

func TestDoctorPassesOccupiedPortForVerifiedRuntimeBackend(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	dir := t.TempDir()
	model := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(model, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "models.yaml")
	config := "runtime:\n  binary: \"" + os.Args[0] + "\"\nmodels:\n  code-agent:\n    path: \"" + model + "\"\n    port: " + strconv.Itoa(port) + "\n    context: 1024\n    batch: 128\n    ubatch: 32\n    gpu_layers: \"0\"\n    parallel: 1\n    temperature: 0.2\n    top_p: 0.9\n    top_k: 40\n"
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/health":
			_, _ = io.WriteString(w, `{"status":"ok"}`)
		case "/api/v1/models":
			_, _ = io.WriteString(w, `[{"id":"code-agent","port":`+strconv.Itoa(port)+`,"state":"READY","pid":237298,"process_instance_id":"process-1","process_identity_status":"VERIFIED","ownership_status":"OWNED_VERIFIED"}]`)
		case "/api/v1/processes":
			_, _ = io.WriteString(w, `[{"model_id":"code-agent","state":"READY","pid":237298,"process_instance_id":"process-1","process_identity_status":"VERIFIED","ownership_status":"OWNED_VERIFIED"}]`)
		case "/api/v1/status":
			_, _ = io.WriteString(w, `{}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	var output, errors strings.Builder
	if code := RunReadOnly([]string{"-config", configPath, "-url", server.URL, "doctor"}, &output, &errors); code != 0 {
		t.Fatalf("doctor code=%d output=%q stderr=%q", code, output.String(), errors.String())
	}
	if !strings.Contains(output.String(), "PORT_"+strconv.Itoa(port)+"\tPASS\tbackend port is occupied by the verified managed backend") {
		t.Fatalf("doctor did not accept verified managed port: %q", output.String())
	}
}

func TestCheckModelPortsAcceptsOnlyVerifiedReadyRuntime(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	cfg := config.Config{Models: map[string]config.ModelConfig{"code-agent": {Port: port}}}
	verified := []doctorProcessStatus{{modelID: "code-agent", state: "READY", pid: 237298, processInstanceID: "process-1", processIdentityStatus: "VERIFIED", ownershipStatus: "OWNED_VERIFIED"}}
	checks := checkModelPorts(cfg, verified)
	if len(checks) != 1 || checks[0].result != "PASS" {
		t.Fatalf("verified managed backend = %+v", checks)
	}

	for _, status := range []doctorProcessStatus{
		{modelID: "code-agent", state: "READY", pid: 237298, processInstanceID: "process-1", processIdentityStatus: "UNVERIFIED", ownershipStatus: "UNKNOWN"},
		{modelID: "code-agent", state: "READY", pid: 237298, processInstanceID: "process-1", processIdentityStatus: "VERIFIED", ownershipStatus: "FOREIGN"},
		{modelID: "code-agent", state: "STARTING", pid: 237298, processInstanceID: "process-1", processIdentityStatus: "VERIFIED", ownershipStatus: "OWNED_VERIFIED"},
	} {
		checks = checkModelPorts(cfg, []doctorProcessStatus{status})
		if len(checks) != 1 || checks[0].result != "FAIL" {
			t.Fatalf("untrusted backend status %+v produced %+v", status, checks)
		}
	}
}

func TestCheckModelPortsHandlesMultipleConfiguredModels(t *testing.T) {
	managed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer managed.Close()
	external, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer external.Close()
	cfg := config.Config{Models: map[string]config.ModelConfig{
		"code-agent": {Port: managed.Addr().(*net.TCPAddr).Port},
		"other":      {Port: external.Addr().(*net.TCPAddr).Port},
	}}
	checks := checkModelPorts(cfg, []doctorProcessStatus{{modelID: "code-agent", state: "READY", pid: 1, processInstanceID: "process-1", processIdentityStatus: "VERIFIED", ownershipStatus: "OWNED_VERIFIED"}})
	results := make(map[string]string, len(checks))
	for _, check := range checks {
		results[check.name] = check.result
	}
	if len(checks) != 2 || results["PORT_"+strconv.Itoa(cfg.Models["code-agent"].Port)] != "PASS" || results["PORT_"+strconv.Itoa(cfg.Models["other"].Port)] != "FAIL" {
		t.Fatalf("multiple model port checks = %+v", checks)
	}
}
