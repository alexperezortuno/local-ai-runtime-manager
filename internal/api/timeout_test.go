package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
	"local-ai-runtime/internal/scheduler"
)

func TestQueueWaitTimeoutCancelsPreparingStartupDeterministically(t *testing.T) {
	barrier := t.TempDir()
	t.Setenv("LARM_API_BACKEND_MODE", "")
	t.Setenv("LARM_API_STARTUP_MODE", "blocked")
	t.Setenv("LARM_API_STARTUP_BARRIER", barrier)
	manager, sched := apiTestScheduler(t)
	registry := inference.New(inference.Options{})
	server := httptest.NewServer(NewHandlerWithConfigAndRegistry(manager, sched, config.ProxyConfig{QueueWaitTimeout: "100ms"}, registry))
	defer server.Close()

	requestID, err := corecontract.NewID()
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"model-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-LARM-Request-ID", requestID)
	responseCh := make(chan *http.Response, 1)
	errorCh := make(chan error, 1)
	preparingID := ""
	go func() {
		response, requestErr := server.Client().Do(request)
		if requestErr != nil {
			errorCh <- requestErr
			return
		}
		responseCh <- response
	}()

	waitForAPI(t, func() bool {
		snapshot := sched.Snapshot()
		if snapshot.Preparing != nil && snapshot.Preparing.Model == "model-1" {
			preparingID = snapshot.Preparing.ID
			return true
		}
		return false
	})
	waitForAPI(t, func() bool {
		_, statErr := os.Stat(filepath.Join(barrier, "preparing"))
		return statErr == nil
	})
	instances := manager.ProcessInstances()
	if len(instances) != 1 || instances[0].State != corecontract.ProcessWaitingReadiness || manager.Status().PID == 0 {
		t.Fatalf("startup did not remain in PREPARING/readiness barrier: instances=%+v status=%+v", instances, manager.Status())
	}

	var response *http.Response
	select {
	case response = <-responseCh:
	case err := <-errorCh:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("queue wait timeout did not return")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("queue timeout status = %d", response.StatusCode)
	}

	waitLeaseStateAPI(t, sched, preparingID, scheduler.Cancelled)
	waitForAPI(t, func() bool { return manager.Status().PID == 0 })
	waitForAPI(t, func() bool {
		_, statErr := os.Stat(filepath.Join(barrier, "cancelled"))
		return statErr == nil
	})
	instance := manager.ProcessInstances()[0]
	if instance.StartupDiagnostic != corecontract.StartupDiagnosticStartupCancelled || !instance.SignalRequestedByLARM || instance.WaitError != "signaled_exit" {
		t.Fatalf("startup cancellation diagnostic = %+v", instance)
	}
	snapshot, err := registry.Get(requestID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.IsAmbiguous() || snapshot.ErrorClass != corecontract.ErrorBackend {
		t.Fatalf("queue timeout inference state = %+v", snapshot)
	}
	if sched.ActiveInferenceCount() != 0 || len(sched.Snapshot().Pending) != 0 || sched.Snapshot().Active != nil {
		t.Fatalf("scheduler retained resources after timeout: %+v", sched.Snapshot())
	}

	t.Setenv("LARM_API_STARTUP_MODE", "")
	next, err := sched.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitLeaseStateAPI(t, sched, next.ID, scheduler.Active)
	if manager.Status().ActiveModel != "model-1" || manager.Status().PID == 0 {
		t.Fatalf("scheduler could not process a subsequent request: %+v", manager.Status())
	}
	if _, err := sched.Release(next.ID); err != nil {
		t.Fatal(err)
	}
}
