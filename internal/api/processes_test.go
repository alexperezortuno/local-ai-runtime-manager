package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"local-ai-runtime/internal/corecontract"
)

func TestProcessEndpointsAreReadOnlyAndMetadataOnly(t *testing.T) {
	manager, sched := apiTestScheduler(t)
	server := httptest.NewServer(NewHandler(manager, sched))
	defer server.Close()
	lease, err := sched.Request("model-1")
	if err != nil {
		t.Fatal(err)
	}
	waitActiveAPI(t, sched, lease.ID)
	response, err := server.Client().Get(server.URL + "/api/v1/processes")
	if err != nil {
		t.Fatal(err)
	}
	var processes []corecontract.ProcessInstance
	if err := json.NewDecoder(response.Body).Decode(&processes); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || len(processes) != 1 || processes[0].ProcessInstanceID == "" || processes[0].OwnershipStatus != "OWNED_VERIFIED" {
		t.Fatalf("process listing: status=%d processes=%+v", response.StatusCode, processes)
	}
	response, err = server.Client().Get(server.URL + "/api/v1/processes/" + processes[0].ProcessInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("process detail status = %d", response.StatusCode)
	}
	if _, err := sched.Release(lease.ID); err != nil {
		t.Fatal(err)
	}
}
