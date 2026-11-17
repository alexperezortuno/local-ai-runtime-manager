package recovery_test

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
	"local-ai-runtime/internal/recovery"
	runtime "local-ai-runtime/internal/runtime"
)

type overrideProcessProvider struct {
	manager   *runtime.Manager
	overrides map[string]corecontract.ProcessInstance
}

func (p *overrideProcessProvider) ProcessInstances() []corecontract.ProcessInstance {
	instances := p.manager.ProcessInstances()
	for i := range instances {
		if override, ok := p.overrides[instances[i].ProcessInstanceID]; ok {
			instances[i] = override
		}
	}
	return instances
}

func TestRefreshWithPersistedProcessesHasStableSnapshotOrder(t *testing.T) {
	dir := t.TempDir()
	journal, err := inference.OpenJournal(inference.JournalOptions{Path: filepath.Join(dir, "runtime.json")})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := inference.Open(inference.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}

	ids := make([]string, 3)
	for i := range ids {
		ids[i], err = corecontract.NewID()
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(ids)
	for i := len(ids) - 1; i >= 0; i-- {
		if err := journal.PersistProcess(corecontract.ProcessInstance{
			SchemaVersion:     corecontract.ContractVersion,
			ProcessInstanceID: ids[i],
			OwnerInstanceID:   journal.OwnerInstanceID(),
			LARMInstanceID:    journal.OwnerInstanceID(),
			ModelID:           "model-" + ids[i],
			ConfigFingerprint: "sha256:fixture",
			State:             corecontract.ProcessRecoveryRequired,
			RecoveryState:     corecontract.RecoveryRequired,
			UpdatedAt:         time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	manager := runtime.NewManager(config.Config{}, journal)
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	provider := &overrideProcessProvider{manager: manager, overrides: make(map[string]corecontract.ProcessInstance)}
	service, err := recovery.Open(registry, nil, filepath.Join(dir, "recovery.json"), provider)
	if err != nil {
		t.Fatal(err)
	}

	incidents, err := service.Incidents()
	if err != nil {
		t.Fatal(err)
	}
	var processIncident recovery.Incident
	for _, incident := range incidents {
		if len(incident.ProcessInstanceIDs) > 0 {
			processIncident = incident
			break
		}
	}
	if processIncident.IncidentID == "" {
		t.Fatalf("persisted process recovery incident missing: %+v", incidents)
	}
	first, err := service.Plan(processIncident.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Refresh(); err != nil {
		t.Fatalf("equivalent Recovery.Refresh failed: %v", err)
	}
	second, err := service.Plan(processIncident.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if first.SnapshotVersion != second.SnapshotVersion || first.PlanID != second.PlanID {
		t.Fatalf("equivalent snapshots differ: first=%+v second=%+v", first, second)
	}

	changed := manager.ProcessInstances()[0]
	changed.State = corecontract.ProcessIdentityUnknown
	provider.overrides[changed.ProcessInstanceID] = changed
	if err := service.Refresh(); err != nil {
		t.Fatalf("Recovery.Refresh rejected a real process state change: %v", err)
	}
	third, err := service.Plan(processIncident.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if third.SnapshotVersion == second.SnapshotVersion || third.PlanID == second.PlanID {
		t.Fatalf("real process state change did not update snapshot: second=%+v third=%+v", second, third)
	}
}
