package scheduler

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
)

type validationReconciliationProtector struct{}

func (validationReconciliationProtector) Protected(string, string, string) (bool, string) {
	return false, ""
}

func (validationReconciliationProtector) ReconciliationAllowed(string, string, []string) error {
	return nil
}

func TestReconcileLeasePersistenceFaultsFailClosedAndRetryIdempotently(t *testing.T) {
	for _, tc := range []struct {
		name  string
		point string
	}{
		{name: "before persistence", point: "before_persist"},
		{name: "during persistence", point: "during_persist"},
		{name: "after durable rename before response", point: "after_persist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, leaseID, request := reconciliationFixture(t)
			var calls atomic.Int32
			s.idempotencyFault = func(point string) error {
				if point == tc.point && calls.Add(1) == 1 {
					return errors.New("injected scheduler persistence failure")
				}
				return nil
			}
			if _, err := s.ReconcileLease(request); err == nil {
				t.Fatal("faulted reconciliation unexpectedly succeeded")
			}
			got, err := s.Get(leaseID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != Active {
				t.Fatalf("faulted persistence published memory transition: %+v", got)
			}
			if _, err := s.ReconcileLease(request); err != nil {
				t.Fatalf("retry: %v", err)
			}
			got, err = s.Get(leaseID)
			if err != nil || got.State != Completed {
				t.Fatalf("retry result = %+v, err=%v", got, err)
			}
			if _, err := s.ReconcileLease(request); err != nil {
				t.Fatalf("identical retry: %v", err)
			}
			data, err := os.ReadFile(s.idempotencyPath)
			if err != nil {
				t.Fatal(err)
			}
			var file idempotencyFile
			if err := json.Unmarshal(data, &file); err != nil {
				t.Fatal(err)
			}
			if len(file.Reconciliations) != 1 {
				t.Fatalf("durable reconciliation records = %d, want 1", len(file.Reconciliations))
			}
		})
	}
}

func TestReconcileLeaseConcurrentIdenticalRequestsCommitOnce(t *testing.T) {
	s, leaseID, request := reconciliationFixture(t)
	var calls atomic.Int32
	s.idempotencyFault = func(point string) error {
		if point == "after_persist" {
			calls.Add(1)
		}
		return nil
	}
	results := make([]error, 2)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i] = s.ReconcileLease(request)
		}(i)
	}
	wg.Wait()
	for _, err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Get(leaseID)
	if err != nil || got.State != Completed {
		t.Fatalf("concurrent result = %+v, err=%v", got, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("durable commit hook count = %d, want 1", calls.Load())
	}
}

func reconciliationFixture(t *testing.T) (*Scheduler, string, ReconciliationRequest) {
	t.Helper()
	s := newScheduler(nil, SchedulerConfigForValidation())
	s.idempotencyPath = filepath.Join(t.TempDir(), "idempotency.json")
	s.releaseProtector = validationReconciliationProtector{}
	leaseID := "lease-validation"
	// ReconcileLease validates identities but the fixture does not need a
	// runtime process: runtime evidence is owned and validated upstream.
	now := time.Now().UTC()
	s.leases[leaseID] = &Lease{ID: leaseID, Model: "model-1", State: Active, CreatedAt: now}
	s.activeID = leaseID
	requestID := "request-validation"
	return s, leaseID, ReconciliationRequest{OperationKey: "operation-validation", IncidentID: "incident-validation", PlanID: "plan-validation", EvidenceDigest: "digest-validation", LeaseID: leaseID, ModelID: "model-1", RequestIDs: []string{requestID}}
}

// Kept local to avoid requiring production configuration parsing in a store
// fault test.
func SchedulerConfigForValidation() config.SchedulerConfig {
	return config.SchedulerConfig{MaxQueueSize: 2, LeaseTTL: "1h", Heartbeat: "10m", MaxLease: "2h"}
}
