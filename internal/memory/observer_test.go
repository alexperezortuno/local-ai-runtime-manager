package memory

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

type fixtureProvider struct {
	mu       sync.Mutex
	process  ProcessMemory
	block    bool
	started  chan struct{}
	finished chan struct{}
}

func (p *fixtureProvider) System(context.Context) SystemMemory {
	now := time.Now().UTC()
	total, available := uint64(16<<30), uint64(8<<30)
	return SystemMemory{Architecture: "fixture", TotalBytes: &total, AvailableBytes: &available, MeasuredAt: &now, Status: Available, Source: "fixture"}
}

func (p *fixtureProvider) Process(ctx context.Context, ref ProcessRef) ProcessMemory {
	if p.block {
		close(p.started)
		<-ctx.Done()
		close(p.finished)
	}
	result := p.process
	result.ModelID, result.PID, result.ProcessInstanceID = ref.ModelID, ref.PID, ref.ProcessInstanceID
	return result
}

func (*fixtureProvider) GPUs(context.Context) GPUResult {
	return GPUResult{Status: Absent, Source: "fixture:none"}
}

func TestObserverPreservesIdentityAndSerializesUnknownAsNull(t *testing.T) {
	provider := &fixtureProvider{process: ProcessMemory{Status: Available, Source: "fixture", GPUStatus: Unavailable}}
	observer := NewObserver(provider, func() []ProcessRef {
		return []ProcessRef{{ModelID: "model", PID: 42, ProcessInstanceID: "instance-2", StartIdentity: "start-2"}}
	}, time.Hour)
	observer.refresh(context.Background())
	snapshot := observer.Snapshot()
	if len(snapshot.Processes) != 1 || snapshot.Processes[0].ProcessInstanceID != "instance-2" {
		t.Fatalf("identity was not preserved: %+v", snapshot.Processes)
	}
	encoded, err := json.Marshal(snapshot.Processes[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || !contains(string(encoded), `"rss_bytes":null`) {
		t.Fatalf("unknown metric was not explicit: %s", encoded)
	}
}

func TestObserverMarksOldSnapshotStale(t *testing.T) {
	provider := &fixtureProvider{}
	observer := NewObserver(provider, nil, time.Millisecond)
	old := time.Now().UTC().Add(-time.Second)
	observer.snapshot = Snapshot{MeasuredAt: &old, Status: Available, System: SystemMemory{Status: Available}}
	if got := observer.Snapshot().Status; got != Stale {
		t.Fatalf("status = %s, want %s", got, Stale)
	}
}

func TestObserverStopsCancelledCollection(t *testing.T) {
	provider := &fixtureProvider{block: true, started: make(chan struct{}), finished: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	observer := NewObserver(provider, func() []ProcessRef { return []ProcessRef{{PID: 1}} }, time.Hour)
	observer.Start(ctx)
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("collection did not start")
	}
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := observer.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func contains(value, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
