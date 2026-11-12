package capacity

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/memory"
)

func TestCAP0103CapacityVerdicts(t *testing.T) {
	modelPath := writeGGUFFixture(t)
	model := config.ModelConfig{Path: modelPath, Context: 1024, Batch: 128, UBatch: 32, Parallel: 1, GPULayers: "0"}
	available := uint64(1 << 30)
	planner := newFixturePlanner(memory.Snapshot{Status: memory.Available, System: memory.SystemMemory{Architecture: "amd64", AvailableBytes: &available, Status: memory.Available}, GPUProvider: memory.GPUProvider{Status: memory.Absent}}, model)
	if got := planner.Plan(context.Background(), "model").Verdict; got != Fit {
		t.Fatalf("fit verdict = %s", got)
	}

	low := uint64(1)
	planner = newFixturePlanner(memory.Snapshot{Status: memory.Available, System: memory.SystemMemory{Architecture: "amd64", AvailableBytes: &low, Status: memory.Available}}, model)
	if got := planner.Plan(context.Background(), "model").Verdict; got != Insufficient {
		t.Fatalf("insufficient verdict = %s", got)
	}
	unknown := newFixturePlanner(memory.Snapshot{Status: memory.Unavailable, System: memory.SystemMemory{Status: memory.Unavailable}}, model)
	if got := unknown.Plan(context.Background(), "model").Verdict; got != Unknown {
		t.Fatalf("unknown verdict = %s", got)
	}
}

func TestCAP0406SnapshotStates(t *testing.T) {
	modelPath := writeGGUFFixture(t)
	model := config.ModelConfig{Path: modelPath, Context: 128, Batch: 8, UBatch: 4, GPULayers: "0", Parallel: 1}
	available := uint64(1 << 30)
	for _, status := range []memory.Availability{memory.Stale, memory.Unavailable} {
		planner := newFixturePlanner(memory.Snapshot{Status: status, System: memory.SystemMemory{Status: status, AvailableBytes: &available}}, model)
		if got := planner.Plan(context.Background(), "model").Verdict; got != Unknown {
			t.Errorf("snapshot %s verdict = %s", status, got)
		}
	}
	planner := newFixturePlanner(memory.Snapshot{Status: memory.Available, MeasuredAt: timePtr(time.Now().UTC()), Source: "fixture", System: memory.SystemMemory{Status: memory.Available, AvailableBytes: &available, Source: "fixture", MeasuredAt: timePtr(time.Now().UTC())}}, model)
	result := planner.Plan(context.Background(), "model")
	if result.Snapshot.Source != "fixture" || result.Snapshot.System.Source != "fixture" || result.Snapshot.MeasuredAt == nil {
		t.Fatalf("snapshot evidence was not preserved: %+v", result.Snapshot)
	}
}

func TestCAP0709UnifiedMemoryDoesNotNeedGPUProvider(t *testing.T) {
	modelPath := writeGGUFFixture(t)
	model := config.ModelConfig{Path: modelPath, Context: 128, Batch: 8, UBatch: 4, GPULayers: "all", Parallel: 1}
	available := uint64(1 << 30)
	planner := newFixturePlanner(memory.Snapshot{Status: memory.Available, System: memory.SystemMemory{Architecture: "unified", Status: memory.Available, AvailableBytes: &available}, GPUProvider: memory.GPUProvider{Status: memory.Unavailable, Source: "apple-unified-memory"}}, model)
	if got := planner.Plan(context.Background(), "model").Verdict; got != Fit {
		t.Fatalf("unified verdict = %s", got)
	}
}

func TestCAP1012NVIDIAFixtureAndAbsentProvider(t *testing.T) {
	modelPath := writeGGUFFixture(t)
	model := config.ModelConfig{Path: modelPath, Context: 128, Batch: 8, UBatch: 4, GPULayers: "all", Parallel: 1}
	system := uint64(1 << 30)
	free := uint64(1 << 30)
	snapshot := memory.Snapshot{Status: memory.Available, System: memory.SystemMemory{Architecture: "amd64", Status: memory.Available, AvailableBytes: &system}, GPUProvider: memory.GPUProvider{Status: memory.Available, Source: "fixture:nvidia-smi"}, GPUs: []memory.GPUMemory{{Status: memory.Available, FreeBytes: &free, Source: "fixture:nvidia-smi"}}}
	if got := newFixturePlanner(snapshot, model).Plan(context.Background(), "model").Verdict; got != Fit {
		t.Fatalf("NVIDIA fixture verdict = %s", got)
	}
	snapshot.GPUProvider.Status = memory.Absent
	if got := newFixturePlanner(snapshot, model).Plan(context.Background(), "model").Verdict; got != Unknown {
		t.Fatalf("absent provider verdict = %s", got)
	}
}

func TestCAP1318GGUFParametersAndMargins(t *testing.T) {
	path := writeGGUFFixture(t)
	base := config.ModelConfig{Path: path, Context: 128, Batch: 8, UBatch: 4, GPULayers: "0", Parallel: 1}
	available := uint64(1 << 30)
	planner := newFixturePlanner(memory.Snapshot{Status: memory.Available, System: memory.SystemMemory{Status: memory.Available, AvailableBytes: &available}}, base)
	a := planner.Plan(context.Background(), "model")
	base.Context = 4096
	b := newFixturePlanner(memory.Snapshot{Status: memory.Available, System: memory.SystemMemory{Status: memory.Available, AvailableBytes: &available}}, base).Plan(context.Background(), "model")
	if b.Estimate.KVCacheBytes <= a.Estimate.KVCacheBytes || b.Estimate.ContextTokens != 4096 || b.Estimate.BatchTokens != 8 {
		t.Fatalf("context was not reflected: a=%+v b=%+v", a.Estimate, b.Estimate)
	}
	margin := Options{SystemMarginPercent: 50, GPUMarginPercent: 50}
	withMarginPlanner, err := NewPlanner(func() memory.Snapshot {
		return memory.Snapshot{Status: memory.Available, System: memory.SystemMemory{Status: memory.Available, AvailableBytes: &available}}
	}, func(string) (config.ModelConfig, bool) { return base, true }, margin)
	if err != nil {
		t.Fatal(err)
	}
	result := withMarginPlanner.Plan(context.Background(), "model")
	if result.Estimate.SystemMarginPercent != 50 || result.Estimate.RequiredBytes == 0 || result.Estimate.SystemRequiredBytes <= result.Estimate.RawBytes {
		t.Fatalf("margin not applied: %+v", result.Estimate)
	}
	if _, err := NewPlanner(func() memory.Snapshot { return memory.Snapshot{} }, func(string) (config.ModelConfig, bool) { return base, true }, Options{SystemMarginPercent: 100}); err == nil {
		t.Fatal("accepted margin at limit")
	}
}

func TestCAP2224PlannerIsConcurrentAndDoesNotUseRSS(t *testing.T) {
	path := writeGGUFFixture(t)
	available := uint64(1 << 30)
	model := config.ModelConfig{Path: path, Context: 128, Batch: 8, UBatch: 4, GPULayers: "0", Parallel: 1}
	snapshot := memory.Snapshot{Status: memory.Available, System: memory.SystemMemory{Status: memory.Available, AvailableBytes: &available}, Processes: []memory.ProcessMemory{{RSSBytes: uint64Ptr(1 << 60), Status: memory.Available}}}
	planner := newFixturePlanner(snapshot, model)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := planner.Plan(context.Background(), "model").Verdict; got != Fit {
				t.Errorf("concurrent verdict = %s", got)
			}
		}()
	}
	wg.Wait()
}

func newFixturePlanner(snapshot memory.Snapshot, model config.ModelConfig) *Planner {
	planner, err := NewPlanner(func() memory.Snapshot { return snapshot }, func(string) (config.ModelConfig, bool) { return model, true }, Options{})
	if err != nil {
		panic(err)
	}
	return planner
}

func writeGGUFFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.gguf")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	file.Write([]byte("GGUF"))
	binary.Write(file, binary.LittleEndian, uint32(3))
	binary.Write(file, binary.LittleEndian, uint64(0))
	binary.Write(file, binary.LittleEndian, uint64(6))
	writeMeta(file, "llama.block_count", uint32(10), uint64(4))
	writeMeta(file, "llama.embedding_length", uint32(10), uint64(512))
	writeMeta(file, "llama.attention.head_count_kv", uint32(10), uint64(4))
	writeMeta(file, "llama.attention.key_length", uint32(10), uint64(128))
	writeMeta(file, "llama.attention.value_length", uint32(10), uint64(128))
	writeMeta(file, "general.architecture", uint32(8), "llama")
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeMeta(file *os.File, key string, typ uint32, value any) {
	binary.Write(file, binary.LittleEndian, uint64(len(key)))
	file.Write([]byte(key))
	binary.Write(file, binary.LittleEndian, typ)
	switch value := value.(type) {
	case uint64:
		binary.Write(file, binary.LittleEndian, value)
	case string:
		binary.Write(file, binary.LittleEndian, uint64(len(value)))
		file.Write([]byte(value))
	}
}

func uint64Ptr(value uint64) *uint64     { return &value }
func timePtr(value time.Time) *time.Time { return &value }
