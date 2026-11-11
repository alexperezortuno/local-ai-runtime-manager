package memory

import (
	"context"
	"sync"
	"time"
)

const DefaultInterval = 2 * time.Second

type Observer struct {
	provider      Provider
	processSource func() []ProcessRef
	interval      time.Duration
	mu            sync.RWMutex
	snapshot      Snapshot
	cancel        context.CancelFunc
	done          chan struct{}
}

func NewObserver(provider Provider, processSource func() []ProcessRef, interval time.Duration) *Observer {
	if interval <= 0 {
		interval = DefaultInterval
	}
	return &Observer{provider: provider, processSource: processSource, interval: interval, snapshot: Snapshot{Status: Unavailable, Source: "larm-memory-observer", System: SystemMemory{Status: Unavailable, Source: "not_measured"}, GPUProvider: GPUProvider{Status: Unavailable, Source: "not_measured"}}}
}

func (o *Observer) Start(ctx context.Context) {
	if o.provider == nil || o.cancel != nil {
		return
	}
	child, cancel := context.WithCancel(ctx)
	o.cancel, o.done = cancel, make(chan struct{})
	go func() {
		defer close(o.done)
		o.refresh(child)
		ticker := time.NewTicker(o.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				o.refresh(child)
			case <-child.Done():
				return
			}
		}
	}()
}

func (o *Observer) Stop(ctx context.Context) error {
	if o.cancel == nil {
		return nil
	}
	o.cancel()
	select {
	case <-o.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *Observer) refresh(ctx context.Context) {
	started := time.Now().UTC()
	s := Snapshot{MeasuredAt: &started, Status: Available, Source: "larm-memory-observer"}
	s.System = o.provider.System(ctx)
	gpu := o.provider.GPUs(ctx)
	s.GPUs, s.GPUProvider = gpu.GPUs, GPUProvider{Status: gpu.Status, Source: gpu.Source, Error: gpu.Error}
	if s.System.Status == Unavailable || s.System.Status == PermissionDenied {
		s.Status = Partial
	}
	refs := []ProcessRef(nil)
	if o.processSource != nil {
		refs = o.processSource()
	}
	s.Processes = make([]ProcessMemory, 0, len(refs))
	for _, ref := range refs {
		if ctx.Err() != nil {
			return
		}
		process := o.provider.Process(ctx, ref)
		if process.Status != Available {
			s.Status = Partial
		}
		s.Processes = append(s.Processes, process)
	}
	o.mu.Lock()
	o.snapshot = s
	o.mu.Unlock()
}

func (o *Observer) Snapshot() Snapshot {
	o.mu.RLock()
	s := cloneSnapshot(o.snapshot)
	o.mu.RUnlock()
	if s.MeasuredAt == nil {
		return s
	}
	if time.Since(*s.MeasuredAt) > 3*o.interval {
		s.Status = Stale
		s.System.Status = Stale
		for i := range s.GPUs {
			s.GPUs[i].Status = Stale
		}
		for i := range s.Processes {
			s.Processes[i].Status = Stale
		}
	}
	return s
}

func cloneSnapshot(s Snapshot) Snapshot {
	s.GPUs = append([]GPUMemory(nil), s.GPUs...)
	s.Processes = append([]ProcessMemory(nil), s.Processes...)
	return s
}

func NewDefaultObserver(processSource func() []ProcessRef) *Observer {
	return NewObserver(newPlatformProvider(), processSource, DefaultInterval)
}
