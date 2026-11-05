package inference

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"local-ai-runtime/internal/corecontract"
)

const journalVersion = 1

var (
	ErrJournalCorrupt   = errors.New("inference journal is corrupt or unsupported")
	ErrRecoveryRequired = errors.New("runtime recovery is required")
)

type JournalOptions struct {
	Path       string
	Durability string
	MaxRecords int
}

type journalFile struct {
	Version        int                            `json:"version"`
	LARMInstanceID string                         `json:"larm_instance_id"`
	CleanShutdown  bool                           `json:"clean_shutdown"`
	UpdatedAt      time.Time                      `json:"updated_at"`
	Records        []Snapshot                     `json:"records"`
	Processes      []corecontract.ProcessInstance `json:"processes,omitempty"`
}

type JournalMeta struct {
	Version        int
	LARMInstanceID string
	CleanShutdown  bool
	Path           string
	Durability     string
	UpdatedAt      time.Time
	Error          string
}

// Journal is the Runtime Core's durable metadata store. It is deliberately a
// snapshot journal: replacing one complete file keeps recovery deterministic
// and never requires replaying an untrusted partial event stream.
type Journal struct {
	mu         sync.Mutex
	path       string
	durability string
	max        int
	meta       JournalMeta
	records    []Snapshot
	processes  []corecontract.ProcessInstance
	fault      func() error
}

func OpenJournal(opts JournalOptions) (*Journal, error) {
	if opts.Path == "" {
		return nil, errors.New("inference journal path is required")
	}
	if opts.Durability == "" {
		opts.Durability = "sync"
	}
	if opts.Durability != "sync" {
		return nil, fmt.Errorf("unsupported inference journal durability %q", opts.Durability)
	}
	if opts.MaxRecords <= 0 {
		opts.MaxRecords = 10000
	}
	if err := os.MkdirAll(filepath.Dir(opts.Path), 0700); err != nil {
		return nil, fmt.Errorf("create inference journal directory: %w", err)
	}
	// The journal file itself is always created as 0600. Do not chmod a
	// caller-owned configured directory (which may also contain the YAML).
	j := &Journal{path: opts.Path, durability: opts.Durability, max: opts.MaxRecords}
	data, err := os.ReadFile(opts.Path)
	if errors.Is(err, os.ErrNotExist) {
		id, idErr := corecontract.NewID()
		if idErr != nil {
			return nil, idErr
		}
		j.meta = JournalMeta{Version: journalVersion, LARMInstanceID: id, CleanShutdown: true, Path: opts.Path, Durability: opts.Durability, UpdatedAt: time.Now().UTC()}
		if err := j.writeLocked(nil, nil, true); err != nil {
			return nil, fmt.Errorf("initialize inference journal: %w", err)
		}
		return j, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read inference journal: %w", err)
	}
	if info, statErr := os.Stat(opts.Path); statErr != nil || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("inference journal must be a private 0600 file")
	}
	var file journalFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJournalCorrupt, err)
	}
	if file.Version != journalVersion || file.LARMInstanceID == "" || file.UpdatedAt.IsZero() || file.Records == nil {
		return nil, fmt.Errorf("%w: invalid version, instance or record set", ErrJournalCorrupt)
	}
	if err := validateJournalRecords(file.Records); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJournalCorrupt, err)
	}
	j.records = cloneSnapshots(file.Records)
	j.processes = cloneProcesses(file.Processes)
	if err := validateProcessRecords(j.processes); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJournalCorrupt, err)
	}
	j.meta = JournalMeta{Version: file.Version, LARMInstanceID: file.LARMInstanceID, CleanShutdown: file.CleanShutdown, Path: opts.Path, Durability: opts.Durability, UpdatedAt: file.UpdatedAt}
	return j, nil
}

func (j *Journal) Records() []Snapshot {
	j.mu.Lock()
	defer j.mu.Unlock()
	return cloneSnapshots(j.records)
}

// ProcessInstances returns durable process metadata. It is diagnostic data;
// opening a journal never adopts or signals any process.
func (j *Journal) ProcessInstances() []corecontract.ProcessInstance {
	j.mu.Lock()
	defer j.mu.Unlock()
	return cloneProcesses(j.processes)
}

// PersistProcess commits a process lifecycle update in the same durable
// journal as inference state. A process is never READY until this succeeds.
func (j *Journal) PersistProcess(process corecontract.ProcessInstance) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := corecontract.ValidateProcessInstance(process); err != nil {
		return fmt.Errorf("persist process instance: %w", err)
	}
	updated := cloneProcesses(j.processes)
	found := false
	for i := range updated {
		if updated[i].ProcessInstanceID == process.ProcessInstanceID {
			updated[i] = process
			found = true
			break
		}
	}
	if !found {
		updated = append(updated, process)
	}
	if err := j.writeLocked(j.records, updated, false); err != nil {
		return err
	}
	j.processes = updated
	j.meta.CleanShutdown = false
	j.meta.UpdatedAt = time.Now().UTC()
	return nil
}

func (j *Journal) Meta() JournalMeta {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.meta
}

func (j *Journal) OwnerInstanceID() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.meta.LARMInstanceID
}

func (j *Journal) RecoveryRequired() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.meta.CleanShutdown {
		return true
	}
	for _, record := range j.records {
		if !record.Inference.IsTerminal() || record.RecoveryState == corecontract.RecoveryRequired {
			return true
		}
	}
	return false
}

func (j *Journal) Persist(records []Snapshot, clean bool) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := validateJournalRecords(records); err != nil {
		return fmt.Errorf("persist inference journal: %w", err)
	}
	if j.max > 0 && len(records) > j.max {
		return fmt.Errorf("persist inference journal: record limit %d exceeded", j.max)
	}
	if err := j.writeLocked(records, j.processes, clean); err != nil {
		return err
	}
	j.records = cloneSnapshots(records)
	j.meta.CleanShutdown = clean
	j.meta.UpdatedAt = time.Now().UTC()
	return nil
}

func (j *Journal) writeLocked(records []Snapshot, processes []corecontract.ProcessInstance, clean bool) error {
	if j.fault != nil {
		if err := j.fault(); err != nil {
			return err
		}
	}
	file := journalFile{Version: journalVersion, LARMInstanceID: j.meta.LARMInstanceID, CleanShutdown: clean, UpdatedAt: time.Now().UTC(), Records: cloneSnapshots(records), Processes: cloneProcesses(processes)}
	data, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(j.path)
	temp, err := os.CreateTemp(dir, ".larm-inference-journal-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync inference journal: %w", err)
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, j.path); err != nil {
		return err
	}
	// The file sync makes bytes durable; syncing the directory makes the
	// atomic rename durable on macOS and Linux filesystems that support it.
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync inference journal directory: %w", err)
	}
	return nil
}

func validateJournalRecords(records []Snapshot) error {
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		if _, ok := seen[record.RequestID]; ok {
			return fmt.Errorf("duplicate request_id %q", record.RequestID)
		}
		seen[record.RequestID] = struct{}{}
		if err := corecontract.ValidateInference(record.Inference); err != nil {
			return err
		}
		if record.RecoveryState != "" && record.RecoveryState != corecontract.RecoveryNormal && record.RecoveryState != corecontract.RecoveryRequired && record.RecoveryState != corecontract.RecoveryResolved {
			return fmt.Errorf("unknown recovery state %q", record.RecoveryState)
		}
		for _, evidence := range record.TerminalEvidence {
			if err := corecontract.ValidateEvidence(evidence); err != nil {
				return err
			}
			if evidence.RequestID != "" && evidence.RequestID != record.RequestID {
				return fmt.Errorf("evidence %q references another request", evidence.EvidenceID)
			}
		}
	}
	return nil
}

func validateProcessRecords(records []corecontract.ProcessInstance) error {
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		if _, ok := seen[record.ProcessInstanceID]; ok {
			return fmt.Errorf("duplicate process_instance_id %q", record.ProcessInstanceID)
		}
		seen[record.ProcessInstanceID] = struct{}{}
		if err := corecontract.ValidateProcessInstance(record); err != nil {
			return err
		}
	}
	return nil
}

func cloneSnapshots(in []Snapshot) []Snapshot {
	out := make([]Snapshot, len(in))
	copy(out, in)
	for i := range out {
		out[i].Observations = append([]Observation(nil), in[i].Observations...)
		out[i].TerminalEvidence = append([]corecontract.TerminalEvidence(nil), in[i].TerminalEvidence...)
		out[i].Inference.TerminalEvidenceIDs = append([]string(nil), in[i].Inference.TerminalEvidenceIDs...)
		if in[i].SSE != nil {
			s := *in[i].SSE
			out[i].SSE = &s
		}
	}
	sort.SliceStable(out, func(i, k int) bool { return out[i].CreatedAt.Before(out[k].CreatedAt) })
	return out
}

func cloneProcesses(in []corecontract.ProcessInstance) []corecontract.ProcessInstance {
	out := make([]corecontract.ProcessInstance, len(in))
	copy(out, in)
	for i := range out {
		out[i].InferenceIDs = append([]string(nil), in[i].InferenceIDs...)
		out[i].Evidence = append([]corecontract.TerminalEvidence(nil), in[i].Evidence...)
		out[i].TerminationSignals = append([]string(nil), in[i].TerminationSignals...)
	}
	return out
}
