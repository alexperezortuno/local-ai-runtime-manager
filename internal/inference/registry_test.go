package inference

import (
	"sync"
	"testing"

	"local-ai-runtime/internal/corecontract"
)

func newTestRecord(t *testing.T, r *Registry, stream bool) string {
	t.Helper()
	requestID, err := r.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	executionID, _ := corecontract.NewID()
	leaseID, _ := corecontract.NewID()
	if _, err := r.Create(CreateInput{RequestID: requestID, ExecutionID: executionID, ModelID: "model", LeaseID: leaseID, Stream: stream}); err != nil {
		t.Fatal(err)
	}
	if err := r.AdmissionAccepted(requestID); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkProcessUnverified(requestID, 8080); err != nil {
		t.Fatal(err)
	}
	if err := r.StartTransport(requestID); err != nil {
		t.Fatal(err)
	}
	return requestID
}

func TestRequestIDGenerationAndCollision(t *testing.T) {
	r := New(Options{})
	id, err := r.Reserve("")
	if err != nil || !ValidID(id) {
		t.Fatalf("generated request id: %q %v", id, err)
	}
	if _, err := r.Reserve(id); err != ErrRequestIDCollision {
		t.Fatalf("collision error = %v", err)
	}
	if _, err := r.Reserve("not-an-id"); err != ErrInvalidRequestID {
		t.Fatalf("invalid id error = %v", err)
	}
}

func TestJSONCompletionAndLateEvents(t *testing.T) {
	r := New(Options{})
	id := newTestRecord(t, r, false)
	if err := r.Headers(id); err != nil {
		t.Fatal(err)
	}
	if err := r.BodyCompleted(id); err != nil {
		t.Fatal(err)
	}
	if err := r.CompleteJSON(id, 200); err != nil {
		t.Fatal(err)
	}
	s, err := r.Get(id)
	if err != nil || !s.IsTerminal() || s.Semantic != corecontract.SemanticSucceeded {
		t.Fatalf("snapshot = %+v, err=%v", s, err)
	}
	if len(s.TerminalEvidence) != 1 || s.TerminalEvidence[0].Type != corecontract.EvidenceHTTPCompleted {
		t.Fatalf("evidence = %+v", s.TerminalEvidence)
	}
	if err := r.FailHTTP(id, 500); err == nil {
		t.Fatal("late event reopened inference")
	}
}

func TestSSEFragmentedDoneAndTruncated(t *testing.T) {
	var parser SSEParser
	if parser.Feed([]byte("data: [DO")) {
		t.Fatal("fragment completed too early")
	}
	if !parser.Feed([]byte("NE]\r\n\r\n")) {
		t.Fatal("fragmented DONE not detected")
	}

	r := New(Options{})
	id := newTestRecord(t, r, true)
	if err := r.Headers(id); err != nil {
		t.Fatal(err)
	}
	if err := r.SSEHeaders(id); err != nil {
		t.Fatal(err)
	}
	if err := r.SSEChunk(id, true, 1); err != nil {
		t.Fatal(err)
	}
	if err := r.SSEEOF(id, true, 1); err != nil {
		t.Fatal(err)
	}
	s, _ := r.Get(id)
	if !s.IsTerminal() || !s.SSE.DoneDetected || !s.SSE.EOF {
		t.Fatalf("complete SSE = %+v", s)
	}

	id = newTestRecord(t, r, true)
	_ = r.Headers(id)
	_ = r.SSEHeaders(id)
	_ = r.SSEChunk(id, false, 1)
	if err := r.SSEEOF(id, false, 1); err != nil {
		t.Fatal(err)
	}
	s, _ = r.Get(id)
	if s.Semantic != corecontract.SemanticTruncated || s.Semantic == corecontract.SemanticSucceeded || s.SSE.DoneDetected {
		t.Fatalf("truncated SSE = %+v", s)
	}
}

func TestDownstreamDisconnectIsUnknown(t *testing.T) {
	r := New(Options{})
	id := newTestRecord(t, r, true)
	if err := r.MarkDownstreamDisconnected(id); err != nil {
		t.Fatal(err)
	}
	s, _ := r.Get(id)
	if s.Transport != corecontract.TransportUnknown || s.Semantic != corecontract.SemanticUnknown || s.Cancellation != corecontract.CancelUnknown || s.IsTerminal() {
		t.Fatalf("disconnect = %+v", s)
	}
}

func TestConcurrentIndependentRecords(t *testing.T) {
	r := New(Options{})
	const n = 32
	ids := make(chan string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := newTestRecord(t, r, false)
			_ = r.Headers(id)
			_ = r.BodyCompleted(id)
			_ = r.CompleteJSON(id, 200)
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
	}
	if len(r.List()) != n {
		t.Fatalf("records = %d, want %d", len(r.List()), n)
	}
}

func TestTerminalRetentionDoesNotRemoveUnknown(t *testing.T) {
	r := New(Options{MaxTerminalRecords: 1})
	first := newTestRecord(t, r, false)
	_ = r.Headers(first)
	_ = r.BodyCompleted(first)
	_ = r.CompleteJSON(first, 200)
	second := newTestRecord(t, r, false)
	_ = r.Headers(second)
	_ = r.BodyCompleted(second)
	_ = r.CompleteJSON(second, 200)
	if _, err := r.Get(first); err != ErrNotFound {
		t.Fatalf("first terminal retained: %v", err)
	}
	unknown := newTestRecord(t, r, false)
	_ = r.MarkDownstreamDisconnected(unknown)
	if _, err := r.Get(unknown); err != nil {
		t.Fatalf("unknown was retained: %v", err)
	}
}

func TestReleaseProtectionDistinguishesTerminalAndUnknown(t *testing.T) {
	r := New(Options{})
	id := newTestRecord(t, r, false)
	if protected, _ := r.Protected("wrong-lease", id, "model"); !protected {
		t.Fatal("unknown lease was not protected")
	}
	leaseID := mustLease(t, r, id)
	if protected, reason := r.Protected(leaseID, id, "model"); !protected || reason == "" {
		t.Fatalf("active inference was not protected: protected=%v reason=%q", protected, reason)
	}
	if err := r.Headers(id); err != nil {
		t.Fatal(err)
	}
	if err := r.BodyCompleted(id); err != nil {
		t.Fatal(err)
	}
	if err := r.CompleteJSON(id, 200); err != nil {
		t.Fatal(err)
	}
	if protected, reason := r.Protected(leaseID, id, "model"); protected || reason != "" {
		t.Fatalf("terminal inference remained protected: protected=%v reason=%q", protected, reason)
	}
	unknown := newTestRecord(t, r, false)
	if err := r.MarkDownstreamDisconnected(unknown); err != nil {
		t.Fatal(err)
	}
	if protected, reason := r.Protected(mustLease(t, r, unknown), unknown, "model"); !protected || reason == "" {
		t.Fatalf("unknown inference was not protected: protected=%v reason=%q", protected, reason)
	}
}

func TestCancelBeforeUpstreamIsTerminalAndReleasable(t *testing.T) {
	r := New(Options{})
	requestID, err := r.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	executionID, _ := corecontract.NewID()
	leaseID, _ := corecontract.NewID()
	if _, err := r.Create(CreateInput{RequestID: requestID, ExecutionID: executionID, ModelID: "model", LeaseID: leaseID}); err != nil {
		t.Fatal(err)
	}
	if err := r.AdmissionAccepted(requestID); err != nil {
		t.Fatal(err)
	}
	if err := r.CancelBeforeUpstream(requestID); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := r.Get(requestID)
	if !snapshot.IsTerminal() || snapshot.Transport != corecontract.TransportNotStarted || snapshot.Cancellation != corecontract.CancelConfirmed {
		t.Fatalf("pre-upstream cancellation = %+v", snapshot)
	}
	if protected, reason := r.Protected(leaseID, requestID, "model"); protected {
		t.Fatalf("terminal pre-upstream cancellation remained protected: %s", reason)
	}
}

func mustLease(t *testing.T, r *Registry, requestID string) string {
	t.Helper()
	snapshot, err := r.Get(requestID)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot.LeaseID
}
