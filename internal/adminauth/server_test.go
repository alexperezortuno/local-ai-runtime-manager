package adminauth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
	"local-ai-runtime/internal/recovery"
)

func TestPrivateSocketIssueInspectAndShutdown(t *testing.T) {
	service, incidentID, planID, instanceID := authServiceFixture(t)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "public.json")
	privatePath := filepath.Join(dir, "private.json")
	writePrivateJSON(t, keyPath, publicKeyFile{Version: 1, Keys: map[string]string{"key-1": base64.RawStdEncoding.EncodeToString(public)}})
	writePrivateJSON(t, privatePath, privateKeyFile{Version: 1, KeyID: "key-1", Private: base64.RawStdEncoding.EncodeToString(private)})
	uid := os.Geteuid()
	socketDir, err := os.MkdirTemp("/tmp", "larm-a-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(socketDir)
	cfg := config.AdminConfig{Enabled: true, SocketPath: filepath.Join(socketDir, "admin.sock"), PublicKeyPath: keyPath, AllowedUID: &uid, Audience: "larm-admin", CapabilityTTL: "5m"}
	server, err := NewServer(cfg, instanceID, service)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Listen(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	parentInfo, err := os.Stat(filepath.Dir(cfg.SocketPath))
	if err != nil || parentInfo.Mode().Perm() != 0700 {
		t.Fatalf("socket parent = %v, err=%v", parentInfo.Mode().Perm(), err)
	}
	socketInfo, err := os.Stat(cfg.SocketPath)
	if err != nil || socketInfo.Mode().Perm() != 0600 {
		t.Fatalf("socket = %v, err=%v", socketInfo.Mode().Perm(), err)
	}
	if identity, err := Status(cfg.SocketPath); err != nil || identity != "uid:"+itoa(uid) {
		t.Fatalf("status identity = %q, err=%v", identity, err)
	}
	output := filepath.Join(dir, "capability.json")
	metadata, err := Issue(cfg.SocketPath, privatePath, output, incidentID, planID, recovery.RecoveryReconcileAction, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if metadata["capability_id"] == nil || metadata["allowed_operation"] != recovery.RecoveryReconcileAction {
		t.Fatalf("metadata = %#v", metadata)
	}
	if _, err := Inspect(cfg.SocketPath, output); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(cfg.SocketPath); !os.IsNotExist(err) {
		t.Fatalf("socket residue: %v", err)
	}
}

func TestExistingSocketIsNeverOverwritten(t *testing.T) {
	service, _, _, instanceID := authServiceFixture(t)
	dir := t.TempDir()
	public, _, _ := ed25519.GenerateKey(rand.Reader)
	keyPath := filepath.Join(dir, "public.json")
	writePrivateJSON(t, keyPath, publicKeyFile{Version: 1, Keys: map[string]string{"key-1": base64.RawStdEncoding.EncodeToString(public)}})
	uid := os.Geteuid()
	cfg := config.AdminConfig{Enabled: true, SocketPath: filepath.Join(dir, "admin.sock"), PublicKeyPath: keyPath, AllowedUID: &uid}
	if err := os.WriteFile(cfg.SocketPath, []byte("not-a-socket"), 0600); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(cfg, instanceID, service)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Listen(); err == nil {
		t.Fatal("existing socket path was overwritten")
	}
	data, err := os.ReadFile(cfg.SocketPath)
	if err != nil || string(data) != "not-a-socket" {
		t.Fatalf("existing path changed: %q, err=%v", data, err)
	}
}

func TestUnsafeParentAndSocketSymlinkAreRejected(t *testing.T) {
	service, _, _, instanceID := authServiceFixture(t)
	public, _, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "public.json")
	writePrivateJSON(t, keyPath, publicKeyFile{Version: 1, Keys: map[string]string{"key-1": base64.RawStdEncoding.EncodeToString(public)}})
	uid := os.Geteuid()
	unsafe := filepath.Join(dir, "unsafe")
	if err := os.Mkdir(unsafe, 0755); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(config.AdminConfig{Enabled: true, SocketPath: filepath.Join(unsafe, "admin.sock"), PublicKeyPath: keyPath, AllowedUID: &uid}, instanceID, service)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Listen(); err == nil {
		t.Fatal("permissive socket parent was accepted")
	}
	target := filepath.Join(dir, "target")
	path := filepath.Join(dir, "admin.sock")
	if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	server, err = NewServer(config.AdminConfig{Enabled: true, SocketPath: path, PublicKeyPath: keyPath, AllowedUID: &uid}, instanceID, service)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Listen(); err == nil {
		t.Fatal("socket symlink was accepted")
	}
}

func authServiceFixture(t *testing.T) (*recovery.Service, string, string, string) {
	t.Helper()
	journal, err := inference.OpenJournal(inference.JournalOptions{Path: filepath.Join(t.TempDir(), "inference.json")})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := inference.Open(inference.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := registry.Reserve("")
	if err != nil {
		t.Fatal(err)
	}
	executionID, _ := corecontract.NewID()
	leaseID, _ := corecontract.NewID()
	if _, err := registry.Create(inference.CreateInput{RequestID: requestID, ExecutionID: executionID, LeaseID: leaseID, ModelID: "model"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.AdmissionAccepted(requestID); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkUnknown(requestID); err != nil {
		t.Fatal(err)
	}
	service, err := recovery.Open(registry, nil, filepath.Join(t.TempDir(), "operator.json"))
	if err != nil {
		t.Fatal(err)
	}
	incidents, err := service.Incidents()
	if err != nil || len(incidents) == 0 {
		t.Fatalf("incidents = %#v, err=%v", incidents, err)
	}
	var chosen recovery.Incident
	for _, candidate := range incidents {
		if len(candidate.RequestIDs) > 0 {
			chosen = candidate
			break
		}
	}
	if chosen.IncidentID == "" {
		t.Fatalf("no request incident in %#v", incidents)
	}
	plan, err := service.Plan(chosen.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	return service, chosen.IncidentID, plan.PlanID, registry.RecoveryStatus().LARMInstanceID
}

func writePrivateJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	return fmt.Sprintf("%d", value)
}
