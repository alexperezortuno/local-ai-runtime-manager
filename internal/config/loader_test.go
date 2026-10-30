package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadValidYAML(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "llama-server")
	model := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(binary, []byte("binary"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(model, []byte("model"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "models.yaml")
	content := "server:\n  port: 8090\nruntime:\n  binary: " + binary + "\nmodels:\n  test-model:\n    path: " + model + "\n    port: 18069\n    context: 4096\n    batch: 512\n    ubatch: 128\n    gpu_layers: all\n    parallel: 1\n    temperature: 0.2\n    top_p: 0.9\n    top_k: 40\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Host != "127.0.0.1" || cfg.Runtime.StartupDuration().String() != "2m0s" || len(cfg.Models) != 1 {
		t.Fatalf("unexpected loaded config: %+v", cfg)
	}
}

func TestLoadRejectsInvalidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("server:\n  host: 0.0.0.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected invalid configuration error")
	}
}

func TestLoadRejectsMissingBinary(t *testing.T) {
	dir := t.TempDir()
	model := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(model, []byte("model"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "missing.yaml")
	content := strings.Replace(validYAML(model), "binary: /test/llama-server", "binary: /missing/llama-server", 1)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected missing binary error")
	}
}

func validYAML(modelPath string) string {
	return "runtime:\n  binary: /test/llama-server\nmodels:\n  test:\n    path: " + modelPath + "\n    port: 18069\n    context: 4096\n    batch: 512\n    ubatch: 128\n    gpu_layers: all\n    parallel: 1\n    top_p: 0.9\n"
}
