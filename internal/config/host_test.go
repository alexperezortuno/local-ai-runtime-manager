package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateModelHostAllowsLoopbackAndExistingAddresses(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "127.0.0.42", "192.169.0.228", ""} {
		t.Run(hostName(host), func(t *testing.T) {
			cfg := validHostConfig(t, host)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("host %q rejected: %v", host, err)
			}
		})
	}
}

func TestValidateModelHostRejectsInvalidAndUnspecifiedAddresses(t *testing.T) {
	for _, host := range []string{"not-an-ip", "127.0.0.999", "0.0.0.0", "::"} {
		t.Run(hostName(host), func(t *testing.T) {
			cfg := validHostConfig(t, host)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "models.model.host") {
				t.Fatalf("host %q error = %v", host, err)
			}
		})
	}
}

func TestValidateServerHostStillRequiresLoopback(t *testing.T) {
	cfg := validHostConfig(t, "127.0.0.1")
	cfg.Server.Host = "192.169.0.228"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "server.host") {
		t.Fatalf("server host error = %v", err)
	}
}

func TestValidateLANHostStillRejectsLoopback(t *testing.T) {
	cfg := validHostConfig(t, "127.0.0.1")
	cfg.Server.LAN.Enabled = true
	cfg.Server.LAN.Host = "127.0.0.1"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "server.lan.host") {
		t.Fatalf("LAN host error = %v", err)
	}
}

func validHostConfig(t *testing.T, host string) Config {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "llama-server")
	model := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(binary, []byte("binary"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(model, []byte("model"), 0600); err != nil {
		t.Fatal(err)
	}
	return Config{
		Runtime: RuntimeConfig{Binary: binary},
		Models: map[string]ModelConfig{
			"model": {
				Path:      model,
				Host:      host,
				Port:      8060,
				Context:   1024,
				Batch:     128,
				UBatch:    32,
				GPULayers: "all",
				Parallel:  1,
			},
		},
	}
}

func hostName(host string) string {
	if host == "" {
		return "default"
	}
	return strings.ReplaceAll(host, ":", "-")
}
