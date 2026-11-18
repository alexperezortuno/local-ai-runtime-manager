package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLANDefaultsDisabledAndOldConfigCompatible(t *testing.T) {
	cfg, err := Load(writeValidConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.LAN.Enabled {
		t.Fatal("LAN must be disabled by default")
	}
}

func TestLANRequiresCredentialsAndSecureTransport(t *testing.T) {
	t.Setenv("LARM_LAN_KEYS", "test-secret")
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	_ = os.WriteFile(certFile, []byte("certificate"), 0600)
	_ = os.WriteFile(keyFile, []byte("key"), 0600)
	base := "server:\n  lan:\n    enabled: true\n    host: 192.168.1.20\n    port: 8443\n    auth:\n      type: bearer\n      api_keys_env: LARM_LAN_KEYS\n    access:\n      allow: [\"192.168.1.0/24\"]\n"
	withoutTLS := base
	withTLS := base + "    tls:\n      cert_file: " + certFile + "\n      key_file: " + keyFile + "\n"
	for name, content := range map[string]string{"without TLS": withoutTLS, "with TLS": withTLS} {
		t.Run(name, func(t *testing.T) {
			path := writeConfigContent(t, content)
			_, err := Load(path)
			if name == "without TLS" && err == nil {
				t.Fatal("expected plaintext LAN configuration to be rejected")
			}
			if name == "with TLS" && err == nil {
				t.Fatal("expected invalid test certificate to be rejected")
			}
		})
	}
}

func TestLANConfigurationValidation(t *testing.T) {
	tests := []struct {
		name, replacement string
	}{
		{"no auth", "    enabled: true\n    host: 192.168.1.20\n    port: 8443"},
		{"no transport", "    enabled: true\n    host: 192.168.1.20\n    port: 8443\n    auth:\n      type: bearer\n      api_keys_env: LARM_LAN_KEYS\n    access:\n      allow: [\"192.168.1.0/24\"]"},
		{"invalid host", "    enabled: true\n    host: invalid\n    port: 8443"},
		{"duplicate port", "    enabled: true\n    host: 192.168.1.20\n    port: 8090"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := Load(writeValidConfig(t, test.replacement))
			if err == nil {
				t.Fatalf("expected failure, got config %+v", cfg.Server.LAN)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error exposed credential material: %v", err)
			}
		})
	}
}

func writeValidConfig(t *testing.T, serverYAML string) string {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "llama-server")
	model := filepath.Join(dir, "model.gguf")
	_ = os.WriteFile(binary, []byte("binary"), 0755)
	_ = os.WriteFile(model, []byte("model"), 0600)
	content := "server:\n  host: 127.0.0.1\n  port: 8090\n" + serverYAML + "runtime:\n  binary: " + binary + "\nmodels:\n  test:\n    path: " + model + "\n    port: 18069\n    context: 4096\n    batch: 512\n    ubatch: 128\n    gpu_layers: all\n    parallel: 1\n"
	return writeConfigContent(t, content)
}

func writeConfigContent(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "models.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLANCredentialEnvironmentMustExist(t *testing.T) {
	t.Setenv("LARM_MISSING_LAN_KEYS", "")
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	_ = os.WriteFile(certFile, []byte("certificate"), 0600)
	_ = os.WriteFile(keyFile, []byte("key"), 0600)
	content := "server:\n  lan:\n    enabled: true\n    host: 192.168.1.20\n    port: 8443\n    auth:\n      type: bearer\n      api_keys_env: LARM_MISSING_LAN_KEYS\n    access:\n      allow: [\"192.168.1.0/24\"]\n    tls:\n      cert_file: " + certFile + "\n      key_file: " + keyFile + "\n"
	if _, err := Load(writeConfigContent(t, content)); err == nil {
		t.Fatal("expected missing credential environment to be rejected")
	}
}
