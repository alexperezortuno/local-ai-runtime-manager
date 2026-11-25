package config

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestLANHTTPAndHTTPSConfiguration(t *testing.T) {
	t.Setenv("LARM_LAN_KEYS", "test-secret")
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	base := "  lan:\n    enabled: true\n    host: 192.168.1.20\n    port: 8443\n    auth:\n      type: bearer\n      api_keys_env: LARM_LAN_KEYS\n    access:\n      allow: [\"192.168.1.0/24\"]\n"
	t.Run("HTTP without certificates", func(t *testing.T) {
		cfg, err := Load(writeValidConfig(t, base+"    tls:\n      enabled: false\n"))
		if err != nil || cfg.Server.LAN.TLS.UsesTLS() {
			t.Fatalf("HTTP config = %+v, err=%v", cfg.Server.LAN.TLS, err)
		}
	})
	t.Run("HTTPS with certificates", func(t *testing.T) {
		writeTestCertificate(t, certFile, keyFile)
		content := base + "    tls:\n      enabled: true\n      cert_file: " + certFile + "\n      key_file: " + keyFile + "\n"
		cfg, err := Load(writeValidConfig(t, content))
		if err != nil || !cfg.Server.LAN.TLS.UsesTLS() {
			t.Fatalf("HTTPS config = %+v, err=%v", cfg.Server.LAN.TLS, err)
		}
	})
	t.Run("HTTPS without certificates", func(t *testing.T) {
		if _, err := Load(writeValidConfig(t, base+"    tls:\n      enabled: true\n")); err == nil {
			t.Fatal("expected HTTPS without certificates to fail")
		}
	})
	t.Run("legacy TLS fields imply HTTPS", func(t *testing.T) {
		writeTestCertificate(t, certFile, keyFile)
		content := base + "    tls:\n      cert_file: " + certFile + "\n      key_file: " + keyFile + "\n"
		cfg, err := Load(writeValidConfig(t, content))
		if err != nil || !cfg.Server.LAN.TLS.UsesTLS() {
			t.Fatalf("legacy TLS config = %+v, err=%v", cfg.Server.LAN.TLS, err)
		}
	})
}

func writeTestCertificate(t *testing.T, certFile, keyFile string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestLANConfigurationValidation(t *testing.T) {
	tests := []struct {
		name, replacement string
	}{
		{"no auth", "    enabled: true\n    host: 192.168.1.20\n    port: 8443"},
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
