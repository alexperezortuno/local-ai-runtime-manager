package config

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader([]byte(os.ExpandEnv(string(data)))))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if cfg.Scheduler.IdempotencyStore == "" {
		cfg.Scheduler.IdempotencyStore = path + ".idempotency.json"
	}
	if cfg.Recovery.JournalPath == "" {
		cfg.Recovery.JournalPath = path + ".recovery.json"
	}
	if host := os.Getenv("LARM_HOST"); host != "" {
		cfg.Server.Host = host
	}
	if port := os.Getenv("LARM_PORT"); port != "" {
		cfg.Server.Port, err = strconv.Atoi(port)
		if err != nil {
			return Config{}, fmt.Errorf("LARM_PORT: %w", err)
		}
	}
	if binary := os.Getenv("LARM_BINARY"); binary != "" {
		cfg.Runtime.Binary = binary
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	if err := cfg.ValidateBackendOptions(); err != nil {
		return Config{}, err
	}
	if cfg.Server.LAN.Enabled {
		if _, err := tls.LoadX509KeyPair(cfg.Server.LAN.TLS.CertFile, cfg.Server.LAN.TLS.KeyFile); err != nil {
			return Config{}, fmt.Errorf("server.lan.tls: %w", err)
		}
		if err := validateLANCredentials(cfg.Server.LAN.Auth.APIKeysEnv); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}

func validateLANCredentials(envName string) error {
	value := strings.TrimSpace(os.Getenv(envName))
	if value == "" {
		return fmt.Errorf("server.lan.auth environment variable %q is not set", envName)
	}
	for _, key := range strings.Split(value, ",") {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("server.lan.auth environment variable %q contains an empty API key", envName)
		}
	}
	return nil
}
