package config

import (
	"bytes"
	"fmt"
	"os"
	"strconv"

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
	return cfg, nil
}
