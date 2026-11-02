package config

import (
	"fmt"
	"math"
	"net"
	"os"
	"regexp"
	"strconv"
	"time"
)

type Config struct {
	Server    ServerConfig           `yaml:"server"`
	Runtime   RuntimeConfig          `yaml:"runtime"`
	Scheduler SchedulerConfig        `yaml:"scheduler"`
	Proxy     ProxyConfig            `yaml:"proxy"`
	Models    map[string]ModelConfig `yaml:"models"`
}

type ProxyConfig struct {
	QueueWaitTimeout  string `yaml:"queue_wait_timeout"`
	InferenceTimeout  string `yaml:"inference_timeout"`
	ConnectTimeout    string `yaml:"backend_connect_timeout"`
	StreamIdleTimeout string `yaml:"stream_idle_timeout"`
}

type SchedulerConfig struct {
	MaxActiveModels int    `yaml:"max_active_models"`
	MaxQueueSize    int    `yaml:"max_queue_size"`
	LeaseTTL        string `yaml:"lease_ttl"`
	Heartbeat       string `yaml:"heartbeat_interval"`
	MaxLease        string `yaml:"max_lease_duration"`
}

type ServerConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type RuntimeConfig struct {
	Backend         string `yaml:"backend"`
	Binary          string `yaml:"binary"`
	StartupTimeout  string `yaml:"startup_timeout"`
	ShutdownTimeout string `yaml:"shutdown_timeout"`
}

type ModelConfig struct {
	Path        string   `yaml:"path"`
	Port        int      `yaml:"port"`
	Context     int      `yaml:"context"`
	Batch       int      `yaml:"batch"`
	UBatch      int      `yaml:"ubatch"`
	GPULayers   string   `yaml:"gpu_layers"`
	CacheTypeK  string   `yaml:"cache_type_k"`
	CacheTypeV  string   `yaml:"cache_type_v"`
	Parallel    int      `yaml:"parallel"`
	Temperature float64  `yaml:"temperature"`
	TopP        float64  `yaml:"top_p"`
	TopK        int      `yaml:"top_k"`
	Reasoning   string   `yaml:"reasoning"`
	ExtraArgs   []string `yaml:"extra_args"`
}

var modelIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func (c *Config) Validate() error {
	if c.Proxy.QueueWaitTimeout == "" {
		c.Proxy.QueueWaitTimeout = "5m"
	}
	if c.Proxy.InferenceTimeout == "" {
		c.Proxy.InferenceTimeout = "10m"
	}
	if c.Proxy.ConnectTimeout == "" {
		c.Proxy.ConnectTimeout = "5s"
	}
	if c.Proxy.StreamIdleTimeout == "" {
		c.Proxy.StreamIdleTimeout = "2m"
	}
	for name, value := range map[string]string{"queue_wait_timeout": c.Proxy.QueueWaitTimeout, "inference_timeout": c.Proxy.InferenceTimeout, "backend_connect_timeout": c.Proxy.ConnectTimeout, "stream_idle_timeout": c.Proxy.StreamIdleTimeout} {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return fmt.Errorf("proxy.%s must be a positive duration", name)
		}
	}
	if c.Scheduler.MaxActiveModels == 0 {
		c.Scheduler.MaxActiveModels = 1
	}
	if c.Scheduler.MaxActiveModels != 1 {
		return fmt.Errorf("scheduler.max_active_models must be 1")
	}
	if c.Scheduler.MaxQueueSize == 0 {
		c.Scheduler.MaxQueueSize = 100
	}
	if c.Scheduler.MaxQueueSize < 1 {
		return fmt.Errorf("scheduler.max_queue_size must be positive")
	}
	if c.Scheduler.LeaseTTL == "" {
		c.Scheduler.LeaseTTL = "10m"
	}
	if c.Scheduler.Heartbeat == "" {
		c.Scheduler.Heartbeat = "30s"
	}
	if c.Scheduler.MaxLease == "" {
		c.Scheduler.MaxLease = "2h"
	}
	for name, value := range map[string]string{"lease_ttl": c.Scheduler.LeaseTTL, "heartbeat_interval": c.Scheduler.Heartbeat, "max_lease_duration": c.Scheduler.MaxLease} {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return fmt.Errorf("scheduler.%s must be a positive duration", name)
		}
	}
	if heartbeat, _ := time.ParseDuration(c.Scheduler.Heartbeat); heartbeat >= c.Scheduler.LeaseTTLDuration() {
		return fmt.Errorf("scheduler.heartbeat_interval must be less than lease_ttl")
	}
	if c.Scheduler.MaxLeaseDuration() < c.Scheduler.LeaseTTLDuration() {
		return fmt.Errorf("scheduler.max_lease_duration must be at least lease_ttl")
	}
	if c.Server.Host == "" {
		c.Server.Host = "127.0.0.1"
	}
	ip := net.ParseIP(c.Server.Host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("server.host must be a loopback IP address")
	}
	if c.Server.Port == 0 {
		c.Server.Port = 8090
	}
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535")
	}
	if c.Runtime.Backend == "" {
		c.Runtime.Backend = "llamacpp"
	}
	if c.Runtime.Backend != "llamacpp" {
		return fmt.Errorf("unsupported runtime backend %q", c.Runtime.Backend)
	}
	if c.Runtime.StartupTimeout == "" {
		c.Runtime.StartupTimeout = "120s"
	}
	if c.Runtime.ShutdownTimeout == "" {
		c.Runtime.ShutdownTimeout = "15s"
	}
	for name, value := range map[string]string{"startup_timeout": c.Runtime.StartupTimeout, "shutdown_timeout": c.Runtime.ShutdownTimeout} {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return fmt.Errorf("runtime.%s must be a positive duration", name)
		}
	}
	if c.Runtime.Binary == "" {
		return fmt.Errorf("runtime.binary is required")
	}
	info, err := os.Stat(c.Runtime.Binary)
	if err != nil {
		return fmt.Errorf("runtime.binary: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return fmt.Errorf("runtime.binary %q must be an executable file", c.Runtime.Binary)
	}
	if len(c.Models) == 0 {
		return fmt.Errorf("at least one model must be configured")
	}
	ports := make(map[int]string)
	for id, model := range c.Models {
		if !modelIDPattern.MatchString(id) {
			return fmt.Errorf("invalid model ID %q", id)
		}
		if model.Path == "" {
			return fmt.Errorf("models.%s.path is required", id)
		}
		file, err := os.Stat(model.Path)
		if err != nil {
			return fmt.Errorf("models.%s.path: %w", id, err)
		}
		if !file.Mode().IsRegular() {
			return fmt.Errorf("models.%s.path must be a regular file", id)
		}
		if model.Port < 1 || model.Port > 65535 {
			return fmt.Errorf("models.%s.port must be between 1 and 65535", id)
		}
		if previous, ok := ports[model.Port]; ok {
			return fmt.Errorf("models %q and %q use duplicate port %d", previous, id, model.Port)
		}
		ports[model.Port] = id
		if model.Port == c.Server.Port {
			return fmt.Errorf("models.%s.port conflicts with server.port", id)
		}
		if model.Context <= 0 || model.Batch <= 0 || model.UBatch <= 0 || model.UBatch > model.Batch || model.Parallel <= 0 {
			return fmt.Errorf("models.%s context, batch, ubatch and parallel must be positive, with ubatch <= batch", id)
		}
		if math.IsNaN(model.Temperature) || math.IsInf(model.Temperature, 0) || math.IsNaN(model.TopP) || math.IsInf(model.TopP, 0) || model.Temperature < 0 || model.TopP <= 0 || model.TopP > 1 || model.TopK < 0 {
			return fmt.Errorf("models.%s has invalid sampling parameters", id)
		}
		if model.GPULayers != "all" {
			layers, err := strconv.Atoi(model.GPULayers)
			if err != nil || layers < 0 {
				return fmt.Errorf("models.%s.gpu_layers must be a non-negative integer or all", id)
			}
		}
	}
	return nil
}

func (s SchedulerConfig) LeaseTTLDuration() time.Duration {
	d, _ := time.ParseDuration(s.LeaseTTL)
	return d
}

func (s SchedulerConfig) HeartbeatDuration() time.Duration {
	d, _ := time.ParseDuration(s.Heartbeat)
	return d
}

func (s SchedulerConfig) MaxLeaseDuration() time.Duration {
	d, _ := time.ParseDuration(s.MaxLease)
	return d
}

func (p ProxyConfig) QueueWaitDuration() time.Duration {
	d, _ := time.ParseDuration(p.QueueWaitTimeout)
	return d
}

func (p ProxyConfig) InferenceDuration() time.Duration {
	d, _ := time.ParseDuration(p.InferenceTimeout)
	return d
}

func (p ProxyConfig) ConnectDuration() time.Duration {
	d, _ := time.ParseDuration(p.ConnectTimeout)
	return d
}

func (p ProxyConfig) StreamIdleDuration() time.Duration {
	d, _ := time.ParseDuration(p.StreamIdleTimeout)
	return d
}

func (r RuntimeConfig) StartupDuration() time.Duration {
	d, _ := time.ParseDuration(r.StartupTimeout)
	return d
}

func (r RuntimeConfig) ShutdownDuration() time.Duration {
	d, _ := time.ParseDuration(r.ShutdownTimeout)
	return d
}
