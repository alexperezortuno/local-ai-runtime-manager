package config

import (
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Server    ServerConfig           `yaml:"server"`
	Runtime   RuntimeConfig          `yaml:"runtime"`
	Scheduler SchedulerConfig        `yaml:"scheduler"`
	Capacity  CapacityConfig         `yaml:"capacity"`
	Proxy     ProxyConfig            `yaml:"proxy"`
	Recovery  RecoveryConfig         `yaml:"recovery"`
	Admin     AdminConfig            `yaml:"admin"`
	Models    map[string]ModelConfig `yaml:"models"`
}

// CapacityConfig controls read-only capacity analysis. Admission is deliberately
// disabled by default and is not wired into Scheduler in this release.
type CapacityConfig struct {
	ShadowMode          bool            `yaml:"shadow_mode"`
	SystemMarginPercent float64         `yaml:"system_margin_percent"`
	GPUMarginPercent    float64         `yaml:"gpu_margin_percent"`
	Admission           AdmissionConfig `yaml:"admission"`
}

type AdmissionConfig struct {
	Enabled              bool   `yaml:"enabled"`
	OnInsufficient       string `yaml:"on_insufficient"`
	OnUnknown            string `yaml:"on_unknown"`
	WaitTimeout          string `yaml:"wait_timeout"`
	ReevaluationInterval string `yaml:"reevaluation_interval"`
	MaxSnapshotAge       string `yaml:"max_snapshot_age"`
}

type ProxyConfig struct {
	QueueWaitTimeout            string `yaml:"queue_wait_timeout"`
	InferenceTimeout            string `yaml:"inference_timeout"`
	ConnectTimeout              string `yaml:"backend_connect_timeout"`
	StreamIdleTimeout           string `yaml:"stream_idle_timeout"`
	InferenceMaxTerminalRecords int    `yaml:"inference_max_terminal_records"`
}

type SchedulerConfig struct {
	MaxActiveModels  int    `yaml:"max_active_models"`
	MaxQueueSize     int    `yaml:"max_queue_size"`
	LeaseTTL         string `yaml:"lease_ttl"`
	Heartbeat        string `yaml:"heartbeat_interval"`
	MaxLease         string `yaml:"max_lease_duration"`
	IdempotencyStore string `yaml:"idempotency_store"`
}

type RecoveryConfig struct {
	JournalPath string `yaml:"journal_path"`
	Durability  string `yaml:"durability"`
	MaxRecords  int    `yaml:"max_records"`
}

// AdminConfig is deliberately disabled unless enabled is explicitly set.
// Private signing keys are not a LARM configuration value; the external
// administrative client owns them.
type AdminConfig struct {
	Enabled       bool   `yaml:"enabled"`
	SocketPath    string `yaml:"socket_path"`
	PublicKeyPath string `yaml:"public_key_path"`
	AllowedUID    *int   `yaml:"allowed_uid"`
	Audience      string `yaml:"audience"`
	CapabilityTTL string `yaml:"capability_ttl"`
}

type ServerConfig struct {
	Host string    `yaml:"host"`
	Port int       `yaml:"port"`
	LAN  LANConfig `yaml:"lan"`
}

type LANConfig struct {
	Enabled bool            `yaml:"enabled"`
	Host    string          `yaml:"host"`
	Port    int             `yaml:"port"`
	Auth    LANAuthConfig   `yaml:"auth"`
	Access  LANAccessConfig `yaml:"access"`
	TLS     LANTLSConfig    `yaml:"tls"`
}

type LANAuthConfig struct {
	Type       string `yaml:"type"`
	APIKeysEnv string `yaml:"api_keys_env"`
}

type LANAccessConfig struct {
	Allow []string `yaml:"allow"`
}

type LANTLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type RuntimeConfig struct {
	Backend         string `yaml:"backend"`
	Binary          string `yaml:"binary"`
	StartupTimeout  string `yaml:"startup_timeout"`
	ShutdownTimeout string `yaml:"shutdown_timeout"`
}

type ModelConfig struct {
	Alias            string   `yaml:"alias"`
	Path             string   `yaml:"path"`
	Port             int      `yaml:"port"`
	Host             string   `yaml:"host"`
	Context          int      `yaml:"context"`
	Batch            int      `yaml:"batch"`
	UBatch           int      `yaml:"ubatch"`
	GPULayers        string   `yaml:"gpu_layers"`
	CacheTypeK       string   `yaml:"cache_type_k"`
	CacheTypeV       string   `yaml:"cache_type_v"`
	Parallel         int      `yaml:"parallel"`
	Temperature      *float64 `yaml:"temperature"`
	TopP             *float64 `yaml:"top_p"`
	TopK             *int     `yaml:"top_k"`
	PresencePenalty  *float64 `yaml:"presence_penalty"`
	FrequencyPenalty *float64 `yaml:"frequency_penalty"`
	NPredict         *int     `yaml:"n_predict"`
	RepeatPenalty    *float64 `yaml:"repeat_penalty"`
	FlashAttn        *string  `yaml:"flash_attn"`
	Device           *string  `yaml:"device"`
	NGPULayers       *string  `yaml:"n_gpu_layers"`
	SplitMode        *string  `yaml:"split_mode"`
	Metrics          *bool    `yaml:"metrics"`
	Fit              *bool    `yaml:"fit"`
	CachePrompt      *bool    `yaml:"cache_prompt"`
	NoNMProj         *bool    `yaml:"no_nmproj"`
	LogFile          string   `yaml:"log_file"`
	Reasoning        string   `yaml:"reasoning"`
	ExtraArgs        []string `yaml:"extra_args"`
	KeepAlive        bool     `yaml:"keep_alive"`
	TimeAlive        string   `yaml:"time_alive"`
}

func (m ModelConfig) TimeAliveDuration() time.Duration {
	d, _ := time.ParseDuration(m.TimeAlive)
	return d
}

var modelIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func (c *Config) Validate() error {
	if c.Capacity.SystemMarginPercent == 0 {
		c.Capacity.SystemMarginPercent = 10
	}
	if c.Capacity.GPUMarginPercent == 0 {
		c.Capacity.GPUMarginPercent = 10
	}
	if c.Capacity.SystemMarginPercent < 0 || c.Capacity.SystemMarginPercent >= 100 {
		return fmt.Errorf("capacity.system_margin_percent must be between 0 and 100")
	}
	if c.Capacity.GPUMarginPercent < 0 || c.Capacity.GPUMarginPercent >= 100 {
		return fmt.Errorf("capacity.gpu_margin_percent must be between 0 and 100")
	}
	if c.Capacity.Admission.OnInsufficient == "" {
		c.Capacity.Admission.OnInsufficient = "wait"
	}
	if c.Capacity.Admission.OnUnknown == "" {
		c.Capacity.Admission.OnUnknown = "allow"
	}
	if c.Capacity.Admission.OnInsufficient != "wait" && c.Capacity.Admission.OnInsufficient != "reject" {
		return fmt.Errorf("capacity.admission.on_insufficient must be wait or reject")
	}
	if c.Capacity.Admission.OnUnknown != "allow" && c.Capacity.Admission.OnUnknown != "wait" && c.Capacity.Admission.OnUnknown != "reject" {
		return fmt.Errorf("capacity.admission.on_unknown must be allow, wait or reject")
	}
	if c.Capacity.Admission.WaitTimeout == "" {
		c.Capacity.Admission.WaitTimeout = "5m"
	}
	if c.Capacity.Admission.ReevaluationInterval == "" {
		c.Capacity.Admission.ReevaluationInterval = "2s"
	}
	if c.Capacity.Admission.MaxSnapshotAge == "" {
		c.Capacity.Admission.MaxSnapshotAge = "6s"
	}
	for name, value := range map[string]string{
		"wait_timeout":          c.Capacity.Admission.WaitTimeout,
		"reevaluation_interval": c.Capacity.Admission.ReevaluationInterval,
		"max_snapshot_age":      c.Capacity.Admission.MaxSnapshotAge,
	} {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return fmt.Errorf("capacity.admission.%s must be a positive duration", name)
		}
	}
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
	if c.Proxy.InferenceMaxTerminalRecords == 0 {
		c.Proxy.InferenceMaxTerminalRecords = 1000
	}
	if c.Proxy.InferenceMaxTerminalRecords < 1 {
		return fmt.Errorf("proxy.inference_max_terminal_records must be positive")
	}
	if c.Recovery.JournalPath == "" {
		c.Recovery.JournalPath = "larm-recovery.json"
	}
	if c.Recovery.Durability == "" {
		c.Recovery.Durability = "sync"
	}
	if c.Recovery.Durability != "sync" {
		return fmt.Errorf("recovery.durability must be sync")
	}
	if c.Recovery.MaxRecords == 0 {
		c.Recovery.MaxRecords = 10000
	}
	if c.Recovery.MaxRecords < 1 {
		return fmt.Errorf("recovery.max_records must be positive")
	}
	if c.Admin.Enabled {
		if c.Admin.SocketPath == "" {
			c.Admin.SocketPath = filepath.Join(filepath.Dir(c.Recovery.JournalPath), "admin.sock")
		}
		if !filepath.IsAbs(c.Admin.SocketPath) {
			return fmt.Errorf("admin.socket_path must be absolute when admin is enabled")
		}
		if c.Admin.PublicKeyPath == "" {
			return fmt.Errorf("admin.public_key_path is required when admin is enabled")
		}
		if c.Admin.AllowedUID == nil || *c.Admin.AllowedUID < 0 {
			return fmt.Errorf("admin.allowed_uid is required and must be non-negative when admin is enabled")
		}
		if c.Admin.Audience == "" {
			c.Admin.Audience = "larm-admin"
		}
		if c.Admin.CapabilityTTL == "" {
			c.Admin.CapabilityTTL = "5m"
		}
		ttl, err := time.ParseDuration(c.Admin.CapabilityTTL)
		if err != nil || ttl <= 0 || ttl > 15*time.Minute {
			return fmt.Errorf("admin.capability_ttl must be positive and at most 15m")
		}
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
	if c.Scheduler.MaxActiveModels < 1 {
		return fmt.Errorf("scheduler.max_active_models must be positive")
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
	if err := c.validateLAN(); err != nil {
		return err
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
		if model.Host != "" {
			ip := net.ParseIP(model.Host)
			if ip == nil || ip.IsUnspecified() {
				return fmt.Errorf("models.%s.host must be a valid, non-unspecified IP address", id)
			}
		}
		if previous, ok := ports[model.Port]; ok {
			return fmt.Errorf("models %q and %q use duplicate port %d", previous, id, model.Port)
		}
		ports[model.Port] = id
		if model.Port == c.Server.Port {
			return fmt.Errorf("models.%s.port conflicts with server.port", id)
		}
		if c.Server.LAN.Enabled && model.Port == c.Server.LAN.Port {
			return fmt.Errorf("models.%s.port conflicts with server.lan.port", id)
		}
		if model.Context <= 0 || model.Batch <= 0 || model.UBatch <= 0 || model.UBatch > model.Batch || model.Parallel <= 0 {
			return fmt.Errorf("models.%s context, batch, ubatch and parallel must be positive, with ubatch <= batch", id)
		}
		if model.Temperature != nil && (math.IsNaN(*model.Temperature) || math.IsInf(*model.Temperature, 0) || *model.Temperature < 0) {
			return fmt.Errorf("models.%s.temperature must be a finite non-negative number", id)
		}
		if model.TopP != nil && (math.IsNaN(*model.TopP) || math.IsInf(*model.TopP, 0) || *model.TopP <= 0 || *model.TopP > 1) {
			return fmt.Errorf("models.%s.top_p must be greater than 0 and at most 1", id)
		}
		if model.TopK != nil && *model.TopK < 0 {
			return fmt.Errorf("models.%s.top_k must be non-negative", id)
		}
		if model.PresencePenalty != nil && !finite(*model.PresencePenalty) {
			return fmt.Errorf("models.%s.presence_penalty must be finite", id)
		}
		if model.FrequencyPenalty != nil && !finite(*model.FrequencyPenalty) {
			return fmt.Errorf("models.%s.frequency_penalty must be finite", id)
		}
		if model.NPredict != nil && *model.NPredict < -1 {
			return fmt.Errorf("models.%s.n_predict must be -1 or non-negative", id)
		}
		if model.RepeatPenalty != nil && (!finite(*model.RepeatPenalty) || *model.RepeatPenalty < 0) {
			return fmt.Errorf("models.%s.repeat_penalty must be a finite non-negative number", id)
		}
		if model.FlashAttn != nil && !oneOf(*model.FlashAttn, "on", "off", "auto") {
			return fmt.Errorf("models.%s.flash_attn must be on, off or auto", id)
		}
		if model.Device != nil {
			if err := validateDevice(*model.Device); err != nil {
				return fmt.Errorf("models.%s.device: %w", id, err)
			}
		}
		if model.GPULayers != "" && model.NGPULayers != nil && model.GPULayers != *model.NGPULayers {
			return fmt.Errorf("models.%s.gpu_layers and models.%s.n_gpu_layers conflict", id, id)
		}
		gpuLayers := model.GPULayers
		if gpuLayers == "" && model.NGPULayers != nil {
			gpuLayers = *model.NGPULayers
		}
		if gpuLayers == "" {
			return fmt.Errorf("models.%s.gpu_layers or models.%s.n_gpu_layers is required", id, id)
		}
		if gpuLayers != "all" {
			layers, err := strconv.Atoi(gpuLayers)
			if err != nil || layers < 0 {
				return fmt.Errorf("models.%s.gpu_layers/n_gpu_layers must be a non-negative integer or all", id)
			}
		}
		if model.SplitMode != nil && !oneOf(*model.SplitMode, "none", "layer", "row", "tensor") {
			return fmt.Errorf("models.%s.split_mode must be none, layer, row or tensor", id)
		}
		if err := validateExtraArgs(id, model); err != nil {
			return err
		}
		if model.TimeAlive == "" {
			model.TimeAlive = "0s"
			c.Models[id] = model
		}
		duration, err := time.ParseDuration(model.TimeAlive)
		if err != nil || duration < 0 {
			return fmt.Errorf("models.%s.time_alive must be a non-negative duration", id)
		}
	}
	return nil
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func validateDevice(value string) error {
	if value == "" {
		return fmt.Errorf("must not be empty")
	}
	if value == "none" {
		return nil
	}
	for _, device := range strings.Split(value, ",") {
		if device == "" || strings.TrimSpace(device) != device || strings.ContainsAny(device, "\t\n\r ") {
			return fmt.Errorf("must be none or a comma-separated list of non-empty device names")
		}
	}
	return nil
}

func validateExtraArgs(id string, model ModelConfig) error {
	flags := map[string]string{}
	add := func(parameter string, names ...string) {
		for _, name := range names {
			flags[name] = parameter
		}
	}
	add("path", "-m", "--model")
	add("host", "--host")
	add("port", "--port")
	add("context", "-c", "--ctx-size")
	add("batch", "-b", "--batch-size")
	add("ubatch", "-ub", "--ubatch-size")
	add("gpu_layers/n_gpu_layers", "-ngl", "--gpu-layers", "--n-gpu-layers")
	add("cache_type_k", "--cache-type-k")
	add("cache_type_v", "--cache-type-v")
	add("parallel", "--parallel")
	add("temperature", "--temp", "--temperature")
	add("top_p", "--top-p")
	add("top_k", "--top-k")
	add("presence_penalty", "--presence-penalty")
	add("frequency_penalty", "--frequency-penalty")
	add("n_predict", "-n", "--predict", "--n-predict")
	add("repeat_penalty", "--repeat-penalty")
	add("flash_attn", "-fa", "--flash-attn")
	add("device", "-dev", "--device")
	add("split_mode", "-sm", "--split-mode")
	add("metrics", "--metrics")
	add("fit", "--fit")
	add("cache_prompt", "--cache-prompt", "--no-cache-prompt")
	add("no_nmproj", "--mmproj-auto", "--no-mmproj", "--no-mmproj-auto")
	add("log_file", "--log-file")
	add("alias", "--alias")
	add("reasoning", "--reasoning-format")

	for _, raw := range model.ExtraArgs {
		flag := strings.TrimSpace(raw)
		if index := strings.IndexByte(flag, '='); index >= 0 {
			flag = flag[:index]
		}
		if parameter, ok := flags[flag]; ok {
			return fmt.Errorf("models.%s.%s duplicates typed flag %q in extra_args", id, parameter, flag)
		}
	}
	return nil
}

func (m ModelConfig) HasExtendedLlamaOptions() bool {
	return m.PresencePenalty != nil ||
		m.FrequencyPenalty != nil ||
		m.NPredict != nil ||
		m.RepeatPenalty != nil ||
		m.FlashAttn != nil ||
		m.Device != nil ||
		m.NGPULayers != nil ||
		m.SplitMode != nil ||
		m.Metrics != nil ||
		m.Fit != nil ||
		m.CachePrompt != nil ||
		m.NoNMProj != nil
}

func (m ModelConfig) EffectiveGPULayers() string {
	if m.GPULayers != "" {
		return m.GPULayers
	}
	if m.NGPULayers != nil {
		return *m.NGPULayers
	}
	return ""
}

func (c *Config) ValidateBackendOptions() error {
	needsInspection := false
	for _, model := range c.Models {
		if model.HasExtendedLlamaOptions() {
			needsInspection = true
			break
		}
	}
	if !needsInspection {
		return nil
	}
	output, err := exec.Command(c.Runtime.Binary, "--help").CombinedOutput()
	if err != nil {
		for id, model := range c.Models {
			if requirements := model.llamaOptionRequirements(); len(requirements) > 0 {
				return fmt.Errorf("models.%s.%s could not be checked because runtime.binary --help failed", id, requirements[0].parameter)
			}
		}
		return fmt.Errorf("runtime.binary --help failed while checking configured llama.cpp options")
	}
	help := string(output)
	for id, model := range c.Models {
		for _, option := range model.llamaOptionRequirements() {
			if !strings.Contains(help, option.flag) {
				return fmt.Errorf("models.%s.%s is not supported by the configured llama.cpp backend (missing %s)", id, option.parameter, option.flag)
			}
		}
	}
	return nil
}

type llamaOptionRequirement struct {
	parameter string
	flag      string
}

func (m ModelConfig) llamaOptionRequirements() []llamaOptionRequirement {
	requirements := make([]llamaOptionRequirement, 0, 12)
	if m.PresencePenalty != nil {
		requirements = append(requirements, llamaOptionRequirement{"presence_penalty", "--presence-penalty"})
	}
	if m.FrequencyPenalty != nil {
		requirements = append(requirements, llamaOptionRequirement{"frequency_penalty", "--frequency-penalty"})
	}
	if m.NPredict != nil {
		requirements = append(requirements, llamaOptionRequirement{"n_predict", "--n-predict"})
	}
	if m.RepeatPenalty != nil {
		requirements = append(requirements, llamaOptionRequirement{"repeat_penalty", "--repeat-penalty"})
	}
	if m.FlashAttn != nil {
		requirements = append(requirements, llamaOptionRequirement{"flash_attn", "--flash-attn"})
	}
	if m.Device != nil {
		requirements = append(requirements, llamaOptionRequirement{"device", "--device"})
	}
	if m.NGPULayers != nil {
		requirements = append(requirements, llamaOptionRequirement{"n_gpu_layers", "--n-gpu-layers"})
	}
	if m.SplitMode != nil {
		requirements = append(requirements, llamaOptionRequirement{"split_mode", "--split-mode"})
	}
	if m.Metrics != nil {
		requirements = append(requirements, llamaOptionRequirement{"metrics", "--metrics"})
	}
	if m.Fit != nil {
		requirements = append(requirements, llamaOptionRequirement{"fit", "--fit"})
	}
	if m.CachePrompt != nil {
		requirements = append(requirements, llamaOptionRequirement{"cache_prompt", "--cache-prompt"})
	}
	if m.NoNMProj != nil {
		requirements = append(requirements, llamaOptionRequirement{"no_nmproj", "--no-mmproj"})
	}
	return requirements
}

func (c *Config) validateLAN() error {
	lan := c.Server.LAN
	if !lan.Enabled {
		return nil
	}
	ip := net.ParseIP(lan.Host)
	if ip == nil || ip.IsLoopback() || ip.IsUnspecified() {
		return fmt.Errorf("server.lan.host must be a non-loopback, non-unspecified IP address")
	}
	if lan.Port < 1 || lan.Port > 65535 {
		return fmt.Errorf("server.lan.port must be between 1 and 65535")
	}
	if lan.Port == c.Server.Port {
		return fmt.Errorf("server.lan.port conflicts with server.port")
	}
	if lan.Auth.Type != "bearer" {
		return fmt.Errorf("server.lan.auth.type must be bearer")
	}
	if lan.Auth.APIKeysEnv == "" {
		return fmt.Errorf("server.lan.auth.api_keys_env is required when LAN is enabled")
	}
	if strings.TrimSpace(os.Getenv(lan.Auth.APIKeysEnv)) == "" {
		return fmt.Errorf("server.lan.auth environment variable %q is not set", lan.Auth.APIKeysEnv)
	}
	if len(lan.Access.Allow) == 0 {
		return fmt.Errorf("server.lan.access.allow must not be empty when LAN is enabled")
	}
	for _, cidr := range lan.Access.Allow {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("server.lan.access.allow contains invalid CIDR %q", cidr)
		}
	}
	if lan.TLS.CertFile == "" || lan.TLS.KeyFile == "" {
		return fmt.Errorf("server.lan.tls cert_file and key_file are required when LAN is enabled")
	}
	if !filepath.IsAbs(lan.TLS.CertFile) || !filepath.IsAbs(lan.TLS.KeyFile) {
		return fmt.Errorf("server.lan.tls cert_file and key_file must be absolute paths")
	}
	for name, path := range map[string]string{"cert_file": lan.TLS.CertFile, "key_file": lan.TLS.KeyFile} {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("server.lan.tls.%s: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("server.lan.tls.%s must be a regular file", name)
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

func (a AdmissionConfig) WaitDuration() time.Duration {
	d, _ := time.ParseDuration(a.WaitTimeout)
	return d
}

func (a AdmissionConfig) ReevaluationDuration() time.Duration {
	d, _ := time.ParseDuration(a.ReevaluationInterval)
	return d
}

func (a AdmissionConfig) SnapshotAgeDuration() time.Duration {
	d, _ := time.ParseDuration(a.MaxSnapshotAge)
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
