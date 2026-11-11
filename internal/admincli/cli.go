// Package admincli implements the administrative CLI boundary. Mutating model
// operations use the private Unix socket; HTTP remains metadata-only.
package admincli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"local-ai-runtime/internal/adminauth"
	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/recovery"
)

const defaultURL = "http://127.0.0.1:8090"

var sensitiveKeyParts = []string{
	"prompt", "response", "token", "secret", "password", "credential", "authorization", "api_key", "apikey", "body",
}

// Run executes an administrative command and returns a process exit code.
// Every server request made by this package is GET-only and metadata-only.
func Run(args []string, stdout, stderr io.Writer) int {
	return run(args, stdout, stderr, false)
}

// RunReadOnly implements the operational, non-privileged top-level commands.
// It shares the same HTTP client as the administrative read-only commands.
func RunReadOnly(args []string, stdout, stderr io.Writer) int {
	return run(args, stdout, stderr, true)
}

func run(args []string, stdout, stderr io.Writer, readOnly bool) int {
	flags := flag.NewFlagSet("larm admin", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		if readOnly {
			_, _ = fmt.Fprintln(stderr, "usage: larm [-config PATH] [-url URL] [-timeout DURATION] <models|status|queue|doctor>")
			return
		}
		_, _ = fmt.Fprintln(stderr, "usage: larm admin [-url URL] [-socket PATH] [-private-key PATH] [-capability-file PATH] [-out PATH] [-timeout DURATION] <models|status|queue|incidents|incident|plan|processes|auth|model|recovery>")
	}
	endpoint := defaultURL
	if value := strings.TrimSpace(os.Getenv("LARM_ADMIN_URL")); value != "" {
		endpoint = value
	}
	baseURL := flags.String("url", endpoint, "LARM HTTP URL")
	socket := flags.String("socket", os.Getenv("LARM_ADMIN_SOCKET"), "protected LARM admin Unix socket")
	privateKey := flags.String("private-key", os.Getenv("LARM_ADMIN_PRIVATE_KEY"), "private capability key file (never sent to LARM)")
	capabilityFile := flags.String("capability-file", os.Getenv("LARM_ADMIN_CAPABILITY_FILE"), "private capability file")
	output := flags.String("out", "", "private output file for an issued capability")
	configPath := flags.String("config", os.Getenv("LARM_CONFIG"), "path to LARM YAML config (doctor)")
	ttl := flags.Duration("capability-ttl", recovery.DefaultCapabilityTTL, "capability lifetime")
	timeout := flags.Duration("timeout", 10*time.Second, "HTTP request timeout")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *timeout <= 0 {
		_, _ = fmt.Fprintln(stderr, "larm admin: timeout must be positive")
		return 2
	}
	client, err := newClient(*baseURL, *timeout)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "larm admin: %v\n", err)
		return 2
	}
	command := flags.Args()
	if len(command) == 0 {
		flags.Usage()
		return 2
	}

	var value any
	switch command[0] {
	case "models":
		if len(command) != 1 {
			return usageError(stderr, "models does not accept arguments")
		}
		value, err = client.get("/api/v1/models")
	case "status":
		if len(command) != 1 {
			return usageError(stderr, "status does not accept arguments")
		}
		value, err = client.status()
	case "queue":
		if len(command) != 1 {
			return usageError(stderr, "queue does not accept arguments")
		}
		value, err = client.get("/api/v1/queue")
	case "doctor":
		if !readOnly {
			return usageError(stderr, "unknown administrative command \"doctor\"")
		}
		if len(command) != 1 {
			return usageError(stderr, "doctor does not accept arguments")
		}
		return doctor(*configPath, client, stdout, stderr)
	case "incidents":
		if len(command) != 1 {
			return usageError(stderr, "incidents does not accept arguments")
		}
		value, err = client.get("/api/v1/recovery/incidents")
	case "incident":
		if len(command) != 2 || strings.TrimSpace(command[1]) == "" {
			return usageError(stderr, "incident requires an incident-id")
		}
		value, err = client.get("/api/v1/recovery/incidents/" + url.PathEscape(command[1]))
	case "plan":
		if len(command) != 2 || strings.TrimSpace(command[1]) == "" {
			return usageError(stderr, "plan requires an incident-id")
		}
		value, err = client.get("/api/v1/recovery/incidents/" + url.PathEscape(command[1]) + "/plan")
	case "processes":
		if len(command) != 1 {
			return usageError(stderr, "processes does not accept arguments")
		}
		value, err = client.get("/api/v1/processes")
	case "auth":
		if *socket == "" {
			return usageError(stderr, "auth requires -socket or LARM_ADMIN_SOCKET")
		}
		if len(command) < 2 {
			return usageError(stderr, "auth requires status, issue, or inspect")
		}
		switch command[1] {
		case "status":
			if len(command) != 2 {
				return usageError(stderr, "auth status does not accept arguments")
			}
			var identity string
			identity, err = adminauth.Status(*socket)
			value = map[string]string{"identity": identity}
		case "issue":
			if len(command) != 4 || *privateKey == "" || *output == "" || *ttl <= 0 {
				return usageError(stderr, "auth issue requires incident-id, plan-id, -private-key, -out, and a positive -capability-ttl")
			}
			value, err = adminauth.Issue(*socket, *privateKey, *output, command[2], command[3], recovery.RecoveryReconcileAction, *ttl)
		case "issue-model-stop":
			if len(command) != 3 || *privateKey == "" || *output == "" || *ttl <= 0 || strings.TrimSpace(command[2]) == "" {
				return usageError(stderr, "auth issue-model-stop requires model-id, -private-key, -out, and a positive -capability-ttl")
			}
			value, err = adminauth.IssueModelStop(*socket, *privateKey, *output, command[2], *ttl)
		case "inspect":
			if len(command) != 3 {
				return usageError(stderr, "auth inspect requires a capability file path")
			}
			value, err = adminauth.Inspect(*socket, command[2])
		default:
			return usageError(stderr, fmt.Sprintf("unknown auth command %q", command[1]))
		}
	case "model":
		if *socket == "" {
			return usageError(stderr, "model operations require -socket or LARM_ADMIN_SOCKET")
		}
		if len(command) != 3 || command[1] != "stop" || strings.TrimSpace(command[2]) == "" || strings.TrimSpace(*capabilityFile) == "" {
			return usageError(stderr, "model stop requires model-id, -socket, and -capability-file")
		}
		value, err = adminauth.StopModel(*socket, *capabilityFile, command[2])
	case "recovery":
		if *socket == "" {
			return usageError(stderr, "recovery requires -socket or LARM_ADMIN_SOCKET")
		}
		if len(command) != 2 || command[1] != "execute" || strings.TrimSpace(*capabilityFile) == "" {
			return usageError(stderr, "recovery execute requires -socket and -capability-file")
		}
		value, err = adminauth.Execute(*socket, *capabilityFile)
	default:
		return usageError(stderr, fmt.Sprintf("unknown administrative command %q", command[0]))
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "larm admin: %v\n", err)
		return 1
	}
	if readOnly {
		switch command[0] {
		case "models":
			if err := renderModels(stdout, value); err != nil {
				_, _ = fmt.Fprintf(stderr, "larm: render models: %v\n", err)
				return 1
			}
		case "status":
			if err := renderStatus(stdout, value); err != nil {
				_, _ = fmt.Fprintf(stderr, "larm: render status: %v\n", err)
				return 1
			}
		case "queue":
			if err := renderQueue(stdout, value); err != nil {
				_, _ = fmt.Fprintf(stderr, "larm: render queue: %v\n", err)
				return 1
			}
		}
		return 0
	}
	if err := writeJSON(stdout, value); err != nil {
		_, _ = fmt.Fprintf(stderr, "larm admin: write output: %v\n", err)
		return 1
	}
	return 0
}

func renderModels(w io.Writer, value any) error {
	models, ok := value.([]any)
	if !ok {
		return errors.New("LARM returned an unexpected models response")
	}
	fmt.Fprintln(w, "MODEL\tSTATE\tRESIDENCY\tPRELOAD\tPID\tPROCESS_INSTANCE_ID\tLEASES\tPROTECTED\tCAPACITY\tRSS_BYTES\tPRIVATE_BYTES\tMEMORY_STATUS")
	for _, raw := range models {
		model, ok := raw.(map[string]any)
		if !ok {
			return errors.New("LARM returned an invalid model entry")
		}
		id := stringValue(model["id"])
		state := stringValue(model["state"])
		residentState := stringValue(model["residency"])
		if residentState != "" {
			state = residentState
		}
		preload := stringValue(model["preload_state"])
		if preload == "" {
			preload = "-"
		}
		residency := ""
		if boolValue(model["keep_alive"]) {
			residency = "keep_alive"
		} else if duration := stringValue(model["time_alive"]); duration != "" && duration != "0s" {
			residency = duration
		}
		if residency == "" {
			residency = "on-demand"
		}
		pid := stringValue(model["pid"])
		if pid == "" || pid == "0" {
			pid = "-"
		}
		instance := stringValue(model["process_instance_id"])
		if instance == "" {
			instance = "-"
		}
		protected := "no"
		if boolValue(model["protected_inference"]) {
			protected = "yes"
		}
		capacity := "free"
		if boolValue(model["capacity_occupied"]) {
			capacity = "occupied"
		}
		rss, private, memoryStatus := "-", "-", "-"
		if observed, ok := model["memory"].(map[string]any); ok {
			rss, private, memoryStatus = byteValue(observed["rss_bytes"]), byteValue(observed["private_bytes"]), stringValue(observed["status"])
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", id, state, residency, preload, pid, instance, stringValue(model["active_leases"]), protected, capacity, rss, private, memoryStatus)
	}
	return nil
}

func renderStatus(w io.Writer, value any) error {
	status, ok := value.(map[string]any)
	if !ok {
		return errors.New("LARM returned an unexpected status response")
	}
	root := status
	if nested, ok := status["status"].(map[string]any); ok {
		root = nested
	}
	scheduler, _ := root["scheduler"].(map[string]any)
	models, _ := status["models"].([]any)
	resident := stringValue(scheduler["capacity_used"])
	if resident == "" {
		resident = strconv.Itoa(len(residentModels(models)))
	}
	recovery := "NORMAL"
	if recoveryStatus, ok := status["recovery"].(map[string]any); ok && stringValue(recoveryStatus["state"]) != "" {
		recovery = stringValue(recoveryStatus["state"])
	} else if boolValue(root["recovery_required"]) {
		recovery = "RECOVERY_REQUIRED"
	}
	fmt.Fprintf(w, "SERVICE\t%s\n", stringValue(root["state"]))
	fmt.Fprintf(w, "BACKEND\t%s\n", stringValue(root["runtime"]))
	fmt.Fprintf(w, "MODELS\t%d\n", len(models))
	fmt.Fprintf(w, "RESIDENT\t%s\n", resident)
	fmt.Fprintf(w, "CAPACITY\t%s/%s\n", resident, stringValue(scheduler["capacity_limit"]))
	fmt.Fprintf(w, "QUEUED\t%s\n", stringValue(root["queued_requests"]))
	fmt.Fprintf(w, "RECOVERY\t%s\n", recovery)
	if observed, ok := root["memory"].(map[string]any); ok {
		system, _ := observed["system"].(map[string]any)
		fmt.Fprintf(w, "RAM_TOTAL_BYTES\t%s\n", byteValue(system["total_bytes"]))
		fmt.Fprintf(w, "RAM_AVAILABLE_BYTES\t%s\n", byteValue(system["available_bytes"]))
		fmt.Fprintf(w, "SWAP_USED_BYTES\t%s\n", byteValue(system["swap_used_bytes"]))
		fmt.Fprintf(w, "MEMORY_STATUS\t%s\n", stringValue(observed["status"]))
		fmt.Fprintf(w, "MEMORY_MEASURED_AT\t%s\n", stringValue(observed["measured_at"]))
		if gpus, ok := observed["gpus"].([]any); ok && len(gpus) > 0 {
			gpu, _ := gpus[0].(map[string]any)
			fmt.Fprintf(w, "GPU\t%s\n", stringValue(gpu["name"]))
			fmt.Fprintf(w, "GPU_FREE_BYTES\t%s\n", byteValue(gpu["free_bytes"]))
		} else {
			fmt.Fprintln(w, "GPU\t-")
			fmt.Fprintln(w, "GPU_FREE_BYTES\t-")
		}
	}
	return nil
}

func renderQueue(w io.Writer, value any) error {
	queue, ok := value.(map[string]any)
	if !ok {
		return errors.New("LARM returned an unexpected queue response")
	}
	fmt.Fprintln(w, "POSITION\tMODEL\tSTATE\tLEASE_ID\tWAIT")
	if lease, ok := queue["active"].(map[string]any); ok {
		fmt.Fprintf(w, "-\t%s\t%s\t%s\t-\n", stringValue(lease["model"]), stringValue(lease["state"]), stringValue(lease["lease_id"]))
	}
	if lease, ok := queue["preparing"].(map[string]any); ok {
		fmt.Fprintf(w, "-\t%s\t%s\t%s\t%s\n", stringValue(lease["model"]), stringValue(lease["state"]), stringValue(lease["lease_id"]), waitValue(lease["created_at"]))
	}
	pending, _ := queue["pending"].([]any)
	for i, raw := range pending {
		lease, ok := raw.(map[string]any)
		if !ok {
			return errors.New("LARM returned an invalid queue entry")
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\n", i+1, stringValue(lease["model"]), stringValue(lease["state"]), stringValue(lease["lease_id"]), waitValue(lease["created_at"]))
	}
	return nil
}

func residentModels(models []any) []any {
	result := make([]any, 0)
	for _, raw := range models {
		if model, ok := raw.(map[string]any); ok && boolValue(model["capacity_occupied"]) {
			result = append(result, raw)
		}
	}
	return result
}

func stringValue(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case float64:
		return strconv.FormatInt(int64(value), 10)
	case json.Number:
		return value.String()
	case bool:
		return strconv.FormatBool(value)
	default:
		return ""
	}
}

func byteValue(value any) string {
	if value == nil {
		return "-"
	}
	return stringValue(value)
}

func boolValue(value any) bool {
	result, _ := value.(bool)
	return result
}

func waitValue(value any) string {
	created, err := time.Parse(time.RFC3339Nano, stringValue(value))
	if err != nil {
		return "-"
	}
	wait := time.Since(created)
	if wait < 0 {
		wait = 0
	}
	return wait.Round(time.Second).String()
}

type doctorCheck struct {
	name   string
	result string
	detail string
}

func doctor(configPath string, client *client, stdout, stderr io.Writer) int {
	if configPath == "" {
		configPath = "configs/models.yaml"
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(stdout, "CHECK\tRESULT\tDETAIL\nCONFIG\tFAIL\tconfiguration is invalid: %v\n", err)
		fmt.Fprintln(stdout, "RUNTIME\tNOT_CHECKED\tconfiguration must be valid before runtime checks")
		return 1
	}
	checks := []doctorCheck{{"CONFIG", "PASS", "configuration loaded and validated"}}
	if info, statErr := os.Stat(cfg.Runtime.Binary); statErr == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
		checks = append(checks, doctorCheck{"LLAMA_SERVER", "PASS", "executable is available"})
	} else {
		checks = append(checks, doctorCheck{"LLAMA_SERVER", "FAIL", "configured executable is unavailable"})
	}
	modelsOK := true
	for id, model := range cfg.Models {
		if info, statErr := os.Stat(model.Path); statErr != nil || !info.Mode().IsRegular() {
			modelsOK = false
			_ = id
		}
	}
	if modelsOK {
		checks = append(checks, doctorCheck{"GGUF", "PASS", "configured model files exist"})
	} else {
		checks = append(checks, doctorCheck{"GGUF", "FAIL", "one or more configured model files are unavailable"})
	}
	checks = append(checks, checkModelPorts(cfg)...)
	_, healthErr := client.get("/health")
	checks = append(checks, checkServerPort(cfg, healthErr))
	checks = append(checks, checkDirectories(cfg)...)
	checks = append(checks, checkObservability(client)...)
	if cfg.Scheduler.MaxActiveModels > 0 {
		checks = append(checks, doctorCheck{"CAPACITY", "PASS", fmt.Sprintf("max_active_models=%d", cfg.Scheduler.MaxActiveModels)})
	} else {
		checks = append(checks, doctorCheck{"CAPACITY", "FAIL", "capacity must be positive"})
	}
	if healthErr != nil {
		checks = append(checks, doctorCheck{"SERVICE", "WARN", "service is stopped or unavailable; this is not treated as corruption"})
		checks = append(checks, doctorCheck{"API", "WARN", "local API connectivity not available"})
	} else {
		checks = append(checks, doctorCheck{"SERVICE", "PASS", "service health endpoint responded"})
		checks = append(checks, doctorCheck{"API", "PASS", "local API connectivity verified"})
	}
	fmt.Fprintln(stdout, "CHECK\tRESULT\tDETAIL")
	fail := false
	for _, check := range checks {
		fmt.Fprintf(stdout, "%s\t%s\t%s\n", check.name, check.result, check.detail)
		fail = fail || check.result == "FAIL"
	}
	_ = stderr
	if fail {
		return 1
	}
	return 0
}

func checkObservability(client *client) []doctorCheck {
	value, err := client.get("/api/v1/status")
	if err != nil {
		return []doctorCheck{
			{"MEMORY_PROVIDER", "WARN", "status snapshot unavailable"},
			{"PROCESS_MEMORY", "WARN", "process metrics could not be checked"},
			{"GPU_PROVIDER", "PASS", "GPU provider not checked while service is unavailable"},
		}
	}
	root, _ := value.(map[string]any)
	memoryValue, _ := root["memory"].(map[string]any)
	system, _ := memoryValue["system"].(map[string]any)
	systemStatus := stringValue(system["status"])
	checks := []doctorCheck{{"MEMORY_PROVIDER", "PASS", "system memory provider is " + systemStatus}}
	if systemStatus == "UNAVAILABLE" || systemStatus == "PARTIAL" || systemStatus == "STALE" || systemStatus == "" {
		checks[0].result, checks[0].detail = "WARN", "system memory metrics are unavailable or stale"
	}
	processResult := doctorCheck{"PROCESS_MEMORY", "PASS", "managed process metrics are available or no process is active"}
	if processes, ok := memoryValue["processes"].([]any); ok {
		for _, raw := range processes {
			process, _ := raw.(map[string]any)
			status := stringValue(process["status"])
			if status == "UNAVAILABLE" || status == "STALE" || status == "" {
				processResult.result, processResult.detail = "WARN", "one or more managed process metrics are unavailable or stale"
				break
			}
		}
	}
	checks = append(checks, processResult)
	gpuProvider, _ := memoryValue["gpu_provider"].(map[string]any)
	gpuStatus := stringValue(gpuProvider["status"])
	gpuResult := doctorCheck{"GPU_PROVIDER", "PASS", "GPU provider is " + gpuStatus}
	if gpuStatus == "UNAVAILABLE" || gpuStatus == "PARTIAL" {
		gpuResult.result, gpuResult.detail = "WARN", "optional GPU provider is unavailable or partial"
	}
	if gpuStatus == "" || gpuStatus == "ABSENT" {
		gpuResult.detail = "no NVIDIA GPU provider detected; optional"
	}
	return append(checks, gpuResult)
}

func checkModelPorts(cfg config.Config) []doctorCheck {
	ports := make([]int, 0, len(cfg.Models))
	for _, model := range cfg.Models {
		ports = append(ports, model.Port)
	}
	sort.Ints(ports)
	checks := make([]doctorCheck, 0, len(ports))
	for _, port := range ports {
		listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			checks = append(checks, doctorCheck{"PORT_" + strconv.Itoa(port), "FAIL", "backend port is already in use"})
			continue
		}
		_ = listener.Close()
		checks = append(checks, doctorCheck{"PORT_" + strconv.Itoa(port), "PASS", "backend port is available"})
	}
	return checks
}

func checkServerPort(cfg config.Config, healthErr error) doctorCheck {
	name := "PORT_" + strconv.Itoa(cfg.Server.Port)
	if healthErr == nil {
		return doctorCheck{name, "PASS", "configured API port is serving LARM"}
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(cfg.Server.Host, strconv.Itoa(cfg.Server.Port)))
	if err != nil {
		return doctorCheck{name, "FAIL", "configured API port is occupied and health did not respond"}
	}
	_ = listener.Close()
	return doctorCheck{name, "WARN", "service is stopped; configured API port is available"}
}

func checkDirectories(cfg config.Config) []doctorCheck {
	paths := map[string]string{"RECOVERY_DIR": filepath.Dir(cfg.Recovery.JournalPath)}
	logConfigured := false
	for _, model := range cfg.Models {
		if model.LogFile != "" {
			logConfigured = true
			paths["LOG_DIR"] = filepath.Dir(model.LogFile)
		}
	}
	if !logConfigured {
		return []doctorCheck{{"LOG_DIR", "WARN", "no model log_file configured; backend logs use process stderr"}, checkDirectory(filepath.Dir(cfg.Recovery.JournalPath), "RECOVERY_DIR")}
	}
	checks := make([]doctorCheck, 0, len(paths))
	for name, path := range paths {
		checks = append(checks, checkDirectory(path, name))
	}
	sort.Slice(checks, func(i, j int) bool { return checks[i].name < checks[j].name })
	return checks
}

func checkDirectory(path, name string) doctorCheck {
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return doctorCheck{name, "FAIL", "directory is unavailable"}
	}
	result := "PASS"
	detail := "directory is accessible"
	if info.Mode().Perm()&0200 == 0 {
		result, detail = "WARN", "directory is readable but not writable by mode"
	}
	return doctorCheck{name, result, detail}
}

func usageError(stderr io.Writer, message string) int {
	_, _ = fmt.Fprintf(stderr, "larm admin: %s\n", message)
	return 2
}

type client struct {
	base   *url.URL
	http   *http.Client
	maxLen int64
}

func newClient(raw string, timeout time.Duration) (*client, error) {
	base, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || base.Scheme != "http" && base.Scheme != "https" || base.Host == "" || base.User != nil {
		return nil, errors.New("-url must be an HTTP(S) URL without user information")
	}
	base.Path = strings.TrimRight(base.Path, "/")
	base.RawQuery = ""
	base.Fragment = ""
	return &client{base: base, http: &http.Client{Timeout: timeout}, maxLen: 4 << 20}, nil
}

func (c *client) status() (any, error) {
	paths := []string{
		"/api/v1/status",
		"/api/v1/models",
		"/api/v1/recovery/status",
		"/api/v1/inferences",
		"/api/v1/recovery/leases",
		"/api/v1/processes",
	}
	result := make(map[string]any, len(paths))
	keys := []string{"status", "models", "recovery", "inferences", "leases", "processes"}
	for i, path := range paths {
		value, err := c.get(path)
		if err != nil {
			return nil, err
		}
		result[keys[i]] = value
	}
	return result, nil
}

func (c *client) get(path string) (any, error) {
	requestURL := *c.base
	requestURL.Path = strings.TrimRight(c.base.Path, "/") + path
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build request %s: %w", path, err)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, classifyRequestError(path, err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("LARM HTTP error: GET %s returned HTTP %d", path, response.StatusCode)
	}
	var value any
	decoder := json.NewDecoder(io.LimitReader(response.Body, c.maxLen))
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("GET %s returned invalid JSON: %w", path, err)
	}
	return redact(value), nil
}

func classifyRequestError(path string, err error) error {
	if strings.Contains(err.Error(), "Client.Timeout") {
		return fmt.Errorf("LARM response timeout: GET %s: %w", path, err)
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return fmt.Errorf("LARM connection timeout: GET %s: %w", path, err)
	}
	return fmt.Errorf("LARM unavailable: GET %s: %w", path, err)
}

func writeJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func redact(value any) any {
	switch value := value.(type) {
	case map[string]any:
		clean := make(map[string]any, len(value))
		for key, child := range value {
			lower := strings.ToLower(key)
			remove := false
			for _, part := range sensitiveKeyParts {
				if strings.Contains(lower, part) {
					remove = true
					break
				}
			}
			if !remove {
				clean[key] = redact(child)
			}
		}
		return clean
	case []any:
		clean := make([]any, len(value))
		for i, child := range value {
			clean[i] = redact(child)
		}
		return clean
	default:
		return value
	}
}
