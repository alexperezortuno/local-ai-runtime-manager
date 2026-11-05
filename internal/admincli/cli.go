// Package admincli implements the read-only administrative CLI boundary.
// Recovery execution is intentionally not exposed here: the LARM server has
// no verifiable administrative authentication or execution authority yet.
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
	"strings"
	"time"

	"local-ai-runtime/internal/adminauth"
	"local-ai-runtime/internal/recovery"
)

const defaultURL = "http://127.0.0.1:8090"

var sensitiveKeyParts = []string{
	"prompt", "response", "token", "secret", "password", "credential", "authorization", "api_key", "apikey", "body",
}

// Run executes an administrative command and returns a process exit code.
// Every server request made by this package is GET-only and metadata-only.
func Run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("larm admin", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "usage: larm admin [-url URL] [-socket PATH] [-private-key PATH] [-capability-file PATH] [-out PATH] [-timeout DURATION] <models|status|incidents|incident|plan|processes|auth|recovery>")
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
		case "inspect":
			if len(command) != 3 {
				return usageError(stderr, "auth inspect requires a capability file path")
			}
			value, err = adminauth.Inspect(*socket, command[2])
		default:
			return usageError(stderr, fmt.Sprintf("unknown auth command %q", command[1]))
		}
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
	if err := writeJSON(stdout, value); err != nil {
		_, _ = fmt.Fprintf(stderr, "larm admin: write output: %v\n", err)
		return 1
	}
	return 0
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
