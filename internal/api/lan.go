package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strings"

	"local-ai-runtime/internal/config"
)

type LANAPIKeys struct {
	hashes [][sha256.Size]byte
}

func NewLANAPIKeys(envName string) (*LANAPIKeys, error) {
	keys := &LANAPIKeys{}
	for _, value := range strings.Split(os.Getenv(envName), ",") {
		value = strings.TrimSpace(value)
		if value != "" {
			keys.hashes = append(keys.hashes, sha256.Sum256([]byte(value)))
		}
	}
	if len(keys.hashes) == 0 {
		return nil, os.ErrNotExist
	}
	return keys, nil
}

func NewLANAPIKeysFromValues(values ...string) *LANAPIKeys {
	keys := &LANAPIKeys{}
	for _, value := range values {
		for _, key := range strings.Split(value, ",") {
			key = strings.TrimSpace(key)
			if key != "" {
				keys.hashes = append(keys.hashes, sha256.Sum256([]byte(key)))
			}
		}
	}
	return keys
}

func (k *LANAPIKeys) authorized(header string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	provided := sha256.Sum256([]byte(strings.TrimSpace(strings.TrimPrefix(header, prefix))))
	valid := 0
	for _, expected := range k.hashes {
		valid |= subtle.ConstantTimeCompare(provided[:], expected[:])
	}
	return valid == 1
}

// NewLANHandler builds the LAN listener handler. The inference routes need
// the local proxy owner, so the handler is created without them: the LAN
// listener then exposes only the health route.
func NewLANHandler(cfg config.LANConfig) (http.Handler, error) {
	keys, err := NewLANAPIKeys(cfg.Auth.APIKeysEnv)
	if err != nil {
		return nil, err
	}
	return NewLANHandlerWithKeys(cfg, keys), nil
}

// NewLANHandlerWithProxy builds the LAN listener handler on top of the very
// same proxy owner the local listener uses.
//
// The LAN requests reuse the local Scheduler, the Runtime Manager, the
// Inference Registry and the backend client of the given handler. There is no
// second scheduler, no second model registry and no second process
// management path: a LAN request is the same execution as a local request.
func NewLANHandlerWithProxy(cfg config.LANConfig, proxy *Handler) (http.Handler, error) {
	keys, err := NewLANAPIKeys(cfg.Auth.APIKeysEnv)
	if err != nil {
		return nil, err
	}
	return NewLANHandlerWithProxyAndKeys(cfg, proxy, keys), nil
}

// NewLANHandlerWithKeys builds the LAN listener handler without the inference
// routes. It keeps the LARM-008.1 contract: only the health route exists.
func NewLANHandlerWithKeys(cfg config.LANConfig, keys *LANAPIKeys) http.Handler {
	return newLANHandler(cfg, keys, nil)
}

// NewLANHandlerWithProxyAndKeys builds the LAN listener handler with the
// inference routes bound to the local proxy owner.
func NewLANHandlerWithProxyAndKeys(cfg config.LANConfig, proxy *Handler, keys *LANAPIKeys) http.Handler {
	return newLANHandler(cfg, keys, proxy)
}

// newLANHandler is the single LAN router. The route table is explicit: the
// health route, and the OpenAI-compatible inference routes when the
// local proxy owner is available. Every other path is denied.
func newLANHandler(cfg config.LANConfig, keys *LANAPIKeys, proxy *Handler) http.Handler {
	allowed := make([]*net.IPNet, 0, len(cfg.Access.Allow))
	for _, cidr := range cfg.Access.Allow {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		allowed = append(allowed, network)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/lan/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	// The inference routes are the local proxy routes, reused verbatim. They
	// keep the whole execution contract: model resolution, FIFO Scheduler,
	// automatic leases, queue wait, Capacity Admission, Recovery, Execution
	// Gate, proxy to llama-server and lease release.
	//
	// The LAN client never owns a lease: leases are only granted on the local
	// listener. A client-supplied X-LARM-Lease-ID is dropped so the LAN
	// request always takes the automatic lease path and the automatic release
	// path, exactly like a local request without an explicit lease.
	if proxy != nil {
		mux.HandleFunc("GET /v1/models", proxy.openAIModels)
		mux.HandleFunc("POST /v1/chat/completions", lanInference(proxy.chatCompletions))
		mux.HandleFunc("POST /v1/responses", lanInference(proxy.responses))
		mux.HandleFunc("POST /v1/embeddings", lanInference(proxy.embeddings))
	}
	// Authentication and CIDR authorization run before any handler that can
	// create a lease or start a model.
	guarded := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !keys.authorized(r.Header.Get("Authorization")) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeLANError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		remote, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil || !lanAddressAllowed(remote, allowed) {
			writeLANError(w, http.StatusForbidden, "forbidden")
			return
		}
		mux.ServeHTTP(w, r)
	})
	// The route table is a strict allowlist. Anything outside it is denied
	// before the mux can match it, so no administrative route, no alternative
	// method, no trailing slash and no equivalent prefix reaches a handler.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/lan/health":
		case proxy != nil && r.Method == http.MethodGet && r.URL.Path == "/v1/models":
		case proxy != nil && r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		case proxy != nil && r.Method == http.MethodPost && r.URL.Path == "/v1/responses":
		case proxy != nil && r.Method == http.MethodPost && r.URL.Path == "/v1/embeddings":
		default:
			writeLANError(w, http.StatusNotFound, "not found")
			return
		}
		guarded.ServeHTTP(w, r)
	})
}

// lanInference adapts a local proxy route for the LAN listener.
//
// The LAN client owns no lease and no request identity: leases are only
// granted on the local listener and request identities are only observable on
// the local listener. A client-supplied X-LARM-Lease-ID and X-LARM-Request-ID
// are dropped so the LAN request always takes the automatic lease path and
// the automatic release path of the shared Scheduler, and it can never
// collide with a local request identity.
//
// The X-LARM-Execution-ID header is left untouched: it carries the execution
// identity of the caller and it is validated by the shared proxy.
func lanInference(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Header.Del("X-LARM-Lease-ID")
		r.Header.Del("X-LARM-Request-ID")
		next(w, r)
	}
}

func lanAddressAllowed(address string, allowed []*net.IPNet) bool {
	ip := net.ParseIP(address)
	if ip == nil {
		return false
	}
	for _, network := range allowed {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func writeLANError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
