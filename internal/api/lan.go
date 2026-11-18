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

func NewLANHandler(cfg config.LANConfig) (http.Handler, error) {
	keys, err := NewLANAPIKeys(cfg.Auth.APIKeysEnv)
	if err != nil {
		return nil, err
	}
	return NewLANHandlerWithKeys(cfg, keys), nil
}

func NewLANHandlerWithKeys(cfg config.LANConfig, keys *LANAPIKeys) http.Handler {
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
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/lan/health" || r.Method != http.MethodGet {
			writeLANError(w, http.StatusNotFound, "not found")
			return
		}
		mux.ServeHTTP(w, r)
	})
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
