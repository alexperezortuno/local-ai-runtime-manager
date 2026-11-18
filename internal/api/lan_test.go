package api

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"local-ai-runtime/internal/config"
)

func TestLANHandlerAuthenticationAndRouteAllowlist(t *testing.T) {
	t.Setenv("LARM_TEST_LAN_KEYS", "correct-key,second-key")
	handler, err := NewLANHandler(config.LANConfig{
		Auth:   config.LANAuthConfig{Type: "bearer", APIKeysEnv: "LARM_TEST_LAN_KEYS"},
		Access: config.LANAccessConfig{Allow: []string{"127.0.0.0/8"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, test := range []struct {
		name, authorization, path string
		status                    int
	}{
		{"missing", "", "/api/v1/lan/health", http.StatusUnauthorized},
		{"wrong", "Bearer wrong", "/api/v1/lan/health", http.StatusUnauthorized},
		{"query not accepted", "", "/api/v1/lan/health?api_key=correct-key", http.StatusUnauthorized},
		{"correct", "Bearer correct-key", "/api/v1/lan/health", http.StatusOK},
		{"administrative route", "Bearer correct-key", "/api/v1/status", http.StatusNotFound},
		{"inference route", "Bearer correct-key", "/v1/chat/completions", http.StatusNotFound},
		{"lease route", "Bearer correct-key", "/api/v1/leases", http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, _ := http.NewRequest(http.MethodGet, server.URL+test.path, nil)
			if test.authorization != "" {
				request.Header.Set("Authorization", test.authorization)
			}
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.status {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.status)
			}
		})
	}
}

func TestLANHandlerConcurrentAuthentication(t *testing.T) {
	t.Setenv("LARM_TEST_LAN_KEYS", "correct-key")
	handler, err := NewLANHandler(config.LANConfig{
		Auth:   config.LANAuthConfig{Type: "bearer", APIKeysEnv: "LARM_TEST_LAN_KEYS"},
		Access: config.LANAccessConfig{Allow: []string{"127.0.0.0/8"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/lan/health", nil)
			request.Header.Set("Authorization", "Bearer correct-key")
			response, err := server.Client().Do(request)
			if err == nil {
				_ = response.Body.Close()
			}
		}()
	}
	wait.Wait()
}

func TestLANBindFailureIsDistinct(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	address := listener.Addr().(*net.TCPAddr)
	if _, err := net.Listen("tcp", net.JoinHostPort(address.IP.String(), strconv.Itoa(address.Port))); err == nil {
		t.Fatal("expected duplicate bind failure")
	}
}
