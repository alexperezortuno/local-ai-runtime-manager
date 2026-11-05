package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"local-ai-runtime/internal/executiongate"
)

func main() {
	defaultDir, err := os.UserConfigDir()
	if err != nil {
		log.Fatal(err)
	}
	stateDir := flag.String("state-dir", filepath.Join(defaultDir, "larm", "session-gate"), "shared coordinator state directory")
	socket := flag.String("socket", filepath.Join(defaultDir, "larm", "session-gate.sock"), "Unix socket for wrapper/plugin requests")
	flag.Parse()

	c, err := executiongate.Open(*stateDir)
	if err != nil {
		log.Fatalf("open coordinator: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(*socket), 0700); err != nil {
		log.Fatalf("create socket directory: %v", err)
	}
	if info, err := os.Lstat(*socket); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			log.Fatalf("refusing to replace non-socket path %s", *socket)
		}
		conn, dialErr := net.DialTimeout("unix", *socket, 150*time.Millisecond)
		if dialErr == nil {
			conn.Close()
			log.Fatalf("coordinator socket already active: %s", *socket)
		}
		if err := os.Remove(*socket); err != nil {
			log.Fatalf("remove stale coordinator socket: %v", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("inspect socket: %v", err)
	}
	listener, err := net.Listen("unix", *socket)
	if err != nil {
		log.Fatalf("listen on coordinator socket: %v", err)
	}
	if err := os.Chmod(*socket, 0600); err != nil {
		log.Fatalf("secure coordinator socket: %v", err)
	}
	server := &http.Server{Handler: executiongate.NewHTTPHandler(c), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("coordinator HTTP server: %v", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("coordinator shutdown: %v", err)
		}
	}
}
