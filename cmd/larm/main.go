package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"local-ai-runtime/internal/api"
	"local-ai-runtime/internal/config"
	runtime "local-ai-runtime/internal/runtime"
)

func main() {
	defaultConfig := os.Getenv("LARM_CONFIG")
	if defaultConfig == "" {
		defaultConfig = "configs/models.yaml"
	}
	configPath := flag.String("config", defaultConfig, "path to LARM YAML config")
	flag.Parse()
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	manager := runtime.NewManager(cfg)
	server := &http.Server{
		Addr:              net.JoinHostPort(cfg.Server.Host, strconv.Itoa(cfg.Server.Port)),
		Handler:           api.NewHandler(manager),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		log.Printf("LARM listening on %s", server.Addr)
		serveErr <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serveErr:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server: %v", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := manager.Shutdown(shutdownCtx); err != nil {
			log.Printf("runtime shutdown: %v", err)
		}
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown: %v", err)
		}
	}
}
