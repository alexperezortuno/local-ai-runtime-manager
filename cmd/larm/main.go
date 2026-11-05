//go:build !larmtest

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
	"strings"
	"syscall"
	"time"

	"local-ai-runtime/internal/adminauth"
	"local-ai-runtime/internal/admincli"
	"local-ai-runtime/internal/api"
	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/inference"
	runtime "local-ai-runtime/internal/runtime"
	"local-ai-runtime/internal/scheduler"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "admin" {
		os.Exit(admincli.Run(os.Args[2:], os.Stdout, os.Stderr))
	}
	if index := readOnlyCommandIndex(os.Args[1:]); index >= 0 {
		original := os.Args[1:]
		command := original[index]
		args := append([]string(nil), original[:index]...)
		args = append(args, original[index+1:]...)
		args = append(args, command)
		os.Exit(admincli.RunReadOnly(args, os.Stdout, os.Stderr))
	}
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
	journal, err := inference.OpenJournal(inference.JournalOptions{Path: cfg.Recovery.JournalPath, Durability: cfg.Recovery.Durability, MaxRecords: cfg.Recovery.MaxRecords})
	if err != nil {
		log.Fatalf("open inference recovery journal: %v", err)
	}
	manager := runtime.NewManager(cfg, journal)
	sched, err := scheduler.NewWithIdempotencyStore(manager, cfg.Scheduler, cfg.Scheduler.IdempotencyStore)
	if err != nil {
		log.Fatalf("open lease idempotency store: %v", err)
	}
	handler, registry, recoveryService, err := api.NewHandlerWithJournalAndRecovery(manager, sched, cfg.Proxy, journal)
	if err != nil {
		log.Fatalf("open inference recovery state: %v", err)
	}
	// Recovery and the Scheduler admission gate are installed by the handler
	// construction before any configured residency load is admitted.
	if err := sched.Preload(); err != nil {
		log.Printf("model preloading unavailable: %v", err)
	}
	server := &http.Server{
		Addr:              net.JoinHostPort(cfg.Server.Host, strconv.Itoa(cfg.Server.Port)),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	var adminServer *adminauth.Server
	if cfg.Admin.Enabled {
		adminServer, err = adminauth.NewServer(cfg.Admin, registry.RecoveryStatus().LARMInstanceID, recoveryService)
		if err != nil {
			log.Fatalf("configure administrative authentication: %v", err)
		}
		adminServer.SetModelStopper(sched)
		if err := adminServer.Listen(); err != nil {
			log.Fatalf("listen administrative socket: %v", err)
		}
		defer adminServer.Close()
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
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown: %v", err)
		}
		if adminServer != nil {
			if err := adminServer.Close(); err != nil {
				log.Printf("administrative socket shutdown: %v", err)
			}
		}
		if err := sched.Shutdown(shutdownCtx); err != nil {
			log.Printf("scheduler shutdown: %v", err)
			// Never stop llama-server while an admitted backend request still owns it.
			if err := sched.Shutdown(context.Background()); err != nil {
				log.Printf("scheduler final shutdown: %v", err)
				return
			}
		}
		if err := manager.Shutdown(shutdownCtx); err != nil {
			log.Printf("runtime shutdown: %v", err)
		}
		if err := registry.MarkCleanShutdown(); err != nil {
			log.Printf("persist inference recovery shutdown: %v", err)
		}
	}
}

func readOnlyCommandIndex(args []string) int {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if strings.HasPrefix(arg, "-") {
			if !strings.Contains(arg, "=") && index+1 < len(args) {
				index++
			}
			continue
		}
		if isReadOnlyCommand(arg) {
			return index
		}
		return -1
	}
	return -1
}

func isReadOnlyCommand(command string) bool {
	switch command {
	case "models", "status", "queue", "doctor":
		return true
	default:
		return false
	}
}
