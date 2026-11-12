package main

import (
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	port := 0
	args := os.Args[1:]
	for i, arg := range args {
		if arg == "--port" && i+1 < len(args) {
			port, _ = strconv.Atoi(args[i+1])
		}
	}
	server := &http.Server{Addr: "127.0.0.1:" + strconv.Itoa(port), Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		http.NotFound(w, r)
	})}
	go func() { _ = server.ListenAndServe() }()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	_ = server.Close()
}
