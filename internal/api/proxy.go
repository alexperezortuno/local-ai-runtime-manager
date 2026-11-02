package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"local-ai-runtime/internal/config"
	runtime "local-ai-runtime/internal/runtime"
	"local-ai-runtime/internal/scheduler"
)

const maxInferenceRequest = 16 << 20

func defaultProxyConfig(cfg config.ProxyConfig) config.ProxyConfig {
	if cfg.QueueWaitTimeout == "" {
		cfg.QueueWaitTimeout = "5m"
	}
	if cfg.InferenceTimeout == "" {
		cfg.InferenceTimeout = "10m"
	}
	if cfg.ConnectTimeout == "" {
		cfg.ConnectTimeout = "5s"
	}
	if cfg.StreamIdleTimeout == "" {
		cfg.StreamIdleTimeout = "2m"
	}
	return cfg
}

func newBackendClient(connectTimeout time.Duration) *http.Client {
	return &http.Client{Transport: &http.Transport{
		Proxy:             nil,
		DialContext:       (&net.Dialer{Timeout: connectTimeout}).DialContext,
		DisableKeepAlives: true,
	}}
}

func (h *Handler) openAIModels(w http.ResponseWriter, _ *http.Request) {
	models := h.manager.Models()
	data := make([]map[string]any, 0, len(models))
	ids := make([]string, 0, len(models))
	for id := range models {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		data = append(data, map[string]any{"id": id, "object": "model", "created": 0, "owned_by": "larm"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (h *Handler) chatCompletions(w http.ResponseWriter, r *http.Request) {
	h.serveInference(w, r, "/v1/chat/completions", true)
}

func (h *Handler) embeddings(w http.ResponseWriter, r *http.Request) {
	h.serveInference(w, r, "/v1/embeddings", false)
}

func (h *Handler) serveInference(w http.ResponseWriter, r *http.Request, path string, supportsStream bool) {
	succeeded := false
	defer func() {
		if succeeded {
			h.completed.Add(1)
		} else {
			h.failed.Add(1)
		}
	}()

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxInferenceRequest))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "request body is too large or unreadable")
		return
	}
	var request struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(body, &request); err != nil || request.Model == "" {
		writeOpenAIError(w, http.StatusBadRequest, "request must be valid JSON and include a model")
		return
	}
	model, ok := h.manager.Models()[request.Model]
	if !ok {
		writeOpenAIError(w, http.StatusNotFound, "model not found")
		return
	}

	leaseID := r.Header.Get("X-LARM-Lease-ID")
	automatic := leaseID == ""
	if automatic {
		lease, err := h.scheduler.Request(request.Model)
		if err != nil {
			writeInferenceSchedulerError(w, err)
			return
		}
		leaseID = lease.ID
		defer func() { _, _ = h.scheduler.Release(leaseID) }()
		queueCtx, cancelQueue := context.WithTimeout(r.Context(), h.proxy.QueueWaitDuration())
		err = h.scheduler.WaitActive(queueCtx, leaseID)
		cancelQueue()
		if err != nil {
			if r.Context().Err() == nil {
				if errors.Is(err, context.DeadlineExceeded) {
					writeOpenAIError(w, http.StatusGatewayTimeout, "timed out waiting for model queue")
				} else {
					writeInferenceSchedulerError(w, err)
				}
			}
			return
		}
	} else {
		lease, err := h.scheduler.Get(leaseID)
		if err != nil {
			writeInferenceSchedulerError(w, err)
			return
		}
		if lease.State != scheduler.Active {
			if lease.State == scheduler.Expired {
				writeOpenAIError(w, http.StatusConflict, "lease has expired")
			} else {
				writeOpenAIError(w, http.StatusConflict, "lease is not active")
			}
			return
		}
		if lease.Model != request.Model {
			writeOpenAIError(w, http.StatusConflict, "lease model does not match request model")
			return
		}
	}

	inferCtx, cancelInference := context.WithTimeout(r.Context(), h.proxy.InferenceDuration())
	endInference, err := h.scheduler.BeginInference(leaseID, request.Model, cancelInference)
	if err != nil {
		cancelInference()
		writeInferenceSchedulerError(w, err)
		return
	}
	defer cancelInference()
	defer endInference()

	backendURL := "http://127.0.0.1:" + strconv.Itoa(model.Port) + path
	backendRequest, err := http.NewRequestWithContext(inferCtx, http.MethodPost, backendURL, bytes.NewReader(body))
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "could not create backend request")
		return
	}
	backendRequest.Header.Set("Content-Type", "application/json")
	if accept := r.Header.Get("Accept"); accept != "" {
		backendRequest.Header.Set("Accept", accept)
	}
	response, err := h.client.Do(backendRequest)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		if errors.Is(inferCtx.Err(), context.DeadlineExceeded) {
			writeOpenAIError(w, http.StatusGatewayTimeout, "inference timed out")
		} else {
			writeOpenAIError(w, http.StatusBadGateway, "backend request failed")
		}
		return
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		copyBackendHeaders(w.Header(), response.Header)
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
		return
	}
	if supportsStream && request.Stream {
		if !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
			writeOpenAIError(w, http.StatusBadGateway, "backend did not return an SSE stream")
			return
		}
		copyBackendHeaders(w.Header(), response.Header)
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(response.StatusCode)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}
		idle := time.AfterFunc(h.proxy.StreamIdleDuration(), cancelInference)
		defer idle.Stop()
		buf := make([]byte, 32*1024)
		for {
			n, readErr := response.Body.Read(buf)
			if n > 0 {
				idle.Reset(h.proxy.StreamIdleDuration())
				if _, err := w.Write(buf[:n]); err != nil {
					cancelInference()
					return
				}
				flusher.Flush()
			}
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					succeeded = true
				}
				return
			}
		}
	}
	copyBackendHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	if _, err := io.Copy(w, response.Body); err != nil {
		return
	}
	succeeded = true
}

func copyBackendHeaders(dst, src http.Header) {
	for _, name := range []string{"Content-Type", "Cache-Control", "X-Request-ID"} {
		if value := src.Get(name); value != "" {
			dst.Set(name, value)
		}
	}
}

func writeOpenAIError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": message, "type": "server_error"}})
}

func writeInferenceSchedulerError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, scheduler.ErrNotFound), errors.Is(err, runtime.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, scheduler.ErrQueueFull):
		status = http.StatusTooManyRequests
	case errors.Is(err, scheduler.ErrConflict), errors.Is(err, scheduler.ErrExpired):
		status = http.StatusConflict
	case errors.Is(err, scheduler.ErrClosed), errors.Is(err, runtime.ErrUnavailable):
		status = http.StatusServiceUnavailable
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	}
	writeOpenAIError(w, status, err.Error())
}
