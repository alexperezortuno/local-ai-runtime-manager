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
	"local-ai-runtime/internal/corecontract"
	"local-ai-runtime/internal/inference"
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
	if cfg.InferenceMaxTerminalRecords <= 0 {
		cfg.InferenceMaxTerminalRecords = 1000
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
	requestID := ""
	defer func() {
		if requestID == "" {
			h.failed.Add(1)
			return
		}
		snapshot, err := h.inferences.Get(requestID)
		if err != nil {
			h.failed.Add(1)
			return
		}
		switch {
		case snapshot.IsAmbiguous():
			h.unknown.Add(1)
		case snapshot.Semantic == corecontract.SemanticSucceeded && snapshot.Transport == corecontract.TransportCompleted:
			h.completed.Add(1)
		default:
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
	requestID, err = h.inferences.Reserve(r.Header.Get("X-LARM-Request-ID"))
	if err != nil {
		if errors.Is(err, inference.ErrRecoveryRequired) {
			writeOpenAIError(w, http.StatusServiceUnavailable, err.Error())
		} else if errors.Is(err, inference.ErrRequestIDCollision) {
			writeOpenAIError(w, http.StatusConflict, err.Error())
		} else {
			writeOpenAIError(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	w.Header().Set("X-LARM-Request-ID", requestID)
	reserved := true
	defer func() {
		if reserved {
			h.inferences.CancelReservation(requestID)
		}
	}()
	executionID := r.Header.Get("X-LARM-Execution-ID")
	if executionID == "" {
		executionID, err = corecontract.NewID()
	} else if !inference.ValidID(executionID) {
		err = inference.ErrInvalidRequestID
	}
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "X-LARM-Execution-ID must be a canonical UUIDv4")
		return
	}
	model, ok := h.manager.Models()[request.Model]
	if !ok {
		writeOpenAIError(w, http.StatusNotFound, "model not found")
		return
	}

	leaseID := r.Header.Get("X-LARM-Lease-ID")
	automatic := leaseID == ""
	releaseReady := false
	defer func() {
		if !automatic {
			return
		}
		if releaseReady {
			_, _ = h.scheduler.ReleaseAutomatic(leaseID, requestID, request.Model)
			return
		}
		_, _ = h.scheduler.Release(leaseID)
	}()
	executionOwnership := "GENERATED_EPHEMERAL"
	if r.Header.Get("X-LARM-Execution-ID") != "" {
		executionOwnership = "DECLARED_UNVERIFIED"
	}
	createRecord := func() error {
		_, createErr := h.inferences.Create(inference.CreateInput{RequestID: requestID, ExecutionID: executionID, ExecutionDeclared: r.Header.Get("X-LARM-Execution-ID") != "", ExecutionOwnership: executionOwnership, ModelID: request.Model, LeaseID: leaseID, Stream: supportsStream && request.Stream})
		if createErr != nil {
			h.inferences.ProtectLease(leaseID, "inference admission was not durably accepted")
			return createErr
		}
		reserved = false
		_ = h.inferences.AdmissionAccepted(requestID)
		return nil
	}
	if automatic {
		lease, err := h.scheduler.RequestWithRequestID(requestID, request.Model)
		if err != nil {
			writeInferenceSchedulerError(w, err)
			return
		}
		leaseID = lease.ID
		if err := createRecord(); err != nil {
			status := http.StatusConflict
			if errors.Is(err, inference.ErrRecoveryRequired) {
				status = http.StatusServiceUnavailable
			}
			writeOpenAIError(w, status, err.Error())
			return
		}
		releaseReady = true
		_ = h.inferences.WaitingLease(requestID)
		queueCtx, cancelQueue := context.WithTimeout(r.Context(), h.proxy.QueueWaitDuration())
		err = h.scheduler.WaitActive(queueCtx, leaseID)
		cancelQueue()
		if err != nil {
			_ = h.inferences.MarkProcessUnverifiedWithPID(requestID, model.Port, h.manager.Models()[request.Model].PID)
			if r.Context().Err() != nil || errors.Is(err, context.Canceled) {
				_ = h.inferences.CancelBeforeUpstream(requestID)
			} else {
				_ = h.inferences.FailBeforeUpstream(requestID, corecontract.ErrorBackend)
			}
			if r.Context().Err() == nil {
				if errors.Is(err, context.DeadlineExceeded) {
					writeOpenAIError(w, http.StatusGatewayTimeout, "timed out waiting for model queue")
				} else {
					writeInferenceSchedulerError(w, err)
				}
			}
			return
		}
		_ = h.inferences.LeaseActive(requestID)
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
		if err := createRecord(); err != nil {
			status := http.StatusConflict
			if errors.Is(err, inference.ErrRecoveryRequired) {
				status = http.StatusServiceUnavailable
			}
			writeOpenAIError(w, status, err.Error())
			return
		}
		releaseReady = false
		_ = h.inferences.LeaseActive(requestID)
	}

	inferCtx, cancelInference := context.WithTimeout(r.Context(), h.proxy.InferenceDuration())
	endInference, err := h.scheduler.BeginInference(leaseID, request.Model, cancelInference, requestID)
	if err != nil {
		cancelInference()
		_ = h.inferences.MarkProcessUnverifiedWithPID(requestID, model.Port, h.manager.Models()[request.Model].PID)
		if r.Context().Err() != nil {
			_ = h.inferences.CancelBeforeUpstream(requestID)
		} else {
			_ = h.inferences.FailBeforeUpstream(requestID, corecontract.ErrorBackend)
		}
		writeInferenceSchedulerError(w, err)
		return
	}
	defer cancelInference()
	defer endInference()
	processInstance, processReady := h.manager.CurrentProcessForModel(request.Model)
	if !processReady {
		_ = h.inferences.MarkProcessUnverifiedWithPID(requestID, model.Port, h.manager.Models()[request.Model].PID)
		_ = h.inferences.FailBeforeUpstream(requestID, corecontract.ErrorBackend)
		writeOpenAIError(w, http.StatusServiceUnavailable, "runtime process identity is not verified")
		return
	}
	if err := h.inferences.AssignProcess(requestID, processInstance, model.Port); err != nil {
		_ = h.inferences.MarkProcessUnverifiedWithPID(requestID, model.Port, processInstance.PID)
		_ = h.inferences.FailBeforeUpstream(requestID, corecontract.ErrorBackend)
		writeOpenAIError(w, http.StatusServiceUnavailable, "runtime process identity could not be durably assigned")
		return
	}
	_ = h.inferences.PrepareTransport(requestID)

	backendURL := "http://127.0.0.1:" + strconv.Itoa(model.Port) + path
	backendRequest, err := http.NewRequestWithContext(inferCtx, http.MethodPost, backendURL, bytes.NewReader(body))
	if err != nil {
		_ = h.inferences.FailBeforeUpstream(requestID, corecontract.ErrorBackend)
		writeOpenAIError(w, http.StatusInternalServerError, "could not create backend request")
		return
	}
	backendRequest.Header.Set("Content-Type", "application/json")
	if accept := r.Header.Get("Accept"); accept != "" {
		backendRequest.Header.Set("Accept", accept)
	}
	_ = h.inferences.StartTransport(requestID)
	response, err := h.client.Do(backendRequest)
	if err != nil {
		if connectionFailedBeforeSend(err) {
			_ = h.inferences.FailBeforeUpstream(requestID, corecontract.ErrorTransport)
		} else {
			_ = h.inferences.FailTransport(requestID, corecontract.ErrorBackend)
		}
		if r.Context().Err() != nil {
			_ = h.inferences.RequestCancellation(requestID)
			_ = h.inferences.MarkCancellationUnknown(requestID)
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
	_ = h.inferences.Headers(requestID)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		copyBackendHeaders(w.Header(), response.Header)
		w.WriteHeader(response.StatusCode)
		_ = h.inferences.BodyStarted(requestID)
		if _, copyErr := io.Copy(w, response.Body); copyErr != nil {
			if r.Context().Err() != nil {
				_ = h.inferences.MarkDownstreamDisconnected(requestID)
				return
			}
			_ = h.inferences.FailTransport(requestID, corecontract.ErrorTransport)
			return
		}
		_ = h.inferences.BodyCompleted(requestID)
		_ = h.inferences.FailHTTP(requestID, response.StatusCode)
		return
	}
	if supportsStream && request.Stream {
		if !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
			_ = h.inferences.FailTransport(requestID, corecontract.ErrorBackend)
			writeOpenAIError(w, http.StatusBadGateway, "backend did not return an SSE stream")
			return
		}
		copyBackendHeaders(w.Header(), response.Header)
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(response.StatusCode)
		flusher, ok := w.(http.Flusher)
		if !ok {
			_ = h.inferences.FailTransport(requestID, corecontract.ErrorTransport)
			return
		}
		idle := time.AfterFunc(h.proxy.StreamIdleDuration(), cancelInference)
		defer idle.Stop()
		buf := make([]byte, 32*1024)
		var sseParser inference.SSEParser
		_ = h.inferences.SSEHeaders(requestID)
		for {
			select {
			case <-r.Context().Done():
				cancelInference()
				_ = h.inferences.MarkDownstreamDisconnected(requestID)
				return
			default:
			}
			n, readErr := response.Body.Read(buf)
			if n > 0 {
				_ = h.inferences.SSEChunk(requestID, sseParser.Feed(buf[:n]), sseParser.Count())
				idle.Reset(h.proxy.StreamIdleDuration())
				select {
				case <-r.Context().Done():
					cancelInference()
					_ = h.inferences.MarkDownstreamDisconnected(requestID)
					return
				default:
				}
				if _, err := w.Write(buf[:n]); err != nil {
					cancelInference()
					_ = h.inferences.MarkDownstreamDisconnected(requestID)
					return
				}
				flusher.Flush()
				select {
				case <-r.Context().Done():
					cancelInference()
					_ = h.inferences.MarkDownstreamDisconnected(requestID)
					return
				default:
				}
			}
			if readErr != nil {
				// Cancellation can reach the upstream transport before the
				// downstream request context is observable here. Preserve the
				// ambiguous cancellation instead of committing a terminal SSE
				// truncation first.
				if r.Context().Err() != nil {
					_ = h.inferences.MarkDownstreamDisconnected(requestID)
				} else if inferCtx.Err() != nil {
					_ = h.inferences.RequestCancellation(requestID)
					_ = h.inferences.MarkCancellationUnknown(requestID)
				} else if errors.Is(readErr, io.EOF) {
					_ = h.inferences.SSEEOF(requestID, sseParser.Finish(), sseParser.Count())
				} else {
					_ = h.inferences.SSEReadError(requestID, sseParser.Count())
				}
				return
			}
		}
	}
	copyBackendHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	_ = h.inferences.BodyStarted(requestID)
	if _, err := io.Copy(w, response.Body); err != nil {
		if r.Context().Err() != nil {
			_ = h.inferences.MarkDownstreamDisconnected(requestID)
			return
		}
		_ = h.inferences.FailTransport(requestID, corecontract.ErrorTransport)
		return
	}
	_ = h.inferences.BodyCompleted(requestID)
	if err := h.inferences.CompleteJSON(requestID, response.StatusCode); err != nil {
		_ = h.inferences.FailHTTP(requestID, response.StatusCode)
		return
	}
}

func connectionFailedBeforeSend(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
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
	case errors.Is(err, scheduler.ErrConflict), errors.Is(err, scheduler.ErrExpired), errors.Is(err, scheduler.ErrInferenceProtected), errors.Is(err, scheduler.ErrCapacity):
		status = http.StatusConflict
	case errors.Is(err, scheduler.ErrClosed), errors.Is(err, runtime.ErrUnavailable):
		status = http.StatusServiceUnavailable
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	}
	writeOpenAIError(w, status, err.Error())
}
