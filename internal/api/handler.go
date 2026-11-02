package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"sync/atomic"

	"local-ai-runtime/internal/config"
	runtime "local-ai-runtime/internal/runtime"
	"local-ai-runtime/internal/scheduler"
)

type Handler struct {
	manager   *runtime.Manager
	scheduler *scheduler.Scheduler
	proxy     config.ProxyConfig
	client    *http.Client
	completed atomic.Uint64
	failed    atomic.Uint64
}

func NewHandler(manager *runtime.Manager, schedulers ...*scheduler.Scheduler) http.Handler {
	var sched *scheduler.Scheduler
	if len(schedulers) > 0 {
		sched = schedulers[0]
	}
	return NewHandlerWithConfig(manager, sched, config.ProxyConfig{})
}

func NewHandlerWithConfig(manager *runtime.Manager, sched *scheduler.Scheduler, proxy config.ProxyConfig) http.Handler {
	proxy = defaultProxyConfig(proxy)
	h := &Handler{manager: manager}
	h.scheduler = sched
	h.proxy = proxy
	h.client = newBackendClient(proxy.ConnectDuration())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /api/v1/models", h.models)
	mux.HandleFunc("GET /api/v1/status", h.status)
	mux.HandleFunc("POST /api/v1/models/{id}/start", h.start)
	mux.HandleFunc("POST /api/v1/models/{id}/stop", h.stop)
	if h.scheduler != nil {
		mux.HandleFunc("POST /api/v1/leases", h.createLease)
		mux.HandleFunc("GET /api/v1/leases/{id}", h.getLease)
		mux.HandleFunc("POST /api/v1/leases/{id}/heartbeat", h.heartbeat)
		mux.HandleFunc("DELETE /api/v1/leases/{id}", h.releaseLease)
		mux.HandleFunc("GET /api/v1/queue", h.queue)
		mux.HandleFunc("GET /v1/models", h.openAIModels)
		mux.HandleFunc("POST /v1/chat/completions", h.chatCompletions)
		mux.HandleFunc("POST /v1/embeddings", h.embeddings)
	}
	return mux
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) status(w http.ResponseWriter, _ *http.Request) {
	if h.scheduler == nil {
		writeJSON(w, http.StatusOK, h.manager.Status())
		return
	}
	snapshot := h.scheduler.Snapshot()
	queued := len(snapshot.Pending)
	writeJSON(w, http.StatusOK, struct {
		runtime.Status
		Scheduler         scheduler.Snapshot `json:"scheduler"`
		QueuedRequests    int                `json:"queued_requests"`
		ActiveInferences  int                `json:"active_inferences"`
		CompletedRequests uint64             `json:"completed_requests"`
		FailedRequests    uint64             `json:"failed_requests"`
	}{Status: h.manager.Status(), Scheduler: snapshot, QueuedRequests: queued, ActiveInferences: h.scheduler.ActiveInferenceCount(), CompletedRequests: h.completed.Load(), FailedRequests: h.failed.Load()})
}

func (h *Handler) models(w http.ResponseWriter, _ *http.Request) {
	models := h.manager.Models()
	ids := make([]string, 0, len(models))
	for id := range models {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	list := make([]runtime.ModelStatus, 0, len(ids))
	for _, id := range ids {
		list = append(list, models[id])
	}
	writeJSON(w, http.StatusOK, list)
}

func (h *Handler) start(w http.ResponseWriter, r *http.Request) {
	var err error
	if h.scheduler != nil {
		err = h.scheduler.ManualStart(r.Context(), r.PathValue("id"))
	} else {
		err = h.manager.Start(r.Context(), r.PathValue("id"))
	}
	if err != nil {
		writeManagerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h.manager.Status())
}

func (h *Handler) stop(w http.ResponseWriter, r *http.Request) {
	var err error
	if h.scheduler != nil {
		err = h.scheduler.ManualStop(r.Context(), r.PathValue("id"))
	} else {
		err = h.manager.Stop(r.Context(), r.PathValue("id"))
	}
	if err != nil {
		writeManagerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h.manager.Status())
}

func (h *Handler) createLease(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Model string `json:"model"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.Model == "" {
		writeError(w, http.StatusBadRequest, "request must contain a model")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request must contain one JSON object")
		return
	}
	lease, err := h.scheduler.Request(request.Model)
	if err != nil {
		writeSchedulerError(w, err)
		return
	}
	status, _ := h.scheduler.Get(lease.ID)
	writeJSON(w, http.StatusAccepted, status)
}

func (h *Handler) getLease(w http.ResponseWriter, r *http.Request) {
	lease, err := h.scheduler.Get(r.PathValue("id"))
	if err != nil {
		writeSchedulerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, lease)
}

func (h *Handler) heartbeat(w http.ResponseWriter, r *http.Request) {
	lease, err := h.scheduler.Heartbeat(r.PathValue("id"))
	if err != nil {
		writeSchedulerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, lease)
}

func (h *Handler) releaseLease(w http.ResponseWriter, r *http.Request) {
	lease, err := h.scheduler.Release(r.PathValue("id"))
	if err != nil {
		writeSchedulerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, lease)
}

func (h *Handler) queue(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.scheduler.Snapshot())
}

func writeSchedulerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, scheduler.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, scheduler.ErrQueueFull):
		writeError(w, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, scheduler.ErrConflict), errors.Is(err, scheduler.ErrExpired):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, scheduler.ErrClosed), errors.Is(err, runtime.ErrUnavailable):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func writeManagerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, runtime.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, runtime.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, runtime.ErrUnavailable):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, scheduler.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, scheduler.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}
