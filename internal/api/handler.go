package api

import (
	"context"
	"errors"
	"net/http"
	"sort"

	runtime "local-ai-runtime/internal/runtime"
)

type Handler struct {
	manager *runtime.Manager
}

func NewHandler(manager *runtime.Manager) http.Handler {
	h := &Handler{manager: manager}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /api/v1/models", h.models)
	mux.HandleFunc("GET /api/v1/status", h.status)
	mux.HandleFunc("POST /api/v1/models/{id}/start", h.start)
	mux.HandleFunc("POST /api/v1/models/{id}/stop", h.stop)
	return mux
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) status(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.manager.Status())
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
	if err := h.manager.Start(r.Context(), r.PathValue("id")); err != nil {
		writeManagerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h.manager.Status())
}

func (h *Handler) stop(w http.ResponseWriter, r *http.Request) {
	if err := h.manager.Stop(r.Context(), r.PathValue("id")); err != nil {
		writeManagerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h.manager.Status())
}

func writeManagerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, runtime.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, runtime.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, runtime.ErrUnavailable):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}
