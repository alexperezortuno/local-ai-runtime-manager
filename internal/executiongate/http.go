package executiongate

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

type HTTPHandler struct{ coordinator *Coordinator }

func NewHTTPHandler(c *Coordinator) http.Handler {
	h := &HTTPHandler{coordinator: c}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/executions", h.createExecution)
	mux.HandleFunc("POST /v1/sessions/acquire", h.acquire)
	mux.HandleFunc("POST /v1/executions/{execution_id}/lease", h.bindLease)
	mux.HandleFunc("POST /v1/executions/{execution_id}/lease-intent", h.leaseIntent)
	mux.HandleFunc("POST /v1/requests", h.createRequest)
	mux.HandleFunc("POST /v1/requests/{request_id}/start", h.startRequest)
	mux.HandleFunc("POST /v1/requests/{request_id}/events", h.observeRequest)
	mux.HandleFunc("POST /v1/executions/{execution_id}/state", h.setState)
	mux.HandleFunc("POST /v1/executions/{execution_id}/begin-finalize", h.beginFinalize)
	mux.HandleFunc("POST /v1/executions/{execution_id}/finalize", h.finalize)
	mux.HandleFunc("POST /v1/executions/{execution_id}/cleanup", h.cleanup)
	mux.HandleFunc("GET /v1/registry", h.snapshot)
	return mux
}

func (h *HTTPHandler) bindLease(w http.ResponseWriter, r *http.Request) {
	var in BindLeaseInput
	if !decodeJSON(w, r, &in) {
		return
	}
	respond(w, map[string]string{"execution_id": r.PathValue("execution_id"), "lease_id": in.LeaseID, "state": in.LeaseState}, h.coordinator.BindLease(r.Context(), r.PathValue("execution_id"), in))
}

func (h *HTTPHandler) leaseIntent(w http.ResponseWriter, r *http.Request) {
	var in LeaseIntentInput
	if !decodeJSON(w, r, &in) {
		return
	}
	respond(w, map[string]string{"execution_id": r.PathValue("execution_id"), "idempotency_key": in.IdempotencyKey, "lease_state": "ACQUIRING"}, h.coordinator.BeginLeaseAcquisition(r.Context(), r.PathValue("execution_id"), in))
}

func (h *HTTPHandler) createExecution(w http.ResponseWriter, r *http.Request) {
	var in CreateExecutionInput
	if !decodeJSON(w, r, &in) {
		return
	}
	out, err := h.coordinator.CreateExecution(r.Context(), in)
	respond(w, out, err)
}

func (h *HTTPHandler) acquire(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ServerID    string `json:"server_id"`
		SessionID   string `json:"session_id"`
		ExecutionID string `json:"execution_id"`
	}
	if !decodeJSON(w, r, &in) {
		return
	}
	out, err := h.coordinator.AcquireSession(r.Context(), in.ServerID, in.SessionID, in.ExecutionID)
	if err == nil && out == Busy {
		w.WriteHeader(http.StatusConflict)
	}
	if errors.Is(err, ErrCancelled) {
		w.WriteHeader(http.StatusRequestTimeout)
		_ = json.NewEncoder(w).Encode(map[string]string{"result": string(Cancelled)})
		return
	}
	if err != nil {
		writeCoordinatorError(w, err)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"result": string(out)})
}

func (h *HTTPHandler) createRequest(w http.ResponseWriter, r *http.Request) {
	var in struct {
		RequestID   string `json:"request_id"`
		ExecutionID string `json:"execution_id"`
		ModelID     string `json:"model_id"`
		Stream      bool   `json:"stream"`
	}
	if !decodeJSON(w, r, &in) {
		return
	}
	out, err := h.coordinator.CreateRequest(r.Context(), in.RequestID, in.ExecutionID, in.ModelID, in.Stream)
	respond(w, out, err)
}

func (h *HTTPHandler) startRequest(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ExecutionID string `json:"execution_id"`
	}
	if !decodeJSON(w, r, &in) {
		return
	}
	respond(w, map[string]string{"request_id": r.PathValue("request_id"), "state": string(RequestInFlight)}, h.coordinator.StartRequest(r.Context(), in.ExecutionID, r.PathValue("request_id")))
}

func (h *HTTPHandler) observeRequest(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ExecutionID string `json:"execution_id"`
		RequestEvent
	}
	if !decodeJSON(w, r, &in) {
		return
	}
	out, err := h.coordinator.ObserveRequest(r.Context(), in.ExecutionID, r.PathValue("request_id"), in.RequestEvent)
	respond(w, out, err)
}

func (h *HTTPHandler) setState(w http.ResponseWriter, r *http.Request) {
	var in struct {
		State ExecutionState `json:"state"`
	}
	if !decodeJSON(w, r, &in) {
		return
	}
	respond(w, map[string]string{"execution_id": r.PathValue("execution_id"), "state": string(in.State)}, h.coordinator.SetExecutionState(r.Context(), r.PathValue("execution_id"), in.State))
}

func (h *HTTPHandler) beginFinalize(w http.ResponseWriter, r *http.Request) {
	var in struct {
		State  ExecutionState `json:"state"`
		Reason string         `json:"reason"`
	}
	if !decodeJSON(w, r, &in) {
		return
	}
	respond(w, map[string]string{"execution_id": r.PathValue("execution_id"), "state": string(ExecutionFinalizing)}, h.coordinator.BeginFinalize(r.Context(), r.PathValue("execution_id"), in.State, in.Reason))
}

func (h *HTTPHandler) finalize(w http.ResponseWriter, r *http.Request) {
	var in struct {
		State  ExecutionState `json:"state"`
		Reason string         `json:"reason"`
	}
	if !decodeJSON(w, r, &in) {
		return
	}
	respond(w, map[string]string{"execution_id": r.PathValue("execution_id"), "state": string(in.State)}, h.coordinator.Finalize(r.Context(), r.PathValue("execution_id"), in.State, in.Reason))
}

func (h *HTTPHandler) cleanup(w http.ResponseWriter, r *http.Request) {
	err := h.coordinator.Cleanup(r.Context(), r.PathValue("execution_id"))
	respond(w, map[string]string{"execution_id": r.PathValue("execution_id"), "state": string(ExecutionDeleted)}, err)
}

func (h *HTTPHandler) snapshot(w http.ResponseWriter, r *http.Request) {
	out, err := h.coordinator.Snapshot(r.Context())
	respond(w, out, err)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return false
	}
	if err := d.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "request must contain one JSON value", http.StatusBadRequest)
		return false
	}
	return true
}

func respond(w http.ResponseWriter, out any, err error) {
	if err != nil {
		writeCoordinatorError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func writeCoordinatorError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrBusy), errors.Is(err, ErrConflict), errors.Is(err, ErrNotOwner), errors.Is(err, ErrActive), errors.Is(err, ErrNotTerminal), errors.Is(err, ErrRecoveryRequired):
		status = http.StatusConflict
	case errors.Is(err, ErrCancelled):
		status = http.StatusRequestTimeout
	}
	http.Error(w, err.Error(), status)
}
