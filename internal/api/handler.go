package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"sync/atomic"

	"local-ai-runtime/internal/capacity"
	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/inference"
	recoveryservice "local-ai-runtime/internal/recovery"
	runtime "local-ai-runtime/internal/runtime"
	"local-ai-runtime/internal/scheduler"
)

type Handler struct {
	manager    *runtime.Manager
	scheduler  *scheduler.Scheduler
	proxy      config.ProxyConfig
	client     *http.Client
	inferences *inference.Registry
	recovery   *recoveryservice.Service
	planner    *capacity.Planner
	completed  atomic.Uint64
	failed     atomic.Uint64
	unknown    atomic.Uint64
}

func NewHandler(manager *runtime.Manager, schedulers ...*scheduler.Scheduler) http.Handler {
	var sched *scheduler.Scheduler
	if len(schedulers) > 0 {
		sched = schedulers[0]
	}
	return NewHandlerWithConfig(manager, sched, config.ProxyConfig{})
}

func NewHandlerWithConfig(manager *runtime.Manager, sched *scheduler.Scheduler, proxy config.ProxyConfig) http.Handler {
	return NewHandlerWithConfigAndRegistry(manager, sched, proxy, nil)
}

func NewHandlerWithConfigAndRegistry(manager *runtime.Manager, sched *scheduler.Scheduler, proxy config.ProxyConfig, registry *inference.Registry) http.Handler {
	return newHandler(manager, sched, proxy, registry, nil, capacity.DefaultOptions())
}

// NewHandlerWithCapacity is the read-only HTTP handler with configured
// dry-run margins. The planner does not participate in scheduler decisions.
func NewHandlerWithCapacity(manager *runtime.Manager, sched *scheduler.Scheduler, proxy config.ProxyConfig, options capacity.Options) http.Handler {
	return newHandler(manager, sched, proxy, nil, nil, options)
}

func newHandler(manager *runtime.Manager, sched *scheduler.Scheduler, proxy config.ProxyConfig, registry *inference.Registry, recovery *recoveryservice.Service, options capacity.Options) http.Handler {
	proxy = defaultProxyConfig(proxy)
	if registry == nil {
		registry = inference.New(inference.Options{MaxTerminalRecords: proxy.InferenceMaxTerminalRecords})
	}
	h := &Handler{manager: manager, inferences: registry, recovery: recovery}
	if manager != nil {
		h.planner, _ = capacity.NewPlanner(manager.MemorySnapshot, manager.ModelConfig, options)
	}
	h.scheduler = sched
	if sched != nil {
		sched.SetReleaseProtector(registry)
		sched.SetAdmissionGate(registry)
	}
	h.proxy = proxy
	h.client = newBackendClient(proxy.ConnectDuration())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /api/v1/models", h.models)
	mux.HandleFunc("GET /api/v1/status", h.status)
	mux.HandleFunc("GET /api/v1/inferences", h.listInferences)
	mux.HandleFunc("GET /api/v1/inferences/{request_id}", h.getInference)
	mux.HandleFunc("GET /api/v1/processes", h.listProcesses)
	mux.HandleFunc("GET /api/v1/processes/{process_instance_id}", h.getProcess)
	mux.HandleFunc("GET /api/v1/capacity", h.capacityQuery)
	mux.HandleFunc("GET /api/v1/capacity/{model}", h.capacityPlan)
	mux.HandleFunc("GET /api/v1/recovery/status", h.recoveryStatus)
	mux.HandleFunc("GET /api/v1/recovery/inferences", h.recoveryInferences)
	mux.HandleFunc("GET /api/v1/recovery/leases", h.recoveryLeases)
	mux.HandleFunc("GET /api/v1/recovery/incidents/{incident_id}/plan", h.recoveryPlan)
	mux.HandleFunc("GET /api/v1/recovery/incidents/{incident_id}", h.recoveryIncident)
	mux.HandleFunc("GET /api/v1/recovery/incidents", h.recoveryIncidents)
	mux.HandleFunc("GET /api/v1/recovery/audit", h.recoveryAudit)
	// Mutating model operations stay on the authenticated Unix socket. Keep the
	// historical paths as explicit conflicts so clients do not mistake removal
	// for a transient 404 and no public request can reach Runtime Manager.
	mux.HandleFunc("POST /api/v1/models/{id}/start", h.publicMutationRejected)
	mux.HandleFunc("POST /api/v1/models/{id}/stop", h.publicMutationRejected)
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

func (h *Handler) publicMutationRejected(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusConflict, "administrative model operations require the protected Unix socket")
}

// NewHandlerWithJournal opens the Runtime Core journal before exposing any
// mutating endpoint. The registry is returned for clean-shutdown bookkeeping.
func NewHandlerWithJournal(manager *runtime.Manager, sched *scheduler.Scheduler, proxy config.ProxyConfig, journal *inference.Journal) (http.Handler, *inference.Registry, error) {
	handler, registry, _, err := NewHandlerWithJournalAndRecovery(manager, sched, proxy, journal)
	return handler, registry, err
}

// NewHandlerWithJournalAndRecovery exposes the existing Recovery Service to
// the private administrative listener without adding an HTTP admin endpoint.
func NewHandlerWithJournalAndRecovery(manager *runtime.Manager, sched *scheduler.Scheduler, proxy config.ProxyConfig, journal *inference.Journal) (http.Handler, *inference.Registry, *recoveryservice.Service, error) {
	return NewHandlerWithJournalAndRecoveryAndCapacity(manager, sched, proxy, journal, capacity.DefaultOptions())
}

// NewHandlerWithJournalAndRecoveryAndCapacity keeps capacity planning behind
// the existing read-only API boundary while allowing margins from config.
func NewHandlerWithJournalAndRecoveryAndCapacity(manager *runtime.Manager, sched *scheduler.Scheduler, proxy config.ProxyConfig, journal *inference.Journal, options capacity.Options) (http.Handler, *inference.Registry, *recoveryservice.Service, error) {
	registry, err := inference.Open(inference.Options{MaxTerminalRecords: proxy.InferenceMaxTerminalRecords, Journal: journal})
	if err != nil {
		return nil, nil, nil, err
	}
	var processProvider recoveryservice.ProcessProvider
	if manager != nil {
		processProvider = manager
	}
	operatorRecovery, err := recoveryservice.Open(registry, sched, journal.Meta().Path+".operator.json", processProvider)
	if err != nil {
		return nil, nil, nil, err
	}
	return newHandler(manager, sched, proxy, registry, operatorRecovery, options), registry, operatorRecovery, nil
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
		UnknownRequests   uint64             `json:"unknown_requests"`
	}{Status: h.manager.Status(), Scheduler: snapshot, QueuedRequests: queued, ActiveInferences: h.scheduler.ActiveInferenceCount(), CompletedRequests: h.completed.Load(), FailedRequests: h.failed.Load(), UnknownRequests: h.unknown.Load()})
}

func (h *Handler) listInferences(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.inferences.List())
}

func (h *Handler) getInference(w http.ResponseWriter, r *http.Request) {
	snapshot, err := h.inferences.Get(r.PathValue("request_id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (h *Handler) listProcesses(w http.ResponseWriter, _ *http.Request) {
	if h.manager == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	writeJSON(w, http.StatusOK, h.manager.ProcessInstances())
}

func (h *Handler) getProcess(w http.ResponseWriter, r *http.Request) {
	if h.manager == nil {
		writeError(w, http.StatusNotFound, "runtime is unavailable")
		return
	}
	process, ok := h.manager.ProcessInstance(r.PathValue("process_instance_id"))
	if !ok {
		writeError(w, http.StatusNotFound, "process instance not found")
		return
	}
	writeJSON(w, http.StatusOK, process)
}

func (h *Handler) capacityQuery(w http.ResponseWriter, r *http.Request) {
	model := r.URL.Query().Get("model")
	if model == "" {
		writeError(w, http.StatusBadRequest, "capacity requires a model query parameter")
		return
	}
	h.writeCapacity(w, r, model)
}

func (h *Handler) capacityPlan(w http.ResponseWriter, r *http.Request) {
	h.writeCapacity(w, r, r.PathValue("model"))
}

func (h *Handler) writeCapacity(w http.ResponseWriter, r *http.Request, model string) {
	if h.planner == nil {
		writeJSON(w, http.StatusOK, capacity.Result{Verdict: capacity.Unknown, ModelID: model, Justification: []string{"capacity planner unavailable"}})
		return
	}
	writeJSON(w, http.StatusOK, h.planner.Plan(r.Context(), model))
}

func (h *Handler) recoveryStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.inferences.RecoveryStatus())
}

func (h *Handler) recoveryInferences(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.inferences.RecoveryInferences())
}

func (h *Handler) recoveryLeases(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.inferences.RecoveryLeases())
}

func (h *Handler) recoveryIncidents(w http.ResponseWriter, _ *http.Request) {
	if h.recovery == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	incidents, err := h.recovery.Incidents()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, incidents)
}

func (h *Handler) recoveryIncident(w http.ResponseWriter, r *http.Request) {
	if h.recovery == nil {
		writeError(w, http.StatusNotFound, "recovery service is unavailable")
		return
	}
	incident, err := h.recovery.Incident(r.PathValue("incident_id"))
	if err != nil {
		writeRecoveryServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, incident)
}

func (h *Handler) recoveryPlan(w http.ResponseWriter, r *http.Request) {
	if h.recovery == nil {
		writeError(w, http.StatusNotFound, "recovery service is unavailable")
		return
	}
	plan, err := h.recovery.Plan(r.PathValue("incident_id"))
	if err != nil {
		writeRecoveryServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (h *Handler) recoveryAudit(w http.ResponseWriter, _ *http.Request) {
	if h.recovery == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	audit, err := h.recovery.Audit()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, audit)
}

func (h *Handler) models(w http.ResponseWriter, _ *http.Request) {
	models := h.manager.Models()
	if h.scheduler != nil {
		models = h.scheduler.ModelsSnapshot()
	}
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
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) > 0 {
		if len(keys) != 1 || keys[0] == "" {
			writeError(w, http.StatusBadRequest, "Idempotency-Key must contain one canonical UUID")
			return
		}
		status, replay, err := h.scheduler.RequestIdempotent(keys[0], request.Model)
		if err != nil {
			switch {
			case errors.Is(err, scheduler.ErrInvalidIdempotencyKey):
				writeError(w, http.StatusBadRequest, err.Error())
			case errors.Is(err, scheduler.ErrIdempotencyConflict):
				writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "state": status.State, "lease": status})
			case errors.Is(err, scheduler.ErrIdempotencyTerminal):
				writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "state": status.State, "lease": status})
			case errors.Is(err, scheduler.ErrIdempotencyRecovery):
				if status.ID == "" {
					writeRecoveryError(w, err)
				} else {
					writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error(), "state": status.State, "lease": status})
				}
			default:
				writeSchedulerError(w, err)
			}
			return
		}
		if replay && status.State == scheduler.Active {
			writeJSON(w, http.StatusOK, status)
			return
		}
		writeJSON(w, http.StatusAccepted, status)
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
	case errors.Is(err, scheduler.ErrConflict), errors.Is(err, scheduler.ErrExpired), errors.Is(err, scheduler.ErrInferenceProtected), errors.Is(err, scheduler.ErrAlreadyStopped), errors.Is(err, scheduler.ErrCapacity):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, scheduler.ErrClosed), errors.Is(err, runtime.ErrUnavailable):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, scheduler.ErrIdempotencyRecovery):
		writeRecoveryError(w, err)
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
	case errors.Is(err, scheduler.ErrIdempotencyRecovery):
		writeRecoveryError(w, err)
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func writeRecoveryError(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error(), "state": string(scheduler.RecoveryRequired)})
}

func writeRecoveryServiceError(w http.ResponseWriter, err error) {
	if errors.Is(err, recoveryservice.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeError(w, http.StatusServiceUnavailable, err.Error())
}
