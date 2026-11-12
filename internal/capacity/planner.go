// Package capacity contains a read-only, dry-run memory capacity estimator.
// It consumes an immutable memory snapshot and model metadata; it does not
// know about or call Observer, Scheduler, Runtime, leases, or Recovery.
package capacity

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"local-ai-runtime/internal/config"
	"local-ai-runtime/internal/memory"
)

const (
	DefaultSystemMarginPercent = 10.0
	DefaultGPUMarginPercent    = 10.0
)

type Verdict string

const (
	Fit          Verdict = "FIT"
	Insufficient Verdict = "INSUFFICIENT"
	Unknown      Verdict = "UNKNOWN"
)

type Options struct {
	ShadowMode          bool
	SystemMarginPercent float64
	GPUMarginPercent    float64
}

func DefaultOptions() Options {
	return Options{SystemMarginPercent: DefaultSystemMarginPercent, GPUMarginPercent: DefaultGPUMarginPercent}
}

func (o Options) Validate() error {
	if math.IsNaN(o.SystemMarginPercent) || math.IsInf(o.SystemMarginPercent, 0) || o.SystemMarginPercent < 0 || o.SystemMarginPercent >= 100 {
		return errors.New("system margin must be between 0 and 100 percent")
	}
	if math.IsNaN(o.GPUMarginPercent) || math.IsInf(o.GPUMarginPercent, 0) || o.GPUMarginPercent < 0 || o.GPUMarginPercent >= 100 {
		return errors.New("GPU margin must be between 0 and 100 percent")
	}
	return nil
}

type SnapshotSource func() memory.Snapshot
type ModelSource func(string) (config.ModelConfig, bool)

type Planner struct {
	snapshot SnapshotSource
	model    ModelSource
	options  Options
}

func NewPlanner(snapshot SnapshotSource, model ModelSource, options Options) (*Planner, error) {
	if snapshot == nil || model == nil {
		return nil, errors.New("capacity planner requires snapshot and model sources")
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return &Planner{snapshot: snapshot, model: model, options: options}, nil
}

type Result struct {
	Verdict       Verdict         `json:"verdict"`
	ModelID       string          `json:"model_id"`
	Snapshot      memory.Snapshot `json:"snapshot"`
	Estimate      Estimate        `json:"estimate"`
	Capacity      Capacity        `json:"capacity"`
	Justification []string        `json:"justification"`
	Limitations   []string        `json:"limitations"`
}

type Capacity struct {
	SystemAvailableBytes *uint64 `json:"system_available_bytes"`
	GPUFreeBytes         *uint64 `json:"gpu_free_bytes"`
	SystemRequiredBytes  uint64  `json:"system_required_bytes"`
	GPURequiredBytes     uint64  `json:"gpu_required_bytes"`
}

type Estimate struct {
	FileBytes           uint64  `json:"file_bytes"`
	KVCacheBytes        uint64  `json:"kv_cache_bytes"`
	BatchWorkspaceBytes uint64  `json:"batch_workspace_bytes"`
	RawBytes            uint64  `json:"raw_bytes"`
	RequiredBytes       uint64  `json:"required_bytes"`
	SystemRequiredBytes uint64  `json:"system_required_bytes"`
	GPURequiredBytes    uint64  `json:"gpu_required_bytes"`
	ContextTokens       int     `json:"context_tokens"`
	BatchTokens         int     `json:"batch_tokens"`
	UBatchTokens        int     `json:"ubatch_tokens"`
	Parallel            int     `json:"parallel"`
	GPULayers           string  `json:"gpu_layers"`
	TotalLayers         uint64  `json:"total_layers"`
	KVHeads             uint64  `json:"kv_heads"`
	KeyLength           uint64  `json:"key_length"`
	ValueLength         uint64  `json:"value_length"`
	CacheTypeK          string  `json:"cache_type_k"`
	CacheTypeV          string  `json:"cache_type_v"`
	EmbeddingLength     uint64  `json:"embedding_length"`
	GGUFMetadata        bool    `json:"gguf_metadata"`
	GGUFMetadataError   string  `json:"gguf_metadata_error,omitempty"`
	SystemMarginPercent float64 `json:"system_margin_percent"`
	GPUMarginPercent    float64 `json:"gpu_margin_percent"`
}

func (p *Planner) Plan(ctx context.Context, modelID string) Result {
	snapshot := p.snapshot()
	result := Result{Verdict: Unknown, ModelID: modelID, Snapshot: snapshot, Estimate: Estimate{SystemMarginPercent: p.options.SystemMarginPercent, GPUMarginPercent: p.options.GPUMarginPercent}, Limitations: []string{
		"estimación conservadora; no equivale al consumo exacto de llama.cpp",
		"available_bytes/free_bytes son observaciones puntuales y pueden cambiar después de la consulta",
		"RSS de procesos no se suma: la disponibilidad del sistema ya refleja presión global y RSS puede ser compartido",
	}}
	model, ok := p.model(modelID)
	if !ok {
		result.Justification = []string{"modelo no encontrado en el registro"}
		return result
	}
	info, err := os.Stat(model.Path)
	if err != nil || !info.Mode().IsRegular() {
		result.Justification = []string{"archivo GGUF no disponible"}
		return result
	}
	metadata, metadataErr := readGGUFMetadata(model.Path)
	if metadataErr != nil {
		result.Estimate.FileBytes = uint64(info.Size())
		result.Estimate.GGUFMetadataError = metadataErr.Error()
		result.Justification = []string{"no se pudo leer metadata GGUF; no se inventan dimensiones del modelo"}
		return result
	}
	if ctx.Err() != nil {
		result.Justification = []string{"consulta cancelada"}
		return result
	}
	estimate, gpuFraction, err := makeEstimate(uint64(info.Size()), model, metadata, p.options)
	if err != nil {
		result.Justification = []string{err.Error()}
		return result
	}
	result.Estimate = estimate

	if snapshot.Status == memory.Stale || snapshot.Status == memory.Unavailable || snapshot.System.Status != memory.Available || snapshot.System.AvailableBytes == nil {
		result.Justification = []string{"snapshot de memoria no es AVAILABLE con available_bytes conocido", fmt.Sprintf("snapshot_status=%s system_status=%s", snapshot.Status, snapshot.System.Status)}
		return result
	}
	systemRaw, gpuRaw := splitBytes(estimate.RawBytes, gpuFraction)
	systemRequired, err := withMargin(systemRaw, p.options.SystemMarginPercent)
	if err != nil {
		result.Justification = []string{err.Error()}
		return result
	}
	gpuRequired, err := withMargin(gpuRaw, p.options.GPUMarginPercent)
	if err != nil {
		result.Justification = []string{err.Error()}
		return result
	}
	estimate.SystemRequiredBytes, estimate.GPURequiredBytes = systemRequired, gpuRequired
	estimate.RequiredBytes, err = add(systemRequired, gpuRequired)
	if err != nil {
		result.Justification = []string{"overflow sumando requerimientos con margen"}
		return result
	}
	result.Estimate = estimate
	result.Capacity.SystemAvailableBytes = snapshot.System.AvailableBytes
	result.Capacity.SystemRequiredBytes = systemRequired
	result.Capacity.GPURequiredBytes = gpuRequired

	if gpuRaw > 0 {
		if snapshot.System.Architecture == "unified" {
			// Unified memory has one physical budget; dedicated GPU telemetry is not needed.
			totalRequired, addErr := add(systemRequired, gpuRequired)
			if addErr != nil || *snapshot.System.AvailableBytes < totalRequired {
				result.Verdict = Insufficient
				result.Justification = []string{"Apple Silicon/unified: la demanda total supera la memoria disponible"}
				return result
			}
			result.Verdict = Fit
			result.Justification = []string{"Apple Silicon/unified: RAM y GPU comparten el presupuesto; se comparó la demanda total con system.available_bytes"}
			return result
		}
		if snapshot.GPUProvider.Status != memory.Available || len(snapshot.GPUs) == 0 {
			result.Justification = []string{"se requiere memoria NVIDIA dedicada pero el proveedor GPU no está AVAILABLE"}
			return result
		}
		var free uint64
		for _, gpu := range snapshot.GPUs {
			if gpu.Status != memory.Available || gpu.FreeBytes == nil {
				result.Justification = []string{"una GPU requerida no tiene free_bytes AVAILABLE; no se inventa VRAM"}
				return result
			}
			free, err = add(free, *gpu.FreeBytes)
			if err != nil {
				result.Justification = []string{"overflow al sumar memoria GPU reportada"}
				return result
			}
		}
		result.Capacity.GPUFreeBytes = &free
		if *snapshot.System.AvailableBytes < systemRequired || free < gpuRequired {
			result.Verdict = Insufficient
			result.Justification = []string{"NVIDIA: la demanda supera RAM disponible o la VRAM libre reportada"}
			return result
		}
		result.Verdict = Fit
		result.Justification = []string{"NVIDIA: se compararon por separado RAM del sistema y VRAM libre del proveedor"}
		return result
	}
	if *snapshot.System.AvailableBytes < systemRequired {
		result.Verdict = Insufficient
		result.Justification = []string{"la demanda de memoria del sistema supera available_bytes"}
		return result
	}
	result.Verdict = Fit
	result.Justification = []string{"la demanda estimada cabe en available_bytes; no se sumaron RSS de procesos"}
	return result
}

func makeEstimate(fileBytes uint64, model config.ModelConfig, metadata ggufMetadata, options Options) (Estimate, float64, error) {
	if model.Context <= 0 || model.Batch <= 0 || model.UBatch <= 0 || model.Parallel <= 0 || model.UBatch > model.Batch {
		return Estimate{}, 0, errors.New("parámetros context/batch/ubatch/parallel inválidos")
	}
	if metadata.Layers == 0 || metadata.Embedding == 0 || metadata.KVHeads == 0 || metadata.KeyLength == 0 || metadata.ValueLength == 0 {
		return Estimate{}, 0, errors.New("metadata GGUF incompleta para estimar KV cache y workspace")
	}
	cacheK, err := cacheBytes(model.CacheTypeK)
	if err != nil {
		return Estimate{}, 0, err
	}
	cacheV, err := cacheBytes(model.CacheTypeV)
	if err != nil {
		return Estimate{}, 0, err
	}
	contextParallel, err := mul(uint64(model.Context), uint64(model.Parallel))
	if err != nil {
		return Estimate{}, 0, errors.New("overflow estimando KV cache")
	}
	kv, err := mul(contextParallel, metadata.Layers)
	if err != nil {
		return Estimate{}, 0, errors.New("overflow estimando KV cache")
	}
	kv, err = mul(kv, metadata.KVHeads)
	if err != nil {
		return Estimate{}, 0, errors.New("overflow estimando KV cache")
	}
	keyBytes, err := mul(metadata.KeyLength, cacheK)
	if err != nil {
		return Estimate{}, 0, errors.New("overflow estimando KV cache")
	}
	valueBytes, err := mul(metadata.ValueLength, cacheV)
	if err != nil {
		return Estimate{}, 0, errors.New("overflow estimando KV cache")
	}
	perToken, err := add(keyBytes, valueBytes)
	if err != nil {
		return Estimate{}, 0, errors.New("overflow estimando KV cache")
	}
	kv, err = mul(kv, perToken)
	if err != nil {
		return Estimate{}, 0, errors.New("overflow estimando KV cache")
	}
	workspace, err := mul(uint64(model.Batch), metadata.Embedding)
	if err != nil {
		return Estimate{}, 0, errors.New("overflow estimando workspace de batch")
	}
	workspace, err = mul(workspace, 4) // two fp16 activation buffers; deliberately not llama.cpp exact accounting.
	if err != nil {
		return Estimate{}, 0, errors.New("overflow estimando workspace de batch")
	}
	raw, err := add(fileBytes, kv)
	if err != nil {
		return Estimate{}, 0, errors.New("overflow estimando memoria")
	}
	raw, err = add(raw, workspace)
	if err != nil {
		return Estimate{}, 0, errors.New("overflow estimando memoria")
	}
	fraction, err := gpuFraction(model.GPULayers, metadata.Layers)
	if err != nil {
		return Estimate{}, 0, err
	}
	return Estimate{FileBytes: fileBytes, KVCacheBytes: kv, BatchWorkspaceBytes: workspace, RawBytes: raw, ContextTokens: model.Context, BatchTokens: model.Batch, UBatchTokens: model.UBatch, Parallel: model.Parallel, GPULayers: model.GPULayers, TotalLayers: metadata.Layers, KVHeads: metadata.KVHeads, KeyLength: metadata.KeyLength, ValueLength: metadata.ValueLength, CacheTypeK: model.CacheTypeK, CacheTypeV: model.CacheTypeV, EmbeddingLength: metadata.Embedding, GGUFMetadata: true, SystemMarginPercent: options.SystemMarginPercent, GPUMarginPercent: options.GPUMarginPercent}, fraction, nil
}

func gpuFraction(value string, layers uint64) (float64, error) {
	if value == "all" {
		return 1, nil
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil || n > layers {
		return 0, errors.New("gpu_layers debe ser all o un número entre 0 y block_count")
	}
	return float64(n) / float64(layers), nil
}

func splitBytes(total uint64, fraction float64) (uint64, uint64) {
	gpu := uint64(float64(total) * fraction)
	return total - gpu, gpu
}

func withMargin(value uint64, percent float64) (uint64, error) {
	if value == 0 {
		return 0, nil
	}
	result := float64(value) * (1 + percent/100)
	if result > float64(^uint64(0)) {
		return 0, errors.New("overflow aplicando margen de seguridad")
	}
	return uint64(math.Ceil(result)), nil
}

func cacheBytes(value string) (uint64, error) {
	if value == "" {
		value = "f16"
	}
	switch strings.ToLower(value) {
	case "f32":
		return 4, nil
	case "f16", "bf16":
		return 2, nil
	case "q8_0", "q8_1", "i8":
		return 1, nil
	case "q4_0", "q4_1", "q5_0", "q5_1", "i4":
		return 1, nil // rounded up per element, conservative.
	default:
		return 0, fmt.Errorf("tipo de cache GGUF no soportado: %q", value)
	}
}

func add(a, b uint64) (uint64, error) {
	if ^uint64(0)-a < b {
		return 0, errors.New("uint64 overflow")
	}
	return a + b, nil
}
func mul(a, b uint64) (uint64, error) {
	if a != 0 && b > ^uint64(0)/a {
		return 0, errors.New("uint64 overflow")
	}
	return a * b, nil
}
