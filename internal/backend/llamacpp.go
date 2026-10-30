package backend

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"local-ai-runtime/internal/config"
)

func Args(model config.ModelConfig) []string {
	args := []string{"-m", model.Path, "--host", "127.0.0.1", "--port", fmt.Sprint(model.Port), "-c", fmt.Sprint(model.Context), "-b", fmt.Sprint(model.Batch), "-ub", fmt.Sprint(model.UBatch), "-ngl", model.GPULayers}
	if model.CacheTypeK != "" {
		args = append(args, "--cache-type-k", model.CacheTypeK)
	}
	if model.CacheTypeV != "" {
		args = append(args, "--cache-type-v", model.CacheTypeV)
	}
	args = append(args, "--parallel", fmt.Sprint(model.Parallel), "--temp", fmt.Sprint(model.Temperature), "--top-p", fmt.Sprint(model.TopP), "--top-k", fmt.Sprint(model.TopK))
	if model.Reasoning != "" {
		reasoning := model.Reasoning
		if reasoning == "off" {
			reasoning = "none"
		}
		args = append(args, "--reasoning-format", reasoning)
	}
	return append(args, model.ExtraArgs...)
}

func Healthy(ctx context.Context, port int) bool {
	client := http.Client{Timeout: 500 * time.Millisecond}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/health", port), nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
