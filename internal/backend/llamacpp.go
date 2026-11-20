package backend

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"local-ai-runtime/internal/config"
)

func Args(model config.ModelConfig) []string {
	host := model.Host
	if host == "" {
		host = "127.0.0.1"
	}
	args := []string{"-m", model.Path, "--host", host, "--port", fmt.Sprint(model.Port), "-c", fmt.Sprint(model.Context), "-b", fmt.Sprint(model.Batch), "-ub", fmt.Sprint(model.UBatch), "-ngl", model.EffectiveGPULayers()}
	if model.CacheTypeK != "" {
		args = append(args, "--cache-type-k", model.CacheTypeK)
	}
	if model.CacheTypeV != "" {
		args = append(args, "--cache-type-v", model.CacheTypeV)
	}
	args = append(args, "--parallel", fmt.Sprint(model.Parallel))
	if model.Temperature != nil {
		args = append(args, "--temp", fmt.Sprint(*model.Temperature))
	}
	if model.TopP != nil {
		args = append(args, "--top-p", fmt.Sprint(*model.TopP))
	}
	if model.TopK != nil {
		args = append(args, "--top-k", fmt.Sprint(*model.TopK))
	}
	if model.PresencePenalty != nil {
		args = append(args, "--presence-penalty", fmt.Sprint(*model.PresencePenalty))
	}
	if model.FrequencyPenalty != nil {
		args = append(args, "--frequency-penalty", fmt.Sprint(*model.FrequencyPenalty))
	}
	if model.NPredict != nil {
		args = append(args, "--n-predict", fmt.Sprint(*model.NPredict))
	}
	if model.RepeatPenalty != nil {
		args = append(args, "--repeat-penalty", fmt.Sprint(*model.RepeatPenalty))
	}
	if model.FlashAttn != nil {
		args = append(args, "--flash-attn", *model.FlashAttn)
	}
	if model.Device != nil {
		args = append(args, "--device", *model.Device)
	}
	if model.SplitMode != nil {
		args = append(args, "--split-mode", *model.SplitMode)
	}
	if model.Metrics != nil && *model.Metrics {
		args = append(args, "--metrics")
	}
	if model.Fit != nil {
		value := "off"
		if *model.Fit {
			value = "on"
		}
		args = append(args, "--fit", value)
	}
	if model.CachePrompt != nil {
		flag := "--no-cache-prompt"
		if *model.CachePrompt {
			flag = "--cache-prompt"
		}
		args = append(args, flag)
	}
	if model.NoNMProj != nil {
		flag := "--mmproj-auto"
		if *model.NoNMProj {
			flag = "--no-mmproj"
		}
		args = append(args, flag)
	}

	if model.LogFile != "" {
		args = append(args, "--log-file", fmt.Sprint(model.LogFile))
	}

	if model.Alias != "" {
		args = append(args, "--alias", fmt.Sprint(model.Alias))
	}

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
	return HealthyAt(ctx, "127.0.0.1", port)
}

func HealthyAt(ctx context.Context, host string, port int) bool {
	client := http.Client{Timeout: 500 * time.Millisecond}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/health", net.JoinHostPort(host, fmt.Sprint(port))), nil)
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
