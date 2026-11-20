package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadValidYAML(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "llama-server")
	model := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(binary, []byte("binary"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(model, []byte("model"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "models.yaml")
	content := "server:\n  port: 8090\nruntime:\n  binary: " + binary + "\nmodels:\n  test-model:\n    path: " + model + "\n    port: 18069\n    context: 4096\n    batch: 512\n    ubatch: 128\n    gpu_layers: all\n    parallel: 1\n    temperature: 0.2\n    top_p: 0.9\n    top_k: 40\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Host != "127.0.0.1" || cfg.Runtime.StartupDuration().String() != "2m0s" || len(cfg.Models) != 1 || cfg.Scheduler.IdempotencyStore != path+".idempotency.json" || cfg.Models["test-model"].KeepAlive || cfg.Models["test-model"].TimeAlive != "0s" {
		t.Fatalf("unexpected loaded config: %+v", cfg)
	}
}

func TestLoadOptionalSamplingParameters(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "llama-server")
	model := filepath.Join(dir, "model.gguf")
	_ = os.WriteFile(binary, []byte("binary"), 0755)
	_ = os.WriteFile(model, []byte("model"), 0600)

	for _, test := range []struct {
		name  string
		extra string
	}{
		{name: "all omitted"},
		{name: "temperature only", extra: "    temperature: 0.2\n"},
		{name: "top_p only", extra: "    top_p: 0.9\n"},
		{name: "top_k only", extra: "    top_k: 40\n"},
		{name: "top_k zero", extra: "    top_k: 0\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(dir, "models.yaml")
			content := validYAML(model)
			content = strings.Replace(content, "binary: /test/llama-server", "binary: "+binary, 1)
			content = strings.Replace(content, "    top_p: 0.9\n", test.extra, 1)
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err != nil {
				t.Fatalf("configuration rejected: %v", err)
			}
		})
	}
}

func TestLoadRejectsOutOfRangeSamplingParameters(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "llama-server")
	model := filepath.Join(dir, "model.gguf")
	_ = os.WriteFile(binary, []byte("binary"), 0755)
	_ = os.WriteFile(model, []byte("model"), 0600)

	for _, test := range []struct {
		name  string
		extra string
	}{
		{name: "negative temperature", extra: "    temperature: -0.1\n"},
		{name: "zero top_p", extra: "    top_p: 0\n"},
		{name: "top_p above one", extra: "    top_p: 1.1\n"},
		{name: "negative top_k", extra: "    top_k: -1\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(dir, "models.yaml")
			content := validYAML(model)
			content = strings.Replace(content, "binary: /test/llama-server", "binary: "+binary, 1)
			content = strings.Replace(content, "    top_p: 0.9\n", test.extra, 1)
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil {
				t.Fatal("expected invalid sampling parameters to be rejected")
			}
		})
	}
}

func TestLoadExtendedLlamaParameters(t *testing.T) {
	dir := t.TempDir()
	binary := writeHelpBinary(t, dir, `--presence-penalty --frequency-penalty --n-predict --repeat-penalty --flash-attn --device --n-gpu-layers --split-mode --metrics --fit --cache-prompt --no-mmproj`)
	model := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(model, []byte("model"), 0600); err != nil {
		t.Fatal(err)
	}
	content := strings.Replace(validYAML(model), "    top_p: 0.9\n", `    presence_penalty: 0
    frequency_penalty: 0
    n_predict: 0
    repeat_penalty: 0
    flash_attn: auto
    device: "none"
    n_gpu_layers: "all"
    split_mode: none
    metrics: false
    fit: false
    cache_prompt: false
    no_nmproj: false
    top_p: 0.9
`, 1)
	content = strings.Replace(content, "binary: /test/llama-server", "binary: "+binary, 1)
	path := filepath.Join(dir, "models.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	modelConfig := cfg.Models["test"]
	if modelConfig.PresencePenalty == nil || *modelConfig.PresencePenalty != 0 ||
		modelConfig.NPredict == nil || *modelConfig.NPredict != 0 ||
		modelConfig.Metrics == nil || *modelConfig.Metrics ||
		modelConfig.CachePrompt == nil || *modelConfig.CachePrompt ||
		modelConfig.NoNMProj == nil || *modelConfig.NoNMProj ||
		modelConfig.EffectiveGPULayers() != "all" {
		t.Fatalf("extended options lost their explicit values: %+v", modelConfig)
	}
}

func TestLoadAcceptsNGPULayersAsGPUAlias(t *testing.T) {
	dir := t.TempDir()
	binary := writeHelpBinary(t, dir, `--n-gpu-layers`)
	model := filepath.Join(dir, "model.gguf")
	_ = os.WriteFile(model, []byte("model"), 0600)
	content := strings.Replace(validYAML(model), "    gpu_layers: all\n", `    n_gpu_layers: "0"
`, 1)
	content = strings.Replace(content, "binary: /test/llama-server", "binary: "+binary, 1)
	path := filepath.Join(dir, "models.yaml")
	_ = os.WriteFile(path, []byte(content), 0600)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Models["test"].EffectiveGPULayers() != "0" {
		t.Fatalf("effective GPU layers = %q", cfg.Models["test"].EffectiveGPULayers())
	}
}

func TestLoadRejectsExtendedParameterErrors(t *testing.T) {
	dir := t.TempDir()
	binary := writeHelpBinary(t, dir, `--presence-penalty --repeat-penalty --n-predict --flash-attn --device --split-mode --no-mmproj`)
	model := filepath.Join(dir, "model.gguf")
	_ = os.WriteFile(model, []byte("model"), 0600)
	for _, test := range []struct {
		name  string
		field string
		want  string
	}{
		{name: "n_predict below infinity sentinel", field: "n_predict: -2", want: "n_predict"},
		{name: "repeat penalty negative", field: "repeat_penalty: -1", want: "repeat_penalty"},
		{name: "flash enum", field: "flash_attn: maybe", want: "flash_attn"},
		{name: "empty device", field: `device: ""`, want: "device"},
		{name: "split enum", field: "split_mode: gpu", want: "split_mode"},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := strings.Replace(validYAML(model), "    top_p: 0.9\n", "    "+test.field+"\n    top_p: 0.9\n", 1)
			content = strings.Replace(content, "binary: /test/llama-server", "binary: "+binary, 1)
			path := filepath.Join(dir, test.name+".yaml")
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), "models.test."+test.want) {
				t.Fatalf("error = %v, want model and parameter %q", err, test.want)
			}
		})
	}
}

func TestLoadRejectsGPUAliasConflictAndExtraArgsDuplicate(t *testing.T) {
	dir := t.TempDir()
	binary := writeHelpBinary(t, dir, `--n-gpu-layers --presence-penalty`)
	model := filepath.Join(dir, "model.gguf")
	_ = os.WriteFile(model, []byte("model"), 0600)
	for _, test := range []struct {
		name  string
		extra string
		want  string
	}{
		{name: "gpu alias conflict", extra: "    n_gpu_layers: \"0\"\n", want: "gpu_layers and models.test.n_gpu_layers conflict"},
		{name: "typed and extra args", extra: "    presence_penalty: 0.2\n    extra_args: [\"--presence-penalty\", \"0.1\"]\n", want: "models.test.presence_penalty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := strings.Replace(validYAML(model), "    top_p: 0.9\n", test.extra+"    top_p: 0.9\n", 1)
			content = strings.Replace(content, "binary: /test/llama-server", "binary: "+binary, 1)
			path := filepath.Join(dir, test.name+".yaml")
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadRejectsUnsupportedExtendedParameter(t *testing.T) {
	dir := t.TempDir()
	binary := writeHelpBinary(t, dir, `--presence-penalty`)
	model := filepath.Join(dir, "model.gguf")
	_ = os.WriteFile(model, []byte("model"), 0600)
	content := strings.Replace(validYAML(model), "    top_p: 0.9\n", "    device: Metal\n    top_p: 0.9\n", 1)
	content = strings.Replace(content, "binary: /test/llama-server", "binary: "+binary, 1)
	path := filepath.Join(dir, "models.yaml")
	_ = os.WriteFile(path, []byte(content), 0600)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "models.test.device") {
		t.Fatalf("unsupported option error = %v", err)
	}
}

func TestLoadRejectsIncorrectExtendedParameterTypes(t *testing.T) {
	dir := t.TempDir()
	binary := writeHelpBinary(t, dir, `--presence-penalty --metrics`)
	model := filepath.Join(dir, "model.gguf")
	_ = os.WriteFile(model, []byte("model"), 0600)
	for _, field := range []string{"presence_penalty: not-a-number", `metrics: "true"`} {
		content := strings.Replace(validYAML(model), "    top_p: 0.9\n", "    "+field+"\n    top_p: 0.9\n", 1)
		content = strings.Replace(content, "binary: /test/llama-server", "binary: "+binary, 1)
		path := filepath.Join(dir, strings.ReplaceAll(field, ":", "-")+".yaml")
		_ = os.WriteFile(path, []byte(content), 0600)
		if _, err := Load(path); err == nil {
			t.Fatalf("accepted incorrect type in %q", field)
		}
	}
}

func TestLoadResidencyPolicy(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "llama-server")
	model := filepath.Join(dir, "model.gguf")
	_ = os.WriteFile(binary, []byte("binary"), 0755)
	_ = os.WriteFile(model, []byte("model"), 0600)
	path := filepath.Join(dir, "models.yaml")
	content := strings.Replace(validYAML(model), "top_p: 0.9", "top_p: 0.9\n    keep_alive: true\n    time_alive: 5m", 1)
	content = strings.Replace(content, "/test/llama-server", binary, 1)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Models["test"].KeepAlive || cfg.Models["test"].TimeAliveDuration().String() != "5m0s" {
		t.Fatalf("residency policy = %+v", cfg.Models["test"])
	}
}

func TestLoadRejectsNegativeResidency(t *testing.T) {
	dir := t.TempDir()
	model := filepath.Join(dir, "model.gguf")
	_ = os.WriteFile(model, []byte("model"), 0600)
	path := filepath.Join(dir, "models.yaml")
	content := strings.Replace(validYAML(model), "top_p: 0.9", "top_p: 0.9\n    time_alive: -1s", 1)
	content = strings.Replace(content, "/test/llama-server", filepath.Join(dir, "llama-server"), 1)
	_ = os.WriteFile(path, []byte(content), 0600)
	_ = os.WriteFile(filepath.Join(dir, "llama-server"), []byte("binary"), 0755)
	if _, err := Load(path); err == nil {
		t.Fatal("expected negative time_alive to be rejected")
	}
}

func TestLoadRejectsInvalidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("server:\n  host: 0.0.0.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected invalid configuration error")
	}
}

func TestLoadRejectsMissingBinary(t *testing.T) {
	dir := t.TempDir()
	model := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(model, []byte("model"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "missing.yaml")
	content := strings.Replace(validYAML(model), "binary: /test/llama-server", "binary: /missing/llama-server", 1)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected missing binary error")
	}
}

func TestLoadAdmissionDefaultsDisabled(t *testing.T) {
	dir := t.TempDir()
	model := filepath.Join(dir, "model.gguf")
	binary := filepath.Join(dir, "llama-server")
	_ = os.WriteFile(model, []byte("model"), 0600)
	_ = os.WriteFile(binary, []byte("binary"), 0755)
	path := filepath.Join(dir, "models.yaml")
	content := strings.Replace(validYAML(model), "/test/llama-server", binary, 1)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	admission := cfg.Capacity.Admission
	if admission.Enabled || admission.OnInsufficient != "wait" || admission.OnUnknown != "allow" || admission.WaitDuration().String() != "5m0s" || admission.ReevaluationDuration().String() != "2s" || admission.SnapshotAgeDuration().String() != "6s" {
		t.Fatalf("admission defaults = %+v", admission)
	}
}

func TestLoadRejectsAdmissionEnumsAndDurations(t *testing.T) {
	dir := t.TempDir()
	model := filepath.Join(dir, "model.gguf")
	binary := filepath.Join(dir, "llama-server")
	_ = os.WriteFile(model, []byte("model"), 0600)
	_ = os.WriteFile(binary, []byte("binary"), 0755)
	for _, admission := range []string{
		"on_unknown: sometimes",
		"on_insufficient: later",
		"wait_timeout: never",
	} {
		path := filepath.Join(dir, "invalid.yaml")
		content := strings.Replace(validYAML(model), "/test/llama-server", binary, 1)
		content = strings.Replace(content, "top_p: 0.9", "top_p: 0.9\ncapacity:\n  admission:\n    "+admission, 1)
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Fatalf("accepted invalid admission value %q", admission)
		}
	}
}

func validYAML(modelPath string) string {
	return "runtime:\n  binary: /test/llama-server\nmodels:\n  test:\n    path: " + modelPath + "\n    port: 18069\n    context: 4096\n    batch: 512\n    ubatch: 128\n    gpu_layers: all\n    parallel: 1\n    top_p: 0.9\n"
}

func writeHelpBinary(t *testing.T, dir, flags string) string {
	t.Helper()
	path := filepath.Join(dir, "llama-server")
	script := "#!/bin/sh\nif [ \"$1\" = \"--help\" ]; then\n  echo '" + flags + "'\nfi\n"
	if err := os.WriteFile(path, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	return path
}
