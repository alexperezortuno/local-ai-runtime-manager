package backend

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"

	"local-ai-runtime/internal/config"
)

func TestArgsHostDefaultsToLoopback(t *testing.T) {
	if got := Args(config.ModelConfig{}); !slices.Contains(got, "127.0.0.1") {
		t.Fatalf("default host = %v", got)
	}
	args := Args(config.ModelConfig{Host: "192.169.0.228"})
	index := slices.Index(args, "--host")
	if index < 0 || index+1 >= len(args) || args[index+1] != "192.169.0.228" {
		t.Fatalf("configured host = %v", args)
	}
}

func TestArgsUsesConfiguredLoopbackHostAndPort(t *testing.T) {
	args := Args(config.ModelConfig{Host: "127.0.0.1", Port: 8060})
	hostIndex := slices.Index(args, "--host")
	portIndex := slices.Index(args, "--port")
	if hostIndex < 0 || hostIndex+1 >= len(args) || args[hostIndex+1] != "127.0.0.1" {
		t.Fatalf("configured host = %v", args)
	}
	if portIndex < 0 || portIndex+1 >= len(args) || args[portIndex+1] != "8060" {
		t.Fatalf("configured port = %v", args)
	}
	if slices.Contains(args, "0.0.0.0") {
		t.Fatalf("wildcard bind in args = %v", args)
	}
}

func TestHealthyAtUsesConfiguredLoopback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Fatalf("health path = %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	address := server.Listener.Addr().String()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	if host != "127.0.0.1" || !HealthyAt(context.Background(), host, portNumber) {
		t.Fatalf("health check did not use loopback: host=%q port=%d", host, portNumber)
	}
}

func TestArgsSamplingParameters(t *testing.T) {
	temperature := 0.2
	topP := 0.9
	topK := 40
	zero := 0

	for _, test := range []struct {
		name  string
		model config.ModelConfig
		want  []string
	}{
		{name: "all omitted"},
		{name: "temperature only", model: config.ModelConfig{Temperature: &temperature}, want: []string{"--temp", "0.2"}},
		{name: "top_p only", model: config.ModelConfig{TopP: &topP}, want: []string{"--top-p", "0.9"}},
		{name: "top_k only", model: config.ModelConfig{TopK: &topK}, want: []string{"--top-k", "40"}},
		{name: "top_k zero", model: config.ModelConfig{TopK: &zero}, want: []string{"--top-k", "0"}},
		{name: "all explicit", model: config.ModelConfig{Temperature: &temperature, TopP: &topP, TopK: &topK}, want: []string{"--temp", "0.2", "--top-p", "0.9", "--top-k", "40"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := Args(test.model)
			for _, flag := range []string{"--temp", "--top-p", "--top-k"} {
				if got := slices.Contains(args, flag); got != slices.Contains(test.want, flag) {
					t.Fatalf("flag %s presence = %t, args = %v", flag, got, args)
				}
			}
			for pairIndex := 0; pairIndex+1 < len(test.want); pairIndex += 2 {
				flag, expected := test.want[pairIndex], test.want[pairIndex+1]
				index := slices.Index(args, flag)
				if index+1 >= len(args) || args[index+1] != expected {
					t.Fatalf("flag %s value = %v, want %q, args = %v", flag, args[min(index+1, len(args)):], expected, args)
				}
			}
		})
	}
}

func TestArgsExtendedLlamaParameters(t *testing.T) {
	presence := 0.25
	frequency := -0.1
	nPredict := 0
	repeat := 1.1
	flash := "on"
	device := "Metal"
	ngl := "all"
	split := "layer"
	metrics := true
	fit := false
	cachePrompt := false
	noNMProj := true

	args := Args(config.ModelConfig{
		GPULayers:        "",
		NGPULayers:       &ngl,
		PresencePenalty:  &presence,
		FrequencyPenalty: &frequency,
		NPredict:         &nPredict,
		RepeatPenalty:    &repeat,
		FlashAttn:        &flash,
		Device:           &device,
		SplitMode:        &split,
		Metrics:          &metrics,
		Fit:              &fit,
		CachePrompt:      &cachePrompt,
		NoNMProj:         &noNMProj,
	})
	want := []string{
		"-ngl", "all",
		"--presence-penalty", "0.25",
		"--frequency-penalty", "-0.1",
		"--n-predict", "0",
		"--repeat-penalty", "1.1",
		"--flash-attn", "on",
		"--device", "Metal",
		"--split-mode", "layer",
		"--metrics",
		"--fit", "off",
		"--no-cache-prompt",
		"--no-mmproj",
	}
	assertSubsequence(t, args, want)
	for _, flag := range []string{"--gpu-layers", "--n-gpu-layers", "--no-cache-prompt", "--no-mmproj"} {
		if count(args, flag) > 1 {
			t.Fatalf("flag %s duplicated in %v", flag, args)
		}
	}
}

func TestArgsExtendedLlamaParametersIndividually(t *testing.T) {
	presence := 0.2
	frequency := 0.3
	nPredict := 0
	repeat := 1.1
	flash := "auto"
	device := "none"
	ngl := "0"
	split := "none"
	metrics := true
	fit := false
	cachePrompt := false
	noNMProj := false
	for _, test := range []struct {
		name  string
		model config.ModelConfig
		want  []string
	}{
		{"presence penalty", config.ModelConfig{PresencePenalty: &presence}, []string{"--presence-penalty", "0.2"}},
		{"frequency penalty", config.ModelConfig{FrequencyPenalty: &frequency}, []string{"--frequency-penalty", "0.3"}},
		{"n predict zero", config.ModelConfig{NPredict: &nPredict}, []string{"--n-predict", "0"}},
		{"repeat penalty", config.ModelConfig{RepeatPenalty: &repeat}, []string{"--repeat-penalty", "1.1"}},
		{"flash attention", config.ModelConfig{FlashAttn: &flash}, []string{"--flash-attn", "auto"}},
		{"device", config.ModelConfig{Device: &device}, []string{"--device", "none"}},
		{"n gpu layers", config.ModelConfig{NGPULayers: &ngl}, []string{"-ngl", "0"}},
		{"split mode", config.ModelConfig{SplitMode: &split}, []string{"--split-mode", "none"}},
		{"metrics", config.ModelConfig{Metrics: &metrics}, []string{"--metrics"}},
		{"fit false", config.ModelConfig{Fit: &fit}, []string{"--fit", "off"}},
		{"cache prompt false", config.ModelConfig{CachePrompt: &cachePrompt}, []string{"--no-cache-prompt"}},
		{"no nmproj false", config.ModelConfig{NoNMProj: &noNMProj}, []string{"--mmproj-auto"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertSubsequence(t, Args(test.model), test.want)
		})
	}
}

func TestArgsExplicitFalseAndZeroRemainDistinct(t *testing.T) {
	zero := 0.0
	nPredict := 0
	falseValue := false
	args := Args(config.ModelConfig{
		PresencePenalty: &zero,
		NPredict:        &nPredict,
		Metrics:         &falseValue,
	})
	assertSubsequence(t, args, []string{"--presence-penalty", "0", "--n-predict", "0"})
	if slices.Contains(args, "--metrics") {
		t.Fatalf("false metrics generated an enabling flag: %v", args)
	}
}

func assertSubsequence(t *testing.T, args, want []string) {
	t.Helper()
	start := 0
	for _, expected := range want {
		index := slices.Index(args[start:], expected)
		if index < 0 {
			t.Fatalf("args = %v, missing %q after index %d", args, expected, start)
		}
		start += index + 1
	}
}

func count(args []string, value string) int {
	result := 0
	for _, arg := range args {
		if arg == value {
			result++
		}
	}
	return result
}
