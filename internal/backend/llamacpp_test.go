package backend

import (
	"slices"
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
