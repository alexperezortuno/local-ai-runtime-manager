package models

import (
	"fmt"

	"local-ai-runtime/internal/config"
)

type Registry struct {
	models map[string]config.ModelConfig
}

func NewRegistry(configured map[string]config.ModelConfig) *Registry {
	copy := make(map[string]config.ModelConfig, len(configured))
	for id, model := range configured {
		model.ExtraArgs = append([]string(nil), model.ExtraArgs...)
		copy[id] = model
	}
	return &Registry{models: copy}
}

func (r *Registry) Get(id string) (config.ModelConfig, error) {
	model, ok := r.models[id]
	if !ok {
		return config.ModelConfig{}, fmt.Errorf("unknown model %q", id)
	}
	return model, nil
}

func (r *Registry) List() map[string]config.ModelConfig {
	copy := make(map[string]config.ModelConfig, len(r.models))
	for id, model := range r.models {
		model.ExtraArgs = append([]string(nil), model.ExtraArgs...)
		copy[id] = model
	}
	return copy
}
