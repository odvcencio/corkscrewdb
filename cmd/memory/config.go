package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config holds environment-driven configuration for the memory service.
type Config struct {
	AddrRW             string
	AddrRO             string
	CorkscrewDBToken   string
	Listen             string
	AgentTokens        map[string]string // agent name → bearer token
	AdminToken         string
	ExpectedProviderID string
}

// LoadConfig reads the memory service's required environment variables
// and returns a validated Config. It returns an error if any required
// variable is unset or if MEMORY_AGENT_TOKENS is malformed or empty.
func LoadConfig() (Config, error) {
	cfg := Config{
		AddrRW:             os.Getenv("CORKSCREWDB_ADDR_RW"),
		AddrRO:             os.Getenv("CORKSCREWDB_ADDR_RO"),
		CorkscrewDBToken:   os.Getenv("CORKSCREWDB_TOKEN"),
		Listen:             os.Getenv("MEMORY_LISTEN"),
		AdminToken:         os.Getenv("MEMORY_ADMIN_TOKEN"),
		ExpectedProviderID: os.Getenv("EXPECTED_PROVIDER_ID"),
	}
	if raw := os.Getenv("MEMORY_AGENT_TOKENS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &cfg.AgentTokens); err != nil {
			return Config{}, fmt.Errorf("MEMORY_AGENT_TOKENS: %w", err)
		}
	}
	if cfg.Listen == "" {
		cfg.Listen = "0.0.0.0:8080"
	}
	if cfg.ExpectedProviderID == "" {
		cfg.ExpectedProviderID = "manta-embed-v0"
	}
	required := map[string]string{
		"CORKSCREWDB_ADDR_RW": cfg.AddrRW,
		"CORKSCREWDB_ADDR_RO": cfg.AddrRO,
		"CORKSCREWDB_TOKEN":   cfg.CorkscrewDBToken,
		"MEMORY_ADMIN_TOKEN":  cfg.AdminToken,
	}
	for k, v := range required {
		if v == "" {
			return Config{}, fmt.Errorf("%s is required", k)
		}
	}
	if len(cfg.AgentTokens) == 0 {
		return Config{}, fmt.Errorf("MEMORY_AGENT_TOKENS must declare at least one agent")
	}
	return cfg, nil
}
