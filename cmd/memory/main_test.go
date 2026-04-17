package main

import (
	"os"
	"testing"
)

func TestLoadConfigFromEnv(t *testing.T) {
	t.Setenv("CORKSCREWDB_ADDR_RW", "corkscrewdb-rw.m31labs.svc.cluster.local:4040")
	t.Setenv("CORKSCREWDB_ADDR_RO", "corkscrewdb-ro.m31labs.svc.cluster.local:4040")
	t.Setenv("CORKSCREWDB_TOKEN", "gRPC-token")
	t.Setenv("MEMORY_LISTEN", "0.0.0.0:8080")
	t.Setenv("MEMORY_AGENT_TOKENS", `{"birch":"tok-a","cedar":"tok-b"}`)
	t.Setenv("MEMORY_ADMIN_TOKEN", "admin-tok")
	t.Setenv("EXPECTED_PROVIDER_ID", "manta-embed-v0")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AddrRW != "corkscrewdb-rw.m31labs.svc.cluster.local:4040" {
		t.Fatalf("addr_rw: %s", cfg.AddrRW)
	}
	if cfg.AddrRO != "corkscrewdb-ro.m31labs.svc.cluster.local:4040" {
		t.Fatalf("addr_ro: %s", cfg.AddrRO)
	}
	if cfg.CorkscrewDBToken != "gRPC-token" {
		t.Fatalf("grpc token: %s", cfg.CorkscrewDBToken)
	}
	if cfg.Listen != "0.0.0.0:8080" {
		t.Fatalf("listen: %s", cfg.Listen)
	}
	if cfg.AdminToken != "admin-tok" {
		t.Fatalf("admin: %s", cfg.AdminToken)
	}
	if cfg.ExpectedProviderID != "manta-embed-v0" {
		t.Fatalf("provider id: %s", cfg.ExpectedProviderID)
	}
	if len(cfg.AgentTokens) != 2 || cfg.AgentTokens["birch"] != "tok-a" || cfg.AgentTokens["cedar"] != "tok-b" {
		t.Fatalf("agent tokens: %v", cfg.AgentTokens)
	}
}

func TestLoadConfigDefaultsListenAndProviderID(t *testing.T) {
	os.Clearenv()
	t.Setenv("CORKSCREWDB_ADDR_RW", "rw:4040")
	t.Setenv("CORKSCREWDB_ADDR_RO", "ro:4040")
	t.Setenv("CORKSCREWDB_TOKEN", "g")
	t.Setenv("MEMORY_AGENT_TOKENS", `{"birch":"t"}`)
	t.Setenv("MEMORY_ADMIN_TOKEN", "a")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "0.0.0.0:8080" {
		t.Fatalf("default listen: %s", cfg.Listen)
	}
	if cfg.ExpectedProviderID != "manta-embed-v0" {
		t.Fatalf("default provider: %s", cfg.ExpectedProviderID)
	}
}

func TestLoadConfigMissingRequired(t *testing.T) {
	cases := []struct {
		name  string
		unset string
	}{
		{"missing rw", "CORKSCREWDB_ADDR_RW"},
		{"missing ro", "CORKSCREWDB_ADDR_RO"},
		{"missing grpc token", "CORKSCREWDB_TOKEN"},
		{"missing admin token", "MEMORY_ADMIN_TOKEN"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			os.Clearenv()
			t.Setenv("CORKSCREWDB_ADDR_RW", "rw:4040")
			t.Setenv("CORKSCREWDB_ADDR_RO", "ro:4040")
			t.Setenv("CORKSCREWDB_TOKEN", "g")
			t.Setenv("MEMORY_AGENT_TOKENS", `{"birch":"t"}`)
			t.Setenv("MEMORY_ADMIN_TOKEN", "a")
			os.Unsetenv(tc.unset)
			if _, err := LoadConfig(); err == nil {
				t.Fatalf("expected error when %s unset", tc.unset)
			}
		})
	}
}

func TestLoadConfigEmptyAgentTokens(t *testing.T) {
	os.Clearenv()
	t.Setenv("CORKSCREWDB_ADDR_RW", "rw:4040")
	t.Setenv("CORKSCREWDB_ADDR_RO", "ro:4040")
	t.Setenv("CORKSCREWDB_TOKEN", "g")
	t.Setenv("MEMORY_ADMIN_TOKEN", "a")
	// MEMORY_AGENT_TOKENS missing
	if _, err := LoadConfig(); err == nil {
		t.Fatal("expected error when MEMORY_AGENT_TOKENS has no agents")
	}
	t.Setenv("MEMORY_AGENT_TOKENS", "{}")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("expected error when MEMORY_AGENT_TOKENS is empty map")
	}
}

func TestLoadConfigMalformedAgentTokens(t *testing.T) {
	os.Clearenv()
	t.Setenv("CORKSCREWDB_ADDR_RW", "rw:4040")
	t.Setenv("CORKSCREWDB_ADDR_RO", "ro:4040")
	t.Setenv("CORKSCREWDB_TOKEN", "g")
	t.Setenv("MEMORY_ADMIN_TOKEN", "a")
	t.Setenv("MEMORY_AGENT_TOKENS", "not json")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("expected error on malformed MEMORY_AGENT_TOKENS")
	}
}
