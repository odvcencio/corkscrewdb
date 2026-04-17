package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newServerHarness spins an in-process primary + clients, composes the
// top-level handler via NewHandler, mounts it on httptest.NewServer, and
// returns the server URL + tokens used for auth. Cleanup tears everything
// down on test exit (listener, clients, primary).
func newServerHarness(t *testing.T) (url, agentTok, adminTok string) {
	t.Helper()
	addr, dbtok := newTestPrimary(t)
	const (
		agent = "agent-bearer"
		admin = "admin-bearer"
	)
	cfg := Config{
		AddrRW:             addr,
		AddrRO:             addr,
		CorkscrewDBToken:   dbtok,
		Listen:             "127.0.0.1:0",
		AgentTokens:        map[string]string{"birch": agent},
		AdminToken:         admin,
		ExpectedProviderID: "manta-embed-v0",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clients, err := NewDBClients(ctx, cfg)
	if err != nil {
		t.Fatalf("NewDBClients: %v", err)
	}
	t.Cleanup(func() { _ = clients.Close() })

	handler := NewHandler(cfg, clients, nil)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	return ts.URL, agent, admin
}

// doReq issues an HTTP request against the harness and returns the
// response + decoded JSON body (or nil on empty/non-JSON body).
func doReq(t *testing.T, method, url, bearer string, body any) (*http.Response, map[string]any, string) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			reader = strings.NewReader(b)
		default:
			buf, err := json.Marshal(body)
			if err != nil {
				t.Fatalf("marshal body: %v", err)
			}
			reader = bytes.NewReader(buf)
		}
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do req: %v", err)
	}
	raw, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return resp, out, string(raw)
}

func TestServer_HealthzUnauthenticated(t *testing.T) {
	url, _, _ := newServerHarness(t)
	resp, body, raw := doReq(t, http.MethodGet, url+"/healthz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", resp.StatusCode, raw)
	}
	if ok, _ := body["ok"].(bool); !ok {
		t.Errorf("ok=%v want true body=%s", body["ok"], raw)
	}
}

func TestServer_ReadyzFlipsOnBootstrap(t *testing.T) {
	url, _, admin := newServerHarness(t)

	// Pre-bootstrap: agent-memory not materialized → 503.
	resp, _, raw := doReq(t, http.MethodGet, url+"/readyz", "", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("pre-bootstrap /readyz status=%d want 503 body=%s", resp.StatusCode, raw)
	}

	// Provision agent-memory via admin API.
	resp, _, raw = doReq(t, http.MethodPost, url+"/admin/collections", admin, map[string]any{
		"name":      "agent-memory",
		"bit_width": 2,
		"retention": "keep-all",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin create status=%d want 200 body=%s", resp.StatusCode, raw)
	}

	// Post-bootstrap: /readyz → 200.
	resp, body, raw := doReq(t, http.MethodGet, url+"/readyz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post-bootstrap /readyz status=%d want 200 body=%s", resp.StatusCode, raw)
	}
	if ok, _ := body["ok"].(bool); !ok {
		t.Errorf("ok=%v want true", body["ok"])
	}
}

func TestServer_AgentAuthScope(t *testing.T) {
	url, agent, admin := newServerHarness(t)

	// Bootstrap agent-memory so the rest of the plumbing has a collection.
	resp, _, raw := doReq(t, http.MethodPost, url+"/admin/collections", admin, map[string]any{
		"name":      "agent-memory",
		"bit_width": 2,
		"retention": "keep-all",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap status=%d body=%s", resp.StatusCode, raw)
	}

	// POST /memories with agent bearer → 200.
	resp, body, raw := doReq(t, http.MethodPost, url+"/memories", agent, map[string]any{
		"collection": "agent-memory",
		"text":       "hello world",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /memories status=%d want 200 body=%s", resp.StatusCode, raw)
	}
	id, _ := body["id"].(string)
	if id == "" {
		t.Fatalf("empty id in response body=%s", raw)
	}

	// GET the entry back.
	resp, _, raw = doReq(t, http.MethodGet, url+"/memories/agent-memory/"+id, agent, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /memories status=%d want 200 body=%s", resp.StatusCode, raw)
	}

	// POST /memories with ADMIN bearer → 401 (wrong scope).
	resp, _, raw = doReq(t, http.MethodPost, url+"/memories", admin, map[string]any{
		"collection": "agent-memory",
		"text":       "should-fail",
	})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("admin on /memories status=%d want 401 body=%s", resp.StatusCode, raw)
	}

	// POST /admin/collections with AGENT bearer → 401 (wrong scope).
	resp, _, raw = doReq(t, http.MethodPost, url+"/admin/collections", agent, map[string]any{
		"name":      "should-fail",
		"bit_width": 2,
		"retention": "keep-all",
	})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("agent on /admin/collections status=%d want 401 body=%s", resp.StatusCode, raw)
	}
}

func TestServer_SearchNeedsBearer(t *testing.T) {
	url, agent, admin := newServerHarness(t)

	// Bootstrap + write so search has data.
	resp, _, raw := doReq(t, http.MethodPost, url+"/admin/collections", admin, map[string]any{
		"name":      "agent-memory",
		"bit_width": 2,
		"retention": "keep-all",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap status=%d body=%s", resp.StatusCode, raw)
	}
	resp, _, raw = doReq(t, http.MethodPost, url+"/memories", agent, map[string]any{
		"collection": "agent-memory",
		"text":       "lorem ipsum dolor sit amet",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed post status=%d body=%s", resp.StatusCode, raw)
	}

	// GET /memories/search with valid agent bearer → 200.
	resp, _, raw = doReq(t, http.MethodGet,
		url+"/memories/search?collection=agent-memory&q=lorem", agent, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("search status=%d want 200 body=%s", resp.StatusCode, raw)
	}

	// GET /memories/search with NO bearer → 401.
	resp, _, raw = doReq(t, http.MethodGet,
		url+"/memories/search?collection=agent-memory&q=lorem", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-bearer search status=%d want 401 body=%s", resp.StatusCode, raw)
	}
}

func TestServer_AdminShardsOK(t *testing.T) {
	url, _, admin := newServerHarness(t)
	resp, body, raw := doReq(t, http.MethodGet, url+"/admin/shards", admin, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", resp.StatusCode, raw)
	}
	if _, ok := body["shards"]; !ok {
		t.Errorf("missing shards field body=%s", raw)
	}
}
