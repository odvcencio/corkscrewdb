package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/odvcencio/corkscrewdb"
)

// newHealthHandlerAndPrimary spins an in-process primary, optionally
// materializes the agent-memory collection with a real Put, wires a
// *DBClients at the primary, and returns the handler plus the underlying
// listener so tests that want to simulate a hung server can close it.
//
// materializeAgentMemory=true models the post-bootstrap steady state
// where the readiness probe must report OK. false models pre-bootstrap
// where the probe must report 503.
func newHealthHandlerAndPrimary(t *testing.T, materializeAgentMemory bool) (*HealthHandler, *DBClients, net.Listener) {
	t.Helper()
	const token = "test-token"

	dir := filepath.Join(t.TempDir(), "primary.csdb")
	db, err := corkscrewdb.Open(dir, corkscrewdb.WithToken(token))
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}

	if materializeAgentMemory {
		// Use the server-side DB directly to materialize agent-memory with
		// a real record. After this point the collection appears in
		// RemoteInfo().Collections, which is what the probe checks.
		if err := db.Collection("agent-memory").Put("__bootstrap__", corkscrewdb.Entry{Text: "{}"}); err != nil {
			_ = db.Close()
			t.Fatalf("bootstrap agent-memory: %v", err)
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = db.Close()
		t.Fatalf("listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- db.Serve(listener) }()

	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("primary.Serve did not exit within 2s")
		}
		_ = db.Close()
	})

	cfg := Config{
		AddrRW:             listener.Addr().String(),
		AddrRO:             listener.Addr().String(),
		CorkscrewDBToken:   token,
		ExpectedProviderID: "manta-embed-v0",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clients, err := NewDBClients(ctx, cfg)
	if err != nil {
		t.Fatalf("NewDBClients: %v", err)
	}
	t.Cleanup(func() { _ = clients.Close() })

	return NewHealthHandler(clients, nil), clients, listener
}

// do issues an in-process request against the supplied handler method.
func doHealth(h http.HandlerFunc, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func decodeHealthBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if rec.Body.Len() == 0 {
		return out
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	return out
}

func TestHealthHandlers_HealthzAlwaysOK(t *testing.T) {
	// Liveness must not depend on external state — a nil *DBClients is
	// deliberately passed so the handler can't accidentally reach into
	// anything.
	h := NewHealthHandler(nil, nil)
	rec := doHealth(h.handleHealth, http.MethodGet, "/healthz")

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") || !strings.Contains(ct, "charset=utf-8") {
		t.Errorf("content-type=%q", ct)
	}
	body := decodeHealthBody(t, rec)
	if ok, _ := body["ok"].(bool); !ok {
		t.Errorf("ok=%v want true", body["ok"])
	}
}

func TestHealthHandlers_ReadyzHealthy(t *testing.T) {
	h, _, _ := newHealthHandlerAndPrimary(t, true)

	start := time.Now()
	rec := doHealth(h.handleReady, http.MethodGet, "/readyz")
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if elapsed > 1200*time.Millisecond {
		t.Errorf("healthy readyz took %v, want <=1.2s", elapsed)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type=%q", ct)
	}
	body := decodeHealthBody(t, rec)
	if ok, _ := body["ok"].(bool); !ok {
		t.Errorf("ok=%v want true (body=%s)", body["ok"], rec.Body.String())
	}
}

func TestHealthHandlers_ReadyzNilClients(t *testing.T) {
	h := NewHealthHandler(nil, nil)
	rec := doHealth(h.handleReady, http.MethodGet, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	body := decodeHealthBody(t, rec)
	if ok, _ := body["ok"].(bool); ok {
		t.Errorf("ok=%v want false", body["ok"])
	}
	if _, ok := body["reason"].(string); !ok {
		t.Errorf("reason missing: %v", body)
	}
}

func TestHealthHandlers_ReadyzRWInfoFails(t *testing.T) {
	// Construct the clients against a working primary, then kill its
	// listener. Subsequent RemoteInfo calls must fail, tripping the
	// "rw info" branch of the readiness probe.
	h, _, listener := newHealthHandlerAndPrimary(t, true)
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	// Small settle so the connection notices the listener is gone. The
	// readiness probe itself enforces the 1s timeout so we don't need to
	// wait for the full TCP keepalive.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	rec := doHealth(h.handleReady, http.MethodGet, "/readyz")
	elapsed := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	if elapsed > 1200*time.Millisecond {
		t.Errorf("failing readyz took %v, want <=1.2s (1s probe budget + slack)", elapsed)
	}
	body := decodeHealthBody(t, rec)
	if ok, _ := body["ok"].(bool); ok {
		t.Errorf("ok=%v want false", body["ok"])
	}
	reason, _ := body["reason"].(string)
	if !strings.Contains(reason, "rw info") {
		t.Errorf("reason=%q want to contain 'rw info'", reason)
	}
}

func TestHealthHandlers_ReadyzAgentMemoryMissing(t *testing.T) {
	// agent-memory is NOT materialized — the probe must fail with a
	// reason mentioning "agent-memory". RemoteInfo itself succeeds.
	h, _, _ := newHealthHandlerAndPrimary(t, false)

	rec := doHealth(h.handleReady, http.MethodGet, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	body := decodeHealthBody(t, rec)
	if ok, _ := body["ok"].(bool); ok {
		t.Errorf("ok=%v want false", body["ok"])
	}
	reason, _ := body["reason"].(string)
	if !strings.Contains(reason, "agent-memory") {
		t.Errorf("reason=%q want to contain 'agent-memory'", reason)
	}
}

func TestHealthHandlers_ReadyzRespectsTimeout(t *testing.T) {
	// Drive the handler with a tightened timeout to confirm the
	// context-based deadline is honored — a health handler with a
	// 10ms probe budget against a working primary is still likely to
	// succeed, so we instead point it at a non-responsive listener to
	// exercise the timeout path deterministically.
	h, _, listener := newHealthHandlerAndPrimary(t, true)

	// Override the probe budget to 200ms so the test runs fast.
	h.timeout = 200 * time.Millisecond

	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	start := time.Now()
	rec := doHealth(h.handleReady, http.MethodGet, "/readyz")
	elapsed := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	// 200ms budget + scheduling slack. Cap well under 1s so a
	// regression that ignores the context is loud.
	if elapsed > 800*time.Millisecond {
		t.Errorf("readyz took %v with 200ms timeout, want <=800ms", elapsed)
	}
}
