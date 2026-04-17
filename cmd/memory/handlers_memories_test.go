package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// newMemoriesHandler spins up an in-process CorkScrewDB primary via the
// same newTestPrimary helper used in dbclient_test.go, wires a
// *DBClients with rw+ro pointing at it, and returns a fresh handler.
func newMemoriesHandler(t *testing.T) *MemoriesHandler {
	t.Helper()
	addr, token := newTestPrimary(t)
	cfg := Config{
		AddrRW:             addr,
		AddrRO:             addr,
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
	return NewMemoriesHandler(clients, nil)
}

// withAgent returns a copy of r whose context has agentContextKey set to
// name. Use "" to simulate a missing-agent request.
func withAgent(r *http.Request, name string) *http.Request {
	if name == "" {
		return r
	}
	ctx := context.WithValue(r.Context(), agentContextKey{}, name)
	return r.WithContext(ctx)
}

// postMemory runs a POST /memories against h with the given body and
// agent. Returns the decoded JSON body and the raw response for status /
// header inspection. On non-200 responses the body map may contain only
// {"error": ...}.
func postMemory(t *testing.T, h *MemoriesHandler, agent string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	switch b := body.(type) {
	case string:
		buf.WriteString(b)
	default:
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/memories", &buf)
	req = withAgent(req, agent)
	rec := httptest.NewRecorder()
	h.handlePost(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			// Non-JSON body; return empty map so callers can still check status.
			return rec, nil
		}
	}
	return rec, out
}

func TestMemoriesHandlers_PostAutoID(t *testing.T) {
	h := newMemoriesHandler(t)
	rec, body := postMemory(t, h, "birch", map[string]any{
		"collection": "t",
		"text":       "hello",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type = %q", ct)
	}
	id, _ := body["id"].(string)
	if id == "" {
		t.Fatalf("empty id; body=%v", body)
	}
	hlc, _ := body["hlc"].(float64)
	if hlc == 0 {
		t.Fatalf("zero hlc; body=%v", body)
	}
}

func TestMemoriesHandlers_PostExplicitIDTwiceDifferentHLC(t *testing.T) {
	h := newMemoriesHandler(t)
	_, body1 := postMemory(t, h, "birch", map[string]any{
		"collection": "t",
		"id":         "doc-1",
		"text":       "v1",
	})
	if got, _ := body1["id"].(string); got != "doc-1" {
		t.Fatalf("first id=%v want doc-1", body1["id"])
	}
	hlc1, _ := body1["hlc"].(float64)
	if hlc1 == 0 {
		t.Fatalf("zero hlc1")
	}

	_, body2 := postMemory(t, h, "birch", map[string]any{
		"collection": "t",
		"id":         "doc-1",
		"text":       "v2",
	})
	if got, _ := body2["id"].(string); got != "doc-1" {
		t.Fatalf("second id=%v want doc-1", body2["id"])
	}
	hlc2, _ := body2["hlc"].(float64)
	if hlc2 == 0 || hlc2 == hlc1 {
		t.Fatalf("hlc2=%v hlc1=%v; expected distinct non-zero", hlc2, hlc1)
	}
}

func TestMemoriesHandlers_GetLatestAndPointInTime(t *testing.T) {
	h := newMemoriesHandler(t)
	_, body1 := postMemory(t, h, "birch", map[string]any{
		"collection": "t",
		"id":         "doc-1",
		"text":       "v1",
	})
	hlc1 := uint64(body1["hlc"].(float64))

	_, body2 := postMemory(t, h, "birch", map[string]any{
		"collection": "t",
		"id":         "doc-1",
		"text":       "v2",
	})
	hlc2 := uint64(body2["hlc"].(float64))
	if hlc2 <= hlc1 {
		t.Fatalf("hlc2=%d hlc1=%d", hlc2, hlc1)
	}

	// GET latest
	req := httptest.NewRequest(http.MethodGet, "/memories/t/doc-1", nil)
	req.SetPathValue("collection", "t")
	req.SetPathValue("id", "doc-1")
	req = withAgent(req, "birch")
	rec := httptest.NewRecorder()
	h.handleGet(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("latest status=%d body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["text"] != "v2" {
		t.Errorf("latest text=%v want v2", got["text"])
	}

	// GET at=hlc1 — should return the earlier version (v1).
	req2 := httptest.NewRequest(http.MethodGet, "/memories/t/doc-1?at="+strconv.FormatUint(hlc1, 10), nil)
	req2.SetPathValue("collection", "t")
	req2.SetPathValue("id", "doc-1")
	req2 = withAgent(req2, "birch")
	rec2 := httptest.NewRecorder()
	h.handleGet(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("at= status=%d body=%s", rec2.Code, rec2.Body.String())
	}
	var got2 map[string]any
	if err := json.Unmarshal(rec2.Body.Bytes(), &got2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got2["text"] != "v1" {
		t.Errorf("at= text=%v want v1", got2["text"])
	}
}

func TestMemoriesHandlers_Versions(t *testing.T) {
	h := newMemoriesHandler(t)
	postMemory(t, h, "birch", map[string]any{"collection": "t", "id": "doc-1", "text": "v1"})
	postMemory(t, h, "birch", map[string]any{"collection": "t", "id": "doc-1", "text": "v2"})

	req := httptest.NewRequest(http.MethodGet, "/memories/t/doc-1/versions", nil)
	req.SetPathValue("collection", "t")
	req.SetPathValue("id", "doc-1")
	req = withAgent(req, "birch")
	rec := httptest.NewRecorder()
	h.handleVersions(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Versions []map[string]any `json:"versions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Versions) < 2 {
		t.Fatalf("versions len=%d, want >=2: %+v", len(out.Versions), out.Versions)
	}
	// Ordered ascending by HLC.
	prev := 0.0
	for i, v := range out.Versions {
		hlc, _ := v["hlc"].(float64)
		if i > 0 && hlc < prev {
			t.Errorf("versions not HLC-ordered at %d: %v < %v", i, hlc, prev)
		}
		prev = hlc
	}
}

func TestMemoriesHandlers_DeleteThenGet404(t *testing.T) {
	h := newMemoriesHandler(t)
	postMemory(t, h, "birch", map[string]any{"collection": "t", "id": "doc-1", "text": "hello"})

	// DELETE
	req := httptest.NewRequest(http.MethodDelete, "/memories/t/doc-1", nil)
	req.SetPathValue("collection", "t")
	req.SetPathValue("id", "doc-1")
	req = withAgent(req, "birch")
	rec := httptest.NewRecorder()
	h.handleDelete(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%s", rec.Code, rec.Body.String())
	}

	// Follow-up GET → 404
	req2 := httptest.NewRequest(http.MethodGet, "/memories/t/doc-1", nil)
	req2.SetPathValue("collection", "t")
	req2.SetPathValue("id", "doc-1")
	req2 = withAgent(req2, "birch")
	rec2 := httptest.NewRecorder()
	h.handleGet(rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("get-after-delete status=%d body=%s", rec2.Code, rec2.Body.String())
	}
	if ct := rec2.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("error content-type = %q", ct)
	}
}

func TestMemoriesHandlers_ResurrectAfterDelete(t *testing.T) {
	h := newMemoriesHandler(t)
	postMemory(t, h, "birch", map[string]any{"collection": "t", "id": "doc-1", "text": "alive"})
	// delete
	req := httptest.NewRequest(http.MethodDelete, "/memories/t/doc-1", nil)
	req.SetPathValue("collection", "t")
	req.SetPathValue("id", "doc-1")
	req = withAgent(req, "birch")
	rec := httptest.NewRecorder()
	h.handleDelete(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d", rec.Code)
	}
	// resurrect via POST
	rec2, body := postMemory(t, h, "birch", map[string]any{"collection": "t", "id": "doc-1", "text": "back"})
	if rec2.Code != http.StatusOK {
		t.Fatalf("resurrect status=%d body=%s", rec2.Code, rec2.Body.String())
	}
	if body["id"] != "doc-1" {
		t.Fatalf("resurrect id=%v want doc-1", body["id"])
	}
	// GET works again
	req3 := httptest.NewRequest(http.MethodGet, "/memories/t/doc-1", nil)
	req3.SetPathValue("collection", "t")
	req3.SetPathValue("id", "doc-1")
	req3 = withAgent(req3, "birch")
	rec3 := httptest.NewRecorder()
	h.handleGet(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("get-after-resurrect status=%d body=%s", rec3.Code, rec3.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(rec3.Body.Bytes(), &got)
	if got["text"] != "back" {
		t.Errorf("resurrect text=%v want back", got["text"])
	}
}

func TestMemoriesHandlers_PostMissingAgent500(t *testing.T) {
	h := newMemoriesHandler(t)
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(map[string]any{"collection": "t", "text": "hello"})
	req := httptest.NewRequest(http.MethodPost, "/memories", &buf)
	// No agent injected.
	rec := httptest.NewRecorder()
	h.handlePost(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type = %q", ct)
	}
	var got map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["error"] != "internal" {
		t.Errorf("error=%q want internal", got["error"])
	}
}

func TestMemoriesHandlers_ReadStalenessHeader(t *testing.T) {
	h := newMemoriesHandler(t)
	postMemory(t, h, "birch", map[string]any{"collection": "t", "id": "doc-1", "text": "hello"})
	req := httptest.NewRequest(http.MethodGet, "/memories/t/doc-1", nil)
	req.SetPathValue("collection", "t")
	req.SetPathValue("id", "doc-1")
	req.Header.Set("X-Read-Staleness", "replica-ok")
	req = withAgent(req, "birch")
	rec := httptest.NewRecorder()
	h.handleGet(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestMemoriesHandlers_PostMalformedJSON400(t *testing.T) {
	h := newMemoriesHandler(t)
	rec, _ := postMemory(t, h, "birch", "not-json{")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type = %q", ct)
	}
}

func TestMemoriesHandlers_PostMissingCollection400(t *testing.T) {
	h := newMemoriesHandler(t)
	rec, _ := postMemory(t, h, "birch", map[string]any{"text": "hello"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rec.Code, rec.Body.String())
	}
}

func TestMemoriesHandlers_PostAgentOverridesSpoof(t *testing.T) {
	h := newMemoriesHandler(t)
	_, body := postMemory(t, h, "birch", map[string]any{
		"collection": "t",
		"id":         "doc-1",
		"text":       "hello",
		"metadata":   map[string]string{"agent": "impostor", "other": "ok"},
	})
	if body["id"] != "doc-1" {
		t.Fatalf("post failed: %v", body)
	}
	// Confirm server-side agent stored in metadata via /versions.
	req := httptest.NewRequest(http.MethodGet, "/memories/t/doc-1/versions", nil)
	req.SetPathValue("collection", "t")
	req.SetPathValue("id", "doc-1")
	req = withAgent(req, "birch")
	rec := httptest.NewRecorder()
	h.handleVersions(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("versions status=%d", rec.Code)
	}
	var out struct {
		Versions []struct {
			Metadata map[string]string `json:"metadata"`
		} `json:"versions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Versions) == 0 {
		t.Fatalf("no versions")
	}
	meta := out.Versions[len(out.Versions)-1].Metadata
	if meta["agent"] != "birch" {
		t.Errorf("metadata[agent] = %q, want birch (spoof must be overridden)", meta["agent"])
	}
	if meta["other"] != "ok" {
		t.Errorf("metadata[other] = %q, want ok (other fields should survive)", meta["other"])
	}
}

