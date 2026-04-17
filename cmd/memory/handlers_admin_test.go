package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newAdminHandler spins up an in-process CorkScrewDB primary, wires a
// *DBClients with rw+ro pointing at it, and returns a fresh
// *AdminHandler for test use.
func newAdminHandler(t *testing.T) *AdminHandler {
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
	return NewAdminHandler(clients, nil)
}

// withAdminCtx returns a copy of r whose context is marked as
// admin-authenticated. Use mark=false to simulate a mis-wired request
// that bypassed AdminAuth.
func withAdminCtx(r *http.Request, mark bool) *http.Request {
	if !mark {
		return r
	}
	ctx := context.WithValue(r.Context(), adminContextKey{}, true)
	return r.WithContext(ctx)
}

// postAdminCollection runs a POST /admin/collections against h with the
// given body and admin-context marker. Returns the raw response and the
// decoded JSON body (may be nil if body is non-JSON).
func postAdminCollection(t *testing.T, h *AdminHandler, adminCtx bool, body any) (*httptest.ResponseRecorder, map[string]any) {
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
	req := httptest.NewRequest(http.MethodPost, "/admin/collections", &buf)
	req = withAdminCtx(req, adminCtx)
	rec := httptest.NewRecorder()
	h.handleCreateCollection(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			return rec, nil
		}
	}
	return rec, out
}

// getAdminCollections runs GET /admin/collections with the admin-context
// marker toggleable, and returns the recorder + parsed body.
func getAdminCollections(t *testing.T, h *AdminHandler, adminCtx bool) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/admin/collections", nil)
	req = withAdminCtx(req, adminCtx)
	rec := httptest.NewRecorder()
	h.handleListCollections(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec, out
}

// getAdminShards runs GET /admin/shards with the admin-context marker
// toggleable, and returns the recorder + parsed body.
func getAdminShards(t *testing.T, h *AdminHandler, adminCtx bool) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/admin/shards", nil)
	req = withAdminCtx(req, adminCtx)
	rec := httptest.NewRecorder()
	h.handleShards(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec, out
}

func TestAdmin_CreateMissingContext500(t *testing.T) {
	h := newAdminHandler(t)
	rec, body := postAdminCollection(t, h, false, map[string]any{
		"name":       "agent-memory",
		"bit_width":  2,
		"retention":  "keep-all",
	})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500 body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type = %q", ct)
	}
	if body["error"] != "internal" {
		t.Errorf("error=%v want internal", body["error"])
	}
}

func TestAdmin_ListMissingContext500(t *testing.T) {
	h := newAdminHandler(t)
	rec, body := getAdminCollections(t, h, false)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", rec.Code)
	}
	if body["error"] != "internal" {
		t.Errorf("error=%v want internal", body["error"])
	}
}

func TestAdmin_ShardsMissingContext500(t *testing.T) {
	h := newAdminHandler(t)
	rec, body := getAdminShards(t, h, false)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", rec.Code)
	}
	if body["error"] != "internal" {
		t.Errorf("error=%v want internal", body["error"])
	}
}

func TestAdmin_CreateCollectionOK(t *testing.T) {
	h := newAdminHandler(t)
	rec, body := postAdminCollection(t, h, true, map[string]any{
		"name":      "agent-memory",
		"bit_width": 2,
		"retention": "keep-all",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type = %q", ct)
	}
	if body["name"] != "agent-memory" {
		t.Errorf("name=%v want agent-memory", body["name"])
	}
	if bw, _ := body["bit_width"].(float64); bw != 2 {
		t.Errorf("bit_width=%v want 2", body["bit_width"])
	}
	if body["retention"] != "keep-all" {
		t.Errorf("retention=%v want keep-all", body["retention"])
	}
	if hlc, _ := body["created_hlc"].(float64); hlc == 0 {
		t.Errorf("created_hlc=%v want non-zero", body["created_hlc"])
	}

	// Verify persistence by listing.
	lrec, lbody := getAdminCollections(t, h, true)
	if lrec.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", lrec.Code, lrec.Body.String())
	}
	colls, _ := lbody["collections"].([]any)
	if len(colls) != 1 {
		t.Fatalf("list collections len=%d want 1: %+v", len(colls), colls)
	}
	first, _ := colls[0].(map[string]any)
	if first["name"] != "agent-memory" {
		t.Errorf("list[0].name=%v want agent-memory", first["name"])
	}
}

func TestAdmin_CreateIdempotent(t *testing.T) {
	h := newAdminHandler(t)
	_, body1 := postAdminCollection(t, h, true, map[string]any{
		"name":      "mirage-runs",
		"bit_width": 2,
		"retention": "keep-all",
	})
	hlc1, _ := body1["created_hlc"].(float64)
	if hlc1 == 0 {
		t.Fatalf("first hlc=0 body=%v", body1)
	}

	rec, body2 := postAdminCollection(t, h, true, map[string]any{
		"name":      "mirage-runs",
		"bit_width": 2,
		"retention": "keep-all",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("second status=%d body=%s", rec.Code, rec.Body.String())
	}
	hlc2, _ := body2["created_hlc"].(float64)
	if hlc2 != hlc1 {
		t.Errorf("idempotent call changed created_hlc: first=%v second=%v", hlc1, hlc2)
	}
}

func TestAdmin_CreateBitWidthMismatch409(t *testing.T) {
	h := newAdminHandler(t)
	_, _ = postAdminCollection(t, h, true, map[string]any{
		"name":      "mirage-runs",
		"bit_width": 2,
		"retention": "keep-all",
	})
	rec, body := postAdminCollection(t, h, true, map[string]any{
		"name":      "mirage-runs",
		"bit_width": 4,
		"retention": "keep-all",
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d want 409 body=%s", rec.Code, rec.Body.String())
	}
	// Body must surface both existing and requested values.
	existing, _ := body["existing"].(map[string]any)
	requested, _ := body["requested"].(map[string]any)
	if existing == nil || requested == nil {
		t.Fatalf("409 body missing existing/requested: %v", body)
	}
	if bw, _ := existing["bit_width"].(float64); bw != 2 {
		t.Errorf("existing.bit_width=%v want 2", existing["bit_width"])
	}
	if bw, _ := requested["bit_width"].(float64); bw != 4 {
		t.Errorf("requested.bit_width=%v want 4", requested["bit_width"])
	}
}

func TestAdmin_CreateRetentionMismatch409(t *testing.T) {
	h := newAdminHandler(t)
	_, _ = postAdminCollection(t, h, true, map[string]any{
		"name":      "mirage-runs",
		"bit_width": 2,
		"retention": "keep-all",
	})
	rec, body := postAdminCollection(t, h, true, map[string]any{
		"name":      "mirage-runs",
		"bit_width": 2,
		"retention": "30d",
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d want 409 body=%s", rec.Code, rec.Body.String())
	}
	existing, _ := body["existing"].(map[string]any)
	requested, _ := body["requested"].(map[string]any)
	if existing == nil || requested == nil {
		t.Fatalf("409 body missing existing/requested: %v", body)
	}
	if existing["retention"] != "keep-all" {
		t.Errorf("existing.retention=%v want keep-all", existing["retention"])
	}
	if requested["retention"] != "30d" {
		t.Errorf("requested.retention=%v want 30d", requested["retention"])
	}
}

func TestAdmin_CreateMalformedJSON400(t *testing.T) {
	h := newAdminHandler(t)
	rec, _ := postAdminCollection(t, h, true, "not-json{")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rec.Code, rec.Body.String())
	}
}

func TestAdmin_CreateMissingName400(t *testing.T) {
	h := newAdminHandler(t)
	rec, _ := postAdminCollection(t, h, true, map[string]any{
		"bit_width": 2,
		"retention": "keep-all",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rec.Code, rec.Body.String())
	}
}

func TestAdmin_ListAfterTwoCreates(t *testing.T) {
	h := newAdminHandler(t)
	_, _ = postAdminCollection(t, h, true, map[string]any{
		"name":      "agent-memory",
		"bit_width": 2,
		"retention": "keep-all",
	})
	_, _ = postAdminCollection(t, h, true, map[string]any{
		"name":      "mirage-runs",
		"bit_width": 2,
		"retention": "keep-all",
	})
	rec, body := getAdminCollections(t, h, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", rec.Code, rec.Body.String())
	}
	colls, _ := body["collections"].([]any)
	if len(colls) != 2 {
		t.Fatalf("list len=%d want 2: %+v", len(colls), colls)
	}
	names := map[string]bool{}
	for _, c := range colls {
		m, _ := c.(map[string]any)
		if m == nil {
			continue
		}
		names[m["name"].(string)] = true
	}
	if !names["agent-memory"] || !names["mirage-runs"] {
		t.Errorf("missing expected names: got %v", names)
	}
}

func TestAdmin_ShardsOK(t *testing.T) {
	h := newAdminHandler(t)
	rec, body := getAdminShards(t, h, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type = %q", ct)
	}
	shards, ok := body["shards"]
	if !ok {
		t.Fatalf("response missing shards field: %v", body)
	}
	if shards == nil {
		// JSON decodes as a slice or nil — we require the key to be a slice.
		t.Fatalf("shards is nil; handler should emit [] for empty")
	}
	if _, isSlice := shards.([]any); !isSlice {
		t.Fatalf("shards is not a slice: %T %v", shards, shards)
	}
}
