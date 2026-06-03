package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// newSearchHandler wires the same in-process primary pattern used by
// the memories-handler tests and returns (searchHandler, memoriesHandler).
// The memories handler is returned so tests can POST seed entries through
// the same code path production uses to write.
func newSearchHandler(t *testing.T) (*SearchHandler, *MemoriesHandler) {
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
	return NewSearchHandler(clients, nil), NewMemoriesHandler(clients, nil)
}

// searchGET runs a GET /memories/search request against h with the raw
// query string and optional agent. Returns the recorder plus decoded
// JSON body (possibly nil on non-JSON responses).
func searchGET(t *testing.T, h *SearchHandler, rawQuery, agent string, headers map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/memories/search?"+rawQuery, nil)
	req = withAgent(req, agent)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.handleSearch(rec, req)
	if rec.Body.Len() == 0 {
		return rec, nil
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		return rec, nil
	}
	return rec, out
}

// seedEntries writes (id, text, metadata) triples to collection "t" via
// the memories handler so writes exercise the same auth/metadata path
// production uses. Returns the HLC echoed back by the POST for each id.
func seedEntries(t *testing.T, mh *MemoriesHandler, entries []struct {
	ID   string
	Text string
	Meta map[string]string
}) []uint64 {
	t.Helper()
	out := make([]uint64, 0, len(entries))
	for _, e := range entries {
		body := map[string]any{"collection": "t", "id": e.ID, "text": e.Text}
		if e.Meta != nil {
			body["metadata"] = e.Meta
		}
		rec, got := postMemory(t, mh, "birch", body)
		if rec.Code != http.StatusOK {
			t.Fatalf("seed %q: status=%d body=%s", e.ID, rec.Code, rec.Body.String())
		}
		hlc, _ := got["hlc"].(float64)
		if hlc == 0 {
			t.Fatalf("seed %q: zero hlc in response %v", e.ID, got)
		}
		out = append(out, uint64(hlc))
	}
	return out
}

func TestSearchHandler_HappyPath(t *testing.T) {
	sh, mh := newSearchHandler(t)
	seedEntries(t, mh, []struct {
		ID   string
		Text string
		Meta map[string]string
	}{
		{ID: "doc-1", Text: "hello world"},
		{ID: "doc-2", Text: "totally unrelated content about databases"},
		{ID: "doc-3", Text: "another day another dollar"},
	})

	rec, body := searchGET(t, sh, "collection=t&q=hello&k=3", "birch", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type = %q", ct)
	}
	results, ok := body["results"].([]any)
	if !ok {
		t.Fatalf("results key missing or wrong type: body=%v", body)
	}
	if len(results) == 0 || len(results) > 3 {
		t.Fatalf("results length = %d, want 1..3", len(results))
	}
	first, ok := results[0].(map[string]any)
	if !ok {
		t.Fatalf("first result not object: %v", results[0])
	}
	if first["id"] != "doc-1" {
		t.Errorf("top hit id = %v, want doc-1", first["id"])
	}
	if _, hasScore := first["score"]; !hasScore {
		t.Errorf("result missing score: %v", first)
	}
	if first["text"] != "hello world" {
		t.Errorf("result text = %v, want hello world", first["text"])
	}
}

func TestSearchHandler_KDefaults(t *testing.T) {
	sh, mh := newSearchHandler(t)
	seed := make([]struct {
		ID   string
		Text string
		Meta map[string]string
	}, 0, 12)
	for i := 0; i < 12; i++ {
		seed = append(seed, struct {
			ID   string
			Text string
			Meta map[string]string
		}{ID: "doc-" + strconv.Itoa(i), Text: "hello " + strconv.Itoa(i)})
	}
	seedEntries(t, mh, seed)

	rec, body := searchGET(t, sh, "collection=t&q=hello", "birch", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	results, _ := body["results"].([]any)
	if len(results) > 10 {
		t.Errorf("default k cap violated: got %d", len(results))
	}
}

func TestSearchHandler_KInvalid(t *testing.T) {
	sh, _ := newSearchHandler(t)
	cases := []string{"k=0", "k=-1", "k=abc", "k=101"}
	for _, kv := range cases {
		t.Run(kv, func(t *testing.T) {
			rec, body := searchGET(t, sh, "collection=t&q=x&"+kv, "birch", nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d want 400 body=%s", rec.Code, rec.Body.String())
			}
			if body == nil || body["error"] == nil {
				t.Errorf("body missing error key: %v", body)
			}
		})
	}
}

func TestSearchHandler_KBoundaryUpper(t *testing.T) {
	sh, mh := newSearchHandler(t)
	seedEntries(t, mh, []struct {
		ID   string
		Text string
		Meta map[string]string
	}{{ID: "doc-1", Text: "hello"}})

	rec, _ := searchGET(t, sh, "collection=t&q=hello&k=100", "birch", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestSearchHandler_MissingCollection(t *testing.T) {
	sh, _ := newSearchHandler(t)
	rec, body := searchGET(t, sh, "q=hello", "birch", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rec.Code, rec.Body.String())
	}
	if body == nil || body["error"] == nil {
		t.Errorf("missing error in body: %v", body)
	}
}

func TestSearchHandler_MissingQ(t *testing.T) {
	sh, _ := newSearchHandler(t)
	rec, body := searchGET(t, sh, "collection=t", "birch", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rec.Code, rec.Body.String())
	}
	if body == nil || body["error"] == nil {
		t.Errorf("missing error in body: %v", body)
	}
}

func TestSearchHandler_PointInTime(t *testing.T) {
	sh, mh := newSearchHandler(t)
	// v1 then v2 of the same id; search at v1's HLC should return v1's text.
	hlcs := seedEntries(t, mh, []struct {
		ID   string
		Text string
		Meta map[string]string
	}{
		{ID: "doc-1", Text: "alpha version one"},
	})
	hlc1 := hlcs[0]
	seedEntries(t, mh, []struct {
		ID   string
		Text string
		Meta map[string]string
	}{
		{ID: "doc-1", Text: "beta version two"},
	})

	// "current": search "beta" should return v2 text.
	rec, body := searchGET(t, sh, "collection=t&q=beta", "birch", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("current status=%d body=%s", rec.Code, rec.Body.String())
	}
	results, _ := body["results"].([]any)
	if len(results) == 0 {
		t.Fatalf("no results for current beta search: %v", body)
	}
	first, _ := results[0].(map[string]any)
	if first["text"] != "beta version two" {
		t.Errorf("current top text=%v want beta version two", first["text"])
	}

	// Point-in-time at hlc1: search "alpha" should return v1.
	rec2, body2 := searchGET(t, sh, "collection=t&q=alpha&at="+strconv.FormatUint(hlc1, 10), "birch", nil)
	if rec2.Code != http.StatusOK {
		t.Fatalf("pit status=%d body=%s", rec2.Code, rec2.Body.String())
	}
	results2, _ := body2["results"].([]any)
	if len(results2) == 0 {
		t.Fatalf("no pit results: %v", body2)
	}
	first2, _ := results2[0].(map[string]any)
	if first2["text"] != "alpha version one" {
		t.Errorf("pit text=%v want alpha version one", first2["text"])
	}
}

func TestSearchHandler_AtInvalid(t *testing.T) {
	sh, _ := newSearchHandler(t)
	rec, body := searchGET(t, sh, "collection=t&q=x&at=not-a-number", "birch", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rec.Code, rec.Body.String())
	}
	if body == nil || body["error"] == nil {
		t.Errorf("missing error key: %v", body)
	}
}

func TestSearchHandler_Filter(t *testing.T) {
	sh, mh := newSearchHandler(t)
	seedEntries(t, mh, []struct {
		ID   string
		Text string
		Meta map[string]string
	}{
		{ID: "doc-1", Text: "shared keyword alpha", Meta: map[string]string{"source": "review"}},
		{ID: "doc-2", Text: "shared keyword alpha"}, // no source
	})

	rec, body := searchGET(t, sh, "collection=t&q=alpha&filter=source:review", "birch", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	results, _ := body["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("filtered result count=%d want 1: %v", len(results), results)
	}
	first, _ := results[0].(map[string]any)
	if first["id"] != "doc-1" {
		t.Errorf("filtered top id=%v want doc-1", first["id"])
	}
}

func TestSearchHandler_MultipleFilters(t *testing.T) {
	sh, mh := newSearchHandler(t)
	seedEntries(t, mh, []struct {
		ID   string
		Text string
		Meta map[string]string
	}{
		{ID: "doc-1", Text: "alpha beta", Meta: map[string]string{"source": "review", "tier": "a"}},
		{ID: "doc-2", Text: "alpha beta", Meta: map[string]string{"source": "review", "tier": "b"}},
		{ID: "doc-3", Text: "alpha beta", Meta: map[string]string{"source": "design", "tier": "a"}},
	})

	rec, body := searchGET(t, sh, "collection=t&q=alpha&filter=source:review&filter=tier:a", "birch", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	results, _ := body["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("multi-filter count=%d want 1: %v", len(results), results)
	}
	first, _ := results[0].(map[string]any)
	if first["id"] != "doc-1" {
		t.Errorf("multi-filter top id=%v want doc-1", first["id"])
	}
}

func TestSearchHandler_FilterMalformed(t *testing.T) {
	sh, _ := newSearchHandler(t)
	rec, body := searchGET(t, sh, "collection=t&q=x&filter=no-colon", "birch", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rec.Code, rec.Body.String())
	}
	if body == nil || body["error"] == nil {
		t.Errorf("missing error key: %v", body)
	}
}

func TestSearchHandler_ReadStalenessHeader(t *testing.T) {
	sh, mh := newSearchHandler(t)
	seedEntries(t, mh, []struct {
		ID   string
		Text string
		Meta map[string]string
	}{{ID: "doc-1", Text: "hello there"}})

	rec, body := searchGET(t, sh, "collection=t&q=hello", "birch", map[string]string{
		"X-Read-Staleness": "replica-ok",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, ok := body["results"]; !ok {
		t.Errorf("missing results key: %v", body)
	}
}

func TestSearchHandler_MissingAgent500(t *testing.T) {
	sh, _ := newSearchHandler(t)
	// Pass empty agent so withAgent does NOT inject the context value.
	rec, body := searchGET(t, sh, "collection=t&q=hello", "", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500 body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type=%q", ct)
	}
	if body == nil || body["error"] != "internal" {
		t.Errorf("error key=%v want internal (body=%v)", body["error"], body)
	}
}

func TestSearchHandler_URLDecodedQ(t *testing.T) {
	sh, mh := newSearchHandler(t)
	seedEntries(t, mh, []struct {
		ID   string
		Text string
		Meta map[string]string
	}{{ID: "doc-1", Text: "RD formula for lagrangian"}})

	raw := "collection=t&q=" + url.QueryEscape("RD formula")
	rec, body := searchGET(t, sh, raw, "birch", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	results, _ := body["results"].([]any)
	if len(results) == 0 {
		t.Fatalf("no results for url-decoded q: %v", body)
	}
}

func TestSearchHandler_EmptyResultsJSON(t *testing.T) {
	sh, _ := newSearchHandler(t)
	// Collection "t" was never written to — search returns no hits, but
	// we want a well-formed 200 with {"results":[]} anyway.
	rec, body := searchGET(t, sh, "collection=t&q=anything", "birch", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	results, ok := body["results"].([]any)
	if !ok {
		t.Fatalf("results not a JSON array: %v (%T)", body["results"], body["results"])
	}
	if len(results) != 0 {
		t.Errorf("results should be empty: %v", results)
	}
}
