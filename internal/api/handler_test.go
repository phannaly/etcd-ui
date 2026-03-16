package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/phannaly/etcd-ui/internal/etcd"
)

func decodeJSON(t *testing.T, body string) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("response is not valid JSON: %v\nBody: %s", err, body)
	}
	return m
}

func doRequest(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:9999"
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestCORS_OptionsReturns204(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)
	rr := doRequest(mux, http.MethodOptions, "/api/keys", "")
	if rr.Code != http.StatusNoContent {
		t.Errorf("OPTIONS status = %d, want 204", rr.Code)
	}
}

func TestCORS_HeadersPresentOnAllRoutes(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)
	rr := doRequest(mux, http.MethodGet, "/api/keys", "")
	if rr.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS header Access-Control-Allow-Origin missing or wrong")
	}
}

func TestWriteJSON_SetsContentType(t *testing.T) {
	rr := httptest.NewRecorder()
	writeJSON(rr, map[string]string{"hello": "world"})
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

func TestWriteErr_SetsStatusAndJSON(t *testing.T) {
	rr := httptest.NewRecorder()
	writeErr(rr, fmt.Errorf("something broke"), http.StatusBadRequest)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
	m := decodeJSON(t, rr.Body.String())
	if m["error"] != "something broke" {
		t.Errorf("error field = %v, want 'something broke'", m["error"])
	}
}

func TestReadOnly_BlocksPUT(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, true)
	mux := http.NewServeMux()
	h.Register(mux)
	rr := doRequest(mux, http.MethodPut, "/api/key/put", `{"key":"/x","value":"v"}`)
	if rr.Code != http.StatusForbidden {
		t.Errorf("read-only PUT status = %d, want 403", rr.Code)
	}
}

func TestReadOnly_BlocksDELETE(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, true)
	mux := http.NewServeMux()
	h.Register(mux)
	rr := doRequest(mux, http.MethodDelete, "/api/key/delete?key=/x", "")
	if rr.Code != http.StatusForbidden {
		t.Errorf("read-only DELETE status = %d, want 403", rr.Code)
	}
}

func TestReadOnly_BlocksImport(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, true)
	mux := http.NewServeMux()
	h.Register(mux)
	rr := doRequest(mux, http.MethodPost, "/api/import", `{"entries":[]}`)
	if rr.Code != http.StatusForbidden {
		t.Errorf("read-only import status = %d, want 403", rr.Code)
	}
}

func TestReadOnly_BlocksBulkDelete(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, true)
	mux := http.NewServeMux()
	h.Register(mux)
	rr := doRequest(mux, http.MethodPost, "/api/keys/bulk-delete", `{"prefix":"/tmp/"}`)
	if rr.Code != http.StatusForbidden {
		t.Errorf("read-only bulk-delete status = %d, want 403", rr.Code)
	}
}

func TestReadOnly_BlocksLeaseRevoke(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, true)
	mux := http.NewServeMux()
	h.Register(mux)
	rr := doRequest(mux, http.MethodPost, "/api/lease/revoke", `{"id":1}`)
	if rr.Code != http.StatusForbidden {
		t.Errorf("read-only lease revoke status = %d, want 403", rr.Code)
	}
}

func TestReadOnly_BlocksAuthEnable(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, true)
	mux := http.NewServeMux()
	h.Register(mux)
	rr := doRequest(mux, http.MethodPost, "/api/auth/enable", "")
	if rr.Code != http.StatusForbidden {
		t.Errorf("read-only auth/enable status = %d, want 403", rr.Code)
	}
}

func TestReadOnly_AllowsGET(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, true)
	mux := http.NewServeMux()
	h.Register(mux)
	rr := doRequest(mux, http.MethodGet, "/api/keys", "")
	if rr.Code == http.StatusForbidden {
		t.Error("GET /api/keys must not be blocked in read-only mode")
	}
}

func TestReadOnly_AllowsExport(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, true)
	mux := http.NewServeMux()
	h.Register(mux)
	rr := doRequest(mux, http.MethodGet, "/api/export", "")
	if rr.Code == http.StatusForbidden {
		t.Error("GET /api/export must not be blocked in read-only mode")
	}
}

func TestRateAllow_AllowsUpToLimit(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	for i := 0; i < rateLimit; i++ {
		if !h.rateAllow("10.0.0.1:1234") {
			t.Fatalf("rateAllow should allow request %d (limit=%d)", i+1, rateLimit)
		}
	}
}

func TestRateAllow_BlocksAfterLimit(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	for i := 0; i < rateLimit; i++ {
		h.rateAllow("10.0.0.2:1234")
	}
	if h.rateAllow("10.0.0.2:1234") {
		t.Errorf("rateAllow should block after %d requests", rateLimit)
	}
}

func TestRateAllow_DifferentIPsAreIndependent(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	for i := 0; i < rateLimit; i++ {
		h.rateAllow("10.0.0.3:1234")
	}
	if !h.rateAllow("10.0.0.4:5678") {
		t.Error("different IP should not be rate limited")
	}
}

func TestRateAllow_WindowExpiry(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	ip := "10.0.0.5:1234"

	h.rmu.Lock()
	past := time.Now().Add(-2 * rateWindow)
	stale := make([]time.Time, rateLimit)
	for i := range stale {
		stale[i] = past
	}
	h.ratemap[ip] = stale
	h.rmu.Unlock()

	if !h.rateAllow(ip) {
		t.Error("should allow after rate window expires")
	}
}

func TestHandleCluster_ReturnsConnectedStatus(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		clusterInfoFn: func(ctx context.Context) *etcd.ClusterInfo {
			return &etcd.ClusterInfo{Connected: true, KeyCount: 7}
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/cluster", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	m := decodeJSON(t, rr.Body.String())
	if m["connected"] != true {
		t.Errorf("connected = %v, want true", m["connected"])
	}
}

func TestHandleCluster_IncludesReadOnlyAndMaxKeys(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, true)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/cluster", "")
	m := decodeJSON(t, rr.Body.String())
	if m["read_only"] != true {
		t.Errorf("read_only = %v, want true", m["read_only"])
	}
	if _, ok := m["max_keys"]; !ok {
		t.Error("max_keys field missing from cluster response")
	}
}

func TestHandleKeys_ReturnsKeyList(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		listAllFn: func(ctx context.Context) ([]*etcd.KeyNode, error) {
			return []*etcd.KeyNode{
				{Key: "/a", Value: "1"},
				{Key: "/b", Value: "2"},
			}, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/keys", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	m := decodeJSON(t, rr.Body.String())
	if m["count"].(float64) != 2 {
		t.Errorf("count = %v, want 2", m["count"])
	}
}

func TestHandleKeys_WithPrefix(t *testing.T) {
	var gotPrefix string
	h, _ := newTestHandler(&mockClient{
		listPrefixFn: func(ctx context.Context, p string) ([]*etcd.KeyNode, error) {
			gotPrefix = p
			return []*etcd.KeyNode{{Key: "/app/config"}}, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	doRequest(mux, http.MethodGet, "/api/keys?prefix=/app/", "")
	if gotPrefix != "/app/" {
		t.Errorf("prefix passed = %q, want /app/", gotPrefix)
	}
}

func TestHandleKeys_EtcdErrorReturns500(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		listAllFn: func(ctx context.Context) ([]*etcd.KeyNode, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/keys", "")
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}

func TestHandleKey_MissingKeyParam(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/key", "")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandleKey_NotFoundReturns404(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		getKeyFn: func(ctx context.Context, key string) (*etcd.KeyNode, error) {
			return nil, fmt.Errorf("key not found: %s", key)
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/key?key=/missing", "")
	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestHandleKey_FoundReturnsNode(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		getKeyFn: func(ctx context.Context, key string) (*etcd.KeyNode, error) {
			return &etcd.KeyNode{Key: key, Value: "hello"}, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/key?key=/exists", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	m := decodeJSON(t, rr.Body.String())
	if m["value"] != "hello" {
		t.Errorf("value = %v, want hello", m["value"])
	}
}

func TestHandleSearch_MissingQuery(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/search", "")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandleSearch_ReturnsResults(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		searchFn: func(ctx context.Context, q string) ([]*etcd.KeyNode, error) {
			if q == "prod" {
				return []*etcd.KeyNode{{Key: "/env", Value: "production"}}, nil
			}
			return nil, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/search?q=prod", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	m := decodeJSON(t, rr.Body.String())
	if m["query"] != "prod" {
		t.Errorf("query echo = %v, want prod", m["query"])
	}
	if m["count"].(float64) != 1 {
		t.Errorf("count = %v, want 1", m["count"])
	}
}

func TestHandlePut_MissingKey(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPut, "/api/key/put", `{"value":"v"}`)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandlePut_InvalidJSON(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPut, "/api/key/put", `not json`)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandlePut_Success(t *testing.T) {
	var gotKey, gotVal string
	var gotTTL int64
	h, alog := newTestHandler(&mockClient{
		putWithTTLFn: func(ctx context.Context, k, v string, ttl int64) error {
			gotKey, gotVal, gotTTL = k, v, ttl
			return nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPut, "/api/key/put", `{"key":"/config","value":"v1","ttl":30}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rr.Code, rr.Body.String())
	}
	if gotKey != "/config" || gotVal != "v1" || gotTTL != 30 {
		t.Errorf("put args = (%q, %q, %d), want (/config, v1, 30)", gotKey, gotVal, gotTTL)
	}
	if len(alog.entries) == 0 || !strings.HasPrefix(alog.entries[0], "PUT:") {
		t.Errorf("audit not recorded, entries = %v", alog.entries)
	}
}

func TestHandlePut_EtcdErrorReturns500(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		putWithTTLFn: func(ctx context.Context, k, v string, ttl int64) error {
			return fmt.Errorf("etcd unavailable")
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPut, "/api/key/put", `{"key":"/x","value":"v"}`)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}

func TestHandlePut_ZeroTTLMeansNoPersist(t *testing.T) {
	var gotTTL int64 = -1
	h, _ := newTestHandler(&mockClient{
		putWithTTLFn: func(ctx context.Context, k, v string, ttl int64) error {
			gotTTL = ttl
			return nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	doRequest(mux, http.MethodPut, "/api/key/put", `{"key":"/x","value":"v"}`)
	if gotTTL != 0 {
		t.Errorf("TTL = %d when not specified, want 0", gotTTL)
	}
}

func TestHandleDelete_MissingKey(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodDelete, "/api/key/delete", "")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandleDelete_Success(t *testing.T) {
	var deletedKey string
	h, alog := newTestHandler(&mockClient{
		deleteFn: func(ctx context.Context, k string) (int64, error) {
			deletedKey = k
			return 1, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodDelete, "/api/key/delete?key=/old-key", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if deletedKey != "/old-key" {
		t.Errorf("deleted key = %q, want /old-key", deletedKey)
	}
	if len(alog.entries) == 0 {
		t.Error("audit not recorded")
	}
}

func TestHandleDelete_EtcdErrorReturns500(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		deleteFn: func(ctx context.Context, k string) (int64, error) {
			return 0, fmt.Errorf("etcd error")
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodDelete, "/api/key/delete?key=/x", "")
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}

func TestHandleBulkDelete_MissingPrefix(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPost, "/api/keys/bulk-delete", `{}`)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandleBulkDelete_Success(t *testing.T) {
	var deletedPrefix string
	h, alog := newTestHandler(&mockClient{
		deletePrefixFn: func(ctx context.Context, p string) (int64, error) {
			deletedPrefix = p
			return 5, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPost, "/api/keys/bulk-delete", `{"prefix":"/tmp/"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rr.Code, rr.Body.String())
	}
	if deletedPrefix != "/tmp/" {
		t.Errorf("prefix = %q, want /tmp/", deletedPrefix)
	}
	m := decodeJSON(t, rr.Body.String())
	if m["deleted"].(float64) != 5 {
		t.Errorf("deleted count = %v, want 5", m["deleted"])
	}
	if len(alog.entries) == 0 {
		t.Error("audit not recorded for bulk delete")
	}
}

func TestHandleBulkDelete_RequiresPOST(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/keys/bulk-delete", "")
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /api/keys/bulk-delete status = %d, want 405", rr.Code)
	}
}

func TestHandleBulkPreview_MissingPrefix(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/keys/bulk-preview", "")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandleBulkPreview_ReturnsMatchingKeys(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		listPrefixFn: func(ctx context.Context, p string) ([]*etcd.KeyNode, error) {
			return []*etcd.KeyNode{
				{Key: "/tmp/a"},
				{Key: "/tmp/b"},
			}, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/keys/bulk-preview?prefix=/tmp/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	m := decodeJSON(t, rr.Body.String())
	if m["count"].(float64) != 2 {
		t.Errorf("count = %v, want 2", m["count"])
	}
}

func TestHandleExport_ReturnsJSONWithEntries(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		exportFn: func(ctx context.Context, p string) ([]etcd.ExportEntry, error) {
			return []etcd.ExportEntry{
				{Key: "/k1", Value: "v1"},
				{Key: "/k2", Value: "v2"},
			}, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/export", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Disposition"); !strings.Contains(ct, "etcd-export.json") {
		t.Errorf("Content-Disposition = %q, want filename", ct)
	}
	m := decodeJSON(t, rr.Body.String())
	if m["count"].(float64) != 2 {
		t.Errorf("count = %v, want 2", m["count"])
	}
}

func TestHandleExport_WithPrefix(t *testing.T) {
	var gotPrefix string
	h, _ := newTestHandler(&mockClient{
		exportFn: func(ctx context.Context, p string) ([]etcd.ExportEntry, error) {
			gotPrefix = p
			return nil, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	doRequest(mux, http.MethodGet, "/api/export?prefix=/app/", "")
	if gotPrefix != "/app/" {
		t.Errorf("export prefix = %q, want /app/", gotPrefix)
	}
}

func TestHandleImport_Success(t *testing.T) {
	var importedCount int
	h, alog := newTestHandler(&mockClient{
		importFn: func(ctx context.Context, entries []etcd.ExportEntry) (int, error) {
			importedCount = len(entries)
			return importedCount, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	body := `{"entries":[{"key":"/a","value":"1"},{"key":"/b","value":"2"}]}`
	rr := doRequest(mux, http.MethodPost, "/api/import", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rr.Code, rr.Body.String())
	}
	if importedCount != 2 {
		t.Errorf("imported = %d, want 2", importedCount)
	}
	m := decodeJSON(t, rr.Body.String())
	if m["imported"].(float64) != 2 {
		t.Errorf("imported field = %v, want 2", m["imported"])
	}
	if len(alog.entries) == 0 {
		t.Error("audit not recorded for import")
	}
}

func TestHandleImport_InvalidJSONBody(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPost, "/api/import", `not valid json`)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandleKeyHistory_MissingKey(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/key/history", "")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandleKeyHistory_ReturnsEntries(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		keyHistoryFn: func(ctx context.Context, key string, limit int) ([]etcd.HistoryEntry, error) {
			if limit != 10 {
				return nil, fmt.Errorf("unexpected limit %d", limit)
			}
			return []etcd.HistoryEntry{
				{Revision: 10, Value: "v2", ValueType: "text"},
				{Revision: 5, Value: "v1", ValueType: "text"},
			}, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/key/history?key=/k&limit=10", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	m := decodeJSON(t, rr.Body.String())
	hist, ok := m["history"].([]interface{})
	if !ok || len(hist) != 2 {
		t.Errorf("history length = %v, want 2", m["history"])
	}
	if m["key"] != "/k" {
		t.Errorf("key echo = %v, want /k", m["key"])
	}
}

func TestHandleKeyHistory_DefaultLimit(t *testing.T) {
	var gotLimit int
	h, _ := newTestHandler(&mockClient{
		keyHistoryFn: func(ctx context.Context, key string, limit int) ([]etcd.HistoryEntry, error) {
			gotLimit = limit
			return nil, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	doRequest(mux, http.MethodGet, "/api/key/history?key=/k", "")
	if gotLimit != 20 {
		t.Errorf("default limit = %d, want 20", gotLimit)
	}
}

func TestHandleLeases_ReturnsLeaseList(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		leaseListFn: func(ctx context.Context) ([]etcd.LeaseInfo, error) {
			return []etcd.LeaseInfo{
				{ID: 1, IDHex: "1", TTL: 30, GrantedTTL: 60, Keys: []string{"/k"}},
			}, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/leases", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	m := decodeJSON(t, rr.Body.String())
	leases := m["leases"].([]interface{})
	if len(leases) != 1 {
		t.Errorf("lease count = %d, want 1", len(leases))
	}
}

func TestHandleLeaseRevoke_Success(t *testing.T) {
	var revokedID int64
	h, _ := newTestHandler(&mockClient{
		leaseRevokeFn: func(ctx context.Context, id int64) error {
			revokedID = id
			return nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPost, "/api/lease/revoke", `{"id":99}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d\nbody: %s", rr.Code, rr.Body.String())
	}
	if revokedID != 99 {
		t.Errorf("revokedID = %d, want 99", revokedID)
	}
}

func TestHandleLeaseRevoke_RequiresPOST(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/lease/revoke", "")
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /api/lease/revoke status = %d, want 405", rr.Code)
	}
}

func TestHandleAuthStatus_ReturnsEnabled(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		authStatusFn: func(ctx context.Context) (*etcd.AuthStatus, error) {
			return &etcd.AuthStatus{Enabled: true}, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/auth/status", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	m := decodeJSON(t, rr.Body.String())
	if m["enabled"] != true {
		t.Errorf("enabled = %v, want true", m["enabled"])
	}
}

func TestHandleAuthEnable_MethodNotAllowed(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/auth/enable", "")
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /api/auth/enable status = %d, want 405", rr.Code)
	}
}

func TestHandleAuthEnable_Success(t *testing.T) {
	called := false
	h, _ := newTestHandler(&mockClient{
		authEnableFn: func(ctx context.Context) error {
			called = true
			return nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPost, "/api/auth/enable", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if !called {
		t.Error("AuthEnable was not called")
	}
}

func TestHandleUsers_GET(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		userListFn: func(ctx context.Context) ([]etcd.UserInfo, error) {
			return []etcd.UserInfo{
				{Name: "root", Roles: []string{"root"}},
				{Name: "alice", Roles: []string{"reader"}},
			}, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/auth/users", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	m := decodeJSON(t, rr.Body.String())
	users := m["users"].([]interface{})
	if len(users) != 2 {
		t.Errorf("user count = %d, want 2", len(users))
	}
}

func TestHandleUsers_POST_MissingName(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPost, "/api/auth/users", `{"password":"p"}`)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandleUsers_POST_Success(t *testing.T) {
	var addedUser string
	h, _ := newTestHandler(&mockClient{
		userAddFn: func(ctx context.Context, name, pass string) error {
			addedUser = name
			return nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPost, "/api/auth/users", `{"name":"bob","password":"secret"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d\nbody: %s", rr.Code, rr.Body.String())
	}
	if addedUser != "bob" {
		t.Errorf("added user = %q, want bob", addedUser)
	}
}

func TestHandleUsers_DELETE_MissingName(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodDelete, "/api/auth/users", "")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandleUser_UnknownAction(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPost, "/api/auth/user", `{"action":"explode","name":"x"}`)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandleUser_GrantRole(t *testing.T) {
	var grantedUser, grantedRole string
	h, _ := newTestHandler(&mockClient{
		userGrantRoleFn: func(ctx context.Context, user, role string) error {
			grantedUser, grantedRole = user, role
			return nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPost, "/api/auth/user",
		`{"action":"grantrole","name":"alice","role":"writer"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if grantedUser != "alice" || grantedRole != "writer" {
		t.Errorf("grant args = (%q, %q), want (alice, writer)", grantedUser, grantedRole)
	}
}

func TestHandleUser_RevokeRole(t *testing.T) {
	var revokedUser, revokedRole string
	h, _ := newTestHandler(&mockClient{
		userRevokeRoleFn: func(ctx context.Context, user, role string) error {
			revokedUser, revokedRole = user, role
			return nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPost, "/api/auth/user",
		`{"action":"revokerole","name":"alice","role":"writer"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if revokedUser != "alice" || revokedRole != "writer" {
		t.Errorf("revoke args = (%q, %q), want (alice, writer)", revokedUser, revokedRole)
	}
}

func TestHandleRoles_POST_MissingName(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodPost, "/api/auth/roles", `{}`)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandleRole_GrantPermission(t *testing.T) {
	var capturedRole, capturedKey, capturedPerm string
	h, _ := newTestHandler(&mockClient{
		roleGrantPermissionFn: func(ctx context.Context, role, key, rangeEnd, perm string) error {
			capturedRole, capturedKey, capturedPerm = role, key, perm
			return nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	body := `{"action":"grant","role":"reader","key":"/data/","range_end":"/data0","perm_type":"READ"}`
	rr := doRequest(mux, http.MethodPost, "/api/auth/role", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d\nbody: %s", rr.Code, rr.Body.String())
	}
	if capturedRole != "reader" || capturedKey != "/data/" || capturedPerm != "READ" {
		t.Errorf("grant args = (%q, %q, %q)", capturedRole, capturedKey, capturedPerm)
	}
}

func TestHandleRole_RevokePermission(t *testing.T) {
	var capturedRole, capturedKey string
	h, _ := newTestHandler(&mockClient{
		roleRevokePermissionFn: func(ctx context.Context, role, key, rangeEnd string) error {
			capturedRole, capturedKey = role, key
			return nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	body := `{"action":"revoke","role":"reader","key":"/data/","range_end":"/data0"}`
	rr := doRequest(mux, http.MethodPost, "/api/auth/role", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if capturedRole != "reader" || capturedKey != "/data/" {
		t.Errorf("revoke args = (%q, %q)", capturedRole, capturedKey)
	}
}

func TestHandleTree_ReturnsTree(t *testing.T) {
	h, _ := newTestHandler(&mockClient{
		listAllFn: func(ctx context.Context) ([]*etcd.KeyNode, error) {
			return []*etcd.KeyNode{
				{Key: "/a/x", Value: "1"},
				{Key: "/a/y", Value: "2"},
				{Key: "/b/z", Value: "3"},
			}, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	rr := doRequest(mux, http.MethodGet, "/api/tree", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	m := decodeJSON(t, rr.Body.String())
	if m["count"].(float64) != 3 {
		t.Errorf("count = %v, want 3", m["count"])
	}
	tree, ok := m["tree"].([]interface{})
	if !ok || len(tree) != 2 {
		t.Errorf("expected 2 top-level dirs (/a and /b), got %v", m["tree"])
	}
}

func TestAudit_PutRecordsKeyInLog(t *testing.T) {
	h, alog := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	doRequest(mux, http.MethodPut, "/api/key/put", `{"key":"/audit-test","value":"val"}`)

	found := false
	for _, e := range alog.entries {
		if e == "PUT:/audit-test" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected audit entry PUT:/audit-test, got %v", alog.entries)
	}
}

func TestAudit_DeleteRecordsKeyInLog(t *testing.T) {
	h, alog := newTestHandler(&mockClient{
		deleteFn: func(ctx context.Context, k string) (int64, error) {
			return 1, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	doRequest(mux, http.MethodDelete, "/api/key/delete?key=/del-audit", "")

	found := false
	for _, e := range alog.entries {
		if e == "DELETE:/del-audit" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected audit entry DELETE:/del-audit, got %v", alog.entries)
	}
}

func TestOrAll(t *testing.T) {
	if orAll("") != "all keys" {
		t.Error(`orAll("") should return "all keys"`)
	}
	if orAll("  ") != "all keys" {
		t.Error(`orAll("  ") should return "all keys"`)
	}
	if orAll("/prefix") != "/prefix" {
		t.Error(`orAll("/prefix") should return "/prefix"`)
	}
}

func TestAllErrorResponsesAreJSON(t *testing.T) {
	h, _ := newTestHandler(&mockClient{}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	cases := []struct{ method, path, body string }{
		{http.MethodGet, "/api/key", ""},
		{http.MethodGet, "/api/search", ""},
		{http.MethodGet, "/api/keys/bulk-preview", ""},
		{http.MethodPut, "/api/key/put", `{"value":"v"}`},
		{http.MethodPost, "/api/auth/users", `{"password":"p"}`},
	}
	for _, tc := range cases {
		rr := doRequest(mux, tc.method, tc.path, tc.body)
		body := strings.TrimSpace(rr.Body.String())
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(body), &m); err != nil {
			t.Errorf("%s %s: not JSON: %v\nbody: %s", tc.method, tc.path, err, body)
			continue
		}
		if _, ok := m["error"]; !ok {
			t.Errorf("%s %s: error field missing\nbody: %s", tc.method, tc.path, body)
		}
	}
}

func BenchmarkRateAllow(b *testing.B) {
	h, _ := newTestHandler(&mockClient{}, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.rateAllow("10.0.0.1:1234")
	}
}

func BenchmarkHandleKeys(b *testing.B) {
	nodes := make([]*etcd.KeyNode, 100)
	for i := range nodes {
		nodes[i] = &etcd.KeyNode{Key: fmt.Sprintf("/key/%d", i), Value: "val"}
	}
	h, _ := newTestHandler(&mockClient{
		listAllFn: func(ctx context.Context) ([]*etcd.KeyNode, error) {
			return nodes, nil
		},
	}, false)
	mux := http.NewServeMux()
	h.Register(mux)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/keys", bytes.NewReader(nil))
		req.RemoteAddr = "127.0.0.1:9999"
		mux.ServeHTTP(rr, req)
	}
}
