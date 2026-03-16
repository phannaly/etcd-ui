package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/phannaly/etcd-ui/internal/etcd"
)

type clientIface interface {
	ClusterInfo(ctx context.Context) *etcd.ClusterInfo
	ListAll(ctx context.Context) ([]*etcd.KeyNode, error)
	ListPrefix(ctx context.Context, prefix string) ([]*etcd.KeyNode, error)
	GetKey(ctx context.Context, key string) (*etcd.KeyNode, error)
	Search(ctx context.Context, query string) ([]*etcd.KeyNode, error)
	Put(ctx context.Context, key, value string) error
	PutWithTTL(ctx context.Context, key, value string, ttlSeconds int64) error
	Delete(ctx context.Context, key string) (int64, error)
	DeletePrefix(ctx context.Context, prefix string) (int64, error)
	Watch(ctx context.Context, prefix string, out chan<- etcd.WatchEvent)
	KeyHistory(ctx context.Context, key string, limit int) ([]etcd.HistoryEntry, error)
	LeaseList(ctx context.Context) ([]etcd.LeaseInfo, error)
	LeaseRevoke(ctx context.Context, id int64) error
	Export(ctx context.Context, prefix string) ([]etcd.ExportEntry, error)
	Import(ctx context.Context, entries []etcd.ExportEntry) (int, error)
	AuthStatus(ctx context.Context) (*etcd.AuthStatus, error)
	AuthEnable(ctx context.Context) error
	AuthDisable(ctx context.Context) error
	UserList(ctx context.Context) ([]etcd.UserInfo, error)
	UserAdd(ctx context.Context, name, password string) error
	UserDelete(ctx context.Context, name string) error
	UserChangePassword(ctx context.Context, name, password string) error
	UserGrantRole(ctx context.Context, user, role string) error
	UserRevokeRole(ctx context.Context, user, role string) error
	RoleList(ctx context.Context) ([]etcd.RoleInfo, error)
	RoleAdd(ctx context.Context, name string) error
	RoleDelete(ctx context.Context, name string) error
	RoleGrantPermission(ctx context.Context, role, key, rangeEnd, permType string) error
	RoleRevokePermission(ctx context.Context, role, key, rangeEnd string) error
	MaxKeys() int
	Ping(ctx context.Context) bool
}

type Handler struct {
	client clientIface
	alog   interface {
		Log(action, key, old, new, user, ip, detail string)
	}
	readOnly bool

	mu       sync.Mutex
	watchers map[string]chan etcd.WatchEvent

	rmu     sync.Mutex
	ratemap map[string][]time.Time
}

func New(client *etcd.Client, alog interface {
	Log(action, key, old, new, user, ip, detail string)
}, readOnly bool,
) *Handler {
	return &Handler{
		client:   client,
		alog:     alog,
		readOnly: readOnly,
		watchers: make(map[string]chan etcd.WatchEvent),
		ratemap:  make(map[string][]time.Time),
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/cluster", cors(h.handleCluster))
	mux.HandleFunc("/api/keys", cors(h.handleKeys))
	mux.HandleFunc("/api/key", cors(h.handleKey))
	mux.HandleFunc("/api/search", cors(h.handleSearch))
	mux.HandleFunc("/api/tree", cors(h.handleTree))
	mux.HandleFunc("/api/watch", cors(h.handleWatch))
	mux.HandleFunc("/api/key/put", cors(h.handlePut))
	mux.HandleFunc("/api/key/delete", cors(h.handleDelete))
	mux.HandleFunc("/api/key/history", cors(h.handleKeyHistory))

	mux.HandleFunc("/api/leases", cors(h.handleLeases))
	mux.HandleFunc("/api/lease/revoke", cors(h.handleLeaseRevoke))

	mux.HandleFunc("/api/export", cors(h.handleExport))
	mux.HandleFunc("/api/import", cors(h.handleImport))

	mux.HandleFunc("/api/keys/bulk-delete", cors(h.handleBulkDelete))
	mux.HandleFunc("/api/keys/bulk-preview", cors(h.handleBulkPreview))

	mux.HandleFunc("/api/auth/status", cors(h.handleAuthStatus))
	mux.HandleFunc("/api/auth/enable", cors(h.handleAuthEnable))
	mux.HandleFunc("/api/auth/disable", cors(h.handleAuthDisable))
	mux.HandleFunc("/api/auth/users", cors(h.handleUsers))
	mux.HandleFunc("/api/auth/user", cors(h.handleUser))
	mux.HandleFunc("/api/auth/roles", cors(h.handleRoles))
	mux.HandleFunc("/api/auth/role", cors(h.handleRole))
	mux.HandleFunc("/api/auth/role/permission", cors(h.handleRolePermission))
}

// GET /api/cluster — cluster info + stats
func (h *Handler) handleCluster(w http.ResponseWriter, r *http.Request) {
	info := h.client.ClusterInfo(r.Context())
	// Inject server-side flags the UI needs
	type enriched struct {
		*etcd.ClusterInfo
		ReadOnly bool `json:"read_only"`
		MaxKeys  int  `json:"max_keys"`
	}
	writeJSON(w, enriched{ClusterInfo: info, ReadOnly: h.readOnly, MaxKeys: h.client.MaxKeys()})
}

// GET /api/keys?prefix=... — flat list of keys
func (h *Handler) handleKeys(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	var (
		nodes []*etcd.KeyNode
		err   error
	)
	if prefix != "" {
		nodes, err = h.client.ListPrefix(r.Context(), prefix)
	} else {
		nodes, err = h.client.ListAll(r.Context())
	}
	if err != nil {
		writeErr(w, err, 500)
		return
	}
	writeJSON(w, map[string]interface{}{
		"keys":  nodes,
		"count": len(nodes),
	})
}

// GET /api/key?key=... — single key with full details
func (h *Handler) handleKey(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeErr(w, fmt.Errorf("key parameter required"), 400)
		return
	}
	node, err := h.client.GetKey(r.Context(), key)
	if err != nil {
		writeErr(w, err, 404)
		return
	}
	writeJSON(w, node)
}

// GET /api/search?q=... — full-text search across keys and values
func (h *Handler) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		writeErr(w, fmt.Errorf("q parameter required"), 400)
		return
	}
	nodes, err := h.client.Search(r.Context(), q)
	if err != nil {
		writeErr(w, err, 500)
		return
	}
	writeJSON(w, map[string]interface{}{
		"results": nodes,
		"count":   len(nodes),
		"query":   q,
	})
}

// GET /api/tree?prefix=... — hierarchical tree view
func (h *Handler) handleTree(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	var (
		nodes []*etcd.KeyNode
		err   error
	)
	if prefix != "" {
		nodes, err = h.client.ListPrefix(r.Context(), prefix)
	} else {
		nodes, err = h.client.ListAll(r.Context())
	}
	if err != nil {
		writeErr(w, err, 500)
		return
	}
	tree := etcd.BuildTree(nodes)
	writeJSON(w, map[string]interface{}{
		"tree":  tree,
		"count": len(nodes),
	})
}

// GET /api/watch?prefix=... — Server-Sent Events stream of live mutations
func (h *Handler) handleWatch(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, fmt.Errorf("streaming not supported"), 500)
		return
	}

	events := make(chan etcd.WatchEvent, 64)
	connID := r.RemoteAddr + r.URL.RawQuery

	h.mu.Lock()
	h.watchers[connID] = events
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		delete(h.watchers, connID)
		h.mu.Unlock()
		close(events)
		log.Printf("[watch] client disconnected: %s", r.RemoteAddr)
	}()

	// Start etcd watch in background
	ctx := r.Context()
	go h.client.Watch(ctx, prefix, events)

	log.Printf("[watch] client connected: %s prefix=%q", r.RemoteAddr, prefix)

	// Send an event so the browser knows the stream is live
	fmt.Fprintf(w, "data: %s\n\n", mustJSON(map[string]string{
		"type": "CONNECTED", "message": "watching " + orAll(prefix),
	}))
	flusher.Flush()

	// Stream events until client disconnects
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			b, _ := json.Marshal(ev)
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func cors(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h(w, r)
	}
}

func mustJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func orAll(s string) string {
	if strings.TrimSpace(s) == "" {
		return "all keys"
	}
	return s
}

// handlePut writes/updates a key. Body: {"key":"...","value":"...","ttl":0}
func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request) {
	if h.readOnly {
		writeErr(w, fmt.Errorf("server is in read-only mode"), http.StatusForbidden)
		return
	}
	if !h.rateAllow(r.RemoteAddr) {
		writeErr(w, fmt.Errorf("rate limit exceeded"), http.StatusTooManyRequests)
		return
	}
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		writeErr(w, fmt.Errorf("method not allowed"), http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Key   string `json:"key"`
		Value string `json:"value"`
		TTL   int64  `json:"ttl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, fmt.Errorf("invalid JSON body: %w", err), http.StatusBadRequest)
		return
	}
	if body.Key == "" {
		writeErr(w, fmt.Errorf("key is required"), http.StatusBadRequest)
		return
	}
	// get old value for audit
	old := h.getOldValue(r.Context(), body.Key)
	if err := h.client.PutWithTTL(r.Context(), body.Key, body.Value, body.TTL); err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	h.alog.Log("PUT", body.Key, old, body.Value, "", r.RemoteAddr, "")
	writeJSON(w, map[string]any{"ok": true, "key": body.Key})
}

// handleDelete deletes a key. Query param: ?key=...
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	if h.readOnly {
		writeErr(w, fmt.Errorf("server is in read-only mode"), http.StatusForbidden)
		return
	}
	if !h.rateAllow(r.RemoteAddr) {
		writeErr(w, fmt.Errorf("rate limit exceeded"), http.StatusTooManyRequests)
		return
	}
	if r.Method != http.MethodDelete && r.Method != http.MethodGet {
		writeErr(w, fmt.Errorf("method not allowed"), http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		writeErr(w, fmt.Errorf("key is required"), http.StatusBadRequest)
		return
	}
	old := h.getOldValue(r.Context(), key)
	deleted, err := h.client.Delete(r.Context(), key)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	if deleted > 0 {
		h.alog.Log("DELETE", key, old, "", "", r.RemoteAddr, "")
	}
	writeJSON(w, map[string]any{"ok": true, "deleted": deleted})
}

func (h *Handler) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	s, err := h.client.AuthStatus(r.Context())
	if err != nil {
		writeErr(w, err, 500)
		return
	}
	writeJSON(w, s)
}

func (h *Handler) handleAuthEnable(w http.ResponseWriter, r *http.Request) {
	if h.readOnly {
		writeErr(w, fmt.Errorf("server is in read-only mode"), http.StatusForbidden)
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, fmt.Errorf("POST required"), 405)
		return
	}
	if err := h.client.AuthEnable(r.Context()); err != nil {
		writeErr(w, err, 500)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (h *Handler) handleAuthDisable(w http.ResponseWriter, r *http.Request) {
	if h.readOnly {
		writeErr(w, fmt.Errorf("server is in read-only mode"), http.StatusForbidden)
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, fmt.Errorf("POST required"), 405)
		return
	}
	if err := h.client.AuthDisable(r.Context()); err != nil {
		writeErr(w, err, 500)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// GET  /api/auth/users         — list all users
// POST /api/auth/users         — create user  body: {name, password}
// DELETE /api/auth/users?name= — delete user
func (h *Handler) handleUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		users, err := h.client.UserList(r.Context())
		if err != nil {
			writeErr(w, err, 500)
			return
		}
		writeJSON(w, map[string]any{"users": users})
	case http.MethodPost:
		var body struct {
			Name     string `json:"name"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, fmt.Errorf("invalid body"), 400)
			return
		}
		if body.Name == "" {
			writeErr(w, fmt.Errorf("name required"), 400)
			return
		}
		if err := h.client.UserAdd(r.Context(), body.Name, body.Password); err != nil {
			writeErr(w, err, 500)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	case http.MethodDelete:
		name := r.URL.Query().Get("name")
		if name == "" {
			writeErr(w, fmt.Errorf("name required"), 400)
			return
		}
		if err := h.client.UserDelete(r.Context(), name); err != nil {
			writeErr(w, err, 500)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	default:
		writeErr(w, fmt.Errorf("method not allowed"), 405)
	}
}

// POST /api/auth/user          — change password or grant/revoke role
// body: {action:"changepassword"|"grantrole"|"revokerole", name, password?, role?}
func (h *Handler) handleUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, fmt.Errorf("POST required"), 405)
		return
	}
	var body struct {
		Action   string `json:"action"`
		Name     string `json:"name"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, fmt.Errorf("invalid body"), 400)
		return
	}
	var err error
	switch body.Action {
	case "changepassword":
		err = h.client.UserChangePassword(r.Context(), body.Name, body.Password)
	case "grantrole":
		err = h.client.UserGrantRole(r.Context(), body.Name, body.Role)
	case "revokerole":
		err = h.client.UserRevokeRole(r.Context(), body.Name, body.Role)
	default:
		writeErr(w, fmt.Errorf("unknown action: %s", body.Action), 400)
		return
	}
	if err != nil {
		writeErr(w, err, 500)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// GET    /api/auth/roles        — list all roles
// POST   /api/auth/roles        — create role  body: {name}
// DELETE /api/auth/roles?name=  — delete role
func (h *Handler) handleRoles(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		roles, err := h.client.RoleList(r.Context())
		if err != nil {
			writeErr(w, err, 500)
			return
		}
		writeJSON(w, map[string]any{"roles": roles})
	case http.MethodPost:
		var body struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Name == "" {
			writeErr(w, fmt.Errorf("name required"), 400)
			return
		}
		if err := h.client.RoleAdd(r.Context(), body.Name); err != nil {
			writeErr(w, err, 500)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	case http.MethodDelete:
		name := r.URL.Query().Get("name")
		if name == "" {
			writeErr(w, fmt.Errorf("name required"), 400)
			return
		}
		if err := h.client.RoleDelete(r.Context(), name); err != nil {
			writeErr(w, err, 500)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	default:
		writeErr(w, fmt.Errorf("method not allowed"), 405)
	}
}

// POST   /api/auth/role         — grant/revoke role permission
// body: {action:"grant"|"revoke", role, key, range_end, perm_type}
func (h *Handler) handleRole(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, fmt.Errorf("POST required"), 405)
		return
	}
	var body struct {
		Action   string `json:"action"`
		Role     string `json:"role"`
		Key      string `json:"key"`
		RangeEnd string `json:"range_end"`
		PermType string `json:"perm_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, fmt.Errorf("invalid body"), 400)
		return
	}
	var err error
	switch body.Action {
	case "grant":
		err = h.client.RoleGrantPermission(r.Context(), body.Role, body.Key, body.RangeEnd, body.PermType)
	case "revoke":
		err = h.client.RoleRevokePermission(r.Context(), body.Role, body.Key, body.RangeEnd)
	default:
		writeErr(w, fmt.Errorf("unknown action"), 400)
		return
	}
	if err != nil {
		writeErr(w, err, 500)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// POST /api/auth/role/permission — alias kept for backward compat
func (h *Handler) handleRolePermission(w http.ResponseWriter, r *http.Request) {
	h.handleRole(w, r)
}

// 20 writes/IP/minute
const (
	rateLimit  = 20
	rateWindow = time.Minute
)

func (h *Handler) rateAllow(remoteAddr string) bool {
	ip := remoteAddr
	if idx := strings.LastIndex(ip, ":"); idx != -1 {
		ip = ip[:idx]
	}
	h.rmu.Lock()
	defer h.rmu.Unlock()
	now := time.Now()
	times := h.ratemap[ip]
	// keep only requests within window
	valid := times[:0]
	for _, t := range times {
		if now.Sub(t) < rateWindow {
			valid = append(valid, t)
		}
	}
	if len(valid) >= rateLimit {
		h.ratemap[ip] = valid
		return false
	}
	h.ratemap[ip] = append(valid, now)
	return true
}

func (h *Handler) getOldValue(ctx context.Context, key string) string {
	tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	node, err := h.client.GetKey(tctx, key)
	if err != nil || node == nil {
		return ""
	}
	return node.Value
}

// GET /api/key/history?key=...&limit=20
func (h *Handler) handleKeyHistory(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeErr(w, fmt.Errorf("key required"), 400)
		return
	}
	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	entries, err := h.client.KeyHistory(r.Context(), key, limit)
	if err != nil {
		writeErr(w, err, 500)
		return
	}
	writeJSON(w, map[string]any{"key": key, "history": entries})
}

// GET /api/leases
func (h *Handler) handleLeases(w http.ResponseWriter, r *http.Request) {
	leases, err := h.client.LeaseList(r.Context())
	if err != nil {
		writeErr(w, err, 500)
		return
	}
	writeJSON(w, map[string]any{"leases": leases})
}

// POST /api/lease/revoke body: {"id": 1234567}
func (h *Handler) handleLeaseRevoke(w http.ResponseWriter, r *http.Request) {
	if h.readOnly {
		writeErr(w, fmt.Errorf("read-only mode"), 403)
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, fmt.Errorf("POST required"), 405)
		return
	}
	var body struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err, 400)
		return
	}
	if err := h.client.LeaseRevoke(r.Context(), body.ID); err != nil {
		writeErr(w, err, 500)
		return
	}
	h.alog.Log("LEASE_REVOKE", "", "", "", "", r.RemoteAddr, strconv.FormatInt(body.ID, 16))
	writeJSON(w, map[string]any{"ok": true})
}

// GET /api/export?prefix=...   → JSON array of {key,value,ttl}
func (h *Handler) handleExport(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	entries, err := h.client.Export(r.Context(), prefix)
	if err != nil {
		writeErr(w, err, 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", `attachment; filename="etcd-export.json"`)
	json.NewEncoder(w).Encode(map[string]any{"count": len(entries), "entries": entries})
}

// POST /api/import  body: {"entries":[{key,value,ttl},...]}
func (h *Handler) handleImport(w http.ResponseWriter, r *http.Request) {
	if h.readOnly {
		writeErr(w, fmt.Errorf("read-only mode"), 403)
		return
	}
	if !h.rateAllow(r.RemoteAddr) {
		writeErr(w, fmt.Errorf("rate limit exceeded"), 429)
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, fmt.Errorf("POST required"), 405)
		return
	}
	var body struct {
		Entries []etcd.ExportEntry `json:"entries"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err, 400)
		return
	}
	n, err := h.client.Import(r.Context(), body.Entries)
	if err != nil {
		writeErr(w, fmt.Errorf("imported %d before error: %w", n, err), 500)
		return
	}
	h.alog.Log("IMPORT", "", "", "", "", r.RemoteAddr, fmt.Sprintf("%d keys", n))
	writeJSON(w, map[string]any{"ok": true, "imported": n})
}

// GET /api/keys/bulk-preview?prefix=... — returns matching keys without deleting
func (h *Handler) handleBulkPreview(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	if prefix == "" {
		writeErr(w, fmt.Errorf("prefix required"), 400)
		return
	}
	nodes, err := h.client.ListPrefix(r.Context(), prefix)
	if err != nil {
		writeErr(w, err, 500)
		return
	}
	keys := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if !n.IsDir {
			keys = append(keys, n.Key)
		}
	}
	writeJSON(w, map[string]any{"count": len(keys), "keys": keys})
}

// POST /api/keys/bulk-delete  body: {"prefix":"..."}
func (h *Handler) handleBulkDelete(w http.ResponseWriter, r *http.Request) {
	if h.readOnly {
		writeErr(w, fmt.Errorf("read-only mode"), 403)
		return
	}
	if !h.rateAllow(r.RemoteAddr) {
		writeErr(w, fmt.Errorf("rate limit exceeded"), 429)
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, fmt.Errorf("POST required"), 405)
		return
	}
	var body struct {
		Prefix string `json:"prefix"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Prefix == "" {
		writeErr(w, fmt.Errorf("prefix required"), 400)
		return
	}
	deleted, err := h.client.DeletePrefix(r.Context(), body.Prefix)
	if err != nil {
		writeErr(w, err, 500)
		return
	}
	h.alog.Log("BULK_DELETE", body.Prefix, "", "", "", r.RemoteAddr, fmt.Sprintf("%d keys", deleted))
	writeJSON(w, map[string]any{"ok": true, "deleted": deleted})
}
