package etcd

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Config struct {
	Endpoints []string
	Username  string
	Password  string
	TLS       *tls.Config
	MaxKeys   int // 0 = use default (5000)
}

type Client struct {
	c         *clientv3.Client
	Endpoints []string
	maxKeys   int
}

type KeyNode struct {
	Key           string     `json:"key"`
	Value         string     `json:"value"`
	ValueType     string     `json:"value_type"`
	BinaryHint    string     `json:"binary_hint"`
	ValueBase64   string     `json:"value_base64"`
	HexPreview    string     `json:"hex_preview"`
	ExtractedJSON string     `json:"extracted_json"`
	Version       int64      `json:"version"`
	CreateRev     int64      `json:"create_rev"`
	ModRev        int64      `json:"mod_rev"`
	Lease         int64      `json:"lease"`
	TTL           int64      `json:"ttl"`
	Size          int        `json:"size"`
	Children      []*KeyNode `json:"children,omitempty"`
	IsDir         bool       `json:"is_dir"`
	ChildCount    int        `json:"child_count"`
}

type ClusterInfo struct {
	Endpoints   []string     `json:"endpoints"`
	Members     []MemberInfo `json:"members"`
	Connected   bool         `json:"connected"`
	Version     string       `json:"version"`
	DBSize      int64        `json:"db_size"`
	DBSizeHuman string       `json:"db_size_human"`
	KeyCount    int64        `json:"key_count"`
}

type MemberInfo struct {
	ID         uint64   `json:"id"`
	Name       string   `json:"name"`
	PeerURLs   []string `json:"peer_urls"`
	ClientURLs []string `json:"client_urls"`
	IsLearner  bool     `json:"is_learner"`
	IsLeader   bool     `json:"is_leader"`
}

type WatchEvent struct {
	Type      string `json:"type"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	OldValue  string `json:"old_value"`
	Timestamp string `json:"timestamp"`
	Revision  int64  `json:"revision"`
}

func New(cfg Config) (*Client, error) {
	ccfg := clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: 8 * time.Second,
		TLS:         cfg.TLS,
	}
	if cfg.Username != "" {
		ccfg.Username = cfg.Username
		ccfg.Password = cfg.Password
	}
	c, err := clientv3.New(ccfg)
	if err != nil {
		return nil, fmt.Errorf("connect to etcd %v: %w", cfg.Endpoints, err)
	}
	mk := cfg.MaxKeys
	if mk <= 0 {
		mk = 5000
	}
	return &Client{c: c, Endpoints: cfg.Endpoints, maxKeys: mk}, nil
}

func (cl *Client) Ping(ctx context.Context) bool {
	tctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	_, err := cl.c.Get(tctx, "__ping__")
	return err == nil
}

func (cl *Client) MaxKeys() int { return cl.maxKeys }

func (cl *Client) Close() { cl.c.Close() }

// ClusterInfo returns live cluster metadata.
// Each sub-call gets its own fresh context so a slow dial on the first
// call does not cascade into all subsequent calls timing out.
func (cl *Client) ClusterInfo(ctx context.Context) *ClusterInfo {
	info := &ClusterInfo{Endpoints: cl.Endpoints}

	// Connectivity check — generous timeout for first-ever gRPC dial.
	pingCtx, pingCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pingCancel()
	if _, err := cl.c.Get(pingCtx, "__ping__"); err != nil {
		return info // not connected
	}
	info.Connected = true

	// Member list — fresh 5s context.
	mlCtx, mlCancel := context.WithTimeout(ctx, 5*time.Second)
	defer mlCancel()
	if mr, err := cl.c.MemberList(mlCtx); err == nil {
		for _, m := range mr.Members {
			info.Members = append(info.Members, MemberInfo{
				ID: m.ID, Name: m.Name,
				PeerURLs: m.PeerURLs, ClientURLs: m.ClientURLs,
				IsLearner: m.IsLearner,
			})
		}
	}

	// Status (DB size, version, leader ID) — query every endpoint so we
	//    always find the leader even in a multi-node cluster.
	if len(cl.Endpoints) > 0 {
		stCtx, stCancel := context.WithTimeout(ctx, 5*time.Second)
		defer stCancel()
		var leaderID uint64
		for _, ep := range cl.Endpoints {
			if sr, err := cl.c.Status(stCtx, ep); err == nil {
				if info.DBSize == 0 {
					info.DBSize = sr.DbSize
					info.DBSizeHuman = humanBytes(sr.DbSize)
					info.Version = sr.Version
				}
				if sr.Leader != 0 {
					leaderID = sr.Leader
				}
			}
		}
		if leaderID != 0 {
			for i := range info.Members {
				if info.Members[i].ID == leaderID {
					info.Members[i].IsLeader = true
					break
				}
			}
		}
	}

	kcCtx, kcCancel := context.WithTimeout(ctx, 5*time.Second)
	defer kcCancel()
	if kr, err := cl.c.Get(kcCtx, "", clientv3.WithFromKey(), clientv3.WithRange("\x00"), clientv3.WithCountOnly()); err == nil {
		info.KeyCount = kr.Count
	}

	return info
}

func (cl *Client) ListAll(ctx context.Context) ([]*KeyNode, error) {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := cl.c.Get(tctx, "", clientv3.WithFromKey(), clientv3.WithRange("\x00"), clientv3.WithLimit(int64(cl.maxKeys)))
	if err != nil {
		return nil, err
	}
	return cl.toNodes(tctx, resp.Kvs), nil
}

func (cl *Client) GetKey(ctx context.Context, key string) (*KeyNode, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := cl.c.Get(tctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return cl.toNode(tctx, resp.Kvs[0]), nil
}

func (cl *Client) ListPrefix(ctx context.Context, prefix string) ([]*KeyNode, error) {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := cl.c.Get(tctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(int64(cl.maxKeys)))
	if err != nil {
		return nil, err
	}
	return cl.toNodes(tctx, resp.Kvs), nil
}

func (cl *Client) Search(ctx context.Context, query string) ([]*KeyNode, error) {
	all, err := cl.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	q := strings.ToLower(query)
	var out []*KeyNode
	for _, n := range all {
		if strings.Contains(strings.ToLower(n.Key), q) ||
			strings.Contains(strings.ToLower(n.Value), q) {
			out = append(out, n)
		}
	}
	return out, nil
}

func BuildTree(nodes []*KeyNode) []*KeyNode {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Key < nodes[j].Key })
	dirs := map[string]*KeyNode{}
	var roots []*KeyNode

	ensureDir := func(path string, depth int, parts []string) *KeyNode {
		if n, ok := dirs[path]; ok {
			return n
		}
		n := &KeyNode{Key: path, IsDir: true}
		dirs[path] = n
		if depth == 1 {
			roots = append(roots, n)
		} else {
			parentPath := "/" + strings.Join(parts[:depth-1], "/")
			if p, ok := dirs[parentPath]; ok {
				p.Children = append(p.Children, n)
				p.ChildCount++
			}
		}
		return n
	}

	for _, node := range nodes {
		trimmed := strings.TrimPrefix(node.Key, "/")
		parts := strings.Split(trimmed, "/")
		if len(parts) <= 1 {
			roots = append(roots, node)
			continue
		}
		for d := 1; d < len(parts); d++ {
			ensureDir("/"+strings.Join(parts[:d], "/"), d, parts)
		}
		parentPath := "/" + strings.Join(parts[:len(parts)-1], "/")
		if p, ok := dirs[parentPath]; ok {
			p.Children = append(p.Children, node)
			p.ChildCount++
		}
	}
	return roots
}

func (cl *Client) Watch(ctx context.Context, prefix string, out chan<- WatchEvent) {
	var wch clientv3.WatchChan
	if prefix == "" || prefix == "/" {
		wch = cl.c.Watch(ctx, "", clientv3.WithFromKey(), clientv3.WithRange("\x00"), clientv3.WithPrevKV())
	} else {
		wch = cl.c.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	}
	for {
		select {
		case <-ctx.Done():
			return
		case wr, ok := <-wch:
			if !ok {
				return
			}
			for _, ev := range wr.Events {
				e := WatchEvent{
					Key:       string(ev.Kv.Key),
					Value:     string(ev.Kv.Value),
					Timestamp: time.Now().Format("15:04:05.000"),
					Revision:  ev.Kv.ModRevision,
				}
				if ev.Type == mvccpb.PUT {
					e.Type = "PUT"
				} else {
					e.Type = "DELETE"
					e.Value = ""
				}
				if ev.PrevKv != nil {
					e.OldValue = string(ev.PrevKv.Value)
				}
				select {
				case out <- e:
				default:
				}
			}
		}
	}
}

func (cl *Client) toNodes(ctx context.Context, kvs []*mvccpb.KeyValue) []*KeyNode {
	out := make([]*KeyNode, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, cl.toNode(ctx, kv))
	}
	return out
}

func (cl *Client) toNode(ctx context.Context, kv *mvccpb.KeyValue) *KeyNode {
	raw := kv.Value
	val := string(raw)
	vtype := detectType(val)
	if vtype == "json" {
		val = prettyJSON(val)
	}
	n := &KeyNode{
		Key: string(kv.Key), Value: val, ValueType: vtype,
		Version: kv.Version, CreateRev: kv.CreateRevision,
		ModRev: kv.ModRevision, Lease: kv.Lease, Size: len(raw),
	}
	if vtype == "binary" {
		n.BinaryHint = detectBinaryHint(raw)
		n.ValueBase64 = base64.StdEncoding.EncodeToString(raw)
		n.HexPreview = hexDump(raw, 512)
		n.ExtractedJSON = extractJSON(raw)
		n.Value = ""
	}
	if kv.Lease != 0 {
		lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if lr, err := cl.c.TimeToLive(lctx, clientv3.LeaseID(kv.Lease)); err == nil {
			n.TTL = lr.TTL
		}
	}
	return n
}

func detectType(v string) string {
	s := strings.TrimSpace(v)
	if s == "" {
		return "text"
	}
	if strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
		var j interface{}
		if json.Unmarshal([]byte(s), &j) == nil {
			return "json"
		}
	}
	for _, b := range []byte(v) {
		if b < 32 && b != '\n' && b != '\r' && b != '\t' {
			return "binary"
		}
	}
	return "text"
}

func detectBinaryHint(b []byte) string {
	if len(b) >= 4 && b[0] == 'k' && b[1] == '8' && b[2] == 's' && b[3] == 0x00 {
		return "k8s-protobuf"
	}
	if len(b) >= 4 {
		wire := 0
		for _, bb := range b[:min(16, len(b))] {
			if bb&0x07 <= 5 { // valid wire type (0-5)
				wire++
			}
		}
		if wire >= 10 {
			return "protobuf"
		}
	}
	return "binary"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// extractJSON scans raw binary bytes for all valid JSON objects/arrays and
// returns them pretty-printed. For k8s protobuf the 4-byte magic is skipped.
// Multiple JSON fragments are wrapped in a top-level array.
func extractJSON(b []byte) string {
	// Skip k8s magic prefix if present.
	if len(b) >= 4 && b[0] == 'k' && b[1] == '8' && b[2] == 's' && b[3] == 0x00 {
		b = b[4:]
	}

	var fragments []json.RawMessage
	i := 0
	for i < len(b) {
		// Only try from opening braces/brackets — JSON objects and arrays.
		if b[i] != '{' && b[i] != '[' {
			i++
			continue
		}
		// Find the matching close brace using a simple stack counter.
		// This avoids scanning strings with nested braces incorrectly but is
		// good enough for extracting embedded JSON in protobuf payloads.
		open := b[i]
		var close byte
		if open == '{' {
			close = '}'
		} else {
			close = ']'
		}
		depth := 0
		inStr := false
		escaped := false
		j := i
		for j < len(b) {
			c := b[j]
			if escaped {
				escaped = false
			} else if inStr {
				if c == '\\' {
					escaped = true
				} else if c == '"' {
					inStr = false
				}
			} else {
				if c == '"' {
					inStr = true
				} else if c == open {
					depth++
				} else if c == close {
					depth--
					if depth == 0 {
						j++
						break
					}
				}
			}
			j++
		}
		if depth != 0 {
			i++
			continue
		}
		candidate := b[i:j]
		// Only keep fragments that are valid JSON and at least a few chars.
		if len(candidate) >= 4 {
			var parsed interface{}
			if err := json.Unmarshal(candidate, &parsed); err == nil {
				pretty, _ := json.MarshalIndent(parsed, "", "  ")
				fragments = append(fragments, pretty)
			}
		}
		i = j
	}

	if len(fragments) == 0 {
		return ""
	}
	if len(fragments) == 1 {
		return string(fragments[0])
	}
	// Multiple fragments → wrap in array with separator comments replaced by
	// a plain array so the result is still valid JSON.
	combined, _ := json.MarshalIndent(fragments, "", "  ")
	return string(combined)
}

// hexDump returns a classic hex-editor view of up to maxBytes bytes.
// Format: "0000  6b 38 73 00 ...  k8s."
func hexDump(b []byte, maxBytes int) string {
	if len(b) > maxBytes {
		b = b[:maxBytes]
	}
	var sb strings.Builder
	for i := 0; i < len(b); i += 16 {
		end := i + 16
		if end > len(b) {
			end = len(b)
		}
		chunk := b[i:end]
		// offset
		fmt.Fprintf(&sb, "%04x  ", i)
		// hex bytes
		for j, bb := range chunk {
			fmt.Fprintf(&sb, "%02x ", bb)
			if j == 7 {
				sb.WriteByte(' ')
			}
		}
		// pad short last line
		pad := 16 - len(chunk)
		for p := 0; p < pad; p++ {
			sb.WriteString("   ")
		}
		if pad > 0 && len(chunk) <= 8 {
			sb.WriteByte(' ')
		}
		// ASCII
		sb.WriteString(" |")
		for _, bb := range chunk {
			if bb >= 32 && bb < 127 {
				sb.WriteByte(bb)
			} else {
				sb.WriteByte('.')
			}
		}
		sb.WriteString("|\n")
	}
	if len(b) == maxBytes {
		fmt.Fprintf(&sb, "... (%d bytes total, showing first %d)", maxBytes, maxBytes)
	}
	return sb.String()
}

func prettyJSON(v string) string {
	var j interface{}
	if err := json.Unmarshal([]byte(v), &j); err != nil {
		return v
	}
	b, _ := json.MarshalIndent(j, "", "  ")
	return string(b)
}

func humanBytes(b int64) string {
	const u = 1024
	if b < u {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(u), 0
	for n := b / u; n >= u; n /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// Put writes or overwrites a key.
func (cl *Client) Put(ctx context.Context, key, value string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.Put(tctx, key, value)
	return err
}

// Delete removes a key.
func (cl *Client) Delete(ctx context.Context, key string) (int64, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := cl.c.Delete(tctx, key)
	if err != nil {
		return 0, err
	}
	return resp.Deleted, nil
}

type AuthStatus struct {
	Enabled bool `json:"enabled"`
}

type UserInfo struct {
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}

type Permission struct {
	Key      string `json:"key"`
	RangeEnd string `json:"range_end"`
	PermType string `json:"perm_type"`
}

type RoleInfo struct {
	Name        string       `json:"name"`
	Permissions []Permission `json:"permissions"`
}

func permTypeName(p int32) string {
	switch p {
	case 0:
		return "READ"
	case 1:
		return "WRITE"
	case 2:
		return "READWRITE"
	}
	return "UNKNOWN"
}

func (cl *Client) AuthStatus(ctx context.Context) (*AuthStatus, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := cl.c.AuthStatus(tctx)
	if err != nil {
		return nil, err
	}
	return &AuthStatus{Enabled: resp.Enabled}, nil
}

func (cl *Client) AuthEnable(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.AuthEnable(tctx)
	return err
}

func (cl *Client) AuthDisable(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.AuthDisable(tctx)
	return err
}

func (cl *Client) UserList(ctx context.Context) ([]UserInfo, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := cl.c.UserList(tctx)
	if err != nil {
		return nil, err
	}
	users := make([]UserInfo, 0, len(resp.Users))
	for _, name := range resp.Users {
		u := UserInfo{Name: name}
		if gr, err2 := cl.c.UserGet(tctx, name); err2 == nil {
			u.Roles = gr.Roles
		}
		if u.Roles == nil {
			u.Roles = []string{}
		}
		users = append(users, u)
	}
	return users, nil
}

func (cl *Client) UserAdd(ctx context.Context, name, password string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.UserAdd(tctx, name, password)
	return err
}

func (cl *Client) UserDelete(ctx context.Context, name string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.UserDelete(tctx, name)
	return err
}

func (cl *Client) UserChangePassword(ctx context.Context, name, password string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.UserChangePassword(tctx, name, password)
	return err
}

func (cl *Client) UserGrantRole(ctx context.Context, user, role string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.UserGrantRole(tctx, user, role)
	return err
}

func (cl *Client) UserRevokeRole(ctx context.Context, user, role string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.UserRevokeRole(tctx, user, role)
	return err
}

func (cl *Client) RoleList(ctx context.Context) ([]RoleInfo, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := cl.c.RoleList(tctx)
	if err != nil {
		return nil, err
	}
	roles := make([]RoleInfo, 0, len(resp.Roles))
	for _, name := range resp.Roles {
		r := RoleInfo{Name: name, Permissions: []Permission{}}
		if gr, err2 := cl.c.RoleGet(tctx, name); err2 == nil {
			for _, p := range gr.Perm {
				r.Permissions = append(r.Permissions, Permission{
					Key:      string(p.Key),
					RangeEnd: string(p.RangeEnd),
					PermType: permTypeName(int32(p.PermType)),
				})
			}
		}
		roles = append(roles, r)
	}
	return roles, nil
}

func (cl *Client) RoleAdd(ctx context.Context, name string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.RoleAdd(tctx, name)
	return err
}

func (cl *Client) RoleDelete(ctx context.Context, name string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.RoleDelete(tctx, name)
	return err
}

func (cl *Client) RoleGrantPermission(ctx context.Context, role, key, rangeEnd, permType string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var pt clientv3.PermissionType
	switch strings.ToUpper(permType) {
	case "READ":
		pt = clientv3.PermissionType(0)
	case "WRITE":
		pt = clientv3.PermissionType(1)
	default:
		pt = clientv3.PermissionType(2) // READWRITE
	}
	_, err := cl.c.RoleGrantPermission(tctx, role, key, rangeEnd, pt)
	return err
}

func (cl *Client) RoleRevokePermission(ctx context.Context, role, key, rangeEnd string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.RoleRevokePermission(tctx, role, key, rangeEnd)
	return err
}

// PutWithTTL writes key=value, optionally attaching a lease for ttlSeconds.
// ttlSeconds <= 0 means no TTL (persists forever).
func (cl *Client) PutWithTTL(ctx context.Context, key, value string, ttlSeconds int64) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if ttlSeconds > 0 {
		lr, err := cl.c.Grant(tctx, ttlSeconds)
		if err != nil {
			return fmt.Errorf("grant lease: %w", err)
		}
		_, err = cl.c.Put(tctx, key, value, clientv3.WithLease(lr.ID))
		return err
	}
	_, err := cl.c.Put(tctx, key, value)
	return err
}

// HistoryEntry holds one past value of a key.
type HistoryEntry struct {
	Revision  int64  `json:"revision"`
	Value     string `json:"value"`
	ValueType string `json:"value_type"`
	ModTime   string `json:"mod_time"` // approximated — etcd does not store wall-clock time per revision
	IsDelete  bool   `json:"is_delete"`
}

// KeyHistory returns the last N revisions of a key using the Watch API in
// historical mode. etcd only retains revisions inside its compaction window.
func (cl *Client) KeyHistory(ctx context.Context, key string, limit int) ([]HistoryEntry, error) {
	// First get the current key to learn createRev
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cur, err := cl.c.Get(tctx, key)
	if err != nil {
		return nil, err
	}

	var fromRev int64 = 1
	if len(cur.Kvs) > 0 {
		fromRev = cur.Kvs[0].CreateRevision
	}

	// Watch from createRevision to now to collect all versions
	wctx, wcancel := context.WithTimeout(ctx, 8*time.Second)
	defer wcancel()

	wch := cl.c.Watch(wctx, key,
		clientv3.WithRev(fromRev),
		clientv3.WithPrevKV(),
	)

	var entries []HistoryEntry
	for resp := range wch {
		for _, ev := range resp.Events {
			val := string(ev.Kv.Value)
			e := HistoryEntry{
				Revision:  ev.Kv.ModRevision,
				IsDelete:  ev.Type == mvccpb.DELETE,
				ValueType: detectType(val),
			}
			if ev.Type == mvccpb.DELETE {
				e.Value = ""
			} else {
				e.Value = val
			}
			entries = append(entries, e)
		}
		if resp.Header.Revision >= cur.Header.Revision {
			break
		}
	}

	// Reverse so newest first, trim to limit
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

// LeaseInfo describes one active lease.
type LeaseInfo struct {
	ID         int64    `json:"id"`
	IDHex      string   `json:"id_hex"`
	TTL        int64    `json:"ttl"`
	GrantedTTL int64    `json:"granted_ttl"`
	Keys       []string `json:"keys"`
}

// LeaseList returns all active leases with their attached keys.
func (cl *Client) LeaseList(ctx context.Context) ([]LeaseInfo, error) {
	tctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	lr, err := cl.c.Leases(tctx)
	if err != nil {
		return nil, err
	}
	out := make([]LeaseInfo, 0, len(lr.Leases))
	for _, l := range lr.Leases {
		info := LeaseInfo{
			ID:    int64(l.ID),
			IDHex: fmt.Sprintf("%x", int64(l.ID)),
			Keys:  []string{},
		}
		if ttlResp, err2 := cl.c.TimeToLive(tctx, l.ID, clientv3.WithAttachedKeys()); err2 == nil {
			info.TTL = ttlResp.TTL
			info.GrantedTTL = ttlResp.GrantedTTL
			for _, k := range ttlResp.Keys {
				info.Keys = append(info.Keys, string(k))
			}
		}
		out = append(out, info)
	}
	return out, nil
}

// LeaseRevoke forcibly removes a lease (and all its attached keys).
func (cl *Client) LeaseRevoke(ctx context.Context, id int64) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := cl.c.Revoke(tctx, clientv3.LeaseID(id))
	return err
}

// ExportEntry is one key/value pair in an export snapshot.
type ExportEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	TTL   int64  `json:"ttl,omitempty"` // remaining TTL, 0 if no lease
}

// Export returns all keys (under optional prefix) as a snapshot slice.
func (cl *Client) Export(ctx context.Context, prefix string) ([]ExportEntry, error) {
	var nodes []*KeyNode
	var err error
	if prefix != "" {
		nodes, err = cl.ListPrefix(ctx, prefix)
	} else {
		nodes, err = cl.ListAll(ctx)
	}
	if err != nil {
		return nil, err
	}
	out := make([]ExportEntry, 0, len(nodes))
	for _, n := range nodes {
		if n.IsDir {
			continue
		}
		out = append(out, ExportEntry{Key: n.Key, Value: n.Value, TTL: n.TTL})
	}
	return out, nil
}

// Import writes a slice of ExportEntry entries into etcd.
// Existing keys are overwritten. Returns count of written keys.
func (cl *Client) Import(ctx context.Context, entries []ExportEntry) (int, error) {
	written := 0
	for _, e := range entries {
		if err := cl.PutWithTTL(ctx, e.Key, e.Value, e.TTL); err != nil {
			return written, fmt.Errorf("key %q: %w", e.Key, err)
		}
		written++
	}
	return written, nil
}

// DeletePrefix deletes all keys with the given prefix. Returns deleted count.
func (cl *Client) DeletePrefix(ctx context.Context, prefix string) (int64, error) {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := cl.c.Delete(tctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	return resp.Deleted, nil
}
