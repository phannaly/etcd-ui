package etcd

import (
	"strings"
	"testing"
)

func TestDetectType(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty string", "", "text"},
		{"plain text", "hello world", "text"},
		{"json object", `{"key":"value"}`, "json"},
		{"json array", `[1,2,3]`, "json"},
		{"json with leading whitespace", "  { \"a\": 1 }  ", "json"},
		{"invalid json – bad value", `{"key": bad}`, "text"},
		{"binary – control char", "hello\x01world", "binary"},
		{"binary – null byte", "\x00", "binary"},
		{"tab and newline are allowed", "line1\nline2\ttab", "text"},
		{"nested json", `{"a":{"b":{"c":1}}}`, "json"},
		{"bare number is not json object", "42", "text"},
		{"bare boolean is not json object", "true", "text"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectType(tt.input)
			if got != tt.want {
				t.Errorf("detectType(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestPrettyJSON(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantContain string
		passThrough bool
	}{
		{
			name:        "compact object gets indented",
			input:       `{"a":1,"b":2}`,
			wantContain: "\"a\": 1",
		},
		{
			name:        "invalid json passes through unchanged",
			input:       `not json`,
			passThrough: true,
		},
		{
			name:        "nested object",
			input:       `{"x":{"y":3}}`,
			wantContain: "\"x\"",
		},
		{
			name:        "array",
			input:       `[1,2,3]`,
			wantContain: "1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := prettyJSON(tt.input)
			if tt.passThrough {
				if got != tt.input {
					t.Errorf("prettyJSON(%q) = %q, want unchanged", tt.input, got)
				}
				return
			}
			if !strings.Contains(got, tt.wantContain) {
				t.Errorf("prettyJSON(%q) = %q, want to contain %q", tt.input, got, tt.wantContain)
			}
		})
	}
}

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1024 * 1024, "1.0 MiB"},
		{1024 * 1024 * 1024, "1.0 GiB"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := humanBytes(tt.input)
			if got != tt.want {
				t.Errorf("humanBytes(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestPermTypeName(t *testing.T) {
	tests := []struct {
		input int32
		want  string
	}{
		{0, "READ"},
		{1, "WRITE"},
		{2, "READWRITE"},
		{99, "UNKNOWN"},
		{-1, "UNKNOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := permTypeName(tt.input)
			if got != tt.want {
				t.Errorf("permTypeName(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestBuildTree(t *testing.T) {
	t.Run("nil input returns empty", func(t *testing.T) {
		roots := BuildTree(nil)
		if len(roots) != 0 {
			t.Errorf("expected empty tree, got %d roots", len(roots))
		}
	})

	t.Run("flat keys have no slashes", func(t *testing.T) {
		nodes := []*KeyNode{{Key: "alpha"}, {Key: "beta"}, {Key: "gamma"}}
		roots := BuildTree(nodes)
		if len(roots) != 3 {
			t.Errorf("expected 3 roots, got %d", len(roots))
		}
		for _, r := range roots {
			if r.IsDir {
				t.Errorf("flat key %q should not be a dir", r.Key)
			}
		}
	})

	t.Run("single nested key creates parent dir node", func(t *testing.T) {
		nodes := []*KeyNode{{Key: "/app/config"}}
		roots := BuildTree(nodes)
		if len(roots) != 1 {
			t.Fatalf("expected 1 root, got %d", len(roots))
		}
		root := roots[0]
		if !root.IsDir {
			t.Error("root should be a directory node")
		}
		if root.Key != "/app" {
			t.Errorf("root.Key = %q, want /app", root.Key)
		}
		if len(root.Children) != 1 {
			t.Fatalf("expected 1 child, got %d", len(root.Children))
		}
		if root.Children[0].Key != "/app/config" {
			t.Errorf("child.Key = %q, want /app/config", root.Children[0].Key)
		}
	})

	t.Run("siblings share parent directory", func(t *testing.T) {
		nodes := []*KeyNode{
			{Key: "/svc/a/addr"},
			{Key: "/svc/a/port"},
			{Key: "/svc/b/addr"},
		}
		roots := BuildTree(nodes)
		if len(roots) != 1 || roots[0].Key != "/svc" {
			t.Fatalf("expected single /svc root, got %v", roots)
		}
		if roots[0].ChildCount != 2 {
			t.Errorf("ChildCount = %d, want 2 (/svc/a and /svc/b)", roots[0].ChildCount)
		}
	})

	t.Run("keys are sorted alphabetically", func(t *testing.T) {
		nodes := []*KeyNode{{Key: "/z"}, {Key: "/a"}, {Key: "/m"}}
		roots := BuildTree(nodes)
		if len(roots) != 3 {
			t.Fatalf("expected 3 roots, got %d", len(roots))
		}
		if roots[0].Key != "/a" || roots[1].Key != "/m" || roots[2].Key != "/z" {
			t.Errorf("wrong order: %v", []string{roots[0].Key, roots[1].Key, roots[2].Key})
		}
	})

	t.Run("deeply nested path builds full hierarchy", func(t *testing.T) {
		nodes := []*KeyNode{{Key: "/a/b/c/d"}}
		roots := BuildTree(nodes)
		if len(roots) != 1 || roots[0].Key != "/a" {
			t.Fatalf("expected root /a")
		}
		l1 := roots[0].Children
		if len(l1) != 1 || l1[0].Key != "/a/b" {
			t.Fatalf("/a should have child /a/b")
		}
		l2 := l1[0].Children
		if len(l2) != 1 || l2[0].Key != "/a/b/c" {
			t.Fatalf("/a/b should have child /a/b/c")
		}
		l3 := l2[0].Children
		if len(l3) != 1 || l3[0].Key != "/a/b/c/d" {
			t.Fatalf("/a/b/c should have child /a/b/c/d")
		}
		if l3[0].IsDir {
			t.Error("leaf node /a/b/c/d should not be a dir")
		}
	})

	t.Run("mixed slash and non-slash keys", func(t *testing.T) {
		nodes := []*KeyNode{
			{Key: "flat-key"},
			{Key: "/nested/key"},
		}
		roots := BuildTree(nodes)
		if len(roots) != 2 {
			t.Errorf("expected 2 roots, got %d", len(roots))
		}
	})

	t.Run("isDir child count tracks correctly", func(t *testing.T) {
		nodes := []*KeyNode{
			{Key: "/x/1"}, {Key: "/x/2"}, {Key: "/x/3"},
		}
		roots := BuildTree(nodes)
		if len(roots) != 1 || roots[0].ChildCount != 3 {
			t.Errorf("expected /x with 3 children, got ChildCount=%d", roots[0].ChildCount)
		}
	})
}
