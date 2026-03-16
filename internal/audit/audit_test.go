package audit

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newWithWriter(w io.Writer) *Logger {
	return &Logger{logger: log.New(w, "", 0)}
}

func TestNew_EmptyPathUsesStdout(t *testing.T) {
	l := New("")
	if l == nil {
		t.Fatal("New(\"\") returned nil")
	}
	if l.logger == nil {
		t.Fatal("logger field is nil")
	}
}

func TestNew_FilePathCreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")

	l := New(path)
	if l == nil {
		t.Fatal("New(path) returned nil")
	}
	l.Log("TEST", "/key", "old", "new", "user", "1.2.3.4:9999", "detail")

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("could not read audit file: %v", err)
	}
	if len(data) == 0 {
		t.Error("audit file is empty after Log call")
	}
}

func TestNew_InvalidPathFallsBackToStdout(t *testing.T) {
	// Writing to a non-existent directory should fall back gracefully.
	l := New("/nonexistent/path/that/does/not/exist/audit.log")
	if l == nil {
		t.Fatal("New with invalid path returned nil")
	}
}

func TestLog_OutputIsValidJSON(t *testing.T) {
	var buf bytes.Buffer
	l := newWithWriter(&buf)

	l.Log("PUT", "/app/config", "old-val", "new-val", "alice", "10.0.0.1:4321", "")

	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("Log produced no output")
	}

	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("Log output is not valid JSON: %v\nOutput: %s", err, line)
	}
}

func TestLog_AllFieldsPresent(t *testing.T) {
	var buf bytes.Buffer
	l := newWithWriter(&buf)

	l.Log("DELETE", "/tmp/foo", "bar", "", "bob", "192.168.1.1:5000", "some detail")

	var entry Entry
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	checks := map[string]string{
		"action":    entry.Action,
		"key":       entry.Key,
		"old_value": entry.OldValue,
		"new_value": entry.NewValue,
		"user":      entry.User,
		"remote_ip": entry.RemoteIP,
		"detail":    entry.Detail,
	}
	want := map[string]string{
		"action":    "DELETE",
		"key":       "/tmp/foo",
		"old_value": "bar",
		"new_value": "",
		"user":      "bob",
		"remote_ip": "192.168.1.1:5000",
		"detail":    "some detail",
	}
	for field, got := range checks {
		if got != want[field] {
			t.Errorf("%s = %q, want %q", field, got, want[field])
		}
	}
}

func TestLog_TimeFieldIsRFC3339(t *testing.T) {
	var buf bytes.Buffer
	l := newWithWriter(&buf)
	l.Log("IMPORT", "", "", "", "", "", "5 keys")

	var entry Entry
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if entry.Time == "" {
		t.Fatal("time field is empty")
	}
	if _, err := time.Parse(time.RFC3339, entry.Time); err != nil {
		t.Errorf("time %q is not RFC3339: %v", entry.Time, err)
	}
}

func TestLog_EmptyFieldsOmittedFromJSON(t *testing.T) {
	var buf bytes.Buffer
	l := newWithWriter(&buf)
	l.Log("AUTH_ENABLE", "", "", "", "", "", "")

	line := strings.TrimSpace(buf.String())
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	for _, field := range []string{"key", "old_value", "new_value", "user", "remote_ip", "detail"} {
		if _, ok := raw[field]; ok {
			t.Errorf("empty field %q should be omitted from JSON but was present", field)
		}
	}
}

func TestLog_MultipleCallsProduceMultipleLines(t *testing.T) {
	var buf bytes.Buffer
	l := newWithWriter(&buf)

	l.Log("PUT", "/a", "", "v1", "", "", "")
	l.Log("PUT", "/b", "", "v2", "", "", "")
	l.Log("DELETE", "/c", "v3", "", "", "", "")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 log lines, got %d\n%s", len(lines), buf.String())
	}

	actions := []string{"PUT", "PUT", "DELETE"}
	for i, line := range lines {
		var e Entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Errorf("line %d not valid JSON: %v", i, err)
			continue
		}
		if e.Action != actions[i] {
			t.Errorf("line %d action = %q, want %q", i, e.Action, actions[i])
		}
	}
}

func TestLog_ConcurrentWrites(t *testing.T) {
	var buf bytes.Buffer
	l := newWithWriter(&buf)

	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func(n int) {
			l.Log("PUT", "/concurrent", "", "val", "", "", "")
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 20; i++ {
		<-done
	}
	// Each call should produce exactly one newline-terminated JSON line.
	// We can't assert exact order, but all lines should be valid JSON.
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 20 {
		t.Errorf("expected 20 lines from concurrent writes, got %d", len(lines))
	}
}
