package audit

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

type Entry struct {
	Time     string `json:"time"`
	Action   string `json:"action"`
	Key      string `json:"key,omitempty"`
	OldValue string `json:"old_value,omitempty"`
	NewValue string `json:"new_value,omitempty"`
	User     string `json:"user,omitempty"`
	RemoteIP string `json:"remote_ip,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

type Logger struct {
	logger *log.Logger
}

func New(filePath string) *Logger {
	var out *os.File
	if filePath == "" {
		out = os.Stdout
	} else {
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
		if err != nil {
			log.Printf("audit log open %q: %v — using stdout", filePath, err)
			out = os.Stdout
		} else {
			out = f
		}
	}
	return &Logger{logger: log.New(out, "", 0)}
}

func (l *Logger) Log(action, key, oldVal, newVal, user, remoteIP, detail string) {
	e := Entry{
		Time:     time.Now().UTC().Format(time.RFC3339),
		Action:   action,
		Key:      key,
		OldValue: oldVal,
		NewValue: newVal,
		User:     user,
		RemoteIP: remoteIP,
		Detail:   detail,
	}
	b, _ := json.Marshal(e)
	l.logger.Println(string(b))
}
