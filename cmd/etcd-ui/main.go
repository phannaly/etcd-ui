package main

import (
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/phannaly/etcd-ui/internal/api"
	"github.com/phannaly/etcd-ui/internal/audit"
	etcdc "github.com/phannaly/etcd-ui/internal/etcd"
	"github.com/phannaly/etcd-ui/internal/webfs"
)

func main() {
	endpoints := flag.String("endpoint", getEnv("ETCD_ENDPOINT", "localhost:2379"), "etcd endpoint(s), comma-separated")
	username := flag.String("username", getEnv("ETCD_USERNAME", ""), "etcd username")
	password := flag.String("password", getEnv("ETCD_PASSWORD", ""), "etcd password")
	tlsCert := flag.String("tls-cert", getEnv("ETCD_TLS_CERT", ""), "client TLS cert file")
	tlsKey := flag.String("tls-key", getEnv("ETCD_TLS_KEY", ""), "client TLS key file")
	tlsCA := flag.String("tls-ca", getEnv("ETCD_TLS_CA", ""), "CA cert file")
	tlsSkip := flag.Bool("tls-skip-verify", getEnvBool("ETCD_TLS_SKIP_VERIFY"), "skip TLS verification (insecure)")
	port := flag.String("port", getEnv("PORT", "8888"), "HTTP listen port")
	readOnly := flag.Bool("read-only", getEnvBool("READ_ONLY"), "disable all write operations")
	uiUser := flag.String("ui-username", getEnv("UI_USERNAME", ""), "UI basic-auth username")
	uiPass := flag.String("ui-password", getEnv("UI_PASSWORD", ""), "UI basic-auth password")
	maxKeys := flag.Int("max-keys", getEnvInt("MAX_KEYS", 5000), "max keys to fetch")
	auditFile := flag.String("audit-log", getEnv("AUDIT_LOG", ""), "audit log file (empty = stdout)")
	flag.Parse()

	eps := splitTrim(*endpoints)

	ro := ""
	if *readOnly {
		ro = " [READ-ONLY]"
	}
	log.Printf("╔══════════════════════════════════════╗")
	log.Printf("║      etcd Browser UI Starting        ║")
	log.Printf("╠══════════════════════════════════════╣")
	log.Printf("║  etcd : %s", strings.Join(eps, ", "))
	log.Printf("║  UI   : http://localhost:%s%s", *port, ro)
	log.Printf("╚══════════════════════════════════════╝")

	var tlsCfg *tls.Config
	if *tlsCert != "" || *tlsCA != "" || *tlsSkip {
		var err error
		if tlsCfg, err = buildTLS(*tlsCert, *tlsKey, *tlsCA, *tlsSkip); err != nil {
			log.Fatalf("TLS: %v", err)
		}
		log.Printf("🔒 TLS enabled")
	}

	alog := audit.New(*auditFile)

	client, err := etcdc.New(etcdc.Config{
		Endpoints: eps,
		Username:  *username,
		Password:  *password,
		TLS:       tlsCfg,
		MaxKeys:   *maxKeys,
	})
	if err != nil {
		log.Fatalf("etcd connect: %v", err)
	}
	defer client.Close()
	log.Printf("Connected to etcd %v", eps)

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if client.Ping(r.Context()) {
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "etcd": "connected"})
		} else {
			w.WriteHeader(503)
			json.NewEncoder(w).Encode(map[string]any{"ok": false, "etcd": "unreachable"})
		}
	})

	api.New(client, alog, *readOnly).Register(mux)

	fs := http.FileServer(http.FS(webfs.FS))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		if r.URL.Path != "/" && r.URL.Path != "/index.html" {
			r.URL.Path = "/index.html"
		}
		fs.ServeHTTP(w, r)
	})

	var h http.Handler = mux
	if *uiUser != "" {
		h = basicAuth(*uiUser, *uiPass, h)
		log.Printf("UI basic auth on (user: %s)", *uiUser)
	}

	srv := &http.Server{
		Addr:         ":" + *port,
		Handler:      h,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	log.Printf("http://localhost:%s", *port)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func buildTLS(cert, key, ca string, skip bool) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: skip}
	if cert != "" {
		c, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{c}
	}
	if ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("bad CA cert")
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func basicAuth(user, pass string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		u, p, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(u), []byte(user)) != 1 ||
			subtle.ConstantTimeCompare([]byte(p), []byte(pass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="etcd-ui"`)
			http.Error(w, "Unauthorized", 401)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func splitTrim(s string) []string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func getEnvBool(k string) bool { v := strings.ToLower(os.Getenv(k)); return v == "true" || v == "1" }
func getEnvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		if _, e := fmt.Sscan(v, &n); e == nil {
			return n
		}
	}
	return def
}
