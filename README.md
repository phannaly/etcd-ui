# etcd Browser UI

A lightweight, single-binary web UI for browsing, searching, watching, and editing keys in an [etcd](https://etcd.io) cluster. The entire frontend is embedded into the binary.


![etcd ui screenshot](/etcd-ui-browser.png)
---

## Features

| Tab | What it does |
|---|---|
| **Explorer** | Browse the key tree, view values with JSON syntax highlighting, copy keys/values |
| **Search** | Full-text search across keys and values with match highlighting |
| **Watch** | Live SSE stream of PUT/DELETE events with type filtering and counters |
| **Cluster** | Cluster health, DB size, member list, version info |

- **Edit keys** — click the Edit button on any key to modify its value or delete it
- **Auth support** — connect with username/password via flags or env vars
- **Pretty/Raw toggle** — view JSON formatted or as raw text
- **Word wrap toggle** — for long values
- **Keyboard shortcuts** — press `?` to see them

---

## Requirements

- Go 1.21+
- An accessible etcd v3 endpoint

---

## Installation

```bash
curl -sL https://raw.githubusercontent.com/phannaly/etcd-ui/main/install.sh | bash
```

```
etcd-ui --help
```

## Run with Go

### Basic (no auth)

```bash
go run ./cmd/etcd-ui/
```

Defaults to `localhost:2379` on port `8888`. Open http://localhost:8888.

### Custom endpoint and port

```bash
go run ./cmd/etcd-ui/ --endpoint=192.168.1.10:2379 --port=9000
```

### With authentication

```bash
go run ./cmd/etcd-ui/ --endpoint=localhost:2379 --username=root --password=secret
```

### Multiple endpoints (cluster)

```bash
go run ./cmd/etcd-ui/ --endpoint=10.0.0.1:2379,10.0.0.2:2379,10.0.0.3:2379
```

### Using environment variables

All flags have environment variable equivalents:

| Flag | Env var | Default |
|---|---|---|
| `--endpoint` | `ETCD_ENDPOINT` | `localhost:2379` |
| `--port` | `PORT` | `8888` |
| `--username` | `ETCD_USERNAME` | *(empty)* |
| `--password` | `ETCD_PASSWORD` | *(empty)* |
| `--tls-cert` | `ETCD_TLS_CERT` | *(optional)* |
| `--tls-key`  | `ETCD_TLS_KEY`  | *(optional)* |
| `--tls-ca`   | `ETCD_TLS_CA`   | *(optional)* |

```bash
ETCD_ENDPOINT=localhost:2379 \
ETCD_USERNAME=root \
ETCD_PASSWORD=secret \
PORT=9000 \
go run ./cmd/etcd-ui/
```

---

## Run with Docker

### Run against an existing etcd

```bash
docker run -p 8888:8888 \
  -e ETCD_ENDPOINT=etcd:2379 \
  phanna/etcd-ui:latest
```

> **Note:** Use `host.docker.internal` on macOS/Windows, or `--network=host` on Linux, to reach etcd running on your host machine.

### Run on Linux host network

```bash
docker run --network=host \
  -e ETCD_ENDPOINT=localhost:2379 \
  phanna/etcd-ui:latest
```

### With authentication

```bash
docker run -p 8888:8888 \
  -e ETCD_ENDPOINT=my-etcd:2379 \
  -e ETCD_USERNAME=root \
  -e ETCD_PASSWORD=secret \
  phanna/etcd-ui:latest
```

---

## Run with Docker Compose

### Connect to your own etcd instead

Edit the `etcd-ui` service in `docker-compose.yml`:

```yaml
etcd-ui:
  image: phanna/etcd-ui:latest
  environment:
    - ETCD_ENDPOINT=your-etcd-host:2379
    - ETCD_USERNAME=root        # optional
    - ETCD_PASSWORD=secret      # optional
  ports:
    - "8888:8888"
```

Or pass flags via `command`:

```yaml
etcd-ui:
  image: phanna/etcd-ui:latest
  command: ["./etcd-ui", "--endpoint=your-etcd-host:2379", "--username=root", "--password=secret"]
  ports:
    - "8888:8888"
```

---

## Keyboard shortcuts

| Key | Action |
|---|---|
| `/` | Focus search bar |
| `R` | Refresh key tree |
| `E` | Switch to Explorer |
| `W` | Switch to Watch |
| `C` | Switch to Cluster |
| `?` | Show shortcuts |
| `Esc` | Close modal / blur input |
