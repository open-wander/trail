# Trail

Self-hosted web analytics for Traefik and Apache/Nginx. No JavaScript tracking. Reads access logs directly and serves a real-time htmx dashboard.

## Features

- Real-time log tailing with automatic backfill on startup
- Hourly aggregation into SQLite (no external database needed)
- Dark theme dashboard with interactive charts
- Status code drilldowns, path trends, visitor overlays
- Bot detection and security threat analysis
- Supports Traefik, Apache, and Nginx log formats with auto-detection
- Basic auth via htpasswd or environment variables
- Single binary, zero runtime dependencies

## Quick Start

### Download a release

```bash
# Linux amd64
curl -LO https://github.com/open-wander/trail/releases/latest/download/trail-linux-amd64.tar.gz
tar xzf trail-linux-amd64.tar.gz

# Linux arm64
curl -LO https://github.com/open-wander/trail/releases/latest/download/trail-linux-arm64.tar.gz
tar xzf trail-linux-arm64.tar.gz
```

### Build from source

```bash
go build -o trail ./cmd/trail
```

### Run

```bash
TRAIL_LOG_FILE=/var/log/traefik/access.log \
TRAIL_DB_PATH=./trail.db \
TRAIL_LISTEN=:8080 \
./trail
```

Open http://localhost:8080. No auth is required when `TRAIL_AUTH_USER` and `TRAIL_HTPASSWD_FILE` are both unset.

Trail will tail the log file, backfill existing data, then stream new entries. The dashboard populates as data is ingested.

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `TRAIL_LOG_FILE` | `/logs/access.log` | Path to access log file |
| `TRAIL_DB_PATH` | `/data/trail.db` | Path to SQLite database |
| `TRAIL_LISTEN` | `:8080` | HTTP listen address |
| `TRAIL_RETENTION_DAYS` | `90` | Auto-delete data older than N days |
| `TRAIL_LOG_FORMAT` | `auto` | Log format: `auto`, `traefik`, or `combined` |
| `TRAIL_HTPASSWD_FILE` | | Path to htpasswd file (bcrypt only) |
| `TRAIL_AUTH_USER` | | Basic auth username |
| `TRAIL_AUTH_PASS` | | Basic auth password |

Authentication priority: htpasswd file > env var credentials > no auth.

### Log format

- **`auto`** (default): Reads the first 10 lines and auto-detects the format
- **`traefik`**: Traefik extended Common Log Format
- **`combined`**: Apache/Nginx Combined Log Format (with optional trailing response time)

## Deployment

### Binary on a Linux server

```bash
# Download and extract
curl -LO https://github.com/open-wander/trail/releases/latest/download/trail-linux-amd64.tar.gz
tar xzf trail-linux-amd64.tar.gz

# Move files into place
sudo mkdir -p /opt/trail
sudo mv trail-linux-amd64 /opt/trail/trail
sudo mv static templates /opt/trail/
sudo mkdir -p /var/lib/trail
```

Create a systemd service at `/etc/systemd/system/trail.service`:

```ini
[Unit]
Description=Trail Analytics
After=network.target

[Service]
Type=simple
User=trail
ExecStart=/opt/trail/trail
Environment=TRAIL_LOG_FILE=/var/log/traefik/access.log
Environment=TRAIL_DB_PATH=/var/lib/trail/trail.db
Environment=TRAIL_LISTEN=:8080
Environment=TRAIL_HTPASSWD_FILE=/etc/trail/.htpasswd
WorkingDirectory=/opt/trail
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo useradd -r -s /usr/sbin/nologin trail
sudo chown -R trail:trail /var/lib/trail
sudo systemctl daemon-reload
sudo systemctl enable --now trail
```

### Docker

```bash
docker build -t trail .

docker run -d --name trail \
  -v /var/log/traefik:/logs:ro \
  -v /var/lib/trail:/data \
  -p 8080:8080 \
  trail
```

### Docker Compose with Traefik

The included `docker-compose.yml` runs Trail behind Traefik with automatic TLS:

```bash
docker compose up -d
```

It expects:
- Traefik access logs mounted at `/mnt/data/traefik/access.log`
- Persistent SQLite storage at `/mnt/data/trail/`
- An htpasswd file at `/mnt/data/htpasswd/htpasswd`
- An external `wander_web` Docker network

To generate a bcrypt htpasswd file:

```bash
htpasswd -cB /mnt/data/htpasswd/htpasswd admin
```

### Docker image from GitHub Container Registry

```bash
docker pull ghcr.io/open-wander/trail:latest

docker run -d --name trail \
  -v /var/log/traefik:/logs:ro \
  -v /var/lib/trail:/data \
  -e TRAIL_AUTH_USER=admin \
  -e TRAIL_AUTH_PASS=changeme \
  -p 8080:8080 \
  ghcr.io/open-wander/trail:latest
```

## Dashboard

### Overview (/)

- Summary stats: requests, visitors, bandwidth, avg response time
- Requests/visitors over time (vertical bar chart with overlay)
- Top paths with sparkline trends
- Top referrers with percentage bars
- Status code breakdown (donut + horizontal bars with drilldown)
- HTTP methods and user agents (donut + bars)
- 404 paths (clickable drilldown)
- Hour-of-day distribution (requests + visitors overlay)

### Security (/security)

- Threat pattern categories (WordPress probes, env file scans, admin panels, scripts)
- Bot vs human traffic breakdown
- 5xx error trends over time
- Error paths and slowest paths

### Filters

- Date range: today, 7 days, 30 days, custom range
- Router/service selector (Traefik service names)
- Include/exclude bot traffic

## Development

```bash
# Run tests
go test ./...

# Build
go build -o trail ./cmd/trail

# Dev mode with hot reload (requires air)
air
```

## Architecture

```
Access log --> Tailer (1s poll) --> Parser --> Aggregator --> SQLite
                                                               |
                                                        Fiber HTTP server
                                                               |
                                                        htmx dashboard
```

- **Tailer**: Poll-based file watcher, handles copytruncate and log rotation
- **Parser**: Regex-based, supports Traefik and Apache/Nginx Combined formats
- **Aggregator**: Batches entries, flushes hourly aggregates every 10s or 1000 lines
- **Bot detector**: Classifies traffic by User-Agent patterns and router field
- **Retention**: Periodic cleanup of data older than configured retention period

## Tech Stack

- Go + Fiber (HTTP framework)
- SQLite via modernc.org/sqlite (pure Go, no CGO)
- htmx for interactive dashboard
- CSS-only charts (donut, bar, sparkline)
- Dark theme, responsive layout
