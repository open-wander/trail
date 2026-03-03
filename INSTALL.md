# Manual Installation Guide

This guide covers standalone installation of Trail on Linux, macOS, or other Unix-like systems.

## Prerequisites

- Go 1.19 or later (for building from source)
- SQLite 3 (usually pre-installed on Linux/macOS)
- Write access to a log file directory (Traefik, Apache, or Nginx logs)
- Port for the web interface (default: 8080)

## Option 1: Build from Source

### 1. Clone and Build

```bash
git clone https://github.com/open-wander/trail.git
cd trail
go build -o trail ./cmd/trail
```

### 2. Verify the Build

```bash
./trail --help  # May not have help yet, but binary should exist
./trail         # Will show usage via error if env vars missing
```

## Option 2: Download Pre-Built Release

```bash
# Linux amd64
curl -LO https://github.com/open-wander/trail/releases/latest/download/trail-linux-amd64.tar.gz
tar xzf trail-linux-amd64.tar.gz

# Linux arm64
curl -LO https://github.com/open-wander/trail/releases/latest/download/trail-linux-arm64.tar.gz
tar xzf trail-linux-arm64.tar.gz

# macOS
curl -LO https://github.com/open-wander/trail/releases/latest/download/trail-macos-amd64.tar.gz
tar xzf trail-macos-amd64.tar.gz
```

## Configuration

Trail is configured entirely via environment variables. Create a `.env` file or export them in your shell:

```bash
# Required
export TRAIL_LOG_FILE=/var/log/traefik/access.log    # Path to access log
export TRAIL_DB_PATH=/var/lib/trail/trail.db         # Database location
export TRAIL_LISTEN=:8080                             # Port to listen on

# Optional - Retention and format
export TRAIL_RETENTION_DAYS=90                        # Auto-delete older data
export TRAIL_LOG_FORMAT=auto                          # auto, traefik, or combined

# Optional - Authentication (choose one)
# Option A: htpasswd file (recommended)
export TRAIL_HTPASSWD_FILE=/etc/trail/htpasswd

# Option B: Environment variables
export TRAIL_AUTH_USER=admin
export TRAIL_AUTH_PASS=yourpassword

# Optional - GeoIP for country reports
export TRAIL_GEOIP_PATH=/usr/share/geoip/dbip-country-lite.mmdb
```

### Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `TRAIL_LOG_FILE` | `/logs/access.log` | Path to access log file |
| `TRAIL_DB_PATH` | `/data/trail.db` | SQLite database location |
| `TRAIL_LISTEN` | `:8080` | HTTP listen address (e.g., `:8080` or `127.0.0.1:8080`) |
| `TRAIL_RETENTION_DAYS` | `90` | Delete analytics data older than N days |
| `TRAIL_LOG_FORMAT` | `auto` | Log format: `auto`, `traefik`, or `combined` |
| `TRAIL_HTPASSWD_FILE` | | Path to htpasswd file (bcrypt hashes only) |
| `TRAIL_AUTH_USER` | | Basic auth username (plaintext) |
| `TRAIL_AUTH_PASS` | | Basic auth password (plaintext) |
| `TRAIL_GEOIP_PATH` | | Path to MaxMind/DB-IP mmdb file |

**Authentication Priority:**
1. htpasswd file (if set)
2. Environment variables (if set)
3. No authentication (if neither is set)

## Running Trail

### Quick Start (No Auth)

```bash
TRAIL_LOG_FILE=/var/log/traefik/access.log \
TRAIL_DB_PATH=./trail.db \
TRAIL_LISTEN=:8080 \
./trail
```

Then visit http://localhost:8080

### With htpasswd Authentication

#### Generate htpasswd file
```bash
# Create new htpasswd file with bcrypt
htpasswd -nbB -C 10 admin yourpassword > /etc/trail/htpasswd

# Add another user
htpasswd -nbB -C 10 -i name2 << EOF
password2
EOF >> /etc/trail/htpasswd

# Set appropriate permissions
chmod 600 /etc/trail/htpasswd
```

#### Run with htpasswd
```bash
TRAIL_LOG_FILE=/var/log/traefik/access.log \
TRAIL_DB_PATH=/var/lib/trail/trail.db \
TRAIL_LISTEN=:8080 \
TRAIL_HTPASSWD_FILE=/etc/trail/htpasswd \
./trail
```

### With Environment Variable Authentication

```bash
TRAIL_LOG_FILE=/var/log/traefik/access.log \
TRAIL_DB_PATH=/var/lib/trail/trail.db \
TRAIL_LISTEN=:8080 \
TRAIL_AUTH_USER=admin \
TRAIL_AUTH_PASS=yourpassword \
./trail
```

## Running as a Service

### systemd Service File

Create `/etc/systemd/system/trail.service`:

```ini
[Unit]
Description=Trail Web Analytics
After=network.target

[Service]
Type=simple
User=trail
Group=trail
WorkingDirectory=/var/lib/trail

# Configuration
Environment="TRAIL_LOG_FILE=/var/log/traefik/access.log"
Environment="TRAIL_DB_PATH=/var/lib/trail/trail.db"
Environment="TRAIL_LISTEN=:8080"
Environment="TRAIL_RETENTION_DAYS=90"
Environment="TRAIL_LOG_FORMAT=auto"
Environment="TRAIL_HTPASSWD_FILE=/etc/trail/htpasswd"

ExecStart=/usr/local/bin/trail
Restart=on-failure
RestartSec=10

# Security
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/trail

[Install]
WantedBy=multi-user.target
```

### Setup and Start Service

```bash
# Create trail user and directories
sudo useradd -r -s /bin/false trail
sudo mkdir -p /var/lib/trail /etc/trail
sudo chown trail:trail /var/lib/trail /etc/trail

# Copy binary
sudo cp trail /usr/local/bin/trail
sudo chmod +x /usr/local/bin/trail

# Generate htpasswd (if using auth)
sudo htpasswd -nbB -C 10 admin yourpassword | sudo tee /etc/trail/htpasswd
sudo chmod 600 /etc/trail/htpasswd
sudo chown trail:trail /etc/trail/htpasswd

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable trail
sudo systemctl start trail

# Check status
sudo systemctl status trail
sudo journalctl -u trail -f
```

### Stop and Restart

```bash
# View logs
sudo journalctl -u trail -f

# Restart service
sudo systemctl restart trail

# Stop service
sudo systemctl stop trail
```

## Setting Up Log File Access

Trail reads from your access log file. Ensure the user running Trail can read it:

### Traefik

Add to `docker-compose.yml` or Traefik config:
```yaml
accessLog:
  filePath: /var/log/traefik/access.log
```

Then:
```bash
sudo chown -R trail:trail /var/log/traefik/
sudo chmod 755 /var/log/traefik/
sudo chmod 644 /var/log/traefik/access.log*
```

### Nginx

Ensure log file is readable:
```bash
sudo touch /var/log/nginx/access.log
sudo chown root:root /var/log/nginx/access.log
sudo chmod 644 /var/log/nginx/access.log
sudo usermod -a -G adm trail  # Add trail to adm group for log access
```

### Apache

```bash
sudo usermod -a -G adm trail  # Add trail to adm group for log access
```

## Optional: GeoIP Country Reports

To enable country reports, download a free GeoIP database:

### Option 1: DB-IP Free Lite (Recommended)

```bash
curl -L https://download.db-ip.com/free/dbip-country-lite-2024-03.mmdb.gz | \
  gunzip > /usr/share/geoip/dbip-country-lite.mmdb

chmod 644 /usr/share/geoip/dbip-country-lite.mmdb
```

Then set in service file or environment:
```bash
TRAIL_GEOIP_PATH=/usr/share/geoip/dbip-country-lite.mmdb
```

### Option 2: MaxMind GeoLite2

Download from [maxmind.com](https://www.maxmind.com/en/geolite2/signup) (requires free account), then:

```bash
sudo cp GeoLite2-Country.mmdb /usr/share/geoip/
sudo chmod 644 /usr/share/geoip/GeoLite2-Country.mmdb
```

Set in configuration:
```bash
TRAIL_GEOIP_PATH=/usr/share/geoip/GeoLite2-Country.mmdb
```

## Troubleshooting

### Trail won't start - "Failed to open database"

Check that the directory exists and is writable:
```bash
sudo mkdir -p /var/lib/trail
sudo chown trail:trail /var/lib/trail
sudo chmod 755 /var/lib/trail
```

### Trail won't access log file - "Permission denied"

Check file permissions:
```bash
# Make trail user a member of adm group (for system logs)
sudo usermod -a -G adm trail

# Or set specific ownership
sudo chown $USER:$USER /var/log/traefik/access.log
```

### Dashboard is slow or not showing data

- Check log file path is correct: `TRAIL_LOG_FILE=path/to/access.log ./trail`
- Verify log format with `head -5 /var/log/traefik/access.log`
- View logs: `journalctl -u trail -f`
- Data accumulates as logs are read; initial startup backfills existing data

### Port already in use

Change `TRAIL_LISTEN`:
```bash
TRAIL_LISTEN=:8081 ./trail
```

## Updating Trail

### From Source

```bash
cd trail
git pull origin main
go build -o trail ./cmd/trail
sudo systemctl restart trail
```

### From Release

```bash
# Download new version
curl -LO https://github.com/open-wander/trail/releases/latest/download/trail-linux-amd64.tar.gz
tar xzf trail-linux-amd64.tar.gz

# Replace binary
sudo cp trail /usr/local/bin/trail
sudo systemctl restart trail
```

## Docker (Alternative)

If you prefer Docker, see the [main README](./README.md#docker) or `docker-compose.yml` in the repository.

## Next Steps

- Open http://localhost:8080 (or your configured port)
- Log in with your credentials (if configured)
- Trail will backfill historical data as it processes the log file
- Visit the Overview and Security tabs to explore analytics
