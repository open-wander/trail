#!/bin/sh
set -e

# Ensure data directory exists and is writable by trail user
# Bind-mounted volumes override Dockerfile chown, so always fix ownership
mkdir -p /data
chown trail:trail /data

exec su-exec trail "$@"
