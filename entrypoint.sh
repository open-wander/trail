#!/bin/sh
set -e

# Ensure data directory exists and is writable
# This handles bind-mounted volumes where host permissions override Dockerfile chown
if [ ! -w "/data" ]; then
    echo "Fixing /data directory permissions..."
    chown trail:trail /data
fi

exec su-exec trail "$@"
