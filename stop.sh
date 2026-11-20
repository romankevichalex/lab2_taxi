#!/usr/bin/env bash
set -e

cd -- "$(dirname -- "$0")"

if [[ -f .app-pids ]]; then
    read -r PROCESSOR_PID GENERATOR_PID DASHBOARD_PID < .app-pids

    kill "$PROCESSOR_PID" "$GENERATOR_PID" "$DASHBOARD_PID" \
        2>/dev/null || true

    rm -f .app-pids
fi

docker compose down