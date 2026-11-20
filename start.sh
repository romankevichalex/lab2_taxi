#!/usr/bin/env bash
set -e

cd -- "$(dirname -- "$0")"

docker compose up -d
uv sync

uv run streamlit run app/dashboard.py &
DASHBOARD_PID=$!

uv run python -u -m app.processor &
PROCESSOR_PID=$!

uv run python -u -m app.generator &
GENERATOR_PID=$!

echo "$PROCESSOR_PID $GENERATOR_PID $DASHBOARD_PID" > .app-pids

cleanup() {
    kill "$PROCESSOR_PID" "$GENERATOR_PID" "$DASHBOARD_PID" \
        2>/dev/null || true
    rm -f .app-pids
    docker compose down
}

trap cleanup EXIT
trap 'exit' INT TERM

wait