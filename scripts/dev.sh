#!/usr/bin/env bash
# Developer commands for macOS / Linux / Git Bash. Same subcommands as scripts/dev.ps1.
#   ./scripts/dev.sh up | down | test | seed | loadtest | reset
set -euo pipefail
cd "$(dirname "$0")/.."

require_env() {
  if [ ! -f .env ]; then
    echo "No .env file found. Create one first:  cp .env.example .env"
    exit 1
  fi
}

case "${1:-help}" in
  up)
    require_env
    docker compose up -d --wait --build
    docker compose ps
    ;;
  down)
    docker compose down
    ;;
  test)
    # -p 1: packages run one at a time so only one set of test containers is alive.
    go test -p 1 ./...
    ;;
  reset)
    require_env
    docker compose down -v
    docker compose up -d --wait --build
    ;;
  seed)
    # Creates 1,000 funded accounts through the API and writes loadtest/accounts.json.
    shift
    go run ./cmd/seed "$@"
    ;;
  loadtest)
    # Runs the k6 scenarios and writes results/loadtest-<scenario>.json. Needs k6 and a seeded stack.
    shift
    go run ./cmd/loadtest "$@"
    ;;
  *)
    echo "usage: ./scripts/dev.sh <up|down|test|seed|loadtest|reset>"
    echo "  up        start Postgres and Redpanda and wait until healthy"
    echo "  down      stop the stack (data is kept)"
    echo "  test      run the Go tests"
    echo "  seed      create 1,000 funded demo accounts (the stack must be up)"
    echo "  loadtest  run the k6 load test and write results/*.json (after seed)"
    echo "  reset     stop the stack, delete its data, start again"
    [ "${1:-help}" = "help" ] || exit 1
    ;;
esac
