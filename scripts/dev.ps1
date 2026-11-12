# Developer commands for Windows PowerShell. Same subcommands as scripts/dev.sh.
#   .\scripts\dev.ps1 up | down | test | seed | loadtest | reset
param([Parameter(Position = 0)][string]$Command = "help")

Set-Location (Split-Path -Parent $PSScriptRoot)

function Assert-EnvFile {
    if (-not (Test-Path .env)) {
        Write-Host "No .env file found. Create one first:  Copy-Item .env.example .env"
        exit 1
    }
}

switch ($Command) {
    "up" {
        Assert-EnvFile
        docker compose up -d --wait --build
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        docker compose ps
    }
    "down" {
        docker compose down
        exit $LASTEXITCODE
    }
    "test" {
        # -p 1: packages run one at a time so only one set of test containers is alive.
        go test -p 1 ./...
        exit $LASTEXITCODE
    }
    "reset" {
        Assert-EnvFile
        docker compose down -v
        docker compose up -d --wait --build
        exit $LASTEXITCODE
    }
    "seed" {
        Write-Host "seed is not built yet (Phase 5). See 'Project status' in the README."
        exit 1
    }
    "loadtest" {
        Write-Host "loadtest is not built yet (Phase 5). See 'Project status' in the README."
        exit 1
    }
    default {
        Write-Host "usage: .\scripts\dev.ps1 <up|down|test|seed|loadtest|reset>"
        Write-Host "  up        start Postgres and Redpanda and wait until healthy"
        Write-Host "  down      stop the stack (data is kept)"
        Write-Host "  test      run the Go tests"
        Write-Host "  seed      create demo accounts (Phase 5)"
        Write-Host "  loadtest  run the k6 scenarios (Phase 5)"
        Write-Host "  reset     stop the stack, delete its data, start again"
        if ($Command -ne "help") { exit 1 }
    }
}
