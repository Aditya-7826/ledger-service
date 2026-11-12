// Package config reads service settings from environment variables.
package config

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the settings shared by the services.
type Config struct {
	DatabaseURL      string
	APIPort          int
	MaxTransferMinor int64
	IdempotencyTTL   time.Duration
	DBMaxConns       int32
	LogLevel         slog.Level
}

// LoadDotEnv sets variables from a KEY=VALUE file, without overriding ones
// that are already set. A missing file is not an error.
func LoadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if _, set := os.LookupEnv(key); !set {
			_ = os.Setenv(key, strings.TrimSpace(value))
		}
	}
}

// Load reads and validates the configuration.
func Load() (Config, error) {
	cfg := Config{DatabaseURL: os.Getenv("DATABASE_URL")}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("DATABASE_URL is required")
	}
	port, err := intEnv("API_PORT", 8080, 1, 65535)
	if err != nil {
		return cfg, err
	}
	cfg.APIPort = int(port)
	if cfg.MaxTransferMinor, err = intEnv("MAX_TRANSFER_MINOR", 100_000_000_000, 1, 1<<62); err != nil {
		return cfg, err
	}
	ttl, err := intEnv("IDEMPOTENCY_TTL_HOURS", 24, 1, 24*365)
	if err != nil {
		return cfg, err
	}
	cfg.IdempotencyTTL = time.Duration(ttl) * time.Hour
	conns, err := intEnv("DB_MAX_CONNS", 20, 1, 90)
	if err != nil {
		return cfg, err
	}
	cfg.DBMaxConns = int32(conns)
	if err := cfg.LogLevel.UnmarshalText([]byte(envOr("LOG_LEVEL", "info"))); err != nil {
		return cfg, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func intEnv(key string, fallback, min, max int64) (int64, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < min || v > max {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", key, min, max)
	}
	return v, nil
}
