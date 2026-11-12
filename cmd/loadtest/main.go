// Command loadtest runs the k6 scenarios against the running stack and
// writes results/loadtest-<scenario>.json.
//
// After each k6 run it waits for the event pipeline to drain, runs the
// ledger's reconciliation, compares the balance read model with the ledger,
// and (for retry_storm) counts transfers and duplicates in SQL. Every number
// in the result file is measured by this program or by k6.
//
//	go run ./cmd/loadtest                       # all three scenarios
//	go run ./cmd/loadtest -scenario uniform -duration 30s
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Aditya-7826/ledger-service/internal/config"
)

var scenarios = []string{"uniform", "hot_account", "retry_storm"}

// Result is the content of results/loadtest-<scenario>.json.
type Result struct {
	Scenario            string         `json:"scenario"`
	Description         string         `json:"description"`
	RanAt               time.Time      `json:"ran_at"`
	VirtualUsers        int            `json:"virtual_users"`
	DurationSeconds     float64        `json:"duration_seconds"`
	Requests            int64          `json:"requests"`
	RequestsPerSecond   float64        `json:"requests_per_second"`
	TransfersCreated    int64          `json:"transfers_created"`
	ThroughputPerSecond float64        `json:"throughput_per_second"` // committed transfers per second
	LatencyMS           Latency        `json:"latency_ms"`
	ErrorRate           float64        `json:"error_rate"`
	Replays             int64          `json:"replays"`
	Mismatches          int64          `json:"mismatches"` // balance-view rows that differ from the ledger after draining
	ReconcileOK         bool           `json:"reconcile_ok"`
	DrainSeconds        float64        `json:"pipeline_drain_seconds"` // time after k6 finished until the read model equalled the ledger
	Drained             bool           `json:"pipeline_drained"`
	Storm               *StormCheck    `json:"retry_storm,omitempty"`
	Duplicates          int64          `json:"duplicates"`
	Hardware            map[string]any `json:"hardware"`
}

// Latency holds request latency percentiles in milliseconds.
type Latency struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// StormCheck is the SQL verification of the retry_storm scenario.
type StormCheck struct {
	LogicalRequests     int64 `json:"logical_requests"`
	HTTPRequestsSent    int64 `json:"http_requests_sent"`
	TransfersInDatabase int64 `json:"transfers_in_database"`
	DuplicateTransfers  int64 `json:"duplicate_transfers"`
	InconsistentRetries int64 `json:"inconsistent_retries"`
	ExactlyOncePerKey   bool  `json:"exactly_once_per_key"`
}

func main() {
	scenario := flag.String("scenario", "all", "uniform, hot_account, retry_storm or all")
	api := flag.String("api", "http://localhost:8080", "ledger API base URL")
	duration := flag.String("duration", "2m", "duration of the uniform and hot_account scenarios")
	vus := flag.Int("vus", 50, "virtual users")
	logical := flag.Int("logical", 10000, "logical requests in retry_storm")
	accounts := flag.String("accounts", filepath.Join("loadtest", "accounts.json"), "account ID file written by the seed command")
	outDir := flag.String("out", "results", "folder for the result files")
	flag.Parse()

	if err := run(*scenario, *api, *duration, *vus, *logical, *accounts, *outDir); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}

func run(scenario, api, duration string, vus, logical int, accounts, outDir string) error {
	todo := scenarios
	if scenario != "all" {
		todo = []string{scenario}
	}
	if _, err := exec.LookPath("k6"); err != nil {
		return errors.New("k6 is not installed or not on PATH (Windows: winget install k6 --source winget)")
	}
	if _, err := os.Stat(accounts); err != nil {
		return fmt.Errorf("%s not found: run the seed command first (scripts/dev seed)", accounts)
	}
	ready, err := http.Get(api + "/readyz")
	if err != nil {
		return errors.New("the API is not ready: start the stack with scripts/dev up")
	}
	_ = ready.Body.Close()
	if ready.StatusCode != http.StatusOK {
		return errors.New("the API is not ready: start the stack with scripts/dev up")
	}
	config.LoadDotEnv(".env")
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return errors.New("DATABASE_URL is not set (copy .env.example to .env)")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("cannot reach PostgreSQL at DATABASE_URL: %w", err)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	hardware := hardwareInfo()

	for _, name := range todo {
		res, err := runScenario(ctx, pool, name, api, duration, vus, logical, accounts)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		res.Hardware = hardware
		raw, _ := json.MarshalIndent(res, "", "  ")
		file := filepath.Join(outDir, "loadtest-"+name+".json")
		if err := os.WriteFile(file, append(raw, '\n'), 0o644); err != nil {
			return err
		}
		fmt.Printf("\n%s: %.1f transfers/s, p50 %.1f ms, p95 %.1f ms, p99 %.1f ms, errors %.2f%%, mismatches %d, duplicates %d -> %s\n\n",
			name, res.ThroughputPerSecond, res.LatencyMS.P50, res.LatencyMS.P95, res.LatencyMS.P99,
			res.ErrorRate*100, res.Mismatches, res.Duplicates, file)
	}
	return nil
}

var descriptions = map[string]string{
	"uniform":     "random transfers between random pairs of 1,000 accounts",
	"hot_account": "80% of transfers credit one merchant account (row-lock contention)",
	"retry_storm": "each logical request is sent 2-3 times with the same idempotency key, half of them concurrently",
}

func runScenario(ctx context.Context, pool *pgxpool.Pool, name, api, duration string, vus, logical int, accounts string) (*Result, error) {
	if _, ok := descriptions[name]; !ok {
		return nil, fmt.Errorf("unknown scenario (use %s)", strings.Join(scenarios, ", "))
	}
	runID := strconv.FormatInt(time.Now().Unix(), 36)
	clientID := "k6-" + name + "-" + runID
	tmp, err := os.MkdirTemp("", "ledger-loadtest-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	summaryFile := filepath.Join(tmp, "summary.json")
	accountsAbs, _ := filepath.Abs(accounts)

	fmt.Printf("=== %s: %s ===\n", name, descriptions[name])
	started := time.Now().UTC()
	cmd := exec.Command("k6", "run", "--quiet", filepath.Join("loadtest", "transfers.js"))
	cmd.Env = append(os.Environ(),
		"SCENARIO="+name, "BASE_URL="+api, "RUN_ID="+runID, "CLIENT_ID="+clientID,
		"VUS="+strconv.Itoa(vus), "DURATION="+duration, "LOGICAL="+strconv.Itoa(logical),
		"ACCOUNTS_FILE="+accountsAbs, "SUMMARY_FILE="+summaryFile)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("k6 failed: %w", err)
	}

	raw, err := os.ReadFile(summaryFile)
	if err != nil {
		return nil, fmt.Errorf("k6 wrote no summary: %w", err)
	}
	var summary struct {
		State struct {
			TestRunDurationMs float64 `json:"testRunDurationMs"`
		} `json:"state"`
		Metrics map[string]struct {
			Values map[string]float64 `json:"values"`
		} `json:"metrics"`
	}
	if err := json.Unmarshal(raw, &summary); err != nil {
		return nil, err
	}
	value := func(metric, key string) float64 { return summary.Metrics[metric].Values[key] }
	seconds := summary.State.TestRunDurationMs / 1000

	res := &Result{
		Scenario: name, Description: descriptions[name], RanAt: started, VirtualUsers: vus,
		DurationSeconds:   round(seconds, 1),
		Requests:          int64(value("http_reqs", "count")),
		RequestsPerSecond: round(value("http_reqs", "count")/seconds, 1),
		LatencyMS: Latency{
			P50: round(value("http_req_duration", "p(50)"), 2),
			P95: round(value("http_req_duration", "p(95)"), 2),
			P99: round(value("http_req_duration", "p(99)"), 2),
		},
		ErrorRate: round(value("http_req_failed", "rate"), 6),
		Replays:   int64(value("transfers_replayed", "count")),
	}

	// Transfers actually committed by this run, counted in the database.
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM transfers WHERE client_id = $1`, clientID).Scan(&res.TransfersCreated); err != nil {
		return nil, err
	}
	res.ThroughputPerSecond = round(float64(res.TransfersCreated)/seconds, 1)

	// Wait until the relay has published everything and the consumer has applied it.
	fmt.Println("waiting for the event pipeline to drain...")
	drainStart := time.Now()
	for {
		var backlog int64
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE published_at IS NULL`).Scan(&backlog); err != nil {
			return nil, err
		}
		if res.Mismatches, err = viewMismatches(ctx, pool); err != nil {
			return nil, err
		}
		if backlog == 0 && res.Mismatches == 0 {
			res.Drained = true
			break
		}
		// Mismatches that remain after this long are reported as they are, with drained=false.
		if time.Since(drainStart) > 15*time.Minute {
			break
		}
		time.Sleep(time.Second)
	}
	res.DrainSeconds = round(time.Since(drainStart).Seconds(), 1)
	if !res.Drained {
		fmt.Printf("the pipeline did not drain within 15 minutes: %d account(s) still differ\n", res.Mismatches)
	}

	var rec struct {
		OK bool `json:"ok"`
	}
	if httpRes, err := http.Get(api + "/v1/reconcile"); err == nil {
		_ = json.NewDecoder(httpRes.Body).Decode(&rec)
		httpRes.Body.Close()
	}
	res.ReconcileOK = rec.OK

	// Duplicates: any (client, key) of this run with more than one transfer.
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM (
			SELECT idempotency_key FROM transfers WHERE client_id = $1
			GROUP BY idempotency_key HAVING COUNT(*) > 1) d`, clientID).Scan(&res.Duplicates); err != nil {
		return nil, err
	}
	if name == "retry_storm" {
		res.Storm = &StormCheck{
			LogicalRequests:     int64(logical),
			HTTPRequestsSent:    res.Requests,
			TransfersInDatabase: res.TransfersCreated,
			DuplicateTransfers:  res.Duplicates,
			InconsistentRetries: int64(value("storm_inconsistent", "count")),
		}
		res.Storm.ExactlyOncePerKey = res.TransfersCreated == int64(logical) && res.Duplicates == 0 && res.Storm.InconsistentRetries == 0
	}
	return res, nil
}

func viewMismatches(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var n int64
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM account_balances b
		LEFT JOIN views.balance_view v ON v.account_id = b.account_id
		WHERE b.last_seq > 0
		  AND (v.account_id IS NULL OR v.balance_minor <> b.balance_minor OR v.last_seq <> b.last_seq)`).Scan(&n)
	return n, err
}

func round(v float64, places int) float64 {
	p := 1.0
	for i := 0; i < places; i++ {
		p *= 10
	}
	return float64(int64(v*p+0.5)) / p
}

// hardwareInfo describes the machine the numbers were measured on.
func hardwareInfo() map[string]any {
	info := map[string]any{
		"os":                      runtime.GOOS + "/" + runtime.GOARCH,
		"logical_cpus":            runtime.NumCPU(),
		"container_memory_limits": "api 256 MB, postgres 512 MB, redpanda 1 GB, relay 256 MB, each consumer 256 MB",
		"load_generator":          "k6 on the same machine as the stack",
	}
	out := func(name string, args ...string) string {
		b, err := exec.Command(name, args...).Output()
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(b))
	}
	switch runtime.GOOS {
	case "windows":
		ps := func(script string) string { return out("powershell", "-NoProfile", "-Command", script) }
		info["cpu"] = ps("(Get-CimInstance Win32_Processor | Select-Object -First 1).Name")
		info["ram_gb"] = ps("[math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)")
		info["os_version"] = ps("(Get-CimInstance Win32_OperatingSystem).Caption")
	case "darwin":
		info["cpu"] = out("sysctl", "-n", "machdep.cpu.brand_string")
		if b, err := strconv.ParseFloat(out("sysctl", "-n", "hw.memsize"), 64); err == nil {
			info["ram_gb"] = round(b/(1<<30), 1)
		}
		info["os_version"] = "macOS " + out("sw_vers", "-productVersion")
	default:
		if raw, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(raw), "\n") {
				if strings.HasPrefix(line, "model name") {
					info["cpu"] = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
					break
				}
			}
		}
		if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
			fields := strings.Fields(strings.SplitN(string(raw), "\n", 2)[0])
			if len(fields) >= 2 {
				if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
					info["ram_gb"] = round(kb/(1<<20), 1)
				}
			}
		}
		info["os_version"] = out("uname", "-sr")
	}
	if mem, err := strconv.ParseFloat(out("docker", "info", "--format", "{{.MemTotal}}"), 64); err == nil {
		info["docker_memory_gb"] = round(mem/(1<<30), 1)
	}
	info["docker_version"] = out("docker", "version", "--format", "{{.Server.Version}}")
	info["k6_version"] = strings.SplitN(out("k6", "version"), " (", 2)[0]
	return info
}
