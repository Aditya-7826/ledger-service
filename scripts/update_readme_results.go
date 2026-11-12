//go:build ignore

// Regenerates the Results section of README.md from results/loadtest-*.json.
// Only the text between the two marker comments is replaced.
//
//	go run scripts/update_readme_results.go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const (
	begin = "<!-- RESULTS:BEGIN -->"
	end   = "<!-- RESULTS:END -->"
)

type result struct {
	Scenario            string  `json:"scenario"`
	Description         string  `json:"description"`
	RanAt               string  `json:"ran_at"`
	VirtualUsers        int     `json:"virtual_users"`
	DurationSeconds     float64 `json:"duration_seconds"`
	Requests            int64   `json:"requests"`
	RequestsPerSecond   float64 `json:"requests_per_second"`
	TransfersCreated    int64   `json:"transfers_created"`
	ThroughputPerSecond float64 `json:"throughput_per_second"`
	LatencyMS           struct {
		P50 float64 `json:"p50"`
		P95 float64 `json:"p95"`
		P99 float64 `json:"p99"`
	} `json:"latency_ms"`
	ErrorRate    float64 `json:"error_rate"`
	Drained      bool    `json:"pipeline_drained"`
	Replays      int64   `json:"replays"`
	Mismatches   int64   `json:"mismatches"`
	ReconcileOK  bool    `json:"reconcile_ok"`
	DrainSeconds float64 `json:"pipeline_drain_seconds"`
	Duplicates   int64   `json:"duplicates"`
	Storm        *struct {
		LogicalRequests     int64 `json:"logical_requests"`
		HTTPRequestsSent    int64 `json:"http_requests_sent"`
		TransfersInDatabase int64 `json:"transfers_in_database"`
		DuplicateTransfers  int64 `json:"duplicate_transfers"`
		InconsistentRetries int64 `json:"inconsistent_retries"`
		ExactlyOncePerKey   bool  `json:"exactly_once_per_key"`
	} `json:"retry_storm"`
	Hardware map[string]any `json:"hardware"`
}

func main() {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		fail(err)
	}
	text := string(readme)
	i, j := strings.Index(text, begin), strings.Index(text, end)
	if i < 0 || j < i {
		fail(fmt.Errorf("README.md has no %s ... %s markers", begin, end))
	}
	section := render()
	out := text[:i+len(begin)] + "\n" + section + "\n" + text[j:]
	if err := os.WriteFile("README.md", []byte(out), 0o644); err != nil {
		fail(err)
	}
	fmt.Println("README.md results section updated")
}

func render() string {
	var rs []result
	for _, name := range []string{"uniform", "hot_account", "retry_storm"} {
		raw, err := os.ReadFile("results/loadtest-" + name + ".json")
		if err != nil {
			continue
		}
		var r result
		if err := json.Unmarshal(raw, &r); err != nil {
			fail(fmt.Errorf("results/loadtest-%s.json: %w", name, err))
		}
		rs = append(rs, r)
	}
	if len(rs) == 0 {
		return "Not yet measured. Run `scripts/dev seed` and then `scripts/dev loadtest`, then regenerate this section."
	}

	var b strings.Builder
	b.WriteString("_Generated from `results/*.json` by `scripts/update_readme_results.go`. Do not edit by hand._\n\n")
	b.WriteString("| Scenario | VUs | Duration | Committed transfers/s | HTTP requests/s | p50 | p95 | p99 | Error rate | Read model caught up after | View mismatches | Duplicates | Reconcile |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range rs {
		fmt.Fprintf(&b, "| `%s` | %d | %.0f s | **%.1f** | %.1f | %.1f ms | %.1f ms | %.1f ms | %.2f%% | %s | %d | %d | %s |\n",
			r.Scenario, r.VirtualUsers, r.DurationSeconds, r.ThroughputPerSecond, r.RequestsPerSecond,
			r.LatencyMS.P50, r.LatencyMS.P95, r.LatencyMS.P99, r.ErrorRate*100, drainText(r), r.Mismatches, r.Duplicates, okText(r.ReconcileOK))
	}
	for _, r := range rs {
		if r.Storm == nil {
			continue
		}
		s := r.Storm
		fmt.Fprintf(&b, "\n**Retry storm check (SQL after the run):** %s logical requests were sent as %s HTTP requests; "+
			"the database holds **%s transfers** for them with **%d duplicates**, and %d logical requests got inconsistent answers. "+
			"Exactly once per key: **%s**.\n", thousands(s.LogicalRequests), thousands(s.HTTPRequestsSent), thousands(s.TransfersInDatabase),
			s.DuplicateTransfers, s.InconsistentRetries, yesNo(s.ExactlyOncePerKey))
	}
	b.WriteString("\n\"Read model caught up after\" is the time from the end of the k6 run until the balance read model equalled the ledger for every account. ")
	b.WriteString("\"View mismatches\" counts accounts that still differed after that. ")
	b.WriteString("\"Duplicates\" counts idempotency keys of the run that produced more than one transfer.\n")

	hw := rs[0].Hardware
	fmt.Fprintf(&b, "\n**Measured on:** %v, %v logical CPUs, %v GB RAM, %v. Docker had %v GB; %v. %v, k6 %v, run on %s.\n",
		hw["cpu"], hw["logical_cpus"], hw["ram_gb"], hw["os_version"], hw["docker_memory_gb"],
		hw["container_memory_limits"], hw["load_generator"], strings.TrimPrefix(strings.TrimPrefix(fmt.Sprint(hw["k6_version"]), "k6.exe "), "k6 "), strings.SplitN(rs[0].RanAt, "T", 2)[0])
	return b.String()
}

func drainText(r result) string {
	if !r.Drained {
		return "not within 15 min"
	}
	if r.DrainSeconds < 1 {
		return "immediately"
	}
	return fmt.Sprintf("%.0f s", r.DrainSeconds)
}

func thousands(n int64) string {
	s := fmt.Sprint(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func okText(ok bool) string {
	if ok {
		return "✓ passed"
	}
	return "✗ failed"
}

func yesNo(ok bool) string {
	if ok {
		return "yes"
	}
	return "no"
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "update_readme_results:", err)
	os.Exit(1)
}
