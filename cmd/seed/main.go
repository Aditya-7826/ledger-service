// Command seed creates demo accounts with an opening deposit through the
// ledger API, and writes their IDs to a file the load test reads.
//
//	go run ./cmd/seed                 # 1,000 accounts with Rs 1,00,000 each
//	go run ./cmd/seed -n 50 -amount 500000
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	api := flag.String("api", "http://localhost:8080", "ledger API base URL")
	n := flag.Int("n", 1000, "number of accounts to create")
	amount := flag.Int64("amount", 10_000_000, "opening deposit per account, in paise")
	out := flag.String("out", filepath.Join("loadtest", "accounts.json"), "file to write the account IDs to")
	workers := flag.Int("workers", 16, "concurrent requests")
	flag.Parse()

	client := &http.Client{Timeout: 30 * time.Second}
	ids := make([]string, *n)
	var failed atomic.Int64
	var firstErr atomic.Value
	jobs := make(chan int)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				id, err := createFunded(client, *api, fmt.Sprintf("Seed account %04d", i+1), *amount)
				if err != nil {
					failed.Add(1)
					firstErr.CompareAndSwap(nil, err)
					continue
				}
				ids[i] = id
			}
		}()
	}
	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	if failed.Load() > 0 {
		fmt.Fprintf(os.Stderr, "seed failed for %d of %d accounts; first error: %v\n", failed.Load(), *n, firstErr.Load())
		fmt.Fprintln(os.Stderr, "Is the stack running? Start it with scripts/dev up.")
		os.Exit(1)
	}
	raw, _ := json.Marshal(ids)
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err == nil {
		err = os.WriteFile(*out, raw, 0o644)
		if err != nil {
			fmt.Fprintln(os.Stderr, "write account file:", err)
			os.Exit(1)
		}
	}
	fmt.Printf("created %d accounts with %d paise each in %s; IDs written to %s\n",
		*n, *amount, time.Since(start).Round(time.Millisecond), *out)
}

// createFunded creates one account and deposits the opening amount. The
// deposit's idempotency key is derived from the account, so a retry of this
// step can never deposit twice.
func createFunded(client *http.Client, api, name string, amount int64) (string, error) {
	var account struct {
		ID string `json:"id"`
	}
	if err := post(client, api+"/v1/accounts", nil, map[string]any{"name": name, "currency": "INR"}, &account); err != nil {
		return "", err
	}
	headers := map[string]string{"Idempotency-Key": "seed-" + account.ID, "X-Client-Id": "seed"}
	body := map[string]any{"account_id": account.ID, "amount_minor": amount, "currency": "INR", "memo": "opening deposit"}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if lastErr = post(client, api+"/v1/deposits", headers, body, nil); lastErr == nil {
			return account.ID, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return "", lastErr
}

func post(client *http.Client, url string, headers map[string]string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("POST %s: status %d: %s", url, res.StatusCode, data)
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}
