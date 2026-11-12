package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/google/uuid"
)

// ErrAccountNotFound means the ledger has no such account.
var ErrAccountNotFound = errors.New("account not found in the ledger")

// HTTPLedger reads account state from the ledger API.
type HTTPLedger struct {
	BaseURL string
	Client  *http.Client
}

func (l *HTTPLedger) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.BaseURL+path, nil)
	if err != nil {
		return err
	}
	client := l.Client
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return ErrAccountNotFound
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("ledger API %s: status %d", path, res.StatusCode)
	}
	return json.NewDecoder(res.Body).Decode(out)
}

// AccountState walks the ledger's entries endpoint for the account (newest
// first, 100 per page) and sums it. Entries are append-only, so everything
// at or below the first page's highest account_seq is stable while paging.
func (l *HTTPLedger) AccountState(ctx context.Context, account uuid.UUID) (balance, lastSeq int64, err error) {
	cursor := ""
	for {
		var page struct {
			Items []struct {
				Direction   string `json:"direction"`
				AmountMinor int64  `json:"amount_minor"`
				AccountSeq  int64  `json:"account_seq"`
			} `json:"items"`
			NextCursor *string `json:"next_cursor"`
		}
		path := "/v1/accounts/" + account.String() + "/entries?limit=100"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		if err := l.get(ctx, path, &page); err != nil {
			return 0, 0, err
		}
		for _, e := range page.Items {
			if e.AccountSeq > lastSeq {
				lastSeq = e.AccountSeq
			}
			if e.Direction == "CREDIT" {
				balance += e.AmountMinor
			} else {
				balance -= e.AmountMinor
			}
		}
		if page.NextCursor == nil {
			return balance, lastSeq, nil
		}
		cursor = *page.NextCursor
	}
}

// LastSeq returns the ledger's current last_seq for the account.
func (l *HTTPLedger) LastSeq(ctx context.Context, account uuid.UUID) (int64, error) {
	var acc struct {
		LastSeq int64 `json:"last_seq"`
	}
	err := l.get(ctx, "/v1/accounts/"+account.String(), &acc)
	return acc.LastSeq, err
}
