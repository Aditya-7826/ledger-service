package httpapi

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Aditya-7826/ledger-service/internal/idempotency"
	"github.com/Aditya-7826/ledger-service/internal/ledger"
)

const (
	defaultLimit = 50
	maxLimit     = 100
)

// parseUUID accepts only the canonical 36-character form.
func parseUUID(s string) (uuid.UUID, bool) {
	if len(s) != 36 {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(s)
	return id, err == nil
}

func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, ok := parseUUID(chi.URLParam(r, "id"))
	if !ok {
		writeProblem(w, r, http.StatusBadRequest, "invalid_uuid", "the id in the path is not a valid UUID")
	}
	return id, ok
}

func bodyUUID(w http.ResponseWriter, r *http.Request, field, value string) (uuid.UUID, bool) {
	id, ok := parseUUID(value)
	if !ok {
		writeProblem(w, r, http.StatusBadRequest, "invalid_uuid", field+" must be a valid UUID")
	}
	return id, ok
}

// writeMeta reads and validates the Idempotency-Key and X-Client-Id headers.
func writeMeta(w http.ResponseWriter, r *http.Request) (ledger.Meta, bool) {
	m := ledger.Meta{ClientID: r.Header.Get("X-Client-Id"), Key: r.Header.Get("Idempotency-Key")}
	if !idempotency.ValidKey(m.Key) {
		writeProblem(w, r, http.StatusBadRequest, "invalid_idempotency_key",
			"header Idempotency-Key is required: 1-64 characters of A-Z a-z 0-9 _ -")
		return m, false
	}
	if !idempotency.ValidKey(m.ClientID) {
		writeProblem(w, r, http.StatusBadRequest, "invalid_client_id",
			"header X-Client-Id is required: 1-64 characters of A-Z a-z 0-9 _ -")
		return m, false
	}
	return m, true
}

func limitParam(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return defaultLimit, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > maxLimit {
		writeProblem(w, r, http.StatusBadRequest, "invalid_limit", "limit must be an integer between 1 and 100")
		return 0, false
	}
	return n, true
}

func uuidCursor(w http.ResponseWriter, r *http.Request) (*uuid.UUID, bool) {
	raw := r.URL.Query().Get("cursor")
	if raw == "" {
		return nil, true
	}
	id, ok := parseUUID(raw)
	if !ok {
		writeProblem(w, r, http.StatusBadRequest, "invalid_cursor", "cursor is not valid")
		return nil, false
	}
	return &id, true
}

// respond writes the stored or fresh response of an idempotent write.
func respond(w http.ResponseWriter, resp ledger.Response) {
	w.Header().Set("Content-Type", "application/json")
	if resp.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Body)
}

// ---------------------------------------------------------------- accounts

func (a *api) createAccount(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name     string `json:"name"`
		Currency string `json:"currency"`
	}
	if !decodeJSON(w, r, &in, false) {
		return
	}
	acc, err := a.svc.CreateAccount(r.Context(), in.Name, in.Currency)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, acc)
}

func (a *api) getAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	acc, err := a.svc.GetAccount(r.Context(), id)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, acc)
}

func (a *api) listAccounts(w http.ResponseWriter, r *http.Request) {
	limit, ok := limitParam(w, r)
	if !ok {
		return
	}
	cursor, ok := uuidCursor(w, r)
	if !ok {
		return
	}
	page, err := a.svc.ListAccounts(r.Context(), cursor, limit)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (a *api) listEntries(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	limit, ok := limitParam(w, r)
	if !ok {
		return
	}
	var before int64
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 1 {
			writeProblem(w, r, http.StatusBadRequest, "invalid_cursor", "cursor is not valid")
			return
		}
		before = n
	}
	page, err := a.svc.ListEntries(r.Context(), id, before, limit)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (a *api) freeze(frozen bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		acc, err := a.svc.SetFrozen(r.Context(), id, frozen)
		if err != nil {
			a.writeError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, acc)
	}
}

// ---------------------------------------------------------------- transfers

func (a *api) createTransfer(w http.ResponseWriter, r *http.Request) {
	meta, ok := writeMeta(w, r)
	if !ok {
		return
	}
	var in struct {
		From        string `json:"from_account_id"`
		To          string `json:"to_account_id"`
		AmountMinor int64  `json:"amount_minor"`
		Currency    string `json:"currency"`
		Memo        string `json:"memo"`
	}
	if !decodeJSON(w, r, &in, false) {
		return
	}
	from, ok := bodyUUID(w, r, "from_account_id", in.From)
	if !ok {
		return
	}
	to, ok := bodyUUID(w, r, "to_account_id", in.To)
	if !ok {
		return
	}
	resp, err := a.svc.Transfer(r.Context(), meta, ledger.TransferInput{
		From: from, To: to, AmountMinor: in.AmountMinor, Currency: in.Currency, Memo: in.Memo})
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	respond(w, resp)
}

func (a *api) createDeposit(w http.ResponseWriter, r *http.Request) {
	meta, ok := writeMeta(w, r)
	if !ok {
		return
	}
	var in struct {
		AccountID   string `json:"account_id"`
		AmountMinor int64  `json:"amount_minor"`
		Currency    string `json:"currency"`
		Memo        string `json:"memo"`
	}
	if !decodeJSON(w, r, &in, false) {
		return
	}
	id, ok := bodyUUID(w, r, "account_id", in.AccountID)
	if !ok {
		return
	}
	resp, err := a.svc.Deposit(r.Context(), meta, ledger.DepositInput{
		AccountID: id, AmountMinor: in.AmountMinor, Currency: in.Currency, Memo: in.Memo})
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	respond(w, resp)
}

func (a *api) reverseTransfer(w http.ResponseWriter, r *http.Request) {
	meta, ok := writeMeta(w, r)
	if !ok {
		return
	}
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var in struct {
		Memo string `json:"memo"`
	}
	if !decodeJSON(w, r, &in, true) {
		return
	}
	resp, err := a.svc.Reverse(r.Context(), meta, id, in.Memo)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	respond(w, resp)
}

func (a *api) getTransfer(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	t, err := a.svc.GetTransfer(r.Context(), id)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (a *api) listTransfers(w http.ResponseWriter, r *http.Request) {
	limit, ok := limitParam(w, r)
	if !ok {
		return
	}
	cursor, ok := uuidCursor(w, r)
	if !ok {
		return
	}
	page, err := a.svc.ListTransfers(r.Context(), cursor, limit)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (a *api) reconcile(w http.ResponseWriter, r *http.Request) {
	rec, err := a.svc.Reconcile(r.Context())
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (a *api) stats(w http.ResponseWriter, r *http.Request) {
	st, err := a.svc.Stats(r.Context())
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}
