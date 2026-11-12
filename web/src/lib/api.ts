// Typed client for the ledger API. Every amount is an integer number of paise.

export interface Account {
  id: string
  name: string
  currency: string
  status: 'ACTIVE' | 'FROZEN' | 'CLOSED'
  kind: 'USER' | 'SYSTEM'
  balance_minor: number
  last_seq: number
  created_at: string
}

export interface Entry {
  id: string
  transfer_id: string
  account_id: string
  direction: 'DEBIT' | 'CREDIT'
  amount_minor: number
  account_seq: number
  created_at: string
}

export interface Transfer {
  id: string
  type: 'TRANSFER' | 'DEPOSIT' | 'REVERSAL'
  from_account_id: string
  to_account_id: string
  amount_minor: number
  currency: string
  memo: string
  reverses_transfer_id: string | null
  reversed_by_transfer_id: string | null
  created_at: string
  entries: Entry[]
}

export interface Page<T> {
  items: T[]
  next_cursor: string | null
}

export interface Stats {
  accounts: number
  transfers_total: number
  transfers_today: number
  volume_today_minor: number
  outbox_backlog: number
  oldest_unpublished_seconds: number
}

export interface Reconciliation {
  ok: boolean
  checks: { name: string; ok: boolean; detail: string }[]
  checked_at: string
}

export interface BalanceView {
  account_id: string
  balance_minor: number
  last_seq: number
  updated_at: string | null
  ledger_last_seq: number
  lag_seq: number
}

export interface SystemView {
  balance_view: { accounts_behind: number; entries_behind: number }
  dlq: { total: number; topics: Record<string, number> }
}

export interface Notification {
  id: number
  transfer_id: string
  account_id: string
  kind: 'DEBITED' | 'CREDITED'
  amount_minor: number
  currency: string
  counterparty_account_id: string
  transfer_type: string
  memo: string
  occurred_at: string
}

export interface LoadTestResult {
  scenario: string
  ran_at: string
  duration_seconds: number
  requests: number
  throughput_per_second: number
  latency_ms: { p50: number; p95: number; p99: number }
  error_rate: number
  mismatches: number
  duplicates?: number
  hardware: Record<string, string | number>
}

/** An error response in RFC 7807 problem+json form (or a network failure). */
export class ApiError extends Error {
  readonly status: number
  readonly code: string
  constructor(status: number, code: string, detail: string) {
    super(detail)
    this.status = status
    this.code = code
  }
}

export interface ApiResponse<T> {
  status: number
  data: T
  replayed: boolean
}

async function request<T>(method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<ApiResponse<T>> {
  let res: Response
  try {
    res = await fetch(path, {
      method,
      headers: body === undefined ? headers : { 'Content-Type': 'application/json', ...headers },
      body: body === undefined ? undefined : JSON.stringify(body),
    })
  } catch {
    throw new ApiError(0, 'network_error', 'Could not reach the server. Is the stack running?')
  }
  const text = await res.text()
  let parsed: unknown = null
  try {
    parsed = text ? JSON.parse(text) : null
  } catch {
    /* not JSON: handled below */
  }
  if (!res.ok) {
    const p = (parsed ?? {}) as { code?: string; detail?: string }
    throw new ApiError(res.status, p.code ?? 'http_error', p.detail ?? `The server answered ${res.status}.`)
  }
  return { status: res.status, data: parsed as T, replayed: res.headers.get('Idempotent-Replayed') === 'true' }
}

const get = async <T>(path: string) => (await request<T>('GET', path)).data

/** Name sent as X-Client-Id by this dashboard. */
export const CLIENT_ID = 'dashboard'

const writeHeaders = (key: string) => ({ 'Idempotency-Key': key, 'X-Client-Id': CLIENT_ID })

const query = (params: Record<string, string | number | null | undefined>) => {
  const q = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) if (v !== null && v !== undefined && v !== '') q.set(k, String(v))
  const s = q.toString()
  return s ? `?${s}` : ''
}

export interface TransferRequest {
  from_account_id: string
  to_account_id: string
  amount_minor: number
  currency: string
  memo: string
}

export const api = {
  stats: () => get<Stats>('/v1/stats'),
  reconcile: () => get<Reconciliation>('/v1/reconcile'),
  system: () => get<SystemView>('/v1/views/system'),
  balanceView: (id: string) => get<BalanceView>(`/v1/views/balances/${id}`),

  accounts: (cursor?: string | null, limit = 50) => get<Page<Account>>(`/v1/accounts${query({ cursor, limit })}`),
  account: (id: string) => get<Account>(`/v1/accounts/${id}`),
  entries: (id: string, cursor?: string | null, limit = 25) =>
    get<Page<Entry>>(`/v1/accounts/${id}/entries${query({ cursor, limit })}`),
  createAccount: async (name: string, currency: string) =>
    (await request<Account>('POST', '/v1/accounts', { name, currency })).data,
  setFrozen: async (id: string, frozen: boolean) =>
    (await request<Account>('POST', `/v1/accounts/${id}/${frozen ? 'freeze' : 'unfreeze'}`)).data,

  transfers: (cursor?: string | null, limit = 20) => get<Page<Transfer>>(`/v1/transfers${query({ cursor, limit })}`),
  createTransfer: (key: string, body: TransferRequest) => request<Transfer>('POST', '/v1/transfers', body, writeHeaders(key)),
  deposit: (key: string, account_id: string, amount_minor: number, currency: string) =>
    request<Transfer>('POST', '/v1/deposits', { account_id, amount_minor, currency }, writeHeaders(key)),
  reverse: (key: string, transferId: string) =>
    request<Transfer>('POST', `/v1/transfers/${transferId}/reverse`, undefined, writeHeaders(key)),

  /** Reads a load-test result file; null when that scenario has not been run. */
  loadTest: async (scenario: string): Promise<LoadTestResult | null> => {
    try {
      const res = await fetch(`/results/loadtest-${scenario}.json`, { cache: 'no-store' })
      if (!res.ok) return null
      return (await res.json()) as LoadTestResult
    } catch {
      return null
    }
  },
}

export const shortId = (id: string) => `${id.slice(0, 8)}…${id.slice(-4)}`

export const formatTime = (iso: string) =>
  new Date(iso).toLocaleString('en-IN', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit' })
