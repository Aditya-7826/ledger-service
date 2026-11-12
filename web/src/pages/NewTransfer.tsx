import { useState, type FormEvent } from 'react'
import { Link } from 'react-router-dom'
import { Badge, Card, EmptyState, ErrorState, PageTitle, Skeleton } from '../components/ui'
import { api, ApiError, shortId, type Transfer, type TransferRequest } from '../lib/api'
import { useAsync } from '../lib/hooks'
import { newIdempotencyKey } from '../lib/idempotency'
import { formatPaise, parseRupeesToPaise } from '../lib/money'

interface Attempt {
  n: number
  status: number
  replayed: boolean
  transferId: string | null
  error: string | null
}

export default function NewTransfer() {
  const accounts = useAsync(() => api.accounts(null, 100), 'transfer-accounts')
  const [from, setFrom] = useState('')
  const [to, setTo] = useState('')
  const [amount, setAmount] = useState('')
  const [memo, setMemo] = useState('')
  const [key, setKey] = useState(newIdempotencyKey)
  const [amountError, setAmountError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  // The exact request of the first send: "send again" repeats it byte for byte.
  const [sent, setSent] = useState<TransferRequest | null>(null)
  const [attempts, setAttempts] = useState<Attempt[]>([])
  const [result, setResult] = useState<Transfer | null>(null)

  const users = (accounts.data?.items ?? []).filter((a) => a.kind === 'USER' && a.status !== 'CLOSED')

  async function send(body: TransferRequest) {
    setBusy(true)
    try {
      const res = await api.createTransfer(key, body)
      setResult(res.data)
      setAttempts((a) => [...a, { n: a.length + 1, status: res.status, replayed: res.replayed, transferId: res.data.id, error: null }])
    } catch (err) {
      const e = err instanceof ApiError ? err : new ApiError(0, 'unknown', 'The request failed.')
      setAttempts((a) => [...a, { n: a.length + 1, status: e.status, replayed: false, transferId: null, error: e.message }])
    } finally {
      setBusy(false)
    }
  }

  function submit(e: FormEvent) {
    e.preventDefault()
    if (busy) return
    const parsed = parseRupeesToPaise(amount)
    if (!parsed.ok) {
      setAmountError(parsed.error)
      return
    }
    setAmountError(null)
    const currency = users.find((a) => a.id === from)?.currency ?? 'INR'
    const body: TransferRequest = { from_account_id: from, to_account_id: to, amount_minor: parsed.paise, currency, memo }
    setSent(body)
    void send(body)
  }

  function reset() {
    setKey(newIdempotencyKey())
    setSent(null)
    setAttempts([])
    setResult(null)
    setAmount('')
    setMemo('')
  }

  const locked = sent !== null
  const preview = parseRupeesToPaise(amount)

  return (
    <>
      <PageTitle title="New transfer" subtitle="Each transfer carries an idempotency key. Sending the same key again can never charge twice." />

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          {accounts.loading ? (
            <Skeleton rows={6} className="h-9" />
          ) : accounts.error ? (
            <ErrorState error={accounts.error} onRetry={accounts.reload} />
          ) : users.length < 2 ? (
            <EmptyState
              title="You need at least two accounts"
              hint="Create accounts and deposit some money first."
              action={
                <Link to="/accounts" className="btn btn-primary">
                  Go to accounts
                </Link>
              }
            />
          ) : (
            <form onSubmit={submit} className="space-y-4" noValidate>
              <label className="block text-sm font-medium">
                From
                <select className="field mt-1" value={from} onChange={(e) => setFrom(e.target.value)} required disabled={locked}>
                  <option value="">Choose the sender…</option>
                  {users.map((a) => (
                    <option key={a.id} value={a.id}>
                      {a.name} · {formatPaise(a.balance_minor)} · {shortId(a.id)}
                    </option>
                  ))}
                </select>
              </label>
              <label className="block text-sm font-medium">
                To
                <select className="field mt-1" value={to} onChange={(e) => setTo(e.target.value)} required disabled={locked}>
                  <option value="">Choose the receiver…</option>
                  {users
                    .filter((a) => a.id !== from)
                    .map((a) => (
                      <option key={a.id} value={a.id}>
                        {a.name} · {shortId(a.id)}
                      </option>
                    ))}
                </select>
              </label>
              <label className="block text-sm font-medium">
                Amount in rupees
                <input
                  className="field num mt-1"
                  inputMode="decimal"
                  value={amount}
                  onChange={(e) => {
                    setAmount(e.target.value)
                    setAmountError(null)
                  }}
                  placeholder="125.50"
                  aria-invalid={amountError !== null}
                  aria-describedby="amount-help"
                  disabled={locked}
                />
                <span id="amount-help" className={`mt-1 block text-xs ${amountError ? 'font-medium text-debit' : 'text-ink-soft'}`} role={amountError ? 'alert' : undefined}>
                  {amountError ??
                    (preview.ok
                      ? `Sent to the API as the integer ${preview.paise.toLocaleString('en-IN')} paise.`
                      : 'Digits with at most 2 decimal places. Converted to paise without floating-point maths.')}
                </span>
              </label>
              <label className="block text-sm font-medium">
                Memo (optional)
                <input className="field mt-1" value={memo} onChange={(e) => setMemo(e.target.value)} maxLength={280} disabled={locked} />
              </label>
              <div className="rounded-xl bg-white/60 p-3 text-sm">
                <p className="font-medium">Idempotency key</p>
                <p className="num mt-1 break-all text-xs text-ink-soft">{key}</p>
                {!locked && (
                  <button type="button" className="mt-2 text-xs font-semibold text-deep underline-offset-2 hover:underline" onClick={() => setKey(newIdempotencyKey())}>
                    Generate a different key
                  </button>
                )}
              </div>
              {!locked ? (
                <button type="submit" className="btn btn-primary w-full" disabled={busy || !from || !to}>
                  {busy ? 'Sending…' : 'Send transfer'}
                </button>
              ) : (
                <div className="grid gap-2 sm:grid-cols-2">
                  <button type="button" className="btn btn-primary" disabled={busy} onClick={() => sent && void send(sent)}>
                    {busy ? 'Sending…' : 'Send again with the same key'}
                  </button>
                  <button type="button" className="btn btn-ghost" disabled={busy} onClick={reset}>
                    Start a new transfer
                  </button>
                </div>
              )}
            </form>
          )}
        </Card>

        <Card>
          <h2 className="text-lg font-bold">What the server answered</h2>
          {attempts.length === 0 ? (
            <p className="mt-2 text-sm text-ink-soft">
              Send a transfer, then press <strong>Send again with the same key</strong>. The second answer is the stored response of the first, marked
              with the header <code className="num rounded bg-white/70 px-1">Idempotent-Replayed: true</code>, and the balance moves only once.
            </p>
          ) : (
            <>
              <ol className="mt-3 space-y-2" aria-live="polite">
                {attempts.map((a) => (
                  <li key={a.n} className="rounded-xl bg-white/60 p-3 text-sm">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="font-semibold">Attempt {a.n}</span>
                      <Badge tone={a.error ? 'bad' : 'ok'}>HTTP {a.status || 'no response'}</Badge>
                      {a.replayed ? <Badge tone="info">↺ Idempotent-Replayed: true</Badge> : !a.error && <Badge tone="ok">✓ New transfer created</Badge>}
                    </div>
                    {a.error ? (
                      <p className="mt-1 text-debit">{a.error}</p>
                    ) : (
                      <p className="num mt-1 break-all text-xs text-ink-soft">transfer id {a.transferId}</p>
                    )}
                  </li>
                ))}
              </ol>
              {attempts.filter((a) => a.transferId).length > 1 && new Set(attempts.filter((a) => a.transferId).map((a) => a.transferId)).size === 1 && (
                <p className="mt-3 rounded-xl bg-credit-soft/70 p-3 text-sm font-medium text-credit">
                  ✓ Every attempt returned the same transfer id: the money moved exactly once.
                </p>
              )}
              {result && (
                <dl className="mt-3 grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
                  <dt className="text-ink-soft">Amount</dt>
                  <dd className="num font-semibold">{formatPaise(result.amount_minor)}</dd>
                  <dt className="text-ink-soft">Entries</dt>
                  <dd className="num">
                    {result.entries.map((e) => `${e.direction === 'DEBIT' ? '−' : '+'} ${e.direction} #${e.account_seq}`).join('   ')}
                  </dd>
                </dl>
              )}
            </>
          )}
        </Card>
      </div>
    </>
  )
}
