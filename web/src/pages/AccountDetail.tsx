import { useState, type FormEvent } from 'react'
import { Link, useParams } from 'react-router-dom'
import { Badge, Card, ConfirmDialog, EmptyState, ErrorState, PageTitle, SignedAmount, Skeleton } from '../components/ui'
import { api, ApiError, formatTime, shortId, type Entry } from '../lib/api'
import { useAsync } from '../lib/hooks'
import { newIdempotencyKey } from '../lib/idempotency'
import { formatPaise, parseRupeesToPaise } from '../lib/money'
import { StatusBadge } from './Accounts'

export default function AccountDetail() {
  const { id = '' } = useParams()
  const [version, setVersion] = useState(0)
  const account = useAsync(() => api.account(id), `account:${id}:${version}`, 5000)
  const view = useAsync(() => api.balanceView(id), `view:${id}:${version}`, 5000)
  const first = useAsync(() => api.entries(id, null, 25), `entries:${id}:${version}`)
  const [more, setMore] = useState<Entry[]>([])
  const [cursor, setCursor] = useState<string | null | undefined>(undefined)
  const [loadingMore, setLoadingMore] = useState(false)

  const [confirm, setConfirm] = useState(false)
  const [busy, setBusy] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)

  const [amount, setAmount] = useState('')
  const [depositKey, setDepositKey] = useState(newIdempotencyKey)
  const [depositMsg, setDepositMsg] = useState<{ ok: boolean; text: string } | null>(null)
  const [depositing, setDepositing] = useState(false)

  const refresh = () => {
    setMore([])
    setCursor(undefined)
    setVersion((v) => v + 1)
  }

  if (account.loading) {
    return (
      <Card>
        <Skeleton rows={5} className="h-8" />
      </Card>
    )
  }
  if (account.error || !account.data) {
    return account.error?.status === 404 ? (
      <EmptyState title="Account not found" hint="It may have been removed by a reset." action={<Link className="btn btn-ghost" to="/accounts">Back to accounts</Link>} />
    ) : (
      <ErrorState error={account.error ?? new ApiError(0, 'unknown', 'Unknown error')} onRetry={account.reload} />
    )
  }
  const a = account.data
  const frozen = a.status === 'FROZEN'
  const entries = [...(first.data?.items ?? []), ...more]
  const nextCursor = cursor === undefined ? first.data?.next_cursor : cursor

  async function toggleFreeze() {
    setBusy(true)
    setActionError(null)
    try {
      await api.setFrozen(a.id, !frozen)
      setConfirm(false)
      refresh()
    } catch (err) {
      setActionError(err instanceof ApiError ? err.message : 'The request failed.')
      setConfirm(false)
    } finally {
      setBusy(false)
    }
  }

  async function deposit(e: FormEvent) {
    e.preventDefault()
    if (depositing) return
    const parsed = parseRupeesToPaise(amount)
    if (!parsed.ok) {
      setDepositMsg({ ok: false, text: parsed.error })
      return
    }
    setDepositing(true)
    setDepositMsg(null)
    try {
      await api.deposit(depositKey, a.id, parsed.paise, a.currency)
      setDepositMsg({ ok: true, text: `Deposited ${formatPaise(parsed.paise)}.` })
      setAmount('')
      setDepositKey(newIdempotencyKey()) // the next deposit is a new payment
      refresh()
    } catch (err) {
      setDepositMsg({ ok: false, text: err instanceof ApiError ? err.message : 'The deposit failed.' })
    } finally {
      setDepositing(false)
    }
  }

  async function loadMore() {
    if (!nextCursor || loadingMore) return
    setLoadingMore(true)
    try {
      const page = await api.entries(a.id, nextCursor, 25)
      setMore((m) => [...m, ...page.items])
      setCursor(page.next_cursor)
    } finally {
      setLoadingMore(false)
    }
  }

  return (
    <>
      <PageTitle
        title={a.name}
        subtitle={a.id}
        action={
          a.kind === 'USER' && a.status !== 'CLOSED' ? (
            <button type="button" className={`btn ${frozen ? 'btn-primary' : 'btn-danger'}`} onClick={() => setConfirm(true)} disabled={busy}>
              {frozen ? 'Unfreeze account' : '❄ Freeze account'}
            </button>
          ) : undefined
        }
      />
      {actionError && (
        <p role="alert" className="mb-4 text-sm font-medium text-debit">
          {actionError}
        </p>
      )}

      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <p className="text-xs font-semibold uppercase tracking-wide text-ink-soft">Ledger balance</p>
          <p className="num mt-2 text-3xl font-bold">{formatPaise(a.balance_minor)}</p>
          <p className="mt-2 flex items-center gap-2 text-sm text-ink-soft">
            <StatusBadge account={a} /> {a.currency} · {a.last_seq} entries
          </p>
        </Card>
        <Card>
          <p className="text-xs font-semibold uppercase tracking-wide text-ink-soft">Balance view (read model)</p>
          {view.data ? (
            <>
              <p className="num mt-2 text-3xl font-bold">{formatPaise(view.data.balance_minor)}</p>
              <p className="mt-2 text-sm">
                {view.data.lag_seq === 0 ? <Badge tone="ok">✓ Caught up</Badge> : <Badge tone="warn">{view.data.lag_seq} entries behind</Badge>}
              </p>
            </>
          ) : view.error ? (
            <p className="mt-2 text-sm text-ink-soft">Unavailable: the balance consumer is not reachable.</p>
          ) : (
            <div className="mt-2">
              <Skeleton rows={2} className="h-7" />
            </div>
          )}
          <p className="mt-2 text-xs text-ink-soft">Built from events; it can lag behind the ledger for a moment.</p>
        </Card>
        <Card>
          <h2 className="text-sm font-bold">Deposit</h2>
          {a.kind === 'SYSTEM' ? (
            <p className="mt-2 text-sm text-ink-soft">Deposits come from this account, so it cannot receive one.</p>
          ) : (
            <form onSubmit={deposit} className="mt-2 space-y-2">
              <label className="block text-sm font-medium">
                Amount in rupees
                <input className="field num mt-1" inputMode="decimal" value={amount} onChange={(e) => setAmount(e.target.value)} placeholder="500.00" />
              </label>
              <button type="submit" className="btn btn-primary w-full" disabled={depositing || frozen}>
                {depositing ? 'Depositing…' : 'Deposit'}
              </button>
              {depositMsg && (
                <p role={depositMsg.ok ? 'status' : 'alert'} className={`text-sm font-medium ${depositMsg.ok ? 'text-credit' : 'text-debit'}`}>
                  {depositMsg.text}
                </p>
              )}
            </form>
          )}
        </Card>
      </div>

      <Card className="mt-4">
        <h2 className="mb-3 text-lg font-bold">Entries</h2>
        {first.loading ? (
          <Skeleton rows={5} className="h-9" />
        ) : first.error ? (
          <ErrorState error={first.error} onRetry={first.reload} />
        ) : entries.length === 0 ? (
          <EmptyState title="No entries yet" hint="Deposit money or make a transfer to see debits and credits here." />
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[32rem] text-left text-sm">
                <thead className="text-xs uppercase tracking-wide text-ink-soft">
                  <tr>
                    <th scope="col" className="py-2 pr-3 font-semibold">Seq</th>
                    <th scope="col" className="py-2 pr-3 font-semibold">Direction and amount</th>
                    <th scope="col" className="py-2 pr-3 font-semibold">Transfer</th>
                    <th scope="col" className="py-2 font-semibold">Time</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-deep/10">
                  {entries.map((e) => (
                    <tr key={e.id}>
                      <td className="num py-2.5 pr-3 font-semibold text-ink-soft">#{e.account_seq}</td>
                      <td className="py-2.5 pr-3">
                        <SignedAmount direction={e.direction} paise={e.amount_minor} />
                      </td>
                      <td className="num py-2.5 pr-3 text-xs text-ink-soft" title={e.transfer_id}>
                        {shortId(e.transfer_id)}
                      </td>
                      <td className="py-2.5 text-xs text-ink-soft">{formatTime(e.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {nextCursor && (
              <button type="button" className="btn btn-ghost mt-4" onClick={loadMore} disabled={loadingMore}>
                {loadingMore ? 'Loading…' : 'Load older entries'}
              </button>
            )}
          </>
        )}
      </Card>

      <ConfirmDialog
        open={confirm}
        title={frozen ? 'Unfreeze this account?' : 'Freeze this account?'}
        body={
          frozen
            ? 'Transfers to and from this account will work again.'
            : 'Every transfer to or from this account will be refused with 403 until it is unfrozen. Transfers already committed are not affected.'
        }
        confirmLabel={frozen ? 'Unfreeze' : 'Freeze'}
        danger={!frozen}
        busy={busy}
        onConfirm={toggleFreeze}
        onCancel={() => setConfirm(false)}
      />
    </>
  )
}
