import { useState, type FormEvent } from 'react'
import { Link } from 'react-router-dom'
import { Badge, Card, EmptyState, ErrorState, PageTitle, Skeleton } from '../components/ui'
import { api, ApiError, shortId, type Account } from '../lib/api'
import { useAsync } from '../lib/hooks'
import { formatPaise } from '../lib/money'

export function StatusBadge({ account }: { account: Account }) {
  if (account.kind === 'SYSTEM') return <Badge tone="info">SYSTEM</Badge>
  if (account.status === 'ACTIVE') return <Badge tone="ok">● ACTIVE</Badge>
  if (account.status === 'FROZEN') return <Badge tone="warn">❄ FROZEN</Badge>
  return <Badge tone="bad">CLOSED</Badge>
}

export default function Accounts() {
  const [version, setVersion] = useState(0)
  const first = useAsync(() => api.accounts(null, 50), `accounts:${version}`)
  const [more, setMore] = useState<Account[]>([])
  const [cursor, setCursor] = useState<string | null | undefined>(undefined)
  const [loadingMore, setLoadingMore] = useState(false)

  const [name, setName] = useState('')
  const [currency, setCurrency] = useState('INR')
  const [busy, setBusy] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)

  const items = [...(first.data?.items ?? []), ...more]
  const nextCursor = cursor === undefined ? first.data?.next_cursor : cursor

  async function create(e: FormEvent) {
    e.preventDefault()
    if (busy) return
    setBusy(true)
    setFormError(null)
    try {
      await api.createAccount(name, currency)
      setName('')
      setMore([])
      setCursor(undefined)
      setVersion((v) => v + 1)
    } catch (err) {
      setFormError(err instanceof ApiError ? err.message : 'Could not create the account.')
    } finally {
      setBusy(false)
    }
  }

  async function loadMore() {
    if (!nextCursor || loadingMore) return
    setLoadingMore(true)
    try {
      const page = await api.accounts(nextCursor, 50)
      setMore((m) => [...m, ...page.items])
      setCursor(page.next_cursor)
    } finally {
      setLoadingMore(false)
    }
  }

  return (
    <>
      <PageTitle title="Accounts" subtitle="Balances come from the ledger, the source of truth." />

      <Card className="mb-4">
        <h2 className="text-lg font-bold">Create an account</h2>
        <form onSubmit={create} className="mt-3 grid gap-3 sm:grid-cols-[1fr_8rem_auto] sm:items-end">
          <label className="block text-sm font-medium">
            Name
            <input className="field mt-1" value={name} onChange={(e) => setName(e.target.value)} maxLength={100} required placeholder="e.g. Asha" />
          </label>
          <label className="block text-sm font-medium">
            Currency
            <input
              className="field mt-1 uppercase"
              value={currency}
              onChange={(e) => setCurrency(e.target.value.toUpperCase())}
              pattern="[A-Z]{3}"
              maxLength={3}
              required
              title="Three letters, for example INR"
            />
          </label>
          <button type="submit" className="btn btn-primary" disabled={busy}>
            {busy ? 'Creating…' : 'Create account'}
          </button>
        </form>
        {formError && (
          <p role="alert" className="mt-2 text-sm font-medium text-debit">
            {formError}
          </p>
        )}
      </Card>

      <Card>
        <h2 className="mb-3 text-lg font-bold">All accounts</h2>
        {first.loading ? (
          <Skeleton rows={6} className="h-9" />
        ) : first.error ? (
          <ErrorState error={first.error} onRetry={first.reload} />
        ) : items.length === 0 ? (
          <EmptyState title="No accounts yet" hint="Create one above, or run the seed command to add 1,000 demo accounts." />
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[30rem] text-left text-sm">
                <thead className="text-xs uppercase tracking-wide text-ink-soft">
                  <tr>
                    <th scope="col" className="py-2 pr-3 font-semibold">Account</th>
                    <th scope="col" className="py-2 pr-3 font-semibold">Status</th>
                    <th scope="col" className="py-2 pr-3 text-right font-semibold">Entries</th>
                    <th scope="col" className="py-2 text-right font-semibold">Balance</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-deep/10">
                  {items.map((a) => (
                    <tr key={a.id} className="transition-colors duration-150 hover:bg-white/50">
                      <td className="py-2.5 pr-3">
                        <Link to={`/accounts/${a.id}`} className="font-semibold text-deep underline-offset-2 hover:underline">
                          {a.name}
                        </Link>
                        <span className="num ml-2 text-xs text-ink-soft">{shortId(a.id)}</span>
                      </td>
                      <td className="py-2.5 pr-3">
                        <StatusBadge account={a} />
                      </td>
                      <td className="num py-2.5 pr-3 text-right text-ink-soft">{a.last_seq}</td>
                      <td className="num py-2.5 text-right font-semibold">
                        {formatPaise(a.balance_minor)} <span className="text-xs font-normal text-ink-soft">{a.currency}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {nextCursor && (
              <button type="button" className="btn btn-ghost mt-4" onClick={loadMore} disabled={loadingMore}>
                {loadingMore ? 'Loading…' : 'Load more'}
              </button>
            )}
          </>
        )}
      </Card>
    </>
  )
}
