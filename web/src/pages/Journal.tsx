import { useState } from 'react'
import { Link } from 'react-router-dom'
import { AccountName, Badge, Card, ConfirmDialog, EmptyState, ErrorState, PageTitle, Skeleton } from '../components/ui'
import { api, ApiError, formatTime, shortId, type Account, type Transfer } from '../lib/api'
import { useAccountNames, useAsync } from '../lib/hooks'
import { newIdempotencyKey } from '../lib/idempotency'
import { formatPaise } from '../lib/money'

/** One transfer drawn as a T-account: the debit on the left, the credit on the right. */
function TAccount({ t, accounts, onReverse }: { t: Transfer; accounts: Map<string, Account>; onReverse: (t: Transfer) => void }) {
  const debit = t.entries.find((e) => e.direction === 'DEBIT')
  const credit = t.entries.find((e) => e.direction === 'CREDIT')
  const canReverse = t.type !== 'REVERSAL' && !t.reversed_by_transfer_id
  return (
    <Card hover>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-2 text-sm">
          <Badge tone={t.type === 'REVERSAL' ? 'warn' : 'info'}>{t.type}</Badge>
          <span className="num text-xs text-ink-soft" title={t.id}>
            {shortId(t.id)}
          </span>
          <span className="text-xs text-ink-soft">{formatTime(t.created_at)}</span>
          {t.reversed_by_transfer_id && <Badge tone="warn">↩ Reversed</Badge>}
          {t.reverses_transfer_id && (
            <span className="text-xs text-ink-soft">
              reverses <span className="num">{shortId(t.reverses_transfer_id)}</span>
            </span>
          )}
        </div>
        {canReverse && (
          <button type="button" className="btn btn-ghost" onClick={() => onReverse(t)}>
            ↩ Reverse
          </button>
        )}
      </div>
      {t.memo && <p className="mt-2 text-sm text-ink-soft">“{t.memo}”</p>}

      <div className="mt-3 grid grid-cols-2 overflow-hidden rounded-xl border border-deep/15 text-sm">
        <div className="border-b border-r border-deep/15 bg-debit-soft/60 px-3 py-1.5 text-xs font-bold tracking-wide text-debit">− DEBIT</div>
        <div className="border-b border-deep/15 bg-credit-soft/60 px-3 py-1.5 text-xs font-bold tracking-wide text-credit">+ CREDIT</div>
        <div className="border-r border-deep/15 bg-white/50 px-3 py-3">
          {debit && (
            <>
              <Link to={`/accounts/${debit.account_id}`} className="text-deep underline-offset-2 hover:underline">
                <AccountName id={debit.account_id} accounts={accounts} />
              </Link>
              <p className="num mt-1 text-base font-bold text-debit">
                <span aria-hidden="true">−</span>
                <span className="sr-only">minus </span>
                {formatPaise(debit.amount_minor)}
              </p>
              <p className="num text-xs text-ink-soft">seq #{debit.account_seq}</p>
            </>
          )}
        </div>
        <div className="bg-white/50 px-3 py-3">
          {credit && (
            <>
              <Link to={`/accounts/${credit.account_id}`} className="text-deep underline-offset-2 hover:underline">
                <AccountName id={credit.account_id} accounts={accounts} />
              </Link>
              <p className="num mt-1 text-base font-bold text-credit">
                <span aria-hidden="true">+</span>
                <span className="sr-only">plus </span>
                {formatPaise(credit.amount_minor)}
              </p>
              <p className="num text-xs text-ink-soft">seq #{credit.account_seq}</p>
            </>
          )}
        </div>
      </div>
      <p className="mt-2 text-xs text-ink-soft">
        {debit && credit && debit.amount_minor === credit.amount_minor ? '✓ Debit equals credit' : '! Entries are not balanced'}
      </p>
    </Card>
  )
}

export default function Journal() {
  const [version, setVersion] = useState(0)
  const first = useAsync(() => api.transfers(null, 20), `journal:${version}`)
  const [more, setMore] = useState<Transfer[]>([])
  const [cursor, setCursor] = useState<string | null | undefined>(undefined)
  const [loadingMore, setLoadingMore] = useState(false)
  const accounts = useAccountNames(String(version))

  const [target, setTarget] = useState<Transfer | null>(null)
  const [reverseKey, setReverseKey] = useState('')
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState<{ ok: boolean; text: string } | null>(null)

  const items = [...(first.data?.items ?? []), ...more]
  const nextCursor = cursor === undefined ? first.data?.next_cursor : cursor

  function askReverse(t: Transfer) {
    setTarget(t)
    setReverseKey(newIdempotencyKey()) // one key per intended reversal; retries reuse it
    setMessage(null)
  }

  async function reverse() {
    if (!target) return
    setBusy(true)
    try {
      await api.reverse(reverseKey, target.id)
      setMessage({ ok: true, text: `Transfer ${shortId(target.id)} was reversed with a new compensating transfer.` })
      setTarget(null)
      setMore([])
      setCursor(undefined)
      setVersion((v) => v + 1)
    } catch (err) {
      setMessage({ ok: false, text: err instanceof ApiError ? `Could not reverse: ${err.message}` : 'Could not reverse the transfer.' })
      setTarget(null)
    } finally {
      setBusy(false)
    }
  }

  async function loadMore() {
    if (!nextCursor || loadingMore) return
    setLoadingMore(true)
    try {
      const page = await api.transfers(nextCursor, 20)
      setMore((m) => [...m, ...page.items])
      setCursor(page.next_cursor)
    } finally {
      setLoadingMore(false)
    }
  }

  return (
    <>
      <PageTitle
        title="Journal"
        subtitle="Every transfer as a balanced pair: what one account is debited, another is credited. Nothing is ever edited; a mistake is fixed by a reversal."
        action={
          <button type="button" className="btn btn-ghost" onClick={() => setVersion((v) => v + 1)}>
            Refresh
          </button>
        }
      />
      {message && (
        <p role={message.ok ? 'status' : 'alert'} className={`mb-4 rounded-xl p-3 text-sm font-medium ${message.ok ? 'bg-credit-soft/70 text-credit' : 'bg-debit-soft/70 text-debit'}`}>
          {message.text}
        </p>
      )}

      {first.loading ? (
        <div className="grid gap-4 md:grid-cols-2">
          {[0, 1, 2, 3].map((i) => (
            <Card key={i}>
              <Skeleton rows={4} className="h-7" />
            </Card>
          ))}
        </div>
      ) : first.error ? (
        <ErrorState error={first.error} onRetry={first.reload} />
      ) : items.length === 0 ? (
        <EmptyState
          title="The journal is empty"
          hint="Transfers, deposits and reversals appear here."
          action={
            <Link to="/transfer" className="btn btn-primary">
              Make a transfer
            </Link>
          }
        />
      ) : (
        <>
          <div className="grid gap-4 md:grid-cols-2">
            {items.map((t) => (
              <TAccount key={t.id} t={t} accounts={accounts} onReverse={askReverse} />
            ))}
          </div>
          {nextCursor && (
            <button type="button" className="btn btn-ghost mt-4" onClick={loadMore} disabled={loadingMore}>
              {loadingMore ? 'Loading…' : 'Load older transfers'}
            </button>
          )}
        </>
      )}

      <ConfirmDialog
        open={target !== null}
        title="Reverse this transfer?"
        body={
          target && (
            <>
              A new transfer of <strong className="num">{formatPaise(target.amount_minor)}</strong> will move the money back. The original stays in the
              journal. A transfer can be reversed only once, and the reversal fails if the receiver no longer has the money.
            </>
          )
        }
        confirmLabel="Reverse transfer"
        danger
        busy={busy}
        onConfirm={reverse}
        onCancel={() => setTarget(null)}
      />
    </>
  )
}
