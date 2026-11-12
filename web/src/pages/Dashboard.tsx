import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { AccountName, Badge, Card, EmptyState, ErrorState, PageTitle, SignedAmount, Skeleton } from '../components/ui'
import { api, formatTime, type Notification } from '../lib/api'
import { useAccountNames, useAsync } from '../lib/hooks'
import { formatPaise } from '../lib/money'
import { ReconnectingStream, type StreamStatus } from '../lib/sse'

const FEED_LIMIT = 40

function useLiveFeed() {
  const [items, setItems] = useState<Notification[]>([])
  const [status, setStatus] = useState<StreamStatus>('connecting')
  useEffect(() => {
    const stream = new ReconnectingStream({
      url: '/v1/stream/events?recent=20', // start with the 20 latest, then live
      eventName: 'notification',
      onStatus: setStatus,
      onEvent: (data) => {
        try {
          const n = JSON.parse(data) as Notification
          setItems((prev) => (prev.some((p) => p.id === n.id) ? prev : [n, ...prev].slice(0, FEED_LIMIT)))
        } catch {
          /* ignore a malformed frame */
        }
      },
    })
    stream.start()
    return () => stream.stop()
  }, [])
  return { items, status }
}

function StreamIndicator({ status }: { status: StreamStatus }) {
  if (status === 'open')
    return (
      <Badge tone="ok">
        <span className="h-2 w-2 animate-pulse rounded-full bg-credit" aria-hidden="true" /> Live
      </Badge>
    )
  if (status === 'connecting') return <Badge tone="info">Connecting…</Badge>
  return <Badge tone="warn">Reconnecting…</Badge>
}

function Stat({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <Card hover>
      <p className="text-xs font-semibold uppercase tracking-wide text-ink-soft">{label}</p>
      <p className="num mt-2 text-2xl font-bold text-ink sm:text-3xl">{value}</p>
      {hint && <p className="mt-1 text-xs text-ink-soft">{hint}</p>}
    </Card>
  )
}

export default function Dashboard() {
  const stats = useAsync(api.stats, 'stats', 4000)
  const reconcile = useAsync(api.reconcile, 'reconcile', 15000)
  const system = useAsync(api.system, 'system', 5000)
  const recent = useAsync(() => api.transfers(null, 30), 'recent', 6000)
  const feed = useLiveFeed()
  const accounts = useAccountNames(String(stats.data?.accounts ?? 0))

  const chart = [...(recent.data?.items ?? [])].reverse().map((t, i) => ({ n: i + 1, rupees: t.amount_minor / 100, type: t.type }))

  return (
    <>
      <PageTitle title="Dashboard" subtitle="Every transfer is one debit and one credit. The numbers below update on their own." />

      {stats.error && !stats.data ? (
        <ErrorState error={stats.error} onRetry={stats.reload} />
      ) : (
        <div className="grid gap-4 sm:grid-cols-3">
          {stats.data ? (
            <>
              <Stat label="Accounts" value={stats.data.accounts.toLocaleString('en-IN')} />
              <Stat
                label="Transfers today"
                value={stats.data.transfers_today.toLocaleString('en-IN')}
                hint={`${stats.data.transfers_total.toLocaleString('en-IN')} in total`}
              />
              <Stat label="Volume today" value={formatPaise(stats.data.volume_today_minor)} hint="All transfer types, UTC day" />
            </>
          ) : (
            [0, 1, 2].map((i) => (
              <Card key={i}>
                <Skeleton rows={2} className="h-7" />
              </Card>
            ))
          )}
        </div>
      )}

      <div className="mt-4 grid gap-4 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <div className="mb-3 flex items-center justify-between gap-3">
            <h2 className="text-lg font-bold">Live transfer feed</h2>
            <StreamIndicator status={feed.status} />
          </div>
          {feed.items.length === 0 ? (
            <EmptyState
              title="Waiting for transfers"
              hint="New debits and credits appear here the moment they are processed."
              action={
                <Link to="/transfer" className="btn btn-primary">
                  Make a transfer
                </Link>
              }
            />
          ) : (
            <ul className="max-h-[26rem] divide-y divide-deep/10 overflow-y-auto" aria-live="polite">
              {feed.items.map((n) => (
                <li key={n.id} className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1 py-2.5 text-sm">
                  <span className="min-w-0">
                    <Link to={`/accounts/${n.account_id}`} className="text-deep underline-offset-2 hover:underline">
                      <AccountName id={n.account_id} accounts={accounts} />
                    </Link>
                    <span className="ml-2 text-xs text-ink-soft">
                      {n.transfer_type.toLowerCase()} · {formatTime(n.occurred_at)}
                    </span>
                  </span>
                  <SignedAmount direction={n.kind === 'DEBITED' ? 'DEBIT' : 'CREDIT'} paise={n.amount_minor} />
                </li>
              ))}
            </ul>
          )}
        </Card>

        <div className="space-y-4">
          <Card>
            <h2 className="text-lg font-bold">Reconciliation</h2>
            {reconcile.error && !reconcile.data ? (
              <div className="mt-3">
                <ErrorState error={reconcile.error} onRetry={reconcile.reload} />
              </div>
            ) : !reconcile.data ? (
              <div className="mt-3">
                <Skeleton rows={3} />
              </div>
            ) : (
              <>
                <p className="mt-2">
                  {reconcile.data.ok ? <Badge tone="ok">✓ Ledger balanced</Badge> : <Badge tone="bad">! Mismatch found</Badge>}
                </p>
                <ul className="mt-3 space-y-1.5 text-sm">
                  {reconcile.data.checks.map((c) => (
                    <li key={c.name} className="flex gap-2">
                      <span className={c.ok ? 'text-credit' : 'text-debit'} aria-hidden="true">
                        {c.ok ? '✓' : '✗'}
                      </span>
                      <span>
                        <span className="sr-only">{c.ok ? 'Passed: ' : 'Failed: '}</span>
                        <span className="num text-ink-soft">{c.detail}</span>
                      </span>
                    </li>
                  ))}
                </ul>
                <p className="mt-2 text-xs text-ink-soft">Checked {formatTime(reconcile.data.checked_at)}</p>
              </>
            )}
          </Card>

          <Card>
            <h2 className="text-lg font-bold">Pipeline</h2>
            <dl className="mt-3 space-y-2 text-sm">
              <div className="flex items-center justify-between gap-3">
                <dt className="text-ink-soft">Outbox backlog</dt>
                <dd className="num font-semibold">{stats.data ? `${stats.data.outbox_backlog} event(s)` : '…'}</dd>
              </div>
              <div className="flex items-center justify-between gap-3">
                <dt className="text-ink-soft">Balance view lag</dt>
                <dd className="num font-semibold">
                  {system.data ? `${system.data.balance_view.entries_behind} entries` : system.error ? 'unavailable' : '…'}
                </dd>
              </div>
            </dl>
            <Link to="/system" className="mt-3 inline-block text-sm font-semibold text-deep underline-offset-2 hover:underline">
              System details →
            </Link>
          </Card>
        </div>
      </div>

      <Card className="mt-4">
        <h2 className="text-lg font-bold">Last 30 transfers</h2>
        <p className="text-sm text-ink-soft">Amount of each transfer in rupees, oldest on the left.</p>
        {recent.error && !recent.data ? (
          <div className="mt-3">
            <ErrorState error={recent.error} onRetry={recent.reload} />
          </div>
        ) : !recent.data ? (
          <div className="mt-3">
            <Skeleton rows={4} className="h-8" />
          </div>
        ) : chart.length === 0 ? (
          <div className="mt-3">
            <EmptyState title="No transfers yet" hint="Seed demo data or make a transfer to see the chart." />
          </div>
        ) : (
          <div className="mt-3 h-56" role="img" aria-label={`Bar chart of the last ${chart.length} transfer amounts`}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,116,144,0.15)" vertical={false} />
                <XAxis dataKey="n" tick={{ fontSize: 11, fill: '#334155' }} tickLine={false} axisLine={false} />
                <YAxis tick={{ fontSize: 11, fill: '#334155' }} tickLine={false} axisLine={false} width={56} />
                <Tooltip
                  cursor={{ fill: 'rgba(125,211,252,0.2)' }}
                  formatter={(v) => [formatPaise(Math.round(Number(v) * 100)), 'Amount']}
                  labelFormatter={(n) => `Transfer ${n}`}
                />
                <Bar dataKey="rupees" fill="#06B6D4" radius={[6, 6, 0, 0]} isAnimationActive={false} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </Card>
    </>
  )
}
