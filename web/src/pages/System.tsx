import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { Badge, Card, EmptyState, ErrorState, PageTitle, Skeleton } from '../components/ui'
import { api, formatTime, type LoadTestResult } from '../lib/api'
import { useAsync } from '../lib/hooks'

const SCENARIOS = ['uniform', 'hot_account', 'retry_storm'] as const

function Metric({ label, value, tone, hint }: { label: string; value: string; tone: 'ok' | 'warn' | 'bad'; hint: string }) {
  return (
    <Card hover>
      <p className="text-xs font-semibold uppercase tracking-wide text-ink-soft">{label}</p>
      <p className="num mt-2 text-3xl font-bold">{value}</p>
      <p className="mt-2 flex flex-wrap items-center gap-2 text-xs text-ink-soft">
        <Badge tone={tone}>{tone === 'ok' ? '✓ Healthy' : tone === 'warn' ? '! Behind' : '✗ Needs attention'}</Badge>
        {hint}
      </p>
    </Card>
  )
}

export default function System() {
  const stats = useAsync(api.stats, 'sys-stats', 3000)
  const system = useAsync(api.system, 'sys-views', 3000)
  const results = useAsync(async () => Promise.all(SCENARIOS.map((s) => api.loadTest(s))), 'sys-results')

  const measured = (results.data ?? []).filter((r): r is LoadTestResult => r !== null)
  const chart = measured.map((r) => ({ scenario: r.scenario, p50: r.latency_ms.p50, p95: r.latency_ms.p95, p99: r.latency_ms.p99 }))

  return (
    <>
      <PageTitle title="System" subtitle="How far each stage of the event pipeline is behind the ledger." />

      <div className="grid gap-4 md:grid-cols-3">
        {stats.error && !stats.data ? (
          <ErrorState error={stats.error} onRetry={stats.reload} />
        ) : !stats.data ? (
          <Card>
            <Skeleton rows={3} className="h-7" />
          </Card>
        ) : (
          <Metric
            label="Outbox backlog"
            value={stats.data.outbox_backlog.toLocaleString('en-IN')}
            tone={stats.data.outbox_backlog === 0 ? 'ok' : stats.data.oldest_unpublished_seconds > 30 ? 'bad' : 'warn'}
            hint={
              stats.data.outbox_backlog === 0
                ? 'Every event has been published.'
                : `Oldest unpublished event is ${stats.data.oldest_unpublished_seconds}s old. Is the relay running?`
            }
          />
        )}

        {system.error && !system.data ? (
          <div className="md:col-span-2">
            <ErrorState error={system.error} onRetry={system.reload} />
          </div>
        ) : !system.data ? (
          <>
            <Card>
              <Skeleton rows={3} className="h-7" />
            </Card>
            <Card>
              <Skeleton rows={3} className="h-7" />
            </Card>
          </>
        ) : (
          <>
            <Metric
              label="Balance view lag"
              value={`${system.data.balance_view.entries_behind.toLocaleString('en-IN')} entries`}
              tone={system.data.balance_view.entries_behind === 0 ? 'ok' : 'warn'}
              hint={
                system.data.balance_view.entries_behind === 0
                  ? 'The read model equals the ledger.'
                  : `${system.data.balance_view.accounts_behind} account(s) are behind the ledger.`
              }
            />
            <Metric
              label="Dead-letter queue"
              value={system.data.dlq.total.toLocaleString('en-IN')}
              tone={system.data.dlq.total === 0 ? 'ok' : 'bad'}
              hint={Object.entries(system.data.dlq.topics)
                .map(([topic, n]) => `${topic}: ${n}`)
                .join(' · ')}
            />
          </>
        )}
      </div>

      <Card className="mt-4">
        <h2 className="text-lg font-bold">Latest load-test results</h2>
        <p className="text-sm text-ink-soft">
          Read from the <code className="num rounded bg-white/70 px-1">results/</code> folder. Only numbers a script actually measured are shown.
        </p>
        {results.loading ? (
          <div className="mt-3">
            <Skeleton rows={4} className="h-8" />
          </div>
        ) : measured.length === 0 ? (
          <div className="mt-3">
            <EmptyState title="Not yet measured" hint="Run the load test (scripts/dev loadtest) to produce results/loadtest-<scenario>.json." />
          </div>
        ) : (
          <>
            <div className="mt-3 overflow-x-auto">
              <table className="w-full min-w-[40rem] text-left text-sm">
                <thead className="text-xs uppercase tracking-wide text-ink-soft">
                  <tr>
                    <th scope="col" className="py-2 pr-3 font-semibold">Scenario</th>
                    <th scope="col" className="py-2 pr-3 text-right font-semibold">Transfers/s</th>
                    <th scope="col" className="py-2 pr-3 text-right font-semibold">p50</th>
                    <th scope="col" className="py-2 pr-3 text-right font-semibold">p95</th>
                    <th scope="col" className="py-2 pr-3 text-right font-semibold">p99</th>
                    <th scope="col" className="py-2 pr-3 text-right font-semibold">Errors</th>
                    <th scope="col" className="py-2 pr-3 text-right font-semibold">Mismatches</th>
                    <th scope="col" className="py-2 font-semibold">Ran</th>
                  </tr>
                </thead>
                <tbody className="num divide-y divide-deep/10">
                  {SCENARIOS.map((name, i) => {
                    const r = results.data?.[i]
                    return r ? (
                      <tr key={name}>
                        <th scope="row" className="py-2.5 pr-3 font-semibold">{name}</th>
                        <td className="py-2.5 pr-3 text-right">{r.throughput_per_second.toFixed(1)}</td>
                        <td className="py-2.5 pr-3 text-right">{r.latency_ms.p50.toFixed(1)} ms</td>
                        <td className="py-2.5 pr-3 text-right">{r.latency_ms.p95.toFixed(1)} ms</td>
                        <td className="py-2.5 pr-3 text-right">{r.latency_ms.p99.toFixed(1)} ms</td>
                        <td className="py-2.5 pr-3 text-right">{(r.error_rate * 100).toFixed(2)}%</td>
                        <td className="py-2.5 pr-3 text-right">
                          {r.mismatches === 0 ? <Badge tone="ok">✓ 0</Badge> : <Badge tone="bad">✗ {r.mismatches}</Badge>}
                        </td>
                        <td className="py-2.5 text-xs text-ink-soft">{formatTime(r.ran_at)}</td>
                      </tr>
                    ) : (
                      <tr key={name}>
                        <th scope="row" className="py-2.5 pr-3 font-semibold">{name}</th>
                        <td colSpan={7} className="py-2.5 text-ink-soft">
                          not yet measured
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
            <div className="mt-4 h-60" role="img" aria-label="Bar chart of p50, p95 and p99 latency per scenario">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,116,144,0.15)" vertical={false} />
                  <XAxis dataKey="scenario" tick={{ fontSize: 12, fill: '#334155' }} tickLine={false} axisLine={false} />
                  <YAxis tick={{ fontSize: 11, fill: '#334155' }} tickLine={false} axisLine={false} width={48} unit=" ms" />
                  <Tooltip cursor={{ fill: 'rgba(125,211,252,0.2)' }} formatter={(v, name) => [`${Number(v).toFixed(1)} ms`, String(name)]} />
                  <Legend />
                  <Bar dataKey="p50" fill="#7DD3FC" radius={[6, 6, 0, 0]} isAnimationActive={false} />
                  <Bar dataKey="p95" fill="#06B6D4" radius={[6, 6, 0, 0]} isAnimationActive={false} />
                  <Bar dataKey="p99" fill="#0E7490" radius={[6, 6, 0, 0]} isAnimationActive={false} />
                </BarChart>
              </ResponsiveContainer>
            </div>
            {measured[0] && (
              <p className="mt-3 text-xs text-ink-soft">
                Hardware:{' '}
                {Object.entries(measured[0].hardware)
                  .map(([k, v]) => `${k.replace(/_/g, ' ')}: ${v}`)
                  .join(' · ')}
              </p>
            )}
          </>
        )}
      </Card>
    </>
  )
}
