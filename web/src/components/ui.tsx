import { useEffect, useRef, type ReactNode } from 'react'
import type { ApiError } from '../lib/api'
import { shortId, type Account } from '../lib/api'
import { formatPaise } from '../lib/money'

export function Card({ children, className = '', hover = false }: { children: ReactNode; className?: string; hover?: boolean }) {
  return <section className={`glass p-5 ${hover ? 'shimmer' : ''} ${className}`}>{children}</section>
}

export function PageTitle({ title, subtitle, action }: { title: string; subtitle?: string; action?: ReactNode }) {
  return (
    <header className="mb-6 flex flex-wrap items-end justify-between gap-3">
      <div>
        <h1 className="text-2xl font-bold tracking-tight text-ink sm:text-3xl">{title}</h1>
        {subtitle && <p className="mt-1 max-w-2xl text-sm text-ink-soft">{subtitle}</p>}
      </div>
      {action}
    </header>
  )
}

/** Loading placeholder: `rows` shimmering bars. */
export function Skeleton({ rows = 3, className = 'h-5' }: { rows?: number; className?: string }) {
  return (
    <div className="space-y-3" role="status" aria-label="Loading">
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} className={`skeleton ${className}`} style={{ width: `${100 - (i % 3) * 12}%` }} />
      ))}
    </div>
  )
}

export function EmptyState({ title, hint, action }: { title: string; hint?: string; action?: ReactNode }) {
  return (
    <div className="rounded-2xl border border-dashed border-deep/30 bg-white/40 px-6 py-10 text-center">
      <p className="font-semibold text-ink">{title}</p>
      {hint && <p className="mx-auto mt-1 max-w-md text-sm text-ink-soft">{hint}</p>}
      {action && <div className="mt-4">{action}</div>}
    </div>
  )
}

export function ErrorState({ error, onRetry }: { error: ApiError; onRetry?: () => void }) {
  return (
    <div role="alert" className="rounded-2xl border border-debit/30 bg-debit-soft/70 px-5 py-4">
      <p className="font-semibold text-debit">Something went wrong</p>
      <p className="mt-1 text-sm text-ink-soft">
        {error.message}
        {error.status > 0 && <span className="num"> (HTTP {error.status})</span>}
      </p>
      {onRetry && (
        <button type="button" className="btn btn-ghost mt-3" onClick={onRetry}>
          Try again
        </button>
      )}
    </div>
  )
}

/**
 * A debit or credit amount. Direction is always shown with a sign and a
 * word, never by colour alone.
 */
export function SignedAmount({ direction, paise, showLabel = true }: { direction: 'DEBIT' | 'CREDIT'; paise: number; showLabel?: boolean }) {
  const debit = direction === 'DEBIT'
  return (
    <span className={`num inline-flex items-center gap-2 font-semibold ${debit ? 'text-debit' : 'text-credit'}`}>
      <span>
        <span aria-hidden="true">{debit ? '−' : '+'}</span>
        <span className="sr-only">{debit ? 'minus ' : 'plus '}</span>
        {formatPaise(paise)}
      </span>
      {showLabel && <DirectionBadge direction={direction} />}
    </span>
  )
}

export function DirectionBadge({ direction }: { direction: 'DEBIT' | 'CREDIT' }) {
  const debit = direction === 'DEBIT'
  return (
    <span className={`rounded-full px-2 py-0.5 text-[0.7rem] font-bold tracking-wide ${debit ? 'bg-debit-soft text-debit' : 'bg-credit-soft text-credit'}`}>
      {debit ? '− DEBIT' : '+ CREDIT'}
    </span>
  )
}

export function Badge({ tone, children }: { tone: 'ok' | 'warn' | 'bad' | 'info'; children: ReactNode }) {
  const tones = {
    ok: 'bg-credit-soft text-credit',
    warn: 'bg-amber-100 text-amber-800',
    bad: 'bg-debit-soft text-debit',
    info: 'bg-sky-100 text-deep',
  }
  return <span className={`inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-bold ${tones[tone]}`}>{children}</span>
}

export function AccountName({ id, accounts }: { id: string; accounts: Map<string, Account> }) {
  const a = accounts.get(id)
  return (
    <span title={id}>
      {a ? <span className="font-medium">{a.name}</span> : null}{' '}
      <span className="num text-xs text-ink-soft">{shortId(id)}</span>
    </span>
  )
}

/** A modal confirmation built on <dialog>: focus is trapped and Esc cancels. */
export function ConfirmDialog(props: {
  open: boolean
  title: string
  body: ReactNode
  confirmLabel: string
  danger?: boolean
  busy?: boolean
  onConfirm: () => void
  onCancel: () => void
}) {
  const ref = useRef<HTMLDialogElement>(null)
  useEffect(() => {
    const d = ref.current
    if (!d) return
    if (props.open && !d.open) d.showModal()
    if (!props.open && d.open) d.close()
  }, [props.open])
  return (
    <dialog
      ref={ref}
      onCancel={(e) => {
        e.preventDefault()
        if (!props.busy) props.onCancel()
      }}
      className="glass m-auto w-[min(92vw,26rem)] p-6 text-ink"
      aria-labelledby="confirm-title"
    >
      <h2 id="confirm-title" className="text-lg font-bold">
        {props.title}
      </h2>
      <div className="mt-2 text-sm text-ink-soft">{props.body}</div>
      <div className="mt-5 flex justify-end gap-2">
        <button type="button" className="btn btn-ghost" onClick={props.onCancel} disabled={props.busy}>
          Cancel
        </button>
        <button type="button" className={`btn ${props.danger ? 'btn-danger' : 'btn-primary'}`} onClick={props.onConfirm} disabled={props.busy}>
          {props.busy ? 'Working…' : props.confirmLabel}
        </button>
      </div>
    </dialog>
  )
}
