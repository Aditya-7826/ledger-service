import { useCallback, useEffect, useRef, useState } from 'react'
import { api, ApiError, type Account } from './api'

export interface AsyncState<T> {
  data: T | null
  error: ApiError | null
  loading: boolean
  reload: () => void
}

/**
 * Loads data when the component mounts and whenever `key` changes. With
 * pollMs it reloads on an interval, keeping the previous data on screen
 * while a refresh is in flight.
 */
export function useAsync<T>(load: () => Promise<T>, key: string, pollMs?: number): AsyncState<T> {
  const [state, setState] = useState<{ data: T | null; error: ApiError | null; loading: boolean }>({
    data: null,
    error: null,
    loading: true,
  })
  const loadRef = useRef(load)
  loadRef.current = load
  const [tick, setTick] = useState(0)
  const reload = useCallback(() => setTick((t) => t + 1), [])

  useEffect(() => {
    let cancelled = false
    const run = (first: boolean) => {
      if (first) setState((s) => ({ ...s, loading: s.data === null }))
      loadRef.current().then(
        (data) => !cancelled && setState({ data, error: null, loading: false }),
        (err: unknown) => {
          if (cancelled) return
          const error = err instanceof ApiError ? err : new ApiError(0, 'unknown', String(err))
          // Keep showing the last good data during a failed background refresh.
          setState((s) => ({ data: first ? null : s.data, error, loading: false }))
        },
      )
    }
    run(true)
    const timer = pollMs ? setInterval(() => run(false), pollMs) : undefined
    return () => {
      cancelled = true
      if (timer) clearInterval(timer)
    }
  }, [key, tick, pollMs])

  return { ...state, reload }
}

/** Maps account IDs to names for display (first 100 accounts). */
export function useAccountNames(refreshKey = ''): Map<string, Account> {
  const { data } = useAsync(() => api.accounts(null, 100), `names:${refreshKey}`)
  const map = new Map<string, Account>()
  for (const a of data?.items ?? []) map.set(a.id, a)
  return map
}
