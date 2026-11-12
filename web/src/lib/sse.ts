// A Server-Sent Events client that reconnects by itself.
//
// The browser's EventSource gives up for good after some failures (for
// example an HTTP error while the service restarts). This wrapper always
// reconnects, with exponential backoff, and passes the id of the last event
// it saw so the server resumes exactly where the stream broke.

export type StreamStatus = 'connecting' | 'open' | 'reconnecting' | 'closed'

/** The part of EventSource this module uses; tests supply a fake. */
export interface EventSourceLike {
  onopen: ((ev: unknown) => void) | null
  onerror: ((ev: unknown) => void) | null
  addEventListener(type: string, listener: (ev: { data: string; lastEventId: string }) => void): void
  close(): void
}

export interface StreamOptions {
  url: string
  /** Name of the SSE event to listen for. */
  eventName: string
  onEvent: (data: string, id: string) => void
  onStatus?: (status: StreamStatus, retryInMs?: number) => void
  createSource?: (url: string) => EventSourceLike
  setTimer?: (fn: () => void, ms: number) => unknown
  clearTimer?: (handle: unknown) => void
  baseDelayMs?: number
  maxDelayMs?: number
}

/** Delay before reconnect attempt n (1-based): base, 2x, 4x ... capped at max. */
export function backoffDelay(attempt: number, baseMs = 500, maxMs = 10_000): number {
  if (attempt <= 1) return Math.min(baseMs, maxMs)
  return Math.min(baseMs * 2 ** Math.min(attempt - 1, 30), maxMs)
}

/** Adds last_event_id to the URL so a new connection resumes after that event. */
export function resumeUrl(url: string, lastEventId: string): string {
  if (lastEventId === '') return url
  return `${url}${url.includes('?') ? '&' : '?'}last_event_id=${encodeURIComponent(lastEventId)}`
}

export class ReconnectingStream {
  private source: EventSourceLike | null = null
  private timer: unknown = null
  private attempts = 0
  private lastEventId = ''
  private stopped = true
  private readonly opts: Required<Omit<StreamOptions, 'onStatus'>> & Pick<StreamOptions, 'onStatus'>

  constructor(options: StreamOptions) {
    this.opts = {
      createSource: (url) => new EventSource(url) as unknown as EventSourceLike,
      setTimer: (fn, ms) => setTimeout(fn, ms),
      clearTimer: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
      baseDelayMs: 500,
      maxDelayMs: 10_000,
      ...options,
    }
  }

  start(): void {
    if (!this.stopped) return
    this.stopped = false
    this.attempts = 0
    this.connect('connecting')
  }

  /** Closes the connection and cancels any pending reconnect. */
  stop(): void {
    this.stopped = true
    if (this.timer !== null) {
      this.opts.clearTimer(this.timer)
      this.timer = null
    }
    this.source?.close()
    this.source = null
    this.opts.onStatus?.('closed')
  }

  private connect(status: StreamStatus): void {
    this.opts.onStatus?.(status)
    const source = this.opts.createSource(resumeUrl(this.opts.url, this.lastEventId))
    this.source = source
    source.onopen = () => {
      if (this.source !== source) return
      this.attempts = 0 // a successful connection resets the backoff
      this.opts.onStatus?.('open')
    }
    source.addEventListener(this.opts.eventName, (ev) => {
      if (this.source !== source) return
      if (ev.lastEventId) this.lastEventId = ev.lastEventId
      this.opts.onEvent(ev.data, ev.lastEventId)
    })
    source.onerror = () => {
      if (this.source !== source || this.stopped) return
      // Take over from the browser's own retry so there is one code path.
      source.close()
      this.source = null
      this.attempts += 1
      const delay = backoffDelay(this.attempts, this.opts.baseDelayMs, this.opts.maxDelayMs)
      this.opts.onStatus?.('reconnecting', delay)
      this.timer = this.opts.setTimer(() => {
        this.timer = null
        if (!this.stopped) this.connect('reconnecting')
      }, delay)
    }
  }
}
