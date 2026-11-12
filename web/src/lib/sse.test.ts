import { describe, expect, it } from 'vitest'
import { backoffDelay, ReconnectingStream, resumeUrl, type EventSourceLike, type StreamStatus } from './sse'

class FakeSource implements EventSourceLike {
  onopen: ((ev: unknown) => void) | null = null
  onerror: ((ev: unknown) => void) | null = null
  closed = false
  private listeners = new Map<string, (ev: { data: string; lastEventId: string }) => void>()
  constructor(readonly url: string) {}
  addEventListener(type: string, listener: (ev: { data: string; lastEventId: string }) => void) {
    this.listeners.set(type, listener)
  }
  close() {
    this.closed = true
  }
  emit(type: string, data: string, id: string) {
    this.listeners.get(type)?.({ data, lastEventId: id })
  }
}

function harness() {
  const sources: FakeSource[] = []
  const timers: { fn: () => void; ms: number; cancelled: boolean }[] = []
  const statuses: StreamStatus[] = []
  const events: [string, string][] = []
  const stream = new ReconnectingStream({
    url: '/v1/stream/events',
    eventName: 'notification',
    onEvent: (data, id) => events.push([data, id]),
    onStatus: (s) => statuses.push(s),
    createSource: (url) => {
      const s = new FakeSource(url)
      sources.push(s)
      return s
    },
    setTimer: (fn, ms) => {
      const t = { fn, ms, cancelled: false }
      timers.push(t)
      return t
    },
    clearTimer: (h) => {
      ;(h as { cancelled: boolean }).cancelled = true
    },
  })
  const latest = () => sources[sources.length - 1]
  const fire = () => {
    const t = timers[timers.length - 1]
    if (!t.cancelled) t.fn()
  }
  return { stream, sources, timers, statuses, events, latest, fire }
}

describe('backoffDelay', () => {
  it('doubles from the base and is capped', () => {
    expect([1, 2, 3, 4, 5, 6, 7].map((n) => backoffDelay(n))).toEqual([500, 1000, 2000, 4000, 8000, 10000, 10000])
    expect(backoffDelay(1000)).toBe(10000)
    expect(backoffDelay(3, 100, 250)).toBe(250)
  })
})

describe('resumeUrl', () => {
  it('adds last_event_id only when there is one', () => {
    expect(resumeUrl('/s', '')).toBe('/s')
    expect(resumeUrl('/s', '42')).toBe('/s?last_event_id=42')
    expect(resumeUrl('/s?x=1', '42')).toBe('/s?x=1&last_event_id=42')
  })
})

describe('ReconnectingStream', () => {
  it('connects, opens and delivers events', () => {
    const h = harness()
    h.stream.start()
    expect(h.sources).toHaveLength(1)
    expect(h.latest().url).toBe('/v1/stream/events')
    h.latest().onopen?.({})
    h.latest().emit('notification', '{"id":7}', '7')
    expect(h.statuses).toEqual(['connecting', 'open'])
    expect(h.events).toEqual([['{"id":7}', '7']])
  })

  it('reconnects after an error and resumes from the last event id', () => {
    const h = harness()
    h.stream.start()
    h.latest().onopen?.({})
    h.latest().emit('notification', 'a', '11')
    h.latest().emit('notification', 'b', '12')
    const first = h.latest()
    first.onerror?.({})
    expect(first.closed).toBe(true)
    expect(h.statuses.at(-1)).toBe('reconnecting')
    expect(h.timers).toHaveLength(1)
    expect(h.timers[0].ms).toBe(500)
    expect(h.sources).toHaveLength(1) // not yet: waits for the timer

    h.fire()
    expect(h.sources).toHaveLength(2)
    expect(h.latest().url).toBe('/v1/stream/events?last_event_id=12')
    h.latest().onopen?.({})
    expect(h.statuses.at(-1)).toBe('open')
  })

  it('backs off further on each failed attempt and resets after a success', () => {
    const h = harness()
    h.stream.start()
    for (let i = 0; i < 6; i++) {
      h.latest().onerror?.({})
      h.fire()
    }
    expect(h.timers.map((t) => t.ms)).toEqual([500, 1000, 2000, 4000, 8000, 10000])

    h.latest().onopen?.({}) // connected again
    h.latest().onerror?.({})
    expect(h.timers.at(-1)?.ms).toBe(500)
  })

  it('ignores events and errors from a connection it already replaced', () => {
    const h = harness()
    h.stream.start()
    const old = h.latest()
    old.onerror?.({})
    h.fire()
    old.emit('notification', 'stale', '99')
    old.onerror?.({})
    expect(h.events).toEqual([])
    expect(h.timers).toHaveLength(1)
    expect(h.latest().url).toBe('/v1/stream/events')
  })

  it('stop closes the connection and cancels a pending reconnect', () => {
    const h = harness()
    h.stream.start()
    h.latest().onerror?.({})
    h.stream.stop()
    expect(h.timers[0].cancelled).toBe(true)
    expect(h.statuses.at(-1)).toBe('closed')
    h.fire()
    expect(h.sources).toHaveLength(1) // no reconnect after stop

    const h2 = harness()
    h2.stream.start()
    const open = h2.latest()
    h2.stream.stop()
    expect(open.closed).toBe(true)
    open.onerror?.({})
    expect(h2.timers).toHaveLength(0)
  })

  it('can be started again after stop, keeping its place in the stream', () => {
    const h = harness()
    h.stream.start()
    h.latest().emit('notification', 'a', '5')
    h.stream.stop()
    h.stream.start()
    expect(h.latest().url).toBe('/v1/stream/events?last_event_id=5')
    h.stream.start() // a second start is a no-op
    expect(h.sources).toHaveLength(2)
  })
})
