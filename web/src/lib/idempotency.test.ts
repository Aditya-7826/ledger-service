import { webcrypto } from 'node:crypto'
import { describe, expect, it } from 'vitest'
import { IDEMPOTENCY_KEY_PATTERN, newIdempotencyKey } from './idempotency'

const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

describe('newIdempotencyKey', () => {
  it('produces keys the API accepts', () => {
    for (let i = 0; i < 100; i++) {
      const key = newIdempotencyKey()
      expect(key).toMatch(IDEMPOTENCY_KEY_PATTERN)
      expect(key.length).toBeLessThanOrEqual(64)
      expect(key).toMatch(UUID_V4)
    }
  })

  it('never repeats', () => {
    const keys = new Set(Array.from({ length: 5000 }, () => newIdempotencyKey()))
    expect(keys.size).toBe(5000)
  })

  it('falls back to getRandomValues when randomUUID is missing', () => {
    const source = { getRandomValues: <T extends ArrayBufferView | null>(a: T) => webcrypto.getRandomValues(a as never) as T }
    const keys = new Set<string>()
    for (let i = 0; i < 200; i++) {
      const key = newIdempotencyKey(source)
      expect(key).toMatch(UUID_V4)
      expect(key).toMatch(IDEMPOTENCY_KEY_PATTERN)
      keys.add(key)
    }
    expect(keys.size).toBe(200)
  })
})

describe('IDEMPOTENCY_KEY_PATTERN', () => {
  it('matches the API rule', () => {
    expect('a'.repeat(64)).toMatch(IDEMPOTENCY_KEY_PATTERN)
    expect('order_42-retry').toMatch(IDEMPOTENCY_KEY_PATTERN)
    for (const bad of ['', 'a'.repeat(65), 'has space', 'semi;colon', 'ünïcode']) {
      expect(bad).not.toMatch(IDEMPOTENCY_KEY_PATTERN)
    }
  })
})
