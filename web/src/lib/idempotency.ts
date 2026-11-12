/** The API accepts 1-64 characters of A-Z a-z 0-9 _ - */
export const IDEMPOTENCY_KEY_PATTERN = /^[A-Za-z0-9_-]{1,64}$/

type RandomSource = Pick<Crypto, 'getRandomValues'> & Partial<Pick<Crypto, 'randomUUID'>>

/**
 * Returns a new random idempotency key (a version 4 UUID, 122 random bits).
 * One key identifies one intended payment: reuse it for every retry of that
 * payment, and generate a new one for the next payment.
 */
export function newIdempotencyKey(source: RandomSource = globalThis.crypto): string {
  if (typeof source.randomUUID === 'function') return source.randomUUID()
  // Fallback for contexts without randomUUID (for example plain http on an old browser).
  const bytes = source.getRandomValues(new Uint8Array(16))
  bytes[6] = (bytes[6] & 0x0f) | 0x40
  bytes[8] = (bytes[8] & 0x3f) | 0x80
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}
