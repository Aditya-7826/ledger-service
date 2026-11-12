// Money is always an integer number of paise. Rupee text is converted with
// exact integer arithmetic: no parseFloat, no multiplication of fractions.

/** Largest transfer the API accepts by default (MAX_TRANSFER_MINOR). */
export const MAX_TRANSFER_PAISE = 100_000_000_000

export type ParseResult = { ok: true; paise: number } | { ok: false; error: string }

const RUPEES = /^(\d+)(?:\.(\d{1,2}))?$/

/**
 * Converts rupee text such as "10", "10.5" or "10.50" to paise.
 * Rejects anything else: signs, exponents ("1e3"), more than two decimals
 * ("10.555"), separators, empty input, zero, and values above maxPaise.
 */
export function parseRupeesToPaise(input: string, maxPaise: number = MAX_TRANSFER_PAISE): ParseResult {
  const text = input.trim()
  if (text === '') return { ok: false, error: 'Enter an amount.' }
  const match = RUPEES.exec(text)
  if (!match) {
    return { ok: false, error: 'Use digits with at most 2 decimal places, for example 125.50.' }
  }
  const [, rupees, fraction = ''] = match
  // BigInt keeps the arithmetic exact even for inputs far beyond 2^53.
  const paise = BigInt(rupees) * 100n + BigInt(fraction.padEnd(2, '0'))
  if (paise <= 0n) return { ok: false, error: 'The amount must be greater than zero.' }
  if (paise > BigInt(maxPaise)) {
    return { ok: false, error: `The amount must not exceed ${formatPaise(maxPaise)}.` }
  }
  return { ok: true, paise: Number(paise) }
}

/** Formats paise as rupees with Indian digit grouping, e.g. 12550050 -> "₹1,25,500.50". */
export function formatPaise(paise: number): string {
  const negative = paise < 0
  const abs = BigInt(Math.trunc(Math.abs(paise)))
  const rupees = (abs / 100n).toLocaleString('en-IN')
  const fraction = (abs % 100n).toString().padStart(2, '0')
  return `${negative ? '−' : ''}₹${rupees}.${fraction}`
}
