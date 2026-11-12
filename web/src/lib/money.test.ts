import { describe, expect, it } from 'vitest'
import { formatPaise, MAX_TRANSFER_PAISE, parseRupeesToPaise } from './money'

const paise = (input: string, max?: number) => {
  const r = parseRupeesToPaise(input, max)
  return r.ok ? r.paise : null
}

describe('parseRupeesToPaise', () => {
  it('converts whole rupees', () => {
    expect(paise('10')).toBe(1000)
    expect(paise('1')).toBe(100)
  })

  it('converts one and two decimal places exactly', () => {
    expect(paise('10.5')).toBe(1050)
    expect(paise('10.50')).toBe(1050)
    expect(paise('0.01')).toBe(1)
    expect(paise('0.10')).toBe(10)
    expect(paise('  125.50  ')).toBe(12550)
  })

  it('is exact where floating point is not', () => {
    // 0.29 * 100 = 28.999999999999996 and 1.15 * 100 = 114.99999999999999 in floating point.
    expect(paise('0.29')).toBe(29)
    expect(paise('1.15')).toBe(115)
    expect(paise('4.35')).toBe(435)
    expect(paise('1000000.07')).toBe(100000007)
  })

  it('rejects exponents', () => {
    expect(paise('1e3')).toBeNull()
    expect(paise('1E3')).toBeNull()
  })

  it('rejects more than two decimal places', () => {
    expect(paise('10.555')).toBeNull()
    expect(paise('0.001')).toBeNull()
  })

  it('rejects negative and signed amounts', () => {
    expect(paise('-5')).toBeNull()
    expect(paise('-0.01')).toBeNull()
    expect(paise('+5')).toBeNull()
  })

  it('rejects zero, empty and malformed input', () => {
    for (const bad of ['0', '0.00', '', '   ', '.5', '5.', '1,000', '1 000', 'abc', '10.5.1', '₹10', '0x10', 'Infinity', 'NaN']) {
      expect(paise(bad), bad).toBeNull()
    }
  })

  it('enforces the maximum', () => {
    expect(paise('1000000000')).toBe(MAX_TRANSFER_PAISE) // exactly the default maximum
    expect(paise('1000000000.01')).toBeNull()
    expect(paise('99999999999999999999999')).toBeNull() // far beyond 2^53: still rejected, not rounded
    expect(paise('50', 5000)).toBe(5000)
    expect(paise('50.01', 5000)).toBeNull()
  })

  it('explains why input was rejected', () => {
    const r = parseRupeesToPaise('10.555')
    expect(r.ok).toBe(false)
    if (!r.ok) expect(r.error).toMatch(/2 decimal places/)
  })
})

describe('formatPaise', () => {
  it('formats with Indian grouping and two decimals', () => {
    expect(formatPaise(0)).toBe('₹0.00')
    expect(formatPaise(5)).toBe('₹0.05')
    expect(formatPaise(12550)).toBe('₹125.50')
    expect(formatPaise(12550050)).toBe('₹1,25,500.50')
    expect(formatPaise(-37450)).toBe('−₹374.50')
  })

  it('round-trips with the parser', () => {
    for (const text of ['10', '10.5', '0.01', '99999.99']) {
      const r = parseRupeesToPaise(text)
      expect(r.ok).toBe(true)
      if (r.ok) expect(parseRupeesToPaise(formatPaise(r.paise).replace(/[₹,]/g, ''))).toEqual(r)
    }
  })
})
