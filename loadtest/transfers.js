// k6 load test for POST /v1/transfers. Run it through `scripts/dev loadtest`
// (which calls cmd/loadtest); that wrapper also checks the results afterwards.
//
// SCENARIO=uniform      50 virtual users, random account pairs, 2 minutes
// SCENARIO=hot_account  80% of transfers go to one merchant account
// SCENARIO=retry_storm  10,000 logical requests, each sent 2-3 times with the
//                       same idempotency key, half of them concurrently
import { check } from 'k6'
import { SharedArray } from 'k6/data'
import exec from 'k6/execution'
import http from 'k6/http'
import { Counter } from 'k6/metrics'

const accounts = new SharedArray('accounts', () => JSON.parse(open(__ENV.ACCOUNTS_FILE)))
const BASE = __ENV.BASE_URL || 'http://localhost:8080'
const SCENARIO = __ENV.SCENARIO || 'uniform'
const RUN_ID = __ENV.RUN_ID || `${Date.now()}`
const CLIENT_ID = __ENV.CLIENT_ID || `k6-${SCENARIO}-${RUN_ID}`
const VUS = Number(__ENV.VUS || 50)
const DURATION = __ENV.DURATION || '2m'
const LOGICAL = Number(__ENV.LOGICAL || 10000)
const SUMMARY_FILE = __ENV.SUMMARY_FILE || 'summary.json'

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(95)', 'p(99)'],
  scenarios:
    SCENARIO === 'retry_storm'
      ? { storm: { executor: 'shared-iterations', vus: VUS, iterations: LOGICAL, maxDuration: '20m' } }
      : { load: { executor: 'constant-vus', vus: VUS, duration: DURATION } },
}

const created = new Counter('transfers_created') // 201 without the replay header
const replayed = new Counter('transfers_replayed') // 201 with Idempotent-Replayed: true
const rejected = new Counter('transfers_rejected') // anything else
const inconsistent = new Counter('storm_inconsistent') // retries that disagreed with each other

const url = `${BASE}/v1/transfers`
const pickIndex = (n) => Math.floor(Math.random() * n)

function pair() {
  let to
  if (SCENARIO === 'hot_account' && Math.random() < 0.8) {
    to = 0 // the merchant
  } else {
    to = pickIndex(accounts.length)
  }
  let from = pickIndex(accounts.length)
  while (from === to) from = pickIndex(accounts.length)
  return [accounts[from], accounts[to]]
}

function record(res) {
  if (res.status === 201) {
    if (res.headers['Idempotent-Replayed'] === 'true') replayed.add(1)
    else created.add(1)
  } else {
    rejected.add(1)
  }
  check(res, { 'status is 201': (r) => r.status === 201 })
}

export default function () {
  const [from, to] = pair()
  const payload = JSON.stringify({
    from_account_id: from,
    to_account_id: to,
    amount_minor: 1 + pickIndex(500),
    currency: 'INR',
    memo: SCENARIO,
  })
  const key = `${RUN_ID}-${exec.vu.idInTest}-${exec.scenario.iterationInTest}`
  const params = {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key, 'X-Client-Id': CLIENT_ID },
    tags: { name: 'transfer' },
  }

  if (SCENARIO !== 'retry_storm') {
    record(http.post(url, payload, params))
    return
  }

  // One logical request, sent 2 or 3 times with the same key.
  const sends = Math.random() < 0.5 ? 2 : 3
  const responses = []
  if (Math.random() < 0.5) {
    // The first two go out at the same time, racing for the key.
    responses.push(...http.batch([['POST', url, payload, params], ['POST', url, payload, params]]))
    if (sends === 3) responses.push(http.post(url, payload, params))
  } else {
    for (let i = 0; i < sends; i++) responses.push(http.post(url, payload, params))
  }
  responses.forEach(record)

  // Every copy must be a 201 with the identical body, and exactly one must be the original.
  const ok = responses.every((r) => r.status === 201 && r.body === responses[0].body)
  const originals = responses.filter((r) => r.headers['Idempotent-Replayed'] !== 'true').length
  if (!ok || originals !== 1) inconsistent.add(1)
}

export function handleSummary(data) {
  return { [SUMMARY_FILE]: JSON.stringify(data) }
}
