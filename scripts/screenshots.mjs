// Captures dashboard screenshots into docs/screenshots/ with a headless
// Chromium-based browser (Edge or Chrome) over the DevTools protocol.
// Needs Node 22+ and the stack running (scripts/dev up). No npm packages.
//
//   node scripts/screenshots.mjs                      # all pages
//   node scripts/screenshots.mjs --base http://localhost:5173 --out some/folder
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i > 0 ? process.argv[i + 1] : fallback
}
const base = arg('base', 'http://localhost:8088')
const outDir = path.resolve(arg('out', path.join(root, 'docs', 'screenshots')))
const only = arg('only', '') // capture just this page, e.g. --only system
const port = 9333

const candidates = [
  process.env.BROWSER_PATH,
  'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
  'C:/Program Files/Microsoft/Edge/Application/msedge.exe',
  'C:/Program Files/Google/Chrome/Application/chrome.exe',
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
  '/usr/bin/google-chrome',
  '/usr/bin/chromium',
  '/usr/bin/chromium-browser',
  '/usr/bin/microsoft-edge',
].filter(Boolean)
const browser = candidates.find((p) => fs.existsSync(p))
if (!browser) {
  console.error('No Chrome or Edge found. Set BROWSER_PATH to the browser executable.')
  process.exit(1)
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

async function firstAccountId() {
  const res = await fetch(`${base}/v1/accounts?limit=100`)
  const page = await res.json()
  const user = page.items.filter((a) => a.kind === 'USER').sort((a, b) => b.last_seq - a.last_seq)[0]
  return user?.id
}

const accountId = await firstAccountId().catch(() => undefined)
const pages = [
  { name: 'dashboard', path: '/', width: 1280, height: 1250 },
  { name: 'accounts', path: '/accounts', width: 1280, height: 900 },
  ...(accountId ? [{ name: 'account-detail', path: `/accounts/${accountId}`, width: 1280, height: 1000 }] : []),
  { name: 'new-transfer', path: '/transfer', width: 1280, height: 860 },
  { name: 'journal', path: '/journal', width: 1280, height: 1000 },
  { name: 'system', path: '/system', width: 1280, height: 1000 },
  { name: 'mobile-dashboard', path: '/', width: 375, height: 1400, mobile: true },
].filter((p) => !only || p.name === only)

fs.mkdirSync(outDir, { recursive: true })
const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'ledger-shots-'))
const child = spawn(browser, [
  '--headless=new', '--disable-gpu', '--hide-scrollbars', '--no-first-run',
  `--remote-debugging-port=${port}`, `--user-data-dir=${profile}`, 'about:blank',
], { stdio: 'ignore' })

try {
  let target
  for (let i = 0; i < 50 && !target; i++) {
    await sleep(200)
    try {
      const list = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json()
      target = list.find((t) => t.type === 'page')
    } catch { /* browser still starting */ }
  }
  if (!target) throw new Error('the browser did not start')

  const ws = new WebSocket(target.webSocketDebuggerUrl)
  await new Promise((resolve, reject) => {
    ws.onopen = resolve
    ws.onerror = reject
  })
  let nextId = 0
  const pending = new Map()
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data)
    if (msg.id && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id)
      pending.delete(msg.id)
      if (msg.error) reject(new Error(msg.error.message))
      else resolve(msg.result)
    }
  }
  const send = (method, params = {}) =>
    new Promise((resolve, reject) => {
      const id = ++nextId
      pending.set(id, { resolve, reject })
      ws.send(JSON.stringify({ id, method, params }))
    })

  await send('Page.enable')
  for (const p of pages) {
    await send('Emulation.setDeviceMetricsOverride', {
      width: p.width, height: p.height, deviceScaleFactor: p.mobile ? 2 : 1, mobile: Boolean(p.mobile),
    })
    await send('Page.navigate', { url: base + p.path })
    await sleep(Number(arg('wait', '4500'))) // let data load and the live feed connect
    const shot = await send('Page.captureScreenshot', { format: 'png' })
    const file = path.join(outDir, `${p.name}.png`)
    fs.writeFileSync(file, Buffer.from(shot.data, 'base64'))
    console.log(`saved ${path.relative(root, file)}`)
  }
  ws.close()
} finally {
  child.kill()
  await sleep(500)
  try {
    fs.rmSync(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 300 })
  } catch {
    // The browser can hold files for a moment after exiting; the OS cleans its temp folder later.
  }
}
