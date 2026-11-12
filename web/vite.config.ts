import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import type { Plugin } from 'vite'
import { defineConfig } from 'vitest/config'

const here = path.dirname(fileURLToPath(import.meta.url))

// In development the browser talks only to Vite; Vite forwards to the three
// services, so there is no CORS. In Docker Compose nginx does the same job.
const api = process.env.API_URL ?? 'http://localhost:8080'
const balances = process.env.BALANCES_URL ?? 'http://localhost:8081'
const notify = process.env.NOTIFY_URL ?? 'http://localhost:8082'

// Serves ../results/*.json (the load-test result files) at /results/.
function serveResults(): Plugin {
  return {
    name: 'serve-results',
    configureServer(server) {
      server.middlewares.use('/results', (req, res) => {
        const name = path.basename((req.url ?? '').split('?')[0])
        const file = path.resolve(here, '../results', name)
        if (!/^[\w.-]+\.json$/.test(name) || !fs.existsSync(file)) {
          res.statusCode = 404
          res.end('not found')
          return
        }
        res.setHeader('Content-Type', 'application/json')
        res.end(fs.readFileSync(file))
      })
    },
  }
}

export default defineConfig({
  plugins: [react(), tailwindcss(), serveResults()],
  server: {
    port: 5173,
    proxy: {
      '/v1/stream': { target: notify, changeOrigin: true },
      '/v1/views': { target: balances, changeOrigin: true },
      '/v1': { target: api, changeOrigin: true },
    },
  },
  test: { environment: 'node', include: ['src/**/*.test.ts'] },
})
