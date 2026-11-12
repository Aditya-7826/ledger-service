import { NavLink, Route, Routes } from 'react-router-dom'
import { EmptyState } from './components/ui'
import AccountDetail from './pages/AccountDetail'
import Accounts from './pages/Accounts'
import Dashboard from './pages/Dashboard'
import Journal from './pages/Journal'
import NewTransfer from './pages/NewTransfer'
import System from './pages/System'

const links = [
  { to: '/', label: 'Dashboard' },
  { to: '/accounts', label: 'Accounts' },
  { to: '/transfer', label: 'New transfer' },
  { to: '/journal', label: 'Journal' },
  { to: '/system', label: 'System' },
]

export default function App() {
  return (
    <>
      {/* Soft blurred blobs behind everything. */}
      <div className="blob left-[-8rem] top-[-6rem] h-[26rem] w-[26rem] bg-accent" aria-hidden="true" />
      <div className="blob right-[-10rem] top-[30%] h-[30rem] w-[30rem] bg-primary/50" aria-hidden="true" />
      <div className="blob bottom-[-10rem] left-[25%] h-[24rem] w-[24rem] bg-sky-200" aria-hidden="true" />

      <a href="#main" className="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:z-50 focus:rounded-lg focus:bg-white focus:px-3 focus:py-2">
        Skip to content
      </a>

      <div className="mx-auto max-w-6xl px-4 pb-16 pt-4 sm:px-6">
        <nav className="glass sticky top-3 z-40 mb-8 flex items-center gap-2 overflow-x-auto px-3 py-2" aria-label="Main">
          <span className="mr-2 flex shrink-0 items-center gap-2 pl-1 font-bold text-deep">
            <span className="grid h-8 w-8 place-items-center rounded-lg bg-primary text-white" aria-hidden="true">
              ₹
            </span>
            <span className="hidden sm:inline">Ledger</span>
          </span>
          {links.map((l) => (
            <NavLink
              key={l.to}
              to={l.to}
              end={l.to === '/'}
              className={({ isActive }) =>
                `shrink-0 rounded-xl px-3 py-2 text-sm font-semibold transition-colors duration-150 ${
                  isActive ? 'bg-deep text-white shadow' : 'text-ink-soft hover:bg-white/70 hover:text-deep'
                }`
              }
            >
              {l.label}
            </NavLink>
          ))}
        </nav>

        <main id="main">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/accounts" element={<Accounts />} />
            <Route path="/accounts/:id" element={<AccountDetail />} />
            <Route path="/transfer" element={<NewTransfer />} />
            <Route path="/journal" element={<Journal />} />
            <Route path="/system" element={<System />} />
            <Route path="*" element={<EmptyState title="Page not found" hint="Use the navigation above." />} />
          </Routes>
        </main>
      </div>
    </>
  )
}
