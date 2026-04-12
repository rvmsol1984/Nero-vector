import { NavLink, Outlet } from "react-router-dom";

const nav = [
  { to: "/dashboard", label: "Dashboard" },
  { to: "/incidents", label: "Incidents" },
  { to: "/watchlist", label: "Watchlist" },
  { to: "/events",    label: "Events"    },
  { to: "/users",     label: "Users"     },
  { to: "/baseline",  label: "Baseline"  },
  { to: "/sources",   label: "Sources"   },
  { to: "/governance",label: "Governance"},
];

// Active item style: left 2px accent bar + accent-tinted background.
// Using inline style so the tint matches the rgba spec exactly.
const ACTIVE_BG = "rgba(88,166,255,0.08)";

export default function Layout() {
  return (
    <div className="flex h-full bg-bg text-slate-100">
      {/* -------- sidebar -------- */}
      <aside className="w-56 shrink-0 border-r border-border bg-surface flex flex-col">
        <div className="px-5 py-5 border-b border-border">
          <div className="font-display text-2xl tracking-wider text-accent leading-none">
            NERO
          </div>
          <div className="font-display text-[10px] tracking-[0.35em] text-muted mt-1">
            VECTOR
          </div>
        </div>

        <nav className="flex-1 py-3">
          {nav.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === "/users" ? false : undefined}
              className={({ isActive }) =>
                `block px-5 py-2 text-[11px] uppercase tracking-[0.22em] border-l-2 transition-colors ${
                  isActive
                    ? "border-accent text-accent"
                    : "border-transparent text-muted hover:text-slate-100 hover:border-border"
                }`
              }
              style={({ isActive }) => ({
                backgroundColor: isActive ? ACTIVE_BG : "transparent",
              })}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>

        <div className="p-4 text-[10px] text-muted border-t border-border leading-tight">
          v0.1.0
          <div className="opacity-60">MSP operator console</div>
        </div>
      </aside>

      {/* -------- main column -------- */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="sticky top-0 z-10 bg-surface/90 backdrop-blur border-b border-border px-6 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <span className="font-display text-sm tracking-[0.25em] text-slate-100">
              NERO VECTOR
            </span>
            <span className="text-muted text-[11px]">// incident correlation</span>
          </div>
          <div className="flex items-center gap-2 text-[11px]">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-success opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-success"></span>
            </span>
            <span className="text-success uppercase tracking-[0.25em]">live</span>
          </div>
        </header>

        <main className="flex-1 overflow-auto p-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
