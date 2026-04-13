import { NavLink, Outlet } from "react-router-dom";

// Top bar with the NERO VECTOR brand + LIVE indicator, followed by a
// horizontal tab strip directly underneath. The strip replaces the
// previous fixed bottom nav entirely.

function Icon({ name }) {
  const p = {
    width: 18,
    height: 18,
    viewBox: "0 0 24 24",
    fill: "none",
    stroke: "currentColor",
    strokeWidth: 2,
    strokeLinecap: "round",
    strokeLinejoin: "round",
  };
  switch (name) {
    case "dashboard":
      return (
        <svg {...p}>
          <rect x="3"  y="3"  width="7" height="9" />
          <rect x="14" y="3"  width="7" height="5" />
          <rect x="14" y="12" width="7" height="9" />
          <rect x="3"  y="16" width="7" height="5" />
        </svg>
      );
    case "events":
      return (
        <svg {...p}>
          <line x1="8" y1="6"  x2="21" y2="6" />
          <line x1="8" y1="12" x2="21" y2="12" />
          <line x1="8" y1="18" x2="21" y2="18" />
          <circle cx="3.5" cy="6"  r="0.5" />
          <circle cx="3.5" cy="12" r="0.5" />
          <circle cx="3.5" cy="18" r="0.5" />
        </svg>
      );
    case "users":
      return (
        <svg {...p}>
          <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" />
          <circle cx="9" cy="7" r="4" />
          <path d="M23 21v-2a4 4 0 0 0-3-3.87" />
          <path d="M16 3.13a4 4 0 0 1 0 7.75" />
        </svg>
      );
    case "governance":
      return (
        <svg {...p}>
          <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
          <path d="M9 12l2 2 4-4" />
        </svg>
      );
    case "sources":
      return (
        <svg {...p}>
          <polygon points="12 2 2 7 12 12 22 7 12 2" />
          <polyline points="2 17 12 22 22 17" />
          <polyline points="2 12 12 17 22 12" />
        </svg>
      );
    default:
      return null;
  }
}

const TABS = [
  { to: "/dashboard",  label: "Dashboard",  icon: "dashboard"  },
  { to: "/events",     label: "Events",     icon: "events"     },
  { to: "/users",      label: "Users",      icon: "users"      },
  { to: "/governance", label: "Governance", icon: "governance" },
  { to: "/sources",    label: "Sources",    icon: "sources"    },
];

export default function Layout() {
  return (
    <div className="flex flex-col h-screen bg-bg text-white">
      {/* -------- top bar + tab strip -------- */}
      <header
        className="sticky top-0 z-20 backdrop-blur border-b border-white/5"
        style={{
          background: "rgba(10,15,30,0.9)",
          paddingTop: "env(safe-area-inset-top)",
        }}
      >
        {/* brand row */}
        <div className="px-5 py-3 flex items-center justify-between gap-4">
          <div className="flex items-center gap-3 min-w-0">
            <div className="font-semibold tracking-[0.18em] text-sm">
              NERO VECTOR
            </div>
            <div className="text-white/40 text-[11px] truncate hidden sm:block">
              // incident correlation
            </div>
          </div>
          <div className="flex items-center gap-4 text-[11px]">
            <span className="text-white/30 hidden md:inline">
              v0.1.0 / MSP operator console
            </span>
            <div className="flex items-center gap-2">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-status-resolved opacity-75" />
                <span className="relative inline-flex rounded-full h-2 w-2 bg-status-resolved" />
              </span>
              <span className="text-status-resolved font-semibold tracking-[0.18em]">
                LIVE
              </span>
            </div>
          </div>
        </div>

        {/* horizontal tab strip */}
        <nav className="px-3 flex items-center gap-1 overflow-x-auto">
          {TABS.map((t) => (
            <NavLink
              key={t.to}
              to={t.to}
              className={({ isActive }) =>
                `flex items-center gap-2 px-3 py-2.5 text-[12px] font-medium border-b-2 -mb-px whitespace-nowrap transition-colors active:scale-95 ${
                  isActive
                    ? "border-primary text-primary-light"
                    : "border-transparent text-white/55 hover:text-white"
                }`
              }
            >
              <Icon name={t.icon} />
              <span>{t.label}</span>
            </NavLink>
          ))}
        </nav>
      </header>

      {/* -------- main -------- */}
      <main
        className="flex-1 overflow-auto"
        style={{ paddingBottom: "env(safe-area-inset-bottom)" }}
      >
        <div className="max-w-6xl mx-auto px-5 py-5">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
