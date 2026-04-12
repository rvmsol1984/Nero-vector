import { NavLink, Outlet } from "react-router-dom";

// Bottom-nav FieldDesk layout. Sticky top bar up top, scrolling content
// in the middle, fixed bottom tab bar anchored to the safe-area inset.

function Icon({ name }) {
  const p = {
    width: 20,
    height: 20,
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
      {/* -------- top bar -------- */}
      <header
        className="sticky top-0 z-20 border-b border-white/5 backdrop-blur"
        style={{
          background: "rgba(10,15,30,0.9)",
          paddingTop: "env(safe-area-inset-top)",
        }}
      >
        <div className="px-5 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3 min-w-0">
            <div className="font-semibold tracking-[0.18em] text-sm">
              NERO VECTOR
            </div>
            <div className="text-white/40 text-[11px] truncate">
              // incident correlation
            </div>
          </div>
          <div className="flex items-center gap-2 text-[11px]">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-status-resolved opacity-75" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-status-resolved" />
            </span>
            <span className="text-status-resolved font-semibold tracking-[0.18em]">
              LIVE
            </span>
          </div>
        </div>
      </header>

      {/* -------- main -------- */}
      <main
        className="flex-1 overflow-auto"
        style={{ paddingBottom: "calc(88px + env(safe-area-inset-bottom))" }}
      >
        <div className="max-w-6xl mx-auto px-5 py-5">
          <Outlet />
        </div>
      </main>

      {/* -------- bottom tab bar -------- */}
      <nav
        className="fixed bottom-0 left-0 right-0 z-20 border-t border-white/5 backdrop-blur"
        style={{
          background: "rgba(10,15,30,0.9)",
          paddingBottom: "env(safe-area-inset-bottom)",
        }}
      >
        <div className="max-w-2xl mx-auto flex items-center justify-around py-2 px-2">
          {TABS.map((t) => (
            <NavLink
              key={t.to}
              to={t.to}
              className={({ isActive }) =>
                `bottom-nav-item ${isActive ? "active" : ""}`
              }
            >
              <Icon name={t.icon} />
              <span>{t.label}</span>
            </NavLink>
          ))}
        </div>
      </nav>

      {/* v-stamp */}
      <div
        className="fixed left-3 z-30 text-[10px] text-white/30 pointer-events-none"
        style={{ bottom: "calc(82px + env(safe-area-inset-bottom))" }}
      >
        v0.1.0 / MSP operator console
      </div>
    </div>
  );
}
