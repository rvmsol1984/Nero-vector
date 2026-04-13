import { useEffect, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";

import { signOut, useAuth } from "../auth.jsx";
import { initialsFrom } from "../utils/format.js";

// Responsive layout:
//   desktop (>= 768px): fixed 220px sidebar on the left, 8 nav items
//   mobile  (<  768px): hamburger -> slide-out drawer mirrors the sidebar
//                       fixed bottom tab bar with the 5 primary items
// Top bar is shared by both breakpoints.

const SIDEBAR_ITEMS = [
  { to: "/dashboard",  label: "Dashboard",  icon: "dashboard"  },
  { to: "/incidents",  label: "Incidents",  icon: "alert",   phase2: true },
  { to: "/watchlist",  label: "Watchlist",  icon: "eye",     phase2: true },
  { to: "/events",     label: "Events",     icon: "events"     },
  { to: "/users",      label: "Users",      icon: "users"      },
  { to: "/baseline",   label: "Baseline",   icon: "activity", phase2: true },
  { to: "/governance", label: "Governance", icon: "governance" },
  { to: "/sources",    label: "Sources",    icon: "sources"    },
];

const BOTTOM_TABS = [
  { to: "/dashboard",  label: "Dashboard",  icon: "dashboard"  },
  { to: "/events",     label: "Events",     icon: "events"     },
  { to: "/users",      label: "Users",      icon: "users"      },
  { to: "/governance", label: "Governance", icon: "governance" },
  { to: "/sources",    label: "Sources",    icon: "sources"    },
];

// ---------------------------------------------------------------------------
// icon set (inline SVGs, no dep)
// ---------------------------------------------------------------------------

function Icon({ name, size = 18 }) {
  const p = {
    width: size,
    height: size,
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
    case "alert":
      return (
        <svg {...p}>
          <path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
          <line x1="12" y1="9"  x2="12" y2="13" />
          <line x1="12" y1="17" x2="12.01" y2="17" />
        </svg>
      );
    case "eye":
      return (
        <svg {...p}>
          <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
          <circle cx="12" cy="12" r="3" />
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
    case "activity":
      return (
        <svg {...p}>
          <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
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
    case "menu":
      return (
        <svg {...p}>
          <line x1="3" y1="6"  x2="21" y2="6" />
          <line x1="3" y1="12" x2="21" y2="12" />
          <line x1="3" y1="18" x2="21" y2="18" />
        </svg>
      );
    case "close":
      return (
        <svg {...p}>
          <line x1="6"  y1="6"  x2="18" y2="18" />
          <line x1="18" y1="6"  x2="6"  y2="18" />
        </svg>
      );
    default:
      return null;
  }
}

function P2Badge() {
  return (
    <span
      className="inline-flex items-center justify-center px-1.5 py-0.5 text-[9px] font-bold rounded tracking-wider"
      style={{
        color: "#3B82F6",
        backgroundColor: "rgba(37,99,235,0.15)",
        border: "1px solid rgba(37,99,235,0.35)",
      }}
    >
      P2
    </span>
  );
}

// ---------------------------------------------------------------------------
// sidebar body (shared between desktop sidebar + mobile drawer)
// ---------------------------------------------------------------------------

function SidebarBody({ onNavigate, showCloseButton, onClose }) {
  return (
    <>
      <div className="flex items-center justify-between px-5 py-5 border-b border-white/5">
        <div className="flex items-center gap-3 min-w-0">
          <img
            src="/logo.png"
            alt="NERO"
            style={{
              height: "32px",
              width: "auto",
              filter: "brightness(0) invert(1)",
            }}
          />
          <div className="text-[10px] tracking-[0.35em] text-white/40 font-semibold">
            VECTOR
          </div>
        </div>
        {showCloseButton && (
          <button
            type="button"
            onClick={onClose}
            className="p-1 text-white/50 hover:text-white active:scale-95 transition-all"
            aria-label="close menu"
          >
            <Icon name="close" size={20} />
          </button>
        )}
      </div>

      <nav className="flex-1 py-3 overflow-y-auto">
        {SIDEBAR_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            onClick={onNavigate}
            className={({ isActive }) =>
              `flex items-center justify-between gap-3 pl-5 pr-4 py-2.5 text-[11px] uppercase tracking-[0.18em] font-semibold border-l-2 transition-colors ${
                isActive
                  ? "border-primary text-primary-light"
                  : "border-transparent text-white/55 hover:text-white hover:bg-white/[0.05]"
              }`
            }
            style={({ isActive }) => ({
              backgroundColor: isActive ? "rgba(37,99,235,0.15)" : undefined,
            })}
          >
            <span className="flex items-center gap-3">
              <Icon name={item.icon} size={16} />
              <span>{item.label}</span>
            </span>
            {item.phase2 && <P2Badge />}
          </NavLink>
        ))}
      </nav>

      <div className="px-5 py-4 text-[10px] text-white/30 border-t border-white/5 leading-tight">
        v0.1.0
        <div className="opacity-80">MSP operator console</div>
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// layout
// ---------------------------------------------------------------------------

export default function Layout() {
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const location = useLocation();
  const user = useAuth();

  // Auto-close the mobile drawer + user menu whenever the user navigates.
  useEffect(() => {
    setDrawerOpen(false);
    setUserMenuOpen(false);
  }, [location.pathname]);

  // Prevent page scroll while the drawer is open on mobile.
  useEffect(() => {
    if (!drawerOpen) return undefined;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = prev;
    };
  }, [drawerOpen]);

  return (
    <div className="flex flex-col h-screen bg-bg text-white">
      {/* -------- top bar -------- */}
      <header
        className="sticky top-0 z-30 backdrop-blur border-b border-white/5"
        style={{
          background: "rgba(10,15,30,0.9)",
          paddingTop: "env(safe-area-inset-top)",
        }}
      >
        <div className="px-4 md:px-5 py-3 flex items-center justify-between gap-3">
          <div className="flex items-center gap-3 min-w-0">
            {/* mobile hamburger */}
            <button
              type="button"
              onClick={() => setDrawerOpen(true)}
              className="md:hidden -ml-2 p-2 text-white/70 hover:text-white active:scale-95 transition-all"
              aria-label="open menu"
            >
              <Icon name="menu" size={22} />
            </button>
            <div className="font-semibold tracking-[0.18em] text-sm">
              NERO VECTOR
            </div>
            <div className="text-white/40 text-[11px] truncate hidden sm:block">
              // incident correlation
            </div>
          </div>
          <div className="flex items-center gap-4 text-[11px]">
            <div className="flex items-center gap-2">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-status-resolved opacity-75" />
                <span className="relative inline-flex rounded-full h-2 w-2 bg-status-resolved" />
              </span>
              <span className="text-status-resolved font-semibold tracking-[0.18em]">
                LIVE
              </span>
            </div>

            {user && (
              <div className="relative">
                <button
                  type="button"
                  onClick={() => setUserMenuOpen((v) => !v)}
                  className="flex items-center gap-2 pl-1 pr-2 py-1 rounded-xl bg-white/5 border border-white/10 hover:bg-white/10 active:scale-95 transition-all"
                  aria-label={`signed in as ${user.email}`}
                  title={user.email}
                >
                  <span
                    className="h-6 w-6 rounded-full flex items-center justify-center text-[9px] font-bold border"
                    style={{
                      color: "#3B82F6",
                      background: "rgba(37,99,235,0.15)",
                      borderColor: "rgba(37,99,235,0.45)",
                    }}
                  >
                    {user.initials || initialsFrom(user.email)}
                  </span>
                  <span className="hidden sm:block text-white/80 max-w-[180px] truncate">
                    {user.name || user.email}
                  </span>
                  <svg
                    width="10"
                    height="10"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    className="text-white/50"
                  >
                    <polyline points="6 9 12 15 18 9" />
                  </svg>
                </button>
                {userMenuOpen && (
                  <>
                    <div
                      className="fixed inset-0 z-40"
                      onClick={() => setUserMenuOpen(false)}
                    />
                    <div
                      className="absolute right-0 top-full mt-2 z-50 min-w-[220px] card p-2 animate-fade-in"
                    >
                      <div className="px-3 py-2">
                        <div className="text-[11px] font-semibold truncate">
                          {user.name || user.email}
                        </div>
                        <div className="text-[10px] text-white/50 truncate">
                          {user.email}
                        </div>
                        {user.role && (
                          <div className="text-[10px] uppercase tracking-wider text-primary-light mt-1">
                            {user.role}
                          </div>
                        )}
                      </div>
                      <div className="border-t border-white/5 my-1" />
                      <button
                        type="button"
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          setUserMenuOpen(false);
                          signOut();
                        }}
                        className="w-full text-left px-3 py-2 text-[11px] font-medium rounded-lg hover:bg-white/5 text-white/80 hover:text-white transition-colors active:scale-[0.98]"
                      >
                        Sign out
                      </button>
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
        </div>
      </header>

      {/* -------- desktop sidebar + main -------- */}
      <div className="flex-1 flex overflow-hidden">
        <aside
          className="hidden md:flex flex-col w-[220px] shrink-0"
          style={{
            background: "#0D1428",
            borderRight: "1px solid rgba(255,255,255,0.05)",
          }}
        >
          <SidebarBody />
        </aside>

        <main className="flex-1 overflow-auto pb-[calc(5.5rem+env(safe-area-inset-bottom))] md:pb-0">
          <div className="max-w-6xl mx-auto px-4 md:px-6 py-5">
            <Outlet />
          </div>
        </main>
      </div>

      {/* -------- mobile bottom tab bar (primary 5 only) -------- */}
      <nav
        className="md:hidden fixed bottom-0 left-0 right-0 z-30 border-t border-white/5 backdrop-blur"
        style={{
          background: "rgba(10,15,30,0.9)",
          paddingBottom: "env(safe-area-inset-bottom)",
        }}
      >
        <div className="flex items-center justify-around py-2 px-2">
          {BOTTOM_TABS.map((t) => (
            <NavLink
              key={t.to}
              to={t.to}
              className={({ isActive }) =>
                `flex flex-col items-center gap-0.5 px-3 py-1.5 rounded-xl transition-all duration-200 active:scale-95 ${
                  isActive ? "text-primary-light" : "text-white/50 hover:text-white"
                }`
              }
            >
              <Icon name={t.icon} size={20} />
              <span className="text-[10px] font-medium tracking-wide">{t.label}</span>
            </NavLink>
          ))}
        </div>
      </nav>

      {/* -------- mobile slide-out drawer -------- */}
      <div
        className={`md:hidden fixed inset-0 bg-black/60 z-40 transition-opacity duration-200 ${
          drawerOpen ? "opacity-100" : "opacity-0 pointer-events-none"
        }`}
        onClick={() => setDrawerOpen(false)}
        aria-hidden={!drawerOpen}
      />
      <aside
        className={`md:hidden fixed left-0 top-0 bottom-0 w-[260px] z-50 flex flex-col transition-transform duration-200 ease-out ${
          drawerOpen ? "translate-x-0" : "-translate-x-full"
        }`}
        style={{
          background: "#0D1428",
          borderRight: "1px solid rgba(255,255,255,0.05)",
          paddingTop: "env(safe-area-inset-top)",
          paddingBottom: "env(safe-area-inset-bottom)",
        }}
        aria-hidden={!drawerOpen}
      >
        <SidebarBody
          onNavigate={() => setDrawerOpen(false)}
          showCloseButton
          onClose={() => setDrawerOpen(false)}
        />
      </aside>
    </div>
  );
}
