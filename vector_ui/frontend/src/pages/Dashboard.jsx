import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "../components/Avatar.jsx";
import EventCard from "../components/EventCard.jsx";
import StatRing from "../components/StatRing.jsx";
import TenantBadge from "../components/TenantBadge.jsx";
import { api } from "../api.js";
import { getEventLabel } from "../utils/eventLabels.js";
import { fmtNumber, fmtRelative } from "../utils/format.js";
import { tenantColor } from "../utils/tenantColor.js";

const TABS = [
  { id: "feed",    label: "Feed"       },
  { id: "types",   label: "By Type"    },
  { id: "tenants", label: "By Tenant"  },
];

export default function Dashboard() {
  const [stats, setStats] = useState(null);
  const [byTenant, setByTenant] = useState([]);
  const [byType, setByType] = useState([]);
  const [recent, setRecent] = useState([]);
  const [iocMatches, setIocMatches] = useState([]);
  const [heroStats, setHeroStats] = useState(null);
  const [err, setErr] = useState(null);
  const [tab, setTab] = useState("feed");

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const [s, bt, by, r, ioc, incStats] = await Promise.all([
          api.stats(),
          api.byTenant(),
          api.byType(),
          api.dashboardFeed({ ual_limit: 50, inky_limit: 20 }),
          api.iocMatches(10).catch(() => []),
          api.incidentStats().catch(() => ({})),
        ]);
        if (cancelled) return;
        setStats(s);
        setByTenant(bt);
        setByType(by);
        setRecent(r);
        setIocMatches(ioc || []);
        setHeroStats(incStats || {});
        setErr(null);
      } catch (e) {
        if (!cancelled) setErr(e.message);
      }
    }

    load();
    const t = setInterval(load, 30000);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, []);

  const maxTenant = Math.max(1, ...byTenant.map((r) => Number(r.count) || 0));
  const maxType = Math.max(1, ...byType.map((r) => Number(r.count) || 0));

  return (
    <div className="space-y-6 animate-fade-in">
      <div>
        <h1 className="text-2xl font-bold">Operations</h1>
        <p className="text-white/50 text-sm mt-1">
          Unified audit telemetry across managed tenants.
        </p>
      </div>

      {err && (
        <div className="card border-critical/30 text-critical text-sm px-4 py-3">
          load error: {err}
        </div>
      )}

      {/* ----- hero stat cards ----- */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <HeroCard
          label="Active Incidents"
          value={heroStats?.open}
          accent="#DC2626"
          loading={heroStats == null}
        />
        <HeroCard
          label="IOC Matches Today"
          value={iocMatches?.length}
          accent="#F97316"
          loading={heroStats == null}
        />
        <HeroCard
          label="Users at Risk"
          value={heroStats?.critical != null ? (heroStats.critical + (heroStats.high || 0)) : undefined}
          accent="#EAB308"
          loading={heroStats == null}
        />
        <HeroCard
          label="Tenants Monitored"
          value={stats?.unique_tenants}
          accent="#3B82F6"
          loading={stats == null}
        />
      </div>

      {/* ----- tenant bubbles row ----- */}
      <div className="flex flex-wrap gap-3 pb-1">
        {byTenant.map((row) => {
          const color = tenantColor(row.client_name);
          return (
            <Link
              key={row.client_name}
              to={`/events?tenant=${encodeURIComponent(row.client_name)}`}
              className="card flex items-center gap-3 px-4 py-3 shrink-0 hover:bg-white/[0.03] active:scale-95 transition-all"
            >
              <Avatar email={row.client_name} tenant={row.client_name} size={40} />
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <span
                    className="h-1.5 w-1.5 rounded-full shrink-0"
                    style={{ background: color }}
                  />
                  <span className="text-sm font-semibold truncate max-w-[180px]">
                    {row.client_name}
                  </span>
                </div>
                <div className="text-[11px] text-white/50 mt-0.5 tabular-nums">
                  {fmtNumber(row.count)} events
                </div>
              </div>
            </Link>
          );
        })}
        {byTenant.length === 0 && (
          <div className="text-white/40 text-sm">no tenants</div>
        )}
      </div>

      {/* ----- 4 stat rings ----- */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatRing
          value={stats?.total_events}
          label="Total Events"
          color="#2563EB"
        />
        <StatRing
          value={stats?.events_24h}
          label="Events · 24h"
          color="#10B981"
        />
        <StatRing
          value={stats?.unique_users}
          label="Unique Users"
          color="#8B5CF6"
        />
        <StatRing
          value={stats?.unique_tenants}
          label="Active Tenants"
          color="#F97316"
        />
      </div>

      {/* ----- IOC matches alert strip (hidden when empty) ----- */}
      <IocMatchesAlert matches={iocMatches} />

      {/* ----- feed / by type / by tenant switcher ----- */}
      <div className="card">
        <div className="px-4 pt-3 flex items-center gap-2 border-b border-white/5">
          {TABS.map((t) => {
            const active = tab === t.id;
            return (
              <button
                key={t.id}
                type="button"
                onClick={() => setTab(t.id)}
                className={`px-3 py-2 text-xs font-medium border-b-2 -mb-px transition-colors ${
                  active
                    ? "border-primary text-primary-light"
                    : "border-transparent text-white/50 hover:text-white"
                }`}
              >
                {t.label}
              </button>
            );
          })}
          <Link
            to="/events"
            className="ml-auto text-[11px] text-primary-light hover:underline py-2"
          >
            open events →
          </Link>
        </div>

        <div className="p-4">
          {tab === "feed" && (
            <div className="space-y-3">
              {recent.map((ev) => (
                <EventCard key={ev.id} event={ev} />
              ))}
              {recent.length === 0 && (
                <div className="text-white/40 text-sm text-center py-8">
                  no events yet — waiting for ingest
                </div>
              )}
            </div>
          )}

          {tab === "types" && (
            <div className="space-y-3">
              {byType.slice(0, 12).map((row) => (
                <Link
                  key={row.event_type ?? "(none)"}
                  to={`/events?event_type=${encodeURIComponent(row.event_type ?? "")}`}
                  className="block group"
                >
                  <div className="flex justify-between text-sm">
                    <span
                      className="truncate group-hover:text-primary-light transition-colors"
                      title={row.event_type ?? ""}
                    >
                      {row.event_type ? getEventLabel(row.event_type) : "(none)"}
                    </span>
                    <span className="text-white/50 tabular-nums">
                      {fmtNumber(row.count)}
                    </span>
                  </div>
                  <div className="h-1.5 bg-black/30 mt-2 rounded-full overflow-hidden">
                    <div
                      className="h-1.5 bg-primary rounded-full"
                      style={{ width: `${(Number(row.count) / maxType) * 100}%` }}
                    />
                  </div>
                </Link>
              ))}
              {byType.length === 0 && (
                <div className="text-white/40 text-sm text-center py-8">no data</div>
              )}
            </div>
          )}

          {tab === "tenants" && (
            <div className="space-y-3">
              {byTenant.map((row) => {
                const color = tenantColor(row.client_name);
                return (
                  <Link
                    key={row.client_name}
                    to={`/events?tenant=${encodeURIComponent(row.client_name)}`}
                    className="block group"
                  >
                    <div className="flex justify-between text-sm">
                      <span
                        className="group-hover:underline transition-colors"
                        style={{ color }}
                      >
                        {row.client_name}
                      </span>
                      <span className="text-white/50 tabular-nums">
                        {fmtNumber(row.count)}
                      </span>
                    </div>
                    <div className="h-1.5 bg-black/30 mt-2 rounded-full overflow-hidden">
                      <div
                        className="h-1.5 rounded-full"
                        style={{
                          width: `${(Number(row.count) / maxTenant) * 100}%`,
                          background: color,
                        }}
                      />
                    </div>
                  </Link>
                );
              })}
              {byTenant.length === 0 && (
                <div className="text-white/40 text-sm text-center py-8">no data</div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// IOC Matches alert strip
// ---------------------------------------------------------------------------
//
// Hidden entirely when the IOC match list is empty so a clean enrichment
// cycle doesn't eat vertical space on the dashboard. When matches exist we
// render up to 5 red alert cards plus a "View all" link into the Governance
// IOC Matches tab.

const IOC_RED = "#EF4444";

function confidenceTierStyles(confidence) {
  const c = Number(confidence) || 0;
  if (c >= 90) return { label: "CRITICAL", color: "#EF4444" };
  if (c >= 75) return { label: "HIGH",     color: "#F97316" };
  if (c >= 50) return { label: "MEDIUM",   color: "#EAB308" };
  return { label: `${c}`, color: "rgba(255,255,255,0.5)" };
}

function ConfidencePill({ confidence }) {
  const cfg = confidenceTierStyles(confidence);
  return (
    <span
      className="inline-flex items-center px-2 py-[2px] text-[10px] font-semibold uppercase tracking-wide rounded-full border whitespace-nowrap"
      style={{
        color: cfg.color,
        borderColor: cfg.color + "55",
        backgroundColor: cfg.color + "14",
      }}
      title={`confidence ${confidence ?? "?"}`}
    >
      {cfg.label} · {Number(confidence) || 0}
    </span>
  );
}

function SkullIcon({ size = 18 }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke={IOC_RED}
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M12 2a8 8 0 0 0-8 8v4a4 4 0 0 0 2 3.46V21a1 1 0 0 0 1 1h10a1 1 0 0 0 1-1v-3.54A4 4 0 0 0 20 14v-4a8 8 0 0 0-8-8Z" />
      <circle cx="9" cy="11" r="1.4" fill={IOC_RED} />
      <circle cx="15" cy="11" r="1.4" fill={IOC_RED} />
      <path d="M10 16h4" />
    </svg>
  );
}

function IocMatchesAlert({ matches }) {
  if (!matches || matches.length === 0) return null;
  const visible = matches.slice(0, 5);
  return (
    <div
      className="card p-4 animate-fade-in"
      style={{
        borderLeft: `3px solid ${IOC_RED}`,
        backgroundColor: "rgba(239,68,68,0.04)",
      }}
    >
      <div className="flex items-center gap-2 mb-3">
        <SkullIcon size={20} />
        <div className="font-semibold text-sm text-white">
          IOC Matches
          <span className="ml-2 text-[11px] font-normal text-white/50">
            {matches.length} active
          </span>
        </div>
        <Link
          to="/governance"
          className="ml-auto text-[11px] font-medium text-red-300 hover:text-red-200"
        >
          View all →
        </Link>
      </div>
      <div className="space-y-2">
        {visible.map((m) => (
          <div
            key={m.id}
            className="flex items-center gap-3 px-3 py-2 rounded-xl border flex-wrap"
            style={{
              borderColor: "rgba(239,68,68,0.25)",
              backgroundColor: "rgba(239,68,68,0.06)",
            }}
          >
            <SkullIcon />
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 flex-wrap">
                <span
                  className="font-mono text-[12px] text-white truncate max-w-[320px]"
                  title={m.ioc_value}
                >
                  {m.ioc_value}
                </span>
                <span className="text-[10px] uppercase tracking-wider text-white/40">
                  {m.ioc_type}
                </span>
                <ConfidencePill confidence={m.confidence} />
              </div>
              <div className="mt-0.5 flex items-center gap-2 text-[11px] text-white/60 min-w-0">
                <span className="truncate" title={m.indicator_name || ""}>
                  {m.indicator_name || <span className="text-white/30">(no indicator name)</span>}
                </span>
                {m.client_name && (
                  <>
                    <span className="opacity-60">·</span>
                    <TenantBadge name={m.client_name} />
                  </>
                )}
              </div>
            </div>
            <div className="text-[11px] text-white/50 whitespace-nowrap tabular-nums">
              {fmtRelative(m.matched_at)}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// hero stat card -- dark card with colored top-border accent
// ---------------------------------------------------------------------------

function HeroCard({ label, value, accent, loading }) {
  return (
    <div
      className="bg-surface border border-white/5 rounded-card overflow-hidden"
      style={{ borderTop: `3px solid ${accent}` }}
    >
      <div className="px-4 py-3">
        <div className="text-[10px] uppercase tracking-wider text-white/40">
          {label}
        </div>
        <div
          className="text-2xl font-bold mt-1 tabular-nums leading-none"
          style={{ color: accent }}
        >
          {loading ? "—" : fmtNumber(value ?? 0)}
        </div>
      </div>
    </div>
  );
}
