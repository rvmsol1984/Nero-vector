import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "../components/Avatar.jsx";
import EventCard from "../components/EventCard.jsx";
import StatRing from "../components/StatRing.jsx";
import { api } from "../api.js";
import { fmtNumber } from "../utils/format.js";
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
  const [err, setErr] = useState(null);
  const [tab, setTab] = useState("feed");

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const [s, bt, by, r] = await Promise.all([
          api.stats(),
          api.byTenant(),
          api.byType(),
          api.recent(25),
        ]);
        if (cancelled) return;
        setStats(s);
        setByTenant(bt);
        setByType(by);
        setRecent(r);
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

      {/* ----- tenant bubbles row ----- */}
      <div className="flex gap-3 overflow-x-auto pb-1 -mx-1 px-1">
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
                    <span className="truncate group-hover:text-primary-light transition-colors">
                      {row.event_type ?? "(none)"}
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
