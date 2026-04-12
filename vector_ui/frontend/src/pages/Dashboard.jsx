import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../api.js";
import StatCard from "../components/StatCard.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import TenantBadge from "../components/TenantBadge.jsx";
import { fmtNumber, fmtTime } from "../utils/format.js";

export default function Dashboard() {
  const [stats, setStats] = useState(null);
  const [byTenant, setByTenant] = useState([]);
  const [byType, setByType] = useState([]);
  const [recent, setRecent] = useState([]);
  const [err, setErr] = useState(null);

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
    <div className="space-y-6">
      <div>
        <h1 className="font-display text-2xl tracking-[0.2em]">OPERATIONS</h1>
        <p className="text-muted text-xs mt-1">
          Unified audit telemetry across managed tenants.
        </p>
      </div>

      {err && (
        <div className="border border-critical/40 bg-critical/10 text-critical text-xs px-3 py-2">
          load error: {err}
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
        <StatCard label="Total Events" value={fmtNumber(stats?.total_events)} />
        <StatCard label="Active Tenants" value={fmtNumber(stats?.unique_tenants)} />
        <StatCard
          label="Events · 24h"
          value={fmtNumber(stats?.events_24h)}
          hint="rolling window"
        />
        <StatCard label="Unique Users" value={fmtNumber(stats?.unique_users)} />
      </div>

      {/* breakdowns */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="bg-surface border border-border p-4">
          <div className="text-[10px] uppercase tracking-[0.25em] text-muted mb-3">
            Event Types
          </div>
          <div className="space-y-2">
            {byType.slice(0, 10).map((row) => (
              <Link
                key={row.event_type ?? "(none)"}
                to={`/events?event_type=${encodeURIComponent(row.event_type ?? "")}`}
                className="block group"
              >
                <div className="flex justify-between text-xs">
                  <span className="truncate group-hover:text-accent transition-colors">
                    {row.event_type ?? "(none)"}
                  </span>
                  <span className="text-muted">{fmtNumber(row.count)}</span>
                </div>
                <div className="h-1 bg-black/60 mt-1">
                  <div
                    className="h-1 bg-accent"
                    style={{ width: `${(Number(row.count) / maxType) * 100}%` }}
                  />
                </div>
              </Link>
            ))}
            {byType.length === 0 && (
              <div className="text-muted text-xs">no data</div>
            )}
          </div>
        </div>

        <div className="bg-surface border border-border p-4">
          <div className="text-[10px] uppercase tracking-[0.25em] text-muted mb-3">
            By Tenant
          </div>
          <div className="space-y-2">
            {byTenant.map((row) => (
              <Link
                key={row.client_name}
                to={`/events?tenant=${encodeURIComponent(row.client_name)}`}
                className="block group"
              >
                <div className="flex justify-between text-xs">
                  <span className="group-hover:text-accent transition-colors">
                    <TenantBadge name={row.client_name} />
                  </span>
                  <span className="text-muted">{fmtNumber(row.count)}</span>
                </div>
                <div className="h-1 bg-black/60 mt-1">
                  <div
                    className="h-1 bg-success"
                    style={{ width: `${(Number(row.count) / maxTenant) * 100}%` }}
                  />
                </div>
              </Link>
            ))}
            {byTenant.length === 0 && (
              <div className="text-muted text-xs">no data</div>
            )}
          </div>
        </div>
      </div>

      {/* recent feed */}
      <div className="bg-surface border border-border">
        <div className="px-4 py-3 border-b border-border text-[10px] uppercase tracking-[0.25em] text-muted flex items-center justify-between">
          <span>Recent Events</span>
          <Link to="/events" className="text-[10px] text-accent hover:underline">
            open events →
          </Link>
        </div>
        <div className="divide-y divide-border">
          {recent.map((ev) => (
            <div
              key={ev.id}
              className="px-4 py-2 flex items-center text-[11px] gap-4 hover:bg-white/[0.03]"
            >
              <span className="text-muted shrink-0 w-44 whitespace-nowrap">
                {fmtTime(ev.timestamp)}
              </span>
              <span className="shrink-0 w-40 truncate">
                <TenantBadge name={ev.client_name} />
              </span>
              <Link
                to={`/users/${encodeURIComponent(ev.entity_key)}`}
                className="shrink-0 w-56 truncate hover:text-accent"
                title={ev.user_id}
              >
                {ev.user_id}
              </Link>
              <Link
                to={`/events?event_type=${encodeURIComponent(ev.event_type ?? "")}`}
                className="text-slate-200 flex-1 truncate hover:text-accent"
              >
                {ev.event_type}
              </Link>
              <span className="shrink-0 text-muted w-36 truncate">
                {ev.workload}
              </span>
              <span className="shrink-0 w-24">
                <StatusBadge status={ev.result_status} />
              </span>
              <span className="shrink-0 text-muted w-32 truncate">
                {ev.client_ip ?? ""}
              </span>
            </div>
          ))}
          {recent.length === 0 && (
            <div className="px-4 py-6 text-muted text-xs text-center">
              no events yet — waiting for ingest
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
