import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";

import EventCard from "../components/EventCard.jsx";
import { api } from "../api.js";
import { getEventLabel } from "../utils/eventLabels.js";

const PAGE = 50;

// Workload → accent color mapping shared by the filter pills and
// the event card left-border tint.
const WORKLOAD_COLORS = {
  exchange:              "#F97316",
  mailitemsaccessed:     "#F97316",
  sharepoint:            "#14B8A6",
  onedrive:              "#14B8A6",
  onedriveforbusiness:   "#14B8A6",
  fileaccessed:          "#14B8A6",
  azureactivedirectory:  "#3B82F6",
  aad:                   "#3B82F6",
  microsoftgraphactivitylogs: "#3B82F6",
  microsoftteams:        "#8B5CF6",
  teams:                 "#8B5CF6",
  threatintelligence:    "#EF4444",
  inky:                  "#EF4444",
  edr:                   "#EF4444",
  defender:              "#EF4444",
  copilot:               "#79c0ff",
};

function workloadAccent(wl) {
  if (!wl) return null;
  return WORKLOAD_COLORS[wl.toLowerCase().replace(/\s/g, "")] || null;
}

function Pill({ active, onClick, children }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`px-3 py-1.5 rounded-xl text-xs font-medium whitespace-nowrap transition-all duration-200 active:scale-95 ${
        active
          ? "bg-primary text-white"
          : "bg-white/10 text-white/70 hover:bg-white/15"
      }`}
    >
      {children}
    </button>
  );
}

function WorkloadPill({ wl, active, onClick }) {
  const color = workloadAccent(wl);
  const label = wl || "(none)";
  if (!color) {
    return (
      <Pill active={active} onClick={onClick}>
        {label}
      </Pill>
    );
  }
  return (
    <button
      type="button"
      onClick={onClick}
      className="px-3 py-1.5 rounded-xl text-xs font-medium whitespace-nowrap transition-all duration-200 active:scale-95 border"
      style={
        active
          ? {
              color: "#fff",
              backgroundColor: color,
              borderColor: color,
            }
          : {
              color,
              backgroundColor: color + "18",
              borderColor: color + "44",
            }
      }
    >
      {label}
    </button>
  );
}

export default function Events() {
  const [searchParams, setSearchParams] = useSearchParams();

  const tenant    = searchParams.get("tenant")     || "";
  const eventType = searchParams.get("event_type") || "";
  const workload  = searchParams.get("workload")   || "";
  const userQuery = searchParams.get("user")       || "";
  const offset    = Number(searchParams.get("offset") || 0);

  const [userInput, setUserInput] = useState(userQuery);
  useEffect(() => {
    const h = setTimeout(() => {
      updateFilter("user", userInput);
    }, 300);
    return () => clearTimeout(h);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [userInput]);

  function updateFilter(key, value) {
    const p = new URLSearchParams(searchParams);
    if (value) p.set(key, value);
    else p.delete(key);
    p.delete("offset");
    setSearchParams(p, { replace: true });
  }

  function setOffset(n) {
    const p = new URLSearchParams(searchParams);
    if (n > 0) p.set("offset", String(n));
    else p.delete("offset");
    setSearchParams(p, { replace: true });
  }

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  const [byTenant, setByTenant] = useState([]);
  const [byType, setByType] = useState([]);
  const [byWorkload, setByWorkload] = useState([]);

  useEffect(() => {
    api.byTenant().then(setByTenant).catch(() => {});
    api.byType(200).then(setByType).catch(() => {});
    api.byWorkload().then(setByWorkload).catch(() => {});
  }, []);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    setErr(null);
    api
      .events({
        limit: PAGE,
        offset,
        tenant: tenant || undefined,
        event_type: eventType || undefined,
        workload: workload || undefined,
        user: userQuery || undefined,
      })
      .then((r) => {
        if (!cancel) setRows(r);
      })
      .catch((e) => {
        if (!cancel) setErr(e.message);
      })
      .finally(() => {
        if (!cancel) setLoading(false);
      });
    return () => {
      cancel = true;
    };
  }, [offset, tenant, eventType, workload, userQuery]);

  const activeCount = [tenant, eventType, workload, userQuery].filter(Boolean).length;

  return (
    <div className="space-y-4 animate-fade-in">
      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold">Events</h1>
          <p className="text-white/50 text-sm mt-1">
            Raw audit stream across all connected tenants.
          </p>
        </div>
        {activeCount > 0 && (
          <button
            type="button"
            onClick={() => {
              setUserInput("");
              setSearchParams({}, { replace: true });
            }}
            className="text-[11px] text-white/60 hover:text-white border border-white/10 bg-white/5 px-3 py-1.5 rounded-xl active:scale-95 transition-all"
          >
            clear ({activeCount})
          </button>
        )}
      </div>

      {/* ----- filter pills ----- */}
      <div className="space-y-2">
        <div className="flex items-center gap-2 flex-wrap">
          <div className="text-[10px] uppercase tracking-wider text-white/40 mr-1">
            tenant
          </div>
          <Pill active={!tenant} onClick={() => updateFilter("tenant", "")}>
            all
          </Pill>
          {byTenant.map((t) => (
            <Pill
              key={t.client_name}
              active={tenant === t.client_name}
              onClick={() => updateFilter("tenant", t.client_name)}
            >
              {t.client_name}
            </Pill>
          ))}
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          <div className="text-[10px] uppercase tracking-wider text-white/40 mr-1">
            workload
          </div>
          <Pill active={!workload} onClick={() => updateFilter("workload", "")}>
            all
          </Pill>
          {byWorkload.map((t) => (
            <WorkloadPill
              key={t.workload ?? ""}
              wl={t.workload}
              active={workload === (t.workload ?? "")}
              onClick={() => updateFilter("workload", t.workload ?? "")}
            />
          ))}
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          <div className="text-[10px] uppercase tracking-wider text-white/40 mr-1">
            event type
          </div>
          <select
            value={eventType}
            onChange={(e) => updateFilter("event_type", e.target.value)}
            className="bg-white/5 border border-white/10 rounded-xl px-3 py-1.5 text-xs text-white focus:outline-none focus:border-primary-light max-w-[240px]"
          >
            <option value="">all event types</option>
            {byType.map((t) => (
              <option key={t.event_type ?? ""} value={t.event_type ?? ""}>
                {t.event_type ? getEventLabel(t.event_type) : "(none)"}
              </option>
            ))}
          </select>

          <input
            type="search"
            placeholder="search user email…"
            value={userInput}
            onChange={(e) => setUserInput(e.target.value)}
            className="bg-white/5 border border-white/10 rounded-xl px-3 py-1.5 text-xs text-white placeholder:text-white/40 focus:outline-none focus:border-primary-light w-64"
          />
        </div>
      </div>

      {err && (
        <div className="card border-critical/30 text-critical text-sm px-4 py-3">
          load error: {err}
        </div>
      )}

      {/* ----- event cards ----- */}
      <div className="space-y-3 min-w-0 overflow-hidden">
        {rows.map((r) => {
          const accent = workloadAccent(r.workload || r.source);
          return (
            <div
              key={r.id}
              className="min-w-0 overflow-hidden"
              style={accent ? { borderLeft: `3px solid ${accent}`, borderRadius: 12 } : undefined}
            >
              <EventCard event={r} />
            </div>
          );
        })}
        {!loading && rows.length === 0 && (
          <div className="card text-white/50 text-sm text-center py-10">
            no events match current filter
          </div>
        )}
      </div>

      {/* ----- pagination ----- */}
      <div className="flex items-center justify-center gap-3 pt-2">
        <button
          type="button"
          disabled={offset === 0 || loading}
          onClick={() => setOffset(Math.max(0, offset - PAGE))}
          className="px-4 py-2 text-xs font-medium rounded-xl bg-white/5 border border-white/10 text-white/80 hover:bg-white/10 disabled:opacity-30 active:scale-95 transition-all"
        >
          prev
        </button>
        <span className="text-xs text-white/50 tabular-nums">
          offset {offset.toLocaleString("en-US")}
        </span>
        <button
          type="button"
          disabled={rows.length < PAGE || loading}
          onClick={() => setOffset(offset + PAGE)}
          className="px-4 py-2 text-xs font-medium rounded-xl bg-white/5 border border-white/10 text-white/80 hover:bg-white/10 disabled:opacity-30 active:scale-95 transition-all"
        >
          next
        </button>
      </div>
    </div>
  );
}
