import { Fragment, useEffect, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "../components/Avatar.jsx";
import TenantBadge from "../components/TenantBadge.jsx";
import { api } from "../api.js";
import { fmtNumber, fmtRelative, fmtTime } from "../utils/format.js";

// ---------------------------------------------------------------------------
// style maps
// ---------------------------------------------------------------------------

const SEVERITY_STYLE = {
  critical: { label: "CRITICAL", color: "#DC2626", solid: true },
  high:     { label: "HIGH",     color: "#F97316" },
  medium:   { label: "MEDIUM",   color: "#EAB308" },
  low:      { label: "LOW",      color: "#64748B" },
};

const STATUS_STYLE = {
  open:          { label: "OPEN",          color: "#3B82F6" },
  investigating: { label: "INVESTIGATING", color: "#EAB308" },
  contained:     { label: "CONTAINED",     color: "#10B981" },
  closed:        { label: "CLOSED",        color: "rgba(255,255,255,0.45)" },
};

// Per-source accent + short label used by the evidence timeline.
const SOURCE_STYLE = {
  ual:          { label: "UAL",          color: "#3B82F6" },
  inky:         { label: "INKY",         color: "#C084FC" },
  edr:          { label: "EDR",          color: "#F97316" },
  threatlocker: { label: "ThreatLocker", color: "#3B82F6" },
  defender:     { label: "Defender",     color: "#06B6D4" },
  ioc:          { label: "IOC",          color: "#EF4444" },
};

// ---------------------------------------------------------------------------
// tiny presentational helpers
// ---------------------------------------------------------------------------

function SeverityPill({ severity }) {
  const cfg = SEVERITY_STYLE[String(severity || "").toLowerCase()] || {
    label: String(severity || "—").toUpperCase(),
    color: "rgba(255,255,255,0.5)",
  };
  // Critical gets a solid red bg with white text + subtle pulse so
  // it jumps out of the row even at a glance.
  if (cfg.solid) {
    return (
      <span
        className="inline-flex items-center px-2.5 py-1 rounded-full text-[10px] font-semibold uppercase tracking-wider border whitespace-nowrap animate-pulse"
        style={{
          color: "#fff",
          backgroundColor: cfg.color,
          borderColor: cfg.color,
          animationDuration: "2s",
        }}
      >
        {cfg.label}
      </span>
    );
  }
  return (
    <span
      className="inline-flex items-center px-2.5 py-1 rounded-full text-[10px] font-semibold uppercase tracking-wider border whitespace-nowrap"
      style={{
        color: cfg.color,
        borderColor: cfg.color + "55",
        backgroundColor: cfg.color + "14",
      }}
    >
      {cfg.label}
    </span>
  );
}

function StatusBadge({ status }) {
  const cfg = STATUS_STYLE[String(status || "").toLowerCase()] || STATUS_STYLE.open;
  return (
    <span
      className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
      style={{
        color: cfg.color,
        borderColor: cfg.color + "55",
        backgroundColor: cfg.color + "14",
      }}
    >
      {cfg.label}
    </span>
  );
}

function IncidentTypeBadge({ incidentType }) {
  const label = String(incidentType || "incident").replace(/_/g, " ").toUpperCase();
  const color = "#8B5CF6";
  return (
    <span
      className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
      style={{
        color,
        borderColor: color + "55",
        backgroundColor: color + "14",
      }}
    >
      {label}
    </span>
  );
}

function SourceBadge({ source }) {
  const key = String(source || "").toLowerCase();
  const cfg = SOURCE_STYLE[key] || {
    label: key || "—",
    color: "rgba(255,255,255,0.5)",
  };
  return (
    <span
      className="inline-flex items-center px-1.5 py-[2px] text-[9px] font-bold uppercase tracking-wider rounded border whitespace-nowrap"
      style={{
        color: cfg.color,
        borderColor: cfg.color + "55",
        backgroundColor: cfg.color + "14",
      }}
    >
      {cfg.label}
    </span>
  );
}

function StatCard({ label, value, color, loading }) {
  return (
    <div className="bg-white/[0.03] border border-white/5 rounded-xl px-4 py-3">
      <div className="text-[10px] uppercase tracking-wider text-white/40">
        {label}
      </div>
      <div
        className="text-2xl font-bold mt-1 tabular-nums leading-none"
        style={{ color }}
      >
        {loading ? "—" : fmtNumber(value || 0)}
      </div>
    </div>
  );
}

function ScoreBar({ score, size = "sm" }) {
  const n = Number(score || 0);
  const pct = Math.min(100, Math.max(0, n));
  const color =
    n >= 90 ? "#EF4444" : n >= 80 ? "#F97316" : n >= 60 ? "#EAB308" : "#3B82F6";
  const isSm = size === "sm";
  return (
    <div className="flex items-center gap-2">
      <div
        className={`flex-1 bg-white/10 rounded-full overflow-hidden ${
          isSm ? "h-1.5 min-w-[36px]" : "h-2 min-w-[48px]"
        }`}
      >
        <div
          className="h-full rounded-full transition-all"
          style={{ width: `${pct}%`, background: color }}
        />
      </div>
      <span
        className={`text-right tabular-nums font-bold whitespace-nowrap ${
          isSm ? "text-[12px]" : "text-sm"
        }`}
        style={{ color }}
      >
        {score ?? "—"}
      </span>
    </div>
  );
}

function Chevron({ open }) {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      className="text-white/40 transition-transform duration-200"
      style={{ transform: open ? "rotate(180deg)" : "rotate(0deg)" }}
    >
      <polyline points="6 9 12 15 18 9" />
    </svg>
  );
}

// ---------------------------------------------------------------------------
// main page
// ---------------------------------------------------------------------------

export default function Incidents() {
  const [stats, setStats] = useState(null);
  const [rows, setRows] = useState(null);
  const [err, setErr] = useState(null);
  const [openId, setOpenId] = useState(null);

  useEffect(() => {
    let cancel = false;

    async function load() {
      try {
        const [s, list] = await Promise.all([
          api.incidentStats(),
          api.incidents({ limit: 100 }),
        ]);
        if (cancel) return;
        setStats(s);
        setRows(list || []);
        setErr(null);
      } catch (e) {
        if (!cancel) setErr(String(e.message || e));
      }
    }

    load();
    const t = setInterval(load, 30000);
    return () => {
      cancel = true;
      clearInterval(t);
    };
  }, []);

  // Called by IncidentDetail to update a single row's status
  // in place without refetching the entire list.
  function applyStatusChange(id, patch) {
    setRows((prev) =>
      Array.isArray(prev)
        ? prev.map((r) => (r.id === id ? { ...r, ...patch } : r))
        : prev,
    );
    // Refresh stats in the background so the header cards flip.
    api.incidentStats().then(setStats).catch(() => {});
  }

  const hasRows = Array.isArray(rows) && rows.length > 0;
  const openCount = stats?.open ?? (hasRows ? rows.filter((r) => r.status === "open").length : 0);

  return (
    <div className="space-y-5 animate-fade-in">
      {/* ----- header ----- */}
      <div className="flex items-center gap-3 flex-wrap">
        <h1 className="text-2xl font-bold">Incidents</h1>
        {stats && (
          <span
            className="inline-flex items-center px-2.5 py-1 rounded-full text-[10px] font-semibold uppercase tracking-wider border whitespace-nowrap tabular-nums"
            style={{
              color: "#EF4444",
              borderColor: "#EF444455",
              backgroundColor: "#EF444414",
            }}
          >
            {fmtNumber(openCount)} OPEN
          </span>
        )}
      </div>
      <p className="text-white/50 text-sm -mt-3">
        Confirmed incidents produced by the Phase 2 scoring engine.
      </p>

      {err && (
        <div className="card border-critical/30 text-critical text-sm px-4 py-3">
          load error: {err}
        </div>
      )}

      {/* ----- stat cards ----- */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard
          label="Open"
          value={stats?.open}
          color="#3B82F6"
          loading={stats == null}
        />
        <StatCard
          label="Critical"
          value={stats?.critical}
          color="#EF4444"
          loading={stats == null}
        />
        <StatCard
          label="High"
          value={stats?.high}
          color="#F97316"
          loading={stats == null}
        />
        <StatCard
          label="Confirmed Today"
          value={stats?.today}
          color="#10B981"
          loading={stats == null}
        />
      </div>

      {/* ----- incidents table ----- */}
      {rows == null ? (
        <div className="card py-12 text-center text-white/40 text-sm">
          loading…
        </div>
      ) : rows.length === 0 ? (
        <EmptyState />
      ) : (
        <IncidentsTable
          rows={rows}
          openId={openId}
          setOpenId={setOpenId}
          onStatusChange={applyStatusChange}
        />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// empty state
// ---------------------------------------------------------------------------

function EmptyState() {
  return (
    <div className="card py-16 flex flex-col items-center text-white/60 text-sm gap-3">
      <svg width="56" height="56" viewBox="0 0 48 48" fill="none">
        <path
          d="M24 4 L8 10 V22 C8 32 16 40 24 44 C32 40 40 32 40 22 V10 Z"
          stroke="#10B981"
          strokeWidth="2"
          strokeLinejoin="round"
          fill="rgba(16,185,129,0.08)"
        />
        <path
          d="M16 24 l6 6 l10-12"
          stroke="#10B981"
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          fill="none"
        />
      </svg>
      <div className="font-semibold">No open incidents — all clear</div>
      <div className="text-white/40 text-[11px]">
        New incidents appear here the moment the scoring engine confirms them.
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// incidents table (with in-place row expand)
// ---------------------------------------------------------------------------

function IncidentsTable({ rows, openId, setOpenId, onStatusChange }) {
  return (
    <div className="card overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full text-[11px]">
          <thead>
            <tr>
              <Th>Severity</Th>
              <Th>Type</Th>
              <Th>Tenant</Th>
              <Th>User</Th>
              <Th>Title</Th>
              <Th align="right">Score</Th>
              <Th>First Seen</Th>
              <Th>Status</Th>
              <Th>{""}</Th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {rows.map((row) => {
              const isOpen = openId === row.id;
              return (
                <Fragment key={row.id}>
                  <tr
                    onClick={() => setOpenId(isOpen ? null : row.id)}
                    className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                  >
                    <td className="px-4 py-2.5">
                      <SeverityPill severity={row.severity} />
                    </td>
                    <td className="px-4 py-2.5">
                      <IncidentTypeBadge incidentType={row.incident_type} />
                    </td>
                    <td className="px-4 py-2.5">
                      {row.client_name ? (
                        <TenantBadge name={row.client_name} />
                      ) : (
                        <span className="text-white/30">—</span>
                      )}
                    </td>
                    <td className="px-4 py-2.5">
                      {row.entity_key ? (
                        <Link
                          to={`/users/${encodeURIComponent(row.entity_key)}`}
                          onClick={(e) => e.stopPropagation()}
                          className="flex items-center gap-2 hover:text-primary-light"
                          title={row.user_id || row.entity_key}
                        >
                          <Avatar
                            email={row.user_id}
                            tenant={row.client_name}
                            size={26}
                          />
                          <span className="truncate max-w-[220px]">
                            {row.user_id || row.entity_key}
                          </span>
                        </Link>
                      ) : (
                        <div className="flex items-center gap-2">
                          <Avatar
                            email={row.user_id}
                            tenant={row.client_name}
                            size={26}
                          />
                          <span className="truncate max-w-[220px]">
                            {row.user_id || "—"}
                          </span>
                        </div>
                      )}
                    </td>
                    <td
                      className="px-4 py-2.5 text-white/80 truncate max-w-[360px]"
                      title={row.title || ""}
                    >
                      {row.title || <span className="text-white/30">—</span>}
                    </td>
                    <td className="px-4 py-2.5 min-w-[100px]">
                      <ScoreBar score={row.score} size="sm" />
                    </td>
                    <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                      {fmtRelative(row.first_seen || row.confirmed_at)}
                    </td>
                    <td className="px-4 py-2.5">
                      <StatusBadge status={row.status} />
                    </td>
                    <td className="px-3 py-2.5 w-8 text-white/40">
                      <Chevron open={isOpen} />
                    </td>
                  </tr>
                  {isOpen && (
                    <tr>
                      <td
                        colSpan={9}
                        className="p-0 border-t border-white/5"
                        style={{ backgroundColor: "#0D1428" }}
                      >
                        <IncidentDetail
                          incident={row}
                          onStatusChange={onStatusChange}
                        />
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Th({ children, align = "left" }) {
  return (
    <th
      className={`px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold ${
        align === "right" ? "text-right" : "text-left"
      }`}
    >
      {children}
    </th>
  );
}

// ---------------------------------------------------------------------------
// expanded incident detail panel
// ---------------------------------------------------------------------------

function IncidentDetail({ incident, onStatusChange }) {
  const [saving, setSaving] = useState(null);
  const [saveErr, setSaveErr] = useState(null);
  const [tab, setTab] = useState("timeline");

  // Parse the evidence array. Scoring engine writes this as a JSON array
  // of signal objects; be defensive against either a JSON string or a
  // real array coming back from Postgres.
  const rawEvidence = incident.evidence;
  const evidence = Array.isArray(rawEvidence)
    ? rawEvidence
    : rawEvidence && typeof rawEvidence === "object"
    ? Object.values(rawEvidence)
    : [];

  async function changeStatus(next) {
    if (saving) return;
    setSaving(next);
    setSaveErr(null);
    try {
      const result = await api.updateIncidentStatus(incident.id, next);
      onStatusChange?.(incident.id, {
        status: result?.status || next,
        contained_at: result?.contained_at ?? incident.contained_at,
        updated_at: result?.updated_at ?? incident.updated_at,
      });
    } catch (e) {
      setSaveErr(String(e.message || e));
    } finally {
      setSaving(null);
    }
  }

  const statusButtons = [
    { id: "investigating", label: "Investigating", color: "#EAB308" },
    { id: "contained",     label: "Contained",     color: "#10B981" },
    { id: "closed",        label: "Close",         color: "rgba(255,255,255,0.45)" },
  ];

  const TABS = [
    { id: "timeline",    label: "Timeline"    },
    { id: "impact",      label: "Impact"      },
    { id: "containment", label: "Containment" },
    { id: "crimescene",  label: "CrimeScene"  },
  ];

  return (
    <div className="px-5 py-5 space-y-4 animate-fade-in">
      {/* ----- user + meta header ----- */}
      <div className="flex items-start gap-4 flex-wrap">
        <div className="flex items-center gap-3 min-w-0">
          <Avatar email={incident.user_id} tenant={incident.client_name} size={44} />
          <div className="min-w-0">
            <div className="font-semibold text-sm truncate max-w-[320px]">
              {incident.user_id || incident.entity_key || "unknown user"}
            </div>
            <div className="mt-0.5 flex items-center gap-2 flex-wrap">
              {incident.client_name && <TenantBadge name={incident.client_name} />}
              <SeverityPill severity={incident.severity} />
              <IncidentTypeBadge incidentType={incident.incident_type} />
              <StatusBadge status={incident.status} />
            </div>
          </div>
        </div>

        <div className="ml-auto flex items-center gap-4 text-[11px]">
          <div className="text-right min-w-[100px]">
            <div className="text-white/40 uppercase tracking-wider text-[9px] mb-1">
              Score
            </div>
            <ScoreBar score={incident.score} size="lg" />
          </div>
          <div className="text-right">
            <div className="text-white/40 uppercase tracking-wider text-[9px]">
              Confirmed
            </div>
            <div className="mt-1 tabular-nums">
              {fmtTime(incident.confirmed_at)}
            </div>
          </div>
          {incident.dwell_time_minutes != null && (
            <div className="text-right">
              <div className="text-white/40 uppercase tracking-wider text-[9px]">
                Dwell
              </div>
              <div className="mt-1 tabular-nums">
                {fmtNumber(incident.dwell_time_minutes)} min
              </div>
            </div>
          )}
        </div>
      </div>

      {/* ----- summary ----- */}
      {incident.summary && (
        <div className="text-[12px] text-white/70 leading-relaxed">
          {incident.summary}
        </div>
      )}

      {/* ----- action buttons (moved to header per spec) ----- */}
      <div className="pt-2 border-t border-white/10 flex items-center gap-3 flex-wrap">
        {incident.entity_key && (
          <Link
            to={`/users/${encodeURIComponent(incident.entity_key)}`}
            onClick={(e) => e.stopPropagation()}
            className="px-4 py-1.5 text-[11px] font-semibold rounded-xl bg-primary/15 border border-primary/40 text-primary-light hover:bg-primary/25 active:scale-95 transition-all"
          >
            View User →
          </Link>
        )}
        <div className="ml-auto flex items-center gap-2">
          {statusButtons.map((btn) => {
            const isCurrent = incident.status === btn.id;
            const isBusy = saving === btn.id;
            return (
              <button
                key={btn.id}
                type="button"
                disabled={isCurrent || !!saving}
                onClick={(e) => {
                  e.stopPropagation();
                  changeStatus(btn.id);
                }}
                className="px-3 py-1.5 text-[11px] font-semibold rounded-xl border transition-all active:scale-95 disabled:opacity-40 disabled:cursor-not-allowed"
                style={{
                  color: btn.color,
                  borderColor: btn.color + "55",
                  backgroundColor: isCurrent ? btn.color + "22" : btn.color + "10",
                }}
              >
                {isBusy ? "saving…" : btn.label}
              </button>
            );
          })}
        </div>
      </div>

      {saveErr && (
        <div className="text-[11px] text-critical">
          status update failed: {saveErr}
        </div>
      )}

      {/* ----- 4-tab investigation view ----- */}
      <div
        className="flex items-center gap-1 border-b border-white/10"
        onClick={(e) => e.stopPropagation()}
      >
        {TABS.map((t) => {
          const active = tab === t.id;
          return (
            <button
              key={t.id}
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                setTab(t.id);
              }}
              className={`px-4 py-2 text-[11px] uppercase tracking-wider font-semibold border-b-2 -mb-px whitespace-nowrap transition-colors ${
                active
                  ? "border-primary text-primary-light"
                  : "border-transparent text-white/50 hover:text-white"
              }`}
            >
              {t.label}
            </button>
          );
        })}
      </div>

      {tab === "timeline"    && <TimelineTab    incident={incident} />}
      {tab === "impact"      && <ImpactTab      incident={incident} />}
      {tab === "containment" && <ContainmentTab incident={incident} />}
      {tab === "crimescene"  && <CrimeSceneTab  incident={incident} />}

      {/* ----- detection signals (below tabs, always visible) ----- */}
      <div className="pt-4 border-t border-white/10">
        <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
          Detection Signals
        </div>
        {evidence.length === 0 ? (
          <div className="text-white/40 text-[11px] py-2">
            no evidence recorded
          </div>
        ) : (
          <div
            className="rounded-lg border border-white/5 divide-y divide-white/5"
            style={{ backgroundColor: "rgba(255,255,255,0.015)" }}
          >
            {evidence.map((sig, i) => (
              <EvidenceRow key={sig.id || i} signal={sig} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab 1: Timeline -- chronological event list for the user during the
// incident dwell window
// ---------------------------------------------------------------------------

function workloadBadgeStyle(workload, eventType) {
  const wl = String(workload || "").toLowerCase();
  const et = String(eventType || "").toLowerCase();
  if (wl.includes("exchange") || et.includes("mail")) {
    return { color: "#F97316", label: `Email: ${eventType || "event"}` };
  }
  if (wl.includes("sharepoint") || wl.includes("onedrive")) {
    return { color: "#14B8A6", label: `File: ${eventType || "event"}` };
  }
  if (wl.includes("azureactivedirectory") || wl.includes("azuread")) {
    return { color: "#3B82F6", label: `Login: ${eventType || "event"}` };
  }
  if (wl.includes("teams")) {
    return { color: "#8B5CF6", label: `Teams: ${eventType || "event"}` };
  }
  if (wl.includes("threat") || wl === "inky" || wl === "edr") {
    return { color: "#EF4444", label: `Phish: ${eventType || "event"}` };
  }
  return {
    color: "rgba(255,255,255,0.4)",
    label: `${workload || "Event"}: ${eventType || "event"}`,
  };
}

function ActionBadge({ workload, eventType }) {
  const cfg = workloadBadgeStyle(workload, eventType);
  return (
    <span
      className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
      style={{
        color: cfg.color,
        borderColor: cfg.color + "55",
        backgroundColor: cfg.color + "14",
      }}
    >
      {cfg.label}
    </span>
  );
}

function TimelineTab({ incident }) {
  const [rows, setRows] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    if (!incident?.user_id) {
      setRows([]);
      return;
    }
    let cancel = false;
    setRows(null);
    setErr(null);
    api
      .events({ user: incident.user_id, limit: 100 })
      .then((r) => { if (!cancel) setRows(r || []); })
      .catch((e) => {
        if (!cancel) { setRows([]); setErr(String(e.message || e)); }
      });
    return () => { cancel = true; };
  }, [incident?.id, incident?.user_id]);

  if (rows === null) {
    return <div className="text-white/40 text-[11px] py-4">loading timeline…</div>;
  }
  if (err) {
    return <div className="text-white/40 text-[11px] py-4">could not load timeline</div>;
  }
  if (rows.length === 0) {
    return <div className="text-white/40 text-[11px] py-4">no events in the incident window</div>;
  }

  return (
    <div
      className="rounded-lg border border-white/5 overflow-hidden"
      style={{ backgroundColor: "rgba(255,255,255,0.015)" }}
    >
      <div className="overflow-x-auto">
        <table className="min-w-full text-[10px]">
          <thead>
            <tr>
              <Th11>Time</Th11>
              <Th11>Action</Th11>
              <Th11>Target</Th11>
              <Th11>IP</Th11>
              <Th11>Location</Th11>
              <Th11>ISP</Th11>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {rows.map((r) => (
              <tr key={r.id} className="hover:bg-white/[0.02]">
                <td className="px-2 py-1.5 text-white/60 whitespace-nowrap tabular-nums"
                    title={r.timestamp}>
                  {fmtRelative(r.timestamp)}
                </td>
                <td className="px-2 py-1.5">
                  <ActionBadge workload={r.workload} eventType={r.event_type} />
                </td>
                <td className="px-2 py-1.5 text-white/70 truncate max-w-[260px]">
                  {timelineTarget(r) || <span className="text-white/30">—</span>}
                </td>
                <td className="px-2 py-1.5 text-white/60 font-mono tabular-nums whitespace-nowrap">
                  {r.client_ip || <span className="text-white/30">—</span>}
                </td>
                <td className="px-2 py-1.5 text-white/60 whitespace-nowrap">
                  {timelineLocation(r) || <span className="text-white/30">—</span>}
                </td>
                <td className="px-2 py-1.5 text-white/50 truncate max-w-[180px]">
                  {timelineIsp(r) || <span className="text-white/30">—</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function timelineTarget(r) {
  const raw = r.raw_json || {};
  return (
    raw.DestinationFileName ||
    raw.ObjectId ||
    raw.Subject ||
    r.result_status ||
    null
  );
}

function timelineLocation(r) {
  const raw = r.raw_json || {};
  const city = raw.City || raw.geo_city || "";
  const country = raw.Country || raw.geo_country || "";
  const parts = [city, country].filter(Boolean);
  return parts.join(", ") || null;
}

function timelineIsp(r) {
  const raw = r.raw_json || {};
  return raw.ASN || raw.geo_asn_org || null;
}

// ---------------------------------------------------------------------------
// Tab 2: Impact -- what the attacker accessed / sent / modified / deleted
// ---------------------------------------------------------------------------

function ImpactTab({ incident }) {
  const [data, setData] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    if (!incident?.id) return;
    let cancel = false;
    setData(null);
    setErr(null);
    fetch(`/api/incidents/${encodeURIComponent(incident.id)}/impact`, {
      credentials: "same-origin",
      headers: {
        Accept: "application/json",
        ...(localStorage.getItem("vector_token")
          ? { Authorization: `Bearer ${localStorage.getItem("vector_token")}` }
          : {}),
      },
    })
      .then((r) => (r.ok ? r.json() : {}))
      .then((d) => { if (!cancel) setData(d || {}); })
      .catch((e) => {
        if (!cancel) { setData({}); setErr(String(e.message || e)); }
      });
    return () => { cancel = true; };
  }, [incident?.id]);

  if (data === null) {
    return <div className="text-white/40 text-[11px] py-4">loading impact…</div>;
  }

  const buckets = [
    { key: "accessed", label: "Accessed", color: "#3B82F6", icon: "👁" },
    { key: "sent",     label: "Sent",     color: "#F97316", icon: "📤" },
    { key: "modified", label: "Modified", color: "#EAB308", icon: "✎" },
    { key: "deleted",  label: "Deleted",  color: "#EF4444", icon: "🗑" },
  ];

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {buckets.map((b) => {
          const bucket = data[b.key] || {};
          const total = (bucket.emails || 0) + (bucket.files || 0);
          return (
            <div
              key={b.key}
              className="rounded-xl border px-4 py-3"
              style={{
                borderColor: b.color + "55",
                backgroundColor: b.color + "0D",
              }}
            >
              <div className="flex items-center gap-2">
                <span className="text-base" aria-hidden="true">{b.icon}</span>
                <span
                  className="text-[10px] uppercase tracking-wider font-semibold"
                  style={{ color: b.color }}
                >
                  {b.label}
                </span>
              </div>
              <div
                className="text-2xl font-bold mt-1 tabular-nums leading-none"
                style={{ color: b.color }}
              >
                {fmtNumber(total)}
              </div>
              <div className="text-[10px] text-white/50 mt-1">
                {fmtNumber(bucket.emails || 0)} emails · {fmtNumber(bucket.files || 0)} files
              </div>
            </div>
          );
        })}
      </div>

      {err && (
        <div className="text-[11px] text-white/40">
          impact data partially unavailable
        </div>
      )}

      <ImpactEventsTable data={data} />
    </div>
  );
}

function ImpactEventsTable({ data }) {
  const rows = [];
  const buckets = ["accessed", "sent", "modified", "deleted"];
  for (const b of buckets) {
    for (const e of (data[b]?.events || [])) {
      rows.push({ ...e, _bucket: b });
    }
  }
  rows.sort((a, b) => {
    const ta = a.timestamp || a.received || "";
    const tb = b.timestamp || b.received || "";
    return tb.localeCompare(ta);
  });
  if (rows.length === 0) {
    return (
      <div className="text-white/40 text-[11px] py-4">
        no impact events recorded in the dwell window
      </div>
    );
  }
  return (
    <div
      className="rounded-lg border border-white/5 overflow-hidden"
      style={{ backgroundColor: "rgba(255,255,255,0.015)" }}
    >
      <div className="overflow-x-auto">
        <table className="min-w-full text-[10px]">
          <thead>
            <tr>
              <Th11>Time</Th11>
              <Th11>Bucket</Th11>
              <Th11>Action</Th11>
              <Th11>Subject / File</Th11>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {rows.slice(0, 50).map((r, i) => {
              const ts = r.timestamp || r.received;
              const isEmail = r.subject != null || r._bucket === "sent";
              const subj = isEmail
                ? (r.subject || "(no subject)")
                : (r.raw_json?.DestinationFileName
                    || r.raw_json?.ObjectId
                    || "(no target)");
              return (
                <tr key={r.id || i} className="hover:bg-white/[0.02]">
                  <td className="px-2 py-1.5 text-white/60 whitespace-nowrap tabular-nums">
                    {fmtRelative(ts)}
                  </td>
                  <td className="px-2 py-1.5">
                    <BucketPill bucket={r._bucket} />
                  </td>
                  <td className="px-2 py-1.5">
                    {isEmail
                      ? <ActionBadge workload="Exchange" eventType={r._bucket === "sent" ? "Send" : "Access"} />
                      : <ActionBadge workload={r.workload} eventType={r.event_type} />
                    }
                  </td>
                  <td className="px-2 py-1.5 text-white/70 truncate max-w-[320px]"
                      title={subj}>
                    {subj}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function BucketPill({ bucket }) {
  const colors = {
    accessed: "#3B82F6",
    sent:     "#F97316",
    modified: "#EAB308",
    deleted:  "#EF4444",
  };
  const color = colors[bucket] || "rgba(255,255,255,0.4)";
  return (
    <span
      className="inline-flex items-center px-1.5 py-[2px] text-[9px] font-bold uppercase tracking-wider rounded border whitespace-nowrap"
      style={{
        color,
        borderColor: color + "55",
        backgroundColor: color + "14",
      }}
    >
      {bucket}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Tab 3: Containment -- 6-step remediation checklist (Phase 4)
// ---------------------------------------------------------------------------

function ContainmentTab({ incident }) {
  const steps = [
    {
      n: 1, name: "Revoke Sessions",
      desc: "Sign out all active sessions via Graph API.",
    },
    {
      n: 2, name: "Disable Account",
      desc: "Temporarily disable the Entra ID account.",
    },
    {
      n: 3, name: "Reset Password",
      desc: "Force password reset on next login.",
    },
    {
      n: 4, name: "Remove Inbox Rules",
      desc: "Delete any suspicious inbox rules created during incident.",
    },
    {
      n: 5, name: "Revoke OAuth Apps",
      desc: "Remove OAuth app consents created during incident.",
    },
    {
      n: 6, name: "Block IP",
      desc: "Add attacker IP to Conditional Access named locations blocklist.",
    },
  ];
  return (
    <div className="space-y-3">
      <div
        className="rounded-lg border border-white/10 px-4 py-2 text-[11px] text-white/60"
        style={{ backgroundColor: "rgba(234,179,8,0.08)" }}
      >
        Automated containment coming in Phase 4. Actions below are
        disabled for now.
      </div>
      <div className="rounded-lg border border-white/5 divide-y divide-white/5"
           style={{ backgroundColor: "rgba(255,255,255,0.015)" }}>
        {steps.map((s) => (
          <div key={s.n} className="flex items-start gap-3 px-4 py-3">
            <div
              className="shrink-0 h-7 w-7 rounded-full flex items-center justify-center text-[11px] font-bold"
              style={{
                color: "#3B82F6",
                background: "rgba(37,99,235,0.15)",
                border: "1px solid rgba(37,99,235,0.45)",
              }}
            >
              {s.n}
            </div>
            <div className="flex-1 min-w-0">
              <div className="text-sm font-semibold text-white">{s.name}</div>
              <div className="text-[11px] text-white/50 mt-0.5">{s.desc}</div>
            </div>
            <div className="flex items-center gap-2 text-[10px]">
              <span
                className="inline-flex items-center px-2 py-[3px] font-semibold uppercase tracking-wider rounded-md border whitespace-nowrap"
                style={{
                  color: "rgba(255,255,255,0.5)",
                  borderColor: "rgba(255,255,255,0.15)",
                  backgroundColor: "rgba(255,255,255,0.05)",
                }}
              >
                PENDING
              </span>
              <span className="text-white/30 tabular-nums w-20 text-right">
                —
              </span>
              <button
                type="button"
                disabled
                className="px-3 py-1 text-[11px] font-semibold rounded-xl border text-white/30 bg-white/[0.02] border-white/10 cursor-not-allowed"
              >
                Run
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab 4: CrimeScene -- AI-written report placeholder (Phase 3)
// ---------------------------------------------------------------------------

function CrimeSceneTab({ incident }) {
  return (
    <div className="space-y-4">
      <div
        className="rounded-xl border p-6 flex items-start gap-4"
        style={{
          borderColor: "rgba(139,92,246,0.4)",
          backgroundColor: "rgba(139,92,246,0.06)",
        }}
      >
        <div
          className="shrink-0 h-12 w-12 rounded-xl flex items-center justify-center text-2xl"
          style={{
            background: "rgba(139,92,246,0.15)",
            border: "1px solid rgba(139,92,246,0.45)",
          }}
          aria-hidden="true"
        >
          📄
        </div>
        <div className="flex-1 min-w-0">
          <div className="text-base font-bold text-white">
            Generate CrimeScene Report
          </div>
          <div className="text-[12px] text-white/60 mt-1 leading-relaxed">
            AI-written executive narrative delivered as PDF. Summarizes
            the incident, attacker behavior, dwell time, and
            remediation steps.
          </div>
          <div className="mt-3 flex items-center gap-3">
            <button
              type="button"
              disabled
              className="px-4 py-2 text-[12px] font-semibold rounded-xl border cursor-not-allowed"
              style={{
                color: "#C4B5FD",
                borderColor: "rgba(139,92,246,0.45)",
                backgroundColor: "rgba(139,92,246,0.18)",
              }}
            >
              Generate Report
            </button>
            <span
              className="inline-flex items-center px-2.5 py-1 rounded-full text-[10px] font-semibold uppercase tracking-wider border whitespace-nowrap"
              style={{
                color: "#3B82F6",
                borderColor: "rgba(37,99,235,0.4)",
                backgroundColor: "rgba(37,99,235,0.14)",
              }}
            >
              Coming in Phase 3
            </span>
          </div>
        </div>
      </div>

      {incident.summary && (
        <div>
          <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
            Report preview (narrative stub)
          </div>
          <div
            className="rounded-lg border border-white/5 px-4 py-3 text-[12px] text-white/70 leading-relaxed"
            style={{ backgroundColor: "rgba(255,255,255,0.015)" }}
          >
            {incident.summary}
          </div>
        </div>
      )}
    </div>
  );
}

function Th11({ children }) {
  return (
    <th className="text-left px-2 py-1.5 text-[9px] uppercase tracking-wider text-white/40 font-semibold bg-white/[0.02]">
      {children}
    </th>
  );
}

// ---------------------------------------------------------------------------
// evidence row
// ---------------------------------------------------------------------------
//
// Each evidence object produced by the scoring engine is an open-ended
// dict; normalize a few common field names so we can render the row
// regardless of which signal type produced it.

// Per-rule icon + border color so the evidence timeline looks
// visually scannable at a glance. Rules that aren't in the map
// get a generic lightning bolt + grey border.
const RULE_STYLE = {
  NewCountryLogin:        { icon: "🌍", color: "#3B82F6" },
  NewCountryLoginRule:    { icon: "🌍", color: "#3B82F6" },
  OffHoursLogin:          { icon: "🕐", color: "#8B5CF6" },
  OffHoursLoginRule:      { icon: "🕐", color: "#8B5CF6" },
  HighVolumeFileAccess:   { icon: "📁", color: "#14B8A6" },
  HighVolumeFileAccessRule: { icon: "📁", color: "#14B8A6" },
  SuspiciousMailbox:      { icon: "📧", color: "#F97316" },
  SuspiciousMailboxRule:  { icon: "📧", color: "#F97316" },
  MalwareDetected:        { icon: "☠️", color: "#DC2626" },
  MalwareDetectedRule:    { icon: "☠️", color: "#DC2626" },
  IOCMatch:               { icon: "🎯", color: "#EF4444" },
  IOCMatchRule:           { icon: "🎯", color: "#EF4444" },
  HighRiskCountryLogin:   { icon: "⚠", color: "#DC2626" },
  HighRiskCountryLoginRule: { icon: "⚠", color: "#DC2626" },
};
const DEFAULT_RULE_STYLE = { icon: "⚡", color: "#6B7280" };

function EvidenceRow({ signal }) {
  if (!signal || typeof signal !== "object") return null;
  const ruleName = signal.rule || signal.name || signal.event_type || "";
  const score = Number(signal.score ?? signal.weight ?? signal.points ?? 0);
  const description =
    signal.description ||
    signal.significance ||
    signal.name ||
    signal.rule ||
    signal.event_type ||
    "signal";
  const ts = signal.timestamp || signal.time || signal.event_time || signal.added_at;
  const style = RULE_STYLE[ruleName] || DEFAULT_RULE_STYLE;

  return (
    <div
      className="px-3 py-2 flex items-start gap-3 text-[11px]"
      style={{ borderLeft: `3px solid ${style.color}` }}
    >
      <span className="text-base leading-none shrink-0 mt-px" aria-hidden="true">
        {style.icon}
      </span>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-white/80 font-semibold truncate" title={ruleName}>
            {ruleName || description}
          </span>
          <SourceBadge source={signal.source || signal.event_source || signal.kind} />
        </div>
        {signal.detail && (
          <div
            className="text-white/40 text-[10px] truncate mt-0.5"
            title={signal.detail}
          >
            {signal.detail}
          </div>
        )}
      </div>
      {score > 0 && (
        <div
          className="font-bold tabular-nums whitespace-nowrap"
          style={{ color: style.color }}
        >
          +{score}
        </div>
      )}
      {ts && (
        <div className="text-white/40 tabular-nums whitespace-nowrap">
          {fmtRelative(ts)}
        </div>
      )}
    </div>
  );
}
