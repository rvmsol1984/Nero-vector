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
  critical: { label: "CRITICAL", color: "#EF4444" },
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
  const [statusFilter, setStatusFilter] = useState("open");
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

      {/* ----- status filter ----- */}
      <div className="flex gap-2">
        {["open", "all", "closed"].map((f) => (
          <button
            key={f}
            onClick={() => setStatusFilter(f)}
            className={`px-3 py-1 rounded-xl text-xs font-semibold uppercase tracking-wider transition-all ${
              statusFilter === f
                ? "bg-primary text-white"
                : "bg-white/5 text-white/50 hover:bg-white/10"
            }`}
          >
            {f === "all" ? "All" : f === "open" ? "Open" : "Closed"}
          </button>
        ))}
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
          rows={statusFilter === "all" ? rows : rows.filter(r => r.status === statusFilter)}
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
                        colSpan={8}
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

      {/* ----- evidence timeline ----- */}
      <div>
        <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
          Evidence timeline
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

      {/* ----- action buttons ----- */}
      <div className="pt-3 border-t border-white/10 flex items-center gap-3 flex-wrap">
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
    </div>
  );
}

// ---------------------------------------------------------------------------
// evidence row
// ---------------------------------------------------------------------------
//
// Each evidence object produced by the scoring engine is an open-ended
// dict; normalize a few common field names so we can render the row
// regardless of which signal type produced it.

function EvidenceRow({ signal }) {
  if (!signal || typeof signal !== "object") return null;
  const source = signal.source || signal.event_source || signal.kind || null;
  const score = Number(signal.score ?? signal.weight ?? signal.points ?? 0);
  const description =
    signal.description ||
    signal.significance ||
    signal.name ||
    signal.rule ||
    signal.event_type ||
    "signal";
  const ts = signal.timestamp || signal.time || signal.event_time || signal.added_at;
  // Extract nested evidence details from scoring engine signal
  const ev = signal.evidence || {};
  const details = [];
  if (ev.new_country)        details.push(`Country: ${ev.new_country}`);
  if (ev.baseline_countries) details.push(`Baseline: ${(ev.baseline_countries || []).join(", ")}`);
  if (ev.ip || ev.client_ip) details.push(`IP: ${ev.ip || ev.client_ip}`);
  if (ev.ioc_value)          details.push(`IOC: ${ev.ioc_value}`);
  if (ev.confidence)         details.push(`Confidence: ${ev.confidence}`);
  if (ev.hour !== undefined) details.push(`Hour: ${ev.hour}:00 UTC`);
  if (ev.login_time)         details.push(`Login: ${ev.login_time}`);
  if (ev.file_count)         details.push(`Files: ${ev.file_count}`);
  if (ev.threat_type)        details.push(`Type: ${ev.threat_type}`);
  const detailStr = signal.detail || details.join(" · ");
  return (
    <div className="px-3 py-2 flex items-start gap-3 text-[11px]">
      <SourceBadge source={source} />
      <div className="flex-1 min-w-0">
        <div className="text-white/80 truncate" title={description}>
          {description}
        </div>
        {detailStr && (
          <div
            className="text-white/40 text-[10px] truncate mt-0.5"
            title={detailStr}
          >
            {detailStr}
          </div>
        )}
      </div>
      {score > 0 && (
        <div
          className="font-bold tabular-nums whitespace-nowrap"
          style={{ color: "#EF4444" }}
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
