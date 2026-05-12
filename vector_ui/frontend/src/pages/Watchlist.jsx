import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "../components/Avatar.jsx";
import TenantBadge from "../components/TenantBadge.jsx";
import { api } from "../api.js";
import { fmtTime } from "../utils/format.js";

// Human-readable label for each watchlist trigger_type.
const TRIGGER_LABELS = {
  inky_phish_delivered: "Phish Delivered",
  inky_click:           "Link Clicked",
};

const STATUS_STYLES = {
  active: {
    label: "ACTIVE",
    color: "#EAB308", // yellow
  },
  escalated: {
    label: "ESCALATED",
    color: "#EF4444", // red
  },
  expired: {
    label: "EXPIRED",
    color: "rgba(255,255,255,0.4)", // grey
  },
};

function StatusPill({ status }) {
  const cfg = STATUS_STYLES[status] || STATUS_STYLES.active;
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

// 48-hour correlation windows are too long for a live MM:SS counter,
// so we render a two-line cell instead:
//   line 1: "in Xh Ym" (remaining time, recomputed on 30s auto-refresh)
//   line 2: absolute expiry timestamp, e.g. "Expires Apr 15, 2026 at 4:00 PM"
// The parent Watchlist page reloads every 30 seconds so the values stay
// current without a per-second setInterval on every row.
function ExpiresCell({ expiresAt }) {
  if (!expiresAt) return <span className="text-white/40">—</span>;
  const target = new Date(expiresAt);
  if (Number.isNaN(target.getTime())) {
    return <span className="text-white/40">—</span>;
  }

  const diffMs = target.getTime() - Date.now();
  if (diffMs <= 0) {
    return (
      <span className="font-mono text-[11px] text-white/40">expired</span>
    );
  }

  const totalMinutes = Math.floor(diffMs / 60000);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  const remaining =
    hours > 0 ? `in ${hours}h ${minutes}m` : `in ${minutes}m`;
  const closing = diffMs < 60 * 60 * 1000; // last hour glows red

  const dateStr = target.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
  const timeStr = target.toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  });

  return (
    <div className="leading-tight whitespace-nowrap">
      <div
        className="text-[11px] font-medium tabular-nums"
        style={{ color: closing ? "#EF4444" : "#EAB308" }}
      >
        {remaining}
      </div>
      <div className="text-[10px] text-white/40 tabular-nums">
        Expires {dateStr} at {timeStr}
      </div>
    </div>
  );
}

function ShieldIcon() {
  return (
    <svg
      width="52"
      height="52"
      viewBox="0 0 24 24"
      fill="none"
      stroke="rgba(255,255,255,0.35)"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
      <path d="M9 12l2 2 4-4" />
    </svg>
  );
}

const STATUS_FILTERS = [
  { id: "all",       label: "All" },
  { id: "active",    label: "Active" },
  { id: "escalated", label: "Escalated" },
  { id: "expired",   label: "Expired" },
];

export default function Watchlist() {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);
  const [statusFilter, setStatusFilter] = useState("all");

  useEffect(() => {
    let cancel = false;
    function load() {
      api
        .watchlist(undefined)
        .then((r) => {
          if (cancel) return;
          setRows(r || []);
          setErr(null);
        })
        .catch((e) => {
          if (!cancel) setErr(e.message);
        })
        .finally(() => {
          if (!cancel) setLoading(false);
        });
    }
    load();
    const tick = setInterval(load, 30000); // auto-refresh every 30s
    return () => {
      cancel = true;
      clearInterval(tick);
    };
  }, []);

  // Filter to displayed rows based on selected status
  const filteredRows = statusFilter === "all"
    ? rows
    : rows.filter((r) => (r.status || "").toLowerCase() === statusFilter);

  // Count by status (for the header summary + filter pill counts).
  const countByStatus = rows.reduce(
    (acc, row) => {
      const s = (row.status || "").toLowerCase();
      acc[s] = (acc[s] || 0) + 1;
      return acc;
    },
    {},
  );

  return (
    <div className="space-y-4 animate-fade-in">
      <div className="flex items-center gap-3 flex-wrap">
        <h1 className="text-2xl font-bold">Watchlist</h1>
        <span className="inline-flex items-center px-2.5 py-1 rounded-full text-[10px] font-semibold uppercase tracking-wider border border-status-waiting/40 bg-status-waiting/10 text-status-waiting tabular-nums">
          {filteredRows.length} {statusFilter === "all" ? "total" : statusFilter}
        </span>
      </div>
      <p className="text-white/50 text-sm -mt-3">
        Users with open correlation windows. Anomalous auth within the
        window auto-escalates.
      </p>

      {/* ----- filter pills ----- */}
      <div className="flex items-center gap-2 flex-wrap">
        {STATUS_FILTERS.map((f) => {
          const active = statusFilter === f.id;
          return (
            <button
              key={f.id}
              type="button"
              onClick={() => setStatusFilter(f.id)}
              className={`px-3 py-1.5 rounded-xl text-xs font-medium transition-all active:scale-95 ${
                active
                  ? "bg-primary text-white"
                  : "bg-white/10 text-white/70 hover:bg-white/15"
              }`}
            >
              {f.label}
              {f.id !== "all" && countByStatus[f.id] != null && (
                <span className="ml-1.5 text-[10px] opacity-70 tabular-nums">
                  {countByStatus[f.id]}
                </span>
              )}
              {f.id === "all" && (
                <span className="ml-1.5 text-[10px] opacity-70 tabular-nums">
                  {rows.length}
                </span>
              )}
            </button>
          );
        })}
      </div>

      {err && (
        <div className="card border-critical/30 text-critical text-sm px-4 py-3">
          load error: {err}
        </div>
      )}

      {!loading && filteredRows.length === 0 && !err ? (
        <div className="card py-14 flex flex-col items-center text-center gap-3">
          <ShieldIcon />
          <div className="text-white/50 text-sm">
            {statusFilter === "all"
              ? "No watchlist entries"
              : `No ${statusFilter} entries`}
          </div>
        </div>
      ) : (
        <div className="card overflow-hidden">
          <div className="overflow-x-auto">
            <table className="min-w-full text-[11px]">
              <thead>
                <tr>
                  <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">User</th>
                  <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Trigger</th>
                  <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Subject</th>
                  <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Sender</th>
                  <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Window</th>
                  <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {filteredRows.map((row) => {
                  const status = (row.status || "").toLowerCase();
                  const isEscalated = status === "escalated";
                  const isExpired = status === "expired";
                  const entityKey =
                    row.tenant_id && row.user_email
                      ? `${row.tenant_id}::${row.user_email}`
                      : null;
                  const subject =
                    row.latest_subject ||
                    (row.trigger_details && row.trigger_details.subject) ||
                    "";
                  const sender =
                    row.latest_sender ||
                    (row.trigger_details && row.trigger_details.sender) ||
                    "";
                  return (
                    <tr
                      key={row.id}
                      className={`hover:bg-white/[0.03] transition-colors ${
                        isExpired ? "opacity-50" : ""
                      }`}
                      style={
                        isEscalated
                          ? { boxShadow: "inset 3px 0 0 #EF4444" }
                          : undefined
                      }
                    >
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-2">
                          <Avatar
                            email={row.user_email}
                            tenant={row.client_name}
                            size={28}
                          />
                          {entityKey ? (
                            <Link
                              to={`/users/${encodeURIComponent(entityKey)}`}
                              className="hover:text-primary-light truncate max-w-[220px]"
                              title={row.user_email}
                            >
                              {row.user_email}
                            </Link>
                          ) : (
                            <span className="truncate max-w-[220px] text-white/80">
                              {row.user_email}
                            </span>
                          )}
                          {row.client_name && (
                            <TenantBadge name={row.client_name} />
                          )}
                        </div>
                      </td>
                      <td className="px-4 py-2.5">
                        <div className="text-white/80">
                          {TRIGGER_LABELS[row.trigger_type] || row.trigger_type}
                        </div>
                        {row.trigger_details && (row.trigger_details.indicator || row.trigger_details.ioc_value) && (
                          <div className="text-[10px] text-white/40 font-mono truncate max-w-[180px]" title={row.trigger_details.indicator || row.trigger_details.ioc_value}>
                            {row.trigger_details.ioc_type ? `${row.trigger_details.ioc_type}: ` : ""}{row.trigger_details.indicator || row.trigger_details.ioc_value}
                          </div>
                        )}
                      </td>
                      <td
                        className="px-4 py-2.5 text-white/70 truncate max-w-[320px]"
                        title={subject}
                      >
                        {subject || <span className="text-white/30">—</span>}
                      </td>
                      <td
                        className="px-4 py-2.5 text-white/60 truncate max-w-[220px]"
                        title={sender}
                      >
                        {sender || <span className="text-white/30">—</span>}
                      </td>
                      <td className="px-4 py-2.5 whitespace-nowrap">
                        <ExpiresCell expiresAt={row.expires_at} />
                      </td>
                      <td className="px-4 py-2.5">
                        <StatusPill status={row.status} />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <div className="px-4 py-2 border-t border-white/5 text-[10px] text-white/40 flex items-center justify-between flex-wrap gap-2">
            <span>
              Correlation windows are 48h · auto-refreshes every 30s
              {statusFilter === "all" && countByStatus && (
                <>
                  {" · "}
                  <span className="text-status-waiting">
                    {countByStatus.active || 0} active
                  </span>
                  {" · "}
                  <span className="text-critical">
                    {countByStatus.escalated || 0} escalated
                  </span>
                  {" · "}
                  <span>{countByStatus.expired || 0} expired</span>
                </>
              )}
            </span>
            <span className="tabular-nums">
              last updated {fmtTime(new Date().toISOString())}
            </span>
          </div>
        </div>
      )}
    </div>
  );
}
