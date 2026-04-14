import { Fragment, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "./Avatar.jsx";
import EventTypeBadge from "./EventTypeBadge.jsx";
import JsonBlock from "./JsonBlock.jsx";
import StatusPill from "./StatusPill.jsx";
import TenantBadge from "./TenantBadge.jsx";
import { api } from "../api.js";
import { fmtRelative, initialsFrom, workloadColor } from "../utils/format.js";

// Accent colors for non-UAL sources.
const INKY_COLOR = "#c084fc"; // purple
const EDR_COLOR  = "#F97316"; // orange

// Verdict pill colors for INKY rows.
const VERDICT_STYLES = {
  danger:  { label: "DANGER",  color: "#EF4444" },
  caution: { label: "CAUTION", color: "#EAB308" },
  neutral: { label: "NEUTRAL", color: "rgba(255,255,255,0.5)" },
};

function VerdictBadge({ verdict }) {
  if (!verdict) return null;
  const cfg = VERDICT_STYLES[String(verdict).toLowerCase()] || {
    label: String(verdict).toUpperCase(),
    color: "rgba(255,255,255,0.5)",
  };
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

// Severity -> color for EDR rows. Keys are normalised lowercase.
const EDR_SEVERITY_COLORS = {
  critical:      "#EF4444",
  high:          "#EF4444",
  medium:        "#F97316",
  low:           "#EAB308",
  informational: "rgba(255,255,255,0.5)",
  info:          "rgba(255,255,255,0.5)",
};

function SeverityBadge({ severity }) {
  if (!severity) return null;
  const key = String(severity).trim().toLowerCase();
  const color = EDR_SEVERITY_COLORS[key] || "rgba(255,255,255,0.5)";
  return (
    <span
      className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
      style={{
        color,
        borderColor: color + "55",
        backgroundColor: color + "14",
      }}
    >
      {severity}
    </span>
  );
}

function FixedAvatar({ initials, background }) {
  return (
    <div
      className="rounded-full flex items-center justify-center font-semibold text-white shrink-0 select-none"
      style={{
        width: 36,
        height: 36,
        background,
        fontSize: 12,
        boxShadow: `0 0 0 1px ${background}66 inset`,
      }}
    >
      {initials}
    </div>
  );
}

export default function EventCard({ event }) {
  const [open, setOpen] = useState(false);
  const [raw, setRaw] = useState(null);

  // Accept either the new `source` field (dashboard feed) or the legacy
  // `kind` field (older /api/feed/recent rows) so EventCard renders
  // correctly regardless of which endpoint the caller used.
  const isInky = event.source === "inky" || event.kind === "inky";
  const isEdr  = event.source === "edr"  || event.kind === "edr";

  const color = isEdr
    ? EDR_COLOR
    : isInky
    ? INKY_COLOR
    : workloadColor(event.workload);

  async function toggle() {
    if (isInky || isEdr) {
      // INKY and EDR rows are already self-describing in the feed --
      // no /api/events/{id} fetch needed.
      setOpen((v) => !v);
      return;
    }
    if (open) {
      setOpen(false);
      return;
    }
    setOpen(true);
    if (raw === null) {
      try {
        const full = await api.eventById(event.id);
        setRaw(full);
      } catch (e) {
        setRaw({ error: e.message });
      }
    }
  }

  const userLink = event.entity_key
    ? `/users/${encodeURIComponent(event.entity_key)}`
    : null;

  return (
    <div
      className="card overflow-hidden transition-all duration-200 animate-fade-in"
      style={{ borderLeft: `3px solid ${color}` }}
    >
      <button
        type="button"
        onClick={toggle}
        className="w-full text-left p-4 flex items-center gap-3 hover:bg-white/[0.03] active:scale-[0.997] transition-all"
      >
        {isEdr ? (
          <FixedAvatar initials="ED" background={EDR_COLOR} />
        ) : (
          <Avatar email={event.user_id} tenant={event.client_name} size={36} />
        )}

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            {isEdr ? (
              <>
                <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-high">
                  EDR Alert
                </span>
                {event.host_name && (
                  <span
                    className="font-mono text-[11px] text-white/80 truncate max-w-[200px]"
                    title={event.host_name}
                  >
                    {event.host_name}
                  </span>
                )}
                <SeverityBadge severity={event.severity} />
                {event.threat_name && (
                  <span
                    className="text-[11px] text-white/70 truncate max-w-[220px]"
                    title={event.threat_name}
                  >
                    {event.threat_name}
                  </span>
                )}
              </>
            ) : (
              <>
                {userLink ? (
                  <Link
                    to={userLink}
                    onClick={(e) => e.stopPropagation()}
                    className="font-medium text-sm text-white hover:text-primary-light truncate max-w-[260px]"
                    title={event.user_id}
                  >
                    {event.user_id}
                  </Link>
                ) : (
                  <span
                    className="font-medium text-sm text-white truncate max-w-[260px]"
                    title={event.user_id}
                  >
                    {event.user_id}
                  </span>
                )}
                {isInky ? (
                  <>
                    <VerdictBadge verdict={event.verdict} />
                    {event.aitm_detected && (
                      <span
                        className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
                        style={{
                          color: "#EF4444",
                          borderColor: "#EF444455",
                          backgroundColor: "#EF444414",
                        }}
                      >
                        AiTM
                      </span>
                    )}
                  </>
                ) : (
                  <EventTypeBadge
                    type={event.event_type}
                    workload={event.workload}
                  />
                )}
              </>
            )}
            <TenantBadge name={event.client_name} />
          </div>
          <div className="flex items-center gap-3 mt-1 text-[11px] text-white/50 min-w-0">
            <span className="tabular-nums whitespace-nowrap">
              {fmtRelative(event.timestamp)}
            </span>
            <span className="opacity-60">·</span>
            {isEdr ? (
              <>
                {event.process_name && (
                  <>
                    <span className="font-mono text-[11px] truncate max-w-[220px]" title={event.process_name}>
                      {event.process_name}
                    </span>
                    <span className="opacity-60">·</span>
                  </>
                )}
                <span className="truncate">
                  {event.action_taken || <span className="text-white/30">no action</span>}
                </span>
                {event.user_id && (
                  <>
                    <span className="opacity-60">·</span>
                    {userLink ? (
                      <Link
                        to={userLink}
                        onClick={(e) => e.stopPropagation()}
                        className="truncate max-w-[220px] hover:text-primary-light"
                        title={event.user_id}
                      >
                        {event.user_id}
                      </Link>
                    ) : (
                      <span className="truncate max-w-[220px]" title={event.user_id}>
                        {event.user_id}
                      </span>
                    )}
                  </>
                )}
              </>
            ) : isInky ? (
              <>
                <span className="truncate" title={event.subject || ""}>
                  {event.subject || (
                    <span className="text-white/30">(no subject)</span>
                  )}
                </span>
                {event.sender && (
                  <>
                    <span className="opacity-60">·</span>
                    <span className="truncate max-w-[220px]" title={event.sender}>
                      {event.sender}
                    </span>
                  </>
                )}
              </>
            ) : (
              <>
                <span className="truncate">{event.workload}</span>
                {event.client_ip && (
                  <>
                    <span className="opacity-60">·</span>
                    <span className="tabular-nums">{event.client_ip}</span>
                  </>
                )}
              </>
            )}
          </div>
        </div>

        {!isInky && !isEdr && <StatusPill status={event.result_status} dot />}
      </button>

      {open && !isInky && !isEdr && (
        <div className="px-4 pb-4">
          <JsonBlock data={raw?.raw_json ?? raw} loading={raw === null} />
        </div>
      )}
      {open && (isInky || isEdr) && (
        <div className="px-4 pb-4">
          <JsonBlock data={event} />
        </div>
      )}
    </div>
  );
}
