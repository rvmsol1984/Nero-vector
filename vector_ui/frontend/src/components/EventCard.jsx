import { Fragment, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "./Avatar.jsx";
import EventTypeBadge from "./EventTypeBadge.jsx";
import JsonBlock from "./JsonBlock.jsx";
import StatusPill from "./StatusPill.jsx";
import TenantBadge from "./TenantBadge.jsx";
import { api } from "../api.js";
import { fmtRelative, workloadColor } from "../utils/format.js";

// INKY-event accent color (purple), distinct from any workload tint so
// email events stand out in a mixed feed.
const INKY_COLOR = "#c084fc";

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

export default function EventCard({ event }) {
  const [open, setOpen] = useState(false);
  const [raw, setRaw] = useState(null);

  // Accept either the new `source` field (dashboard feed) or the legacy
  // `kind` field (older /api/feed/recent rows) so EventCard renders
  // correctly regardless of which endpoint the caller used.
  const isInky = event.source === "inky" || event.kind === "inky";
  const color = isInky ? INKY_COLOR : workloadColor(event.workload);

  async function toggle() {
    if (isInky) {
      // INKY events don't have a fetchable raw JSON endpoint in this
      // session — the whole row is already self-describing.
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
        <Avatar email={event.user_id} tenant={event.client_name} size={36} />

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
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
            <TenantBadge name={event.client_name} />
          </div>
          <div className="flex items-center gap-3 mt-1 text-[11px] text-white/50 min-w-0">
            <span className="tabular-nums whitespace-nowrap">
              {fmtRelative(event.timestamp)}
            </span>
            <span className="opacity-60">·</span>
            {isInky ? (
              <>
                <span className="truncate" title={event.subject || ""}>
                  {event.subject || <span className="text-white/30">(no subject)</span>}
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

        {!isInky && <StatusPill status={event.result_status} dot />}
      </button>

      {open && !isInky && (
        <div className="px-4 pb-4">
          <JsonBlock data={raw?.raw_json ?? raw} loading={raw === null} />
        </div>
      )}
      {open && isInky && (
        <div className="px-4 pb-4">
          <JsonBlock data={event} />
        </div>
      )}
    </div>
  );
}
