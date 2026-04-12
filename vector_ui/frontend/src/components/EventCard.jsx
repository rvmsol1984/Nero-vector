import { Fragment, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "./Avatar.jsx";
import EventTypeBadge from "./EventTypeBadge.jsx";
import JsonBlock from "./JsonBlock.jsx";
import StatusPill from "./StatusPill.jsx";
import TenantBadge from "./TenantBadge.jsx";
import { api } from "../api.js";
import { fmtRelative, workloadColor } from "../utils/format.js";

// Click-to-expand event row rendered as a "card" with a 3px left border
// colored by the event's workload. Used on the Events page and on the
// Dashboard recent feed.

export default function EventCard({ event }) {
  const [open, setOpen] = useState(false);
  const [raw, setRaw] = useState(null);
  const color = workloadColor(event.workload);

  async function toggle() {
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
            <Link
              to={`/users/${encodeURIComponent(event.entity_key)}`}
              onClick={(e) => e.stopPropagation()}
              className="font-medium text-sm text-white hover:text-primary-light truncate max-w-[260px]"
              title={event.user_id}
            >
              {event.user_id}
            </Link>
            <EventTypeBadge type={event.event_type} workload={event.workload} />
            <TenantBadge name={event.client_name} />
          </div>
          <div className="flex items-center gap-3 mt-1 text-[11px] text-white/50">
            <span className="tabular-nums whitespace-nowrap">
              {fmtRelative(event.timestamp)}
            </span>
            <span className="opacity-60">·</span>
            <span className="truncate">{event.workload}</span>
            {event.client_ip && (
              <>
                <span className="opacity-60">·</span>
                <span className="tabular-nums">{event.client_ip}</span>
              </>
            )}
          </div>
        </div>

        <StatusPill status={event.result_status} dot />
      </button>

      {open && (
        <div className="px-4 pb-4">
          <JsonBlock
            data={raw?.raw_json ?? raw}
            loading={raw === null}
          />
        </div>
      )}
    </div>
  );
}
