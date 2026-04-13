import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";

import Avatar from "../components/Avatar.jsx";
import EventTypeBadge from "../components/EventTypeBadge.jsx";
import JsonBlock from "../components/JsonBlock.jsx";
import StatusPill from "../components/StatusPill.jsx";
import TenantBadge from "../components/TenantBadge.jsx";
import { api } from "../api.js";
import {
  deviceBrowser,
  deviceName,
  deviceOs,
  filenameFromObjectId,
  fmtNumber,
  fmtTime,
  siteDomain,
} from "../utils/format.js";

const TABS = [
  { id: "timeline", label: "Timeline" },
  { id: "files",    label: "Files"    },
  { id: "email",    label: "Email"    },
  { id: "logins",   label: "Logins"   },
  { id: "raw",      label: "Raw"      },
];

// The backend accepts workloads/event_types as comma-separated lists, so we
// can push most filtering into SQL. Files is the exception — see FilesTab.
const TAB_QUERY = {
  timeline: {},
  logins:   {
    workloads: "AzureActiveDirectory",
    event_types: "UserLoggedIn,UserLoginFailed,UserLoggedOut",
  },
  raw:      {},
};

const TAB_PAGE = { raw: 25 };

export default function UserDetail() {
  const { entityKey: rawKey } = useParams();
  // Router v6 normally decodes path params, but per spec we double-check.
  let entityKey = rawKey || "";
  try {
    entityKey = decodeURIComponent(entityKey);
  } catch {
    /* already decoded */
  }

  const [profile, setProfile] = useState(null);
  const [profileErr, setProfileErr] = useState(null);

  const [tab, setTab] = useState("timeline");

  useEffect(() => {
    setProfile(null);
    setProfileErr(null);
    api.userProfile(entityKey).then(setProfile).catch((e) => setProfileErr(e.message));
  }, [entityKey]);

  const header = useMemo(() => {
    if (profileErr) {
      return (
        <div className="card border-critical/30 text-critical text-sm px-4 py-3">
          {profileErr}
        </div>
      );
    }
    if (!profile) {
      return <div className="text-white/40 text-sm">loading user…</div>;
    }
    return (
      <div className="card p-6 animate-fade-in">
        <div className="flex flex-wrap items-start gap-5">
          <Avatar email={profile.user_id} tenant={profile.client_name} size={64} />
          <div className="flex-1 min-w-0">
            <div className="text-2xl font-bold break-all">{profile.user_id}</div>
            <div className="mt-2 flex items-center gap-3 flex-wrap">
              <TenantBadge name={profile.client_name} />
              <span className="text-[11px] text-white/40">{profile.tenant_id}</span>
            </div>
          </div>
          <div className="text-right text-[11px]">
            <div className="text-white/40 uppercase tracking-wider">first seen</div>
            <div className="mt-1">{fmtTime(profile.first_seen)}</div>
            <div className="text-white/40 uppercase tracking-wider mt-3">last seen</div>
            <div className="mt-1">{fmtTime(profile.last_seen)}</div>
          </div>
        </div>

        <div className="mt-6 grid grid-cols-2 md:grid-cols-4 gap-3">
          <MiniStat label="Events"   value={profile.total_events}      color="#2563EB" />
          <MiniStat label="Types"    value={profile.unique_event_types} color="#8B5CF6" />
          <MiniStat label="IPs"      value={profile.unique_ips}         color="#F97316" />
          <MiniStat label="Devices"  value={profile.unique_devices}     color="#10B981" />
        </div>
      </div>
    );
  }, [profile, profileErr]);

  return (
    <div className="space-y-4 animate-fade-in">
      <div className="text-[11px] text-white/40">
        <Link to="/users" className="hover:text-primary-light">users</Link>
        <span className="mx-2">/</span>
        <span className="text-white/70 break-all">{entityKey}</span>
      </div>

      {header}

      <div className="border-b border-white/5 flex items-center gap-1 flex-wrap overflow-x-auto">
        {TABS.map((t) => {
          const active = tab === t.id;
          return (
            <button
              key={t.id}
              type="button"
              onClick={() => setTab(t.id)}
              className={`px-4 py-2 text-xs font-medium border-b-2 -mb-px whitespace-nowrap transition-colors ${
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

      {tab === "timeline" && <ServerTab entityKey={entityKey} kind="timeline" />}
      {tab === "files"    && <FilesTab    entityKey={entityKey} />}
      {tab === "email"    && <EmailTraceTab entityKey={entityKey} />}
      {tab === "logins"   && <ServerTab entityKey={entityKey} kind="logins" />}
      {tab === "raw"      && <ServerTab entityKey={entityKey} kind="raw" />}
    </div>
  );
}

// ---------------------------------------------------------------------------

function MiniStat({ label, value, color }) {
  return (
    <div className="bg-white/[0.03] border border-white/5 rounded-xl px-4 py-3">
      <div className="text-[10px] uppercase tracking-wider text-white/40">{label}</div>
      <div
        className="text-2xl font-bold mt-1 tabular-nums leading-none"
        style={{ color }}
      >
        {fmtNumber(value)}
      </div>
    </div>
  );
}

// ---- server-driven tabs (Timeline / Email / Logins / Raw) -----------------

function ServerTab({ entityKey, kind }) {
  const q = TAB_QUERY[kind] || {};
  const pageSize = TAB_PAGE[kind] || 100;

  const [offset, setOffset] = useState(0);
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState(null);

  useEffect(() => {
    setOffset(0);
    setExpanded(null);
  }, [kind, entityKey]);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    api
      .userEvents(entityKey, { ...q, limit: pageSize, offset })
      .then((r) => {
        if (!cancel) setRows(r || []);
      })
      .catch(() => {
        if (!cancel) setRows([]);
      })
      .finally(() => {
        if (!cancel) setLoading(false);
      });
    return () => {
      cancel = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [entityKey, kind, offset]);

  const hasNext = rows.length >= pageSize;

  return (
    <div className="space-y-3">
      {rows.map((r) => (
        <Fragment key={r.id}>
          <button
            type="button"
            onClick={() =>
              setExpanded((prev) => (prev === r.id ? null : r.id))
            }
            className="card w-full text-left p-4 hover:bg-white/[0.03] active:scale-[0.997] transition-all"
            style={{
              borderLeft:
                kind !== "raw"
                  ? `3px solid ${workloadBorder(r.workload)}`
                  : undefined,
            }}
          >
            {kind === "timeline" && <TimelineRow row={r} />}
            {kind === "logins"   && <LoginRow    row={r} />}
            {kind === "raw"      && <RawRow      row={r} />}
          </button>
          {expanded === r.id && (
            <div className="px-1 animate-slide-up">
              <JsonBlock data={r.raw_json} />
            </div>
          )}
        </Fragment>
      ))}

      {!loading && rows.length === 0 && (
        <div className="card text-white/50 text-sm text-center py-10">
          no events
        </div>
      )}

      <Pager
        offset={offset}
        pageSize={pageSize}
        loading={loading}
        hasNext={hasNext}
        setOffset={setOffset}
      />
    </div>
  );
}

function workloadBorder(workload) {
  switch (workload) {
    case "AzureActiveDirectory":
      return "#8B5CF6";
    case "Exchange":
      return "#F97316";
    case "SharePoint":
      return "#3B82F6";
    case "OneDrive":
    case "OneDriveForBusiness":
      return "#22C55E";
    default:
      return "rgba(255,255,255,0.1)";
  }
}

// ---- Files tab -----------------------------------------------------------
// Fetches without a workload filter, keeps the last good result in a ref so
// switching back to the tab doesn't flash an empty state while the fetch
// is in flight. Filtering is done client-side per spec.

function FilesTab({ entityKey }) {
  const cacheRef = useRef({ key: null, data: [] });
  const [rows, setRows] = useState(
    cacheRef.current.key === entityKey ? cacheRef.current.data : [],
  );
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState(null);

  useEffect(() => {
    // Only reset if we're looking at a different user - switching tabs on
    // the same user should keep the cached rows visible.
    if (cacheRef.current.key !== entityKey) {
      cacheRef.current = { key: entityKey, data: [] };
      setRows([]);
      setExpanded(null);
    }
    let cancel = false;
    setLoading(true);
    api
      .userEvents(entityKey, { limit: 500 })
      .then((all) => {
        if (cancel) return;
        const filtered = (all || []).filter(
          (e) =>
            e.workload === "OneDrive" ||
            e.workload === "SharePoint" ||
            e.workload === "OneDriveForBusiness" ||
            e.event_type === "FileCreatedOnRemovableMedia",
        );
        cacheRef.current = { key: entityKey, data: filtered };
        setRows(filtered);
      })
      .catch(() => {
        /* keep previous rows on error */
      })
      .finally(() => {
        if (!cancel) setLoading(false);
      });
    return () => {
      cancel = true;
    };
  }, [entityKey]);

  return (
    <div className="space-y-3">
      {rows.map((r) => {
        const isRemovable = r.event_type === "FileCreatedOnRemovableMedia";
        const filename = filenameFromObjectId(r.raw_json?.ObjectId);
        const site = siteDomain(r.raw_json);
        return (
          <Fragment key={r.id}>
            <button
              type="button"
              onClick={() =>
                setExpanded((prev) => (prev === r.id ? null : r.id))
              }
              className="card w-full text-left p-4 hover:bg-white/[0.03] active:scale-[0.997] transition-all"
              style={{
                borderLeft: `3px solid ${workloadBorder(r.workload)}`,
              }}
            >
              <div className="flex items-center gap-3 flex-wrap">
                {isRemovable ? (
                  <span
                    className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
                    style={{
                      color: "#F97316",
                      borderColor: "#F9731666",
                      backgroundColor: "#F9731622",
                    }}
                  >
                    Removable Media
                  </span>
                ) : (
                  <EventTypeBadge type={r.event_type} workload={r.workload} />
                )}
                <div className="flex-1 min-w-0 font-medium text-sm truncate">
                  {filename || <span className="text-white/40">—</span>}
                </div>
                <div className="text-[11px] text-white/50 whitespace-nowrap">
                  {fmtTime(r.timestamp)}
                </div>
              </div>
              <div className="mt-1 flex items-center gap-3 text-[11px] text-white/50">
                <span className="truncate">
                  {site || <span className="text-white/30">no site</span>}
                </span>
                {r.client_ip && (
                  <>
                    <span className="opacity-60">·</span>
                    <span className="tabular-nums">{r.client_ip}</span>
                  </>
                )}
              </div>
            </button>
            {expanded === r.id && (
              <div className="px-1 animate-slide-up">
                <JsonBlock data={r.raw_json} />
              </div>
            )}
          </Fragment>
        );
      })}

      {!loading && rows.length === 0 && (
        <div className="card text-white/50 text-sm text-center py-10">
          no file activity
        </div>
      )}
    </div>
  );
}

// ---- row renderers -------------------------------------------------------

function TimelineRow({ row }) {
  return (
    <div className="flex items-center gap-3 flex-wrap">
      <EventTypeBadge type={row.event_type} workload={row.workload} />
      <div className="flex-1 min-w-0 text-sm text-white/80 truncate">
        {row.workload}
      </div>
      <StatusPill status={row.result_status} dot />
      <div className="text-[11px] text-white/50 whitespace-nowrap tabular-nums">
        {fmtTime(row.timestamp)}
      </div>
      {row.client_ip && (
        <div className="w-full text-[11px] text-white/40 tabular-nums">
          {row.client_ip}
        </div>
      )}
    </div>
  );
}

// ---- Email (Office 365 MessageTrace) ----------------------------------------
//
// Different shape from the other tabs: backed by vector_message_trace, not
// vector_events, so we fetch from /api/users/{key}/emails with a direction
// filter and a subject search input. Rows carry message envelope metadata
// only -- no bodies, no attachments.

function fmtBytes(n) {
  if (n === null || n === undefined || n === "") return "—";
  const v = Number(n);
  if (!Number.isFinite(v) || v <= 0) return "—";
  if (v < 1024) return `${v} B`;
  if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KB`;
  if (v < 1024 * 1024 * 1024) return `${(v / (1024 * 1024)).toFixed(1)} MB`;
  return `${(v / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function DirectionBadge({ direction }) {
  if (!direction) return <span className="text-white/30 text-[10px]">—</span>;
  const isIn = String(direction).toUpperCase().startsWith("IN");
  const color = isIn ? "#3B82F6" : "#F97316";
  const label = isIn ? "IN" : "OUT";
  return (
    <span
      className="inline-flex items-center px-1.5 py-[2px] text-[9px] font-bold uppercase tracking-wide rounded border whitespace-nowrap"
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

const EMAIL_PAGE = 50;

function EmailTraceTab({ entityKey }) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [direction, setDirection] = useState("");
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [offset, setOffset] = useState(0);

  // Debounce search input -> committed search value.
  useEffect(() => {
    const t = setTimeout(() => setSearch(searchInput.trim()), 300);
    return () => clearTimeout(t);
  }, [searchInput]);

  // Reset pagination whenever the filter changes.
  useEffect(() => {
    setOffset(0);
  }, [entityKey, direction, search]);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    api
      .userEmails(entityKey, {
        direction: direction || undefined,
        search: search || undefined,
        limit: EMAIL_PAGE,
        offset,
      })
      .then((r) => {
        if (!cancel) setRows(r || []);
      })
      .catch(() => {
        if (!cancel) setRows([]);
      })
      .finally(() => {
        if (!cancel) setLoading(false);
      });
    return () => {
      cancel = true;
    };
  }, [entityKey, direction, search, offset]);

  const hasNext = rows.length >= EMAIL_PAGE;

  return (
    <div className="space-y-3">
      {/* filter bar */}
      <div className="flex flex-wrap items-center gap-2 text-xs">
        <div className="flex items-center gap-1">
          {[
            { id: "",    label: "All" },
            { id: "IN",  label: "Inbound" },
            { id: "OUT", label: "Outbound" },
          ].map((d) => (
            <button
              key={d.id || "all"}
              type="button"
              onClick={() => setDirection(d.id)}
              className={`px-3 py-1.5 rounded-xl text-xs font-medium transition-all active:scale-95 ${
                direction === d.id
                  ? "bg-primary text-white"
                  : "bg-white/10 text-white/70 hover:bg-white/15"
              }`}
            >
              {d.label}
            </button>
          ))}
        </div>
        <input
          type="search"
          placeholder="search subject…"
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          className="bg-white/5 border border-white/10 rounded-xl px-3 py-1.5 text-xs text-white placeholder:text-white/40 focus:outline-none focus:border-primary-light w-64"
        />
        <div className="ml-auto flex items-center gap-2 text-[11px]">
          <button
            type="button"
            disabled={offset === 0 || loading}
            onClick={() => setOffset(Math.max(0, offset - EMAIL_PAGE))}
            className="border border-white/10 bg-white/5 px-3 py-1 rounded-xl disabled:opacity-30 hover:border-primary-light hover:text-primary-light transition-colors"
          >
            prev
          </button>
          <span className="text-white/50 tabular-nums">
            offset {offset.toLocaleString("en-US")}
          </span>
          <button
            type="button"
            disabled={!hasNext || loading}
            onClick={() => setOffset(offset + EMAIL_PAGE)}
            className="border border-white/10 bg-white/5 px-3 py-1 rounded-xl disabled:opacity-30 hover:border-primary-light hover:text-primary-light transition-colors"
          >
            next
          </button>
        </div>
      </div>

      {/* table */}
      <div className="bg-surface border border-white/5 rounded-card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full text-[11px]">
            <thead>
              <tr>
                <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Timestamp</th>
                <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Dir</th>
                <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">From</th>
                <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">To</th>
                <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Subject</th>
                <th className="text-right px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Size</th>
                <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {rows.map((r) => (
                <tr key={r.id} className="hover:bg-white/[0.03]">
                  <td className="px-3 py-2 text-white/50 whitespace-nowrap tabular-nums">
                    {fmtTime(r.received)}
                  </td>
                  <td className="px-3 py-2">
                    <DirectionBadge direction={r.direction} />
                  </td>
                  <td
                    className="px-3 py-2 truncate max-w-[220px]"
                    title={r.sender_address || ""}
                  >
                    {r.sender_address || <span className="text-white/30">—</span>}
                  </td>
                  <td
                    className="px-3 py-2 truncate max-w-[220px]"
                    title={r.recipient_address || ""}
                  >
                    {r.recipient_address || <span className="text-white/30">—</span>}
                  </td>
                  <td
                    className="px-3 py-2 text-white/80 truncate max-w-[360px]"
                    title={r.subject || ""}
                  >
                    {r.subject || <span className="text-white/30">(no subject)</span>}
                  </td>
                  <td className="px-3 py-2 text-right text-white/50 tabular-nums whitespace-nowrap">
                    {fmtBytes(r.size_bytes)}
                  </td>
                  <td className="px-3 py-2 text-white/60 whitespace-nowrap">
                    {r.status || <span className="text-white/30">—</span>}
                  </td>
                </tr>
              ))}
              {!loading && rows.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-3 py-10 text-center text-white/40">
                    Email trace data populates within 15 minutes
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <div className="px-4 py-2 border-t border-white/5 text-[10px] text-white/40">
          Email metadata only. Content not stored.
        </div>
      </div>
    </div>
  );
}

function LoginRow({ row }) {
  let status;
  if (row.event_type === "UserLoggedIn") status = "Success";
  else if (row.event_type === "UserLoginFailed") status = "Failed";
  else status = "Logged Out";
  const name = deviceName(row.raw_json);
  const os = deviceOs(row.raw_json);
  const browser = deviceBrowser(row.raw_json, row.user_agent);
  return (
    <div className="space-y-1">
      <div className="flex items-center gap-3 flex-wrap">
        <StatusPill status={status} dot />
        <div className="flex-1 text-sm text-white/80 truncate">
          {name || <span className="text-white/40">unknown device</span>}
        </div>
        <div className="text-[11px] text-white/50 whitespace-nowrap tabular-nums">
          {fmtTime(row.timestamp)}
        </div>
      </div>
      <div className="flex items-center gap-3 text-[11px] text-white/50">
        {row.client_ip && <span className="tabular-nums">{row.client_ip}</span>}
        {os && (
          <>
            <span className="opacity-60">·</span>
            <span>{os}</span>
          </>
        )}
        {browser && (
          <>
            <span className="opacity-60">·</span>
            <span>{browser}</span>
          </>
        )}
      </div>
    </div>
  );
}

function RawRow({ row }) {
  return (
    <div className="flex items-center gap-3 flex-wrap">
      <EventTypeBadge type={row.event_type} workload={row.workload} />
      <span className="text-sm text-white/70">{row.workload}</span>
      <StatusPill status={row.result_status} dot />
      <div className="ml-auto text-[11px] text-white/50 whitespace-nowrap tabular-nums">
        {fmtTime(row.timestamp)}
      </div>
    </div>
  );
}

function Pager({ offset, pageSize, loading, hasNext, setOffset }) {
  if (offset === 0 && !hasNext) return null;
  return (
    <div className="flex items-center justify-center gap-3 pt-2">
      <button
        type="button"
        disabled={offset === 0 || loading}
        onClick={() => setOffset(Math.max(0, offset - pageSize))}
        className="px-4 py-2 text-xs font-medium rounded-xl bg-white/5 border border-white/10 text-white/80 hover:bg-white/10 disabled:opacity-30 active:scale-95 transition-all"
      >
        prev
      </button>
      <span className="text-xs text-white/50 tabular-nums">
        offset {offset.toLocaleString("en-US")}
      </span>
      <button
        type="button"
        disabled={!hasNext || loading}
        onClick={() => setOffset(offset + pageSize)}
        className="px-4 py-2 text-xs font-medium rounded-xl bg-white/5 border border-white/10 text-white/80 hover:bg-white/10 disabled:opacity-30 active:scale-95 transition-all"
      >
        next
      </button>
    </div>
  );
}
