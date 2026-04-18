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
  fmtRelative,
  fmtTime,
  siteDomain,
} from "../utils/format.js";

const TABS = [
  { id: "timeline",      label: "Timeline"     },
  { id: "files",         label: "Files"        },
  { id: "email",         label: "Email"        },
  { id: "logins",        label: "Logins"       },
  { id: "endpoint",      label: "Endpoint"     },
  { id: "threatlocker",  label: "ThreatLocker" },
  { id: "raw",           label: "Raw"          },
  { id: "auth",          label: "Auth Methods" },
  { id: "permissions",   label: "Permissions"  },
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
  let entityKey = rawKey || "";
  try {
    entityKey = decodeURIComponent(entityKey);
  } catch {
    /* already decoded */
  }

  const [profile, setProfile] = useState(null);
  const [profileErr, setProfileErr] = useState(null);
  const [enriched, setEnriched] = useState(null);
  const [iocMatches, setIocMatches] = useState([]);

  const [tab, setTab] = useState("timeline");

  useEffect(() => {
    setProfile(null);
    setProfileErr(null);
    setEnriched(null);
    setIocMatches([]);
    api.userProfile(entityKey).then(setProfile).catch((e) => setProfileErr(e.message));
    api.userIoc(entityKey).then((r) => setIocMatches(r || [])).catch(() => setIocMatches([]));
    // Enriched profile (Graph + stats) — fetch independently so the
    // page renders with DB data first while the Graph calls finish.
    fetch(`/api/users/${encodeURIComponent(entityKey)}/profile`, {
      credentials: "same-origin",
      headers: {
        Accept: "application/json",
        ...(localStorage.getItem("vector_token")
          ? { Authorization: `Bearer ${localStorage.getItem("vector_token")}` }
          : {}),
      },
    })
      .then((r) => (r.ok ? r.json() : {}))
      .then((d) => setEnriched(d || {}))
      .catch(() => setEnriched({}));
  }, [entityKey]);

  if (profileErr) {
    return (
      <div className="space-y-4 animate-fade-in">
        <BreadcrumbBar entityKey={entityKey} />
        <div className="card border-critical/30 text-critical text-sm px-4 py-3">
          {profileErr}
        </div>
      </div>
    );
  }
  if (!profile) {
    return (
      <div className="space-y-4 animate-fade-in">
        <BreadcrumbBar entityKey={entityKey} />
        <div className="text-white/40 text-sm">loading user…</div>
      </div>
    );
  }

  const ep = enriched || {};
  const displayName = ep.display_name || profile.user_id;

  return (
    <div className="space-y-4 animate-fade-in">
      <BreadcrumbBar entityKey={entityKey} />

      {/* ---- rich profile card ---------------------------------------- */}
      <div className="card p-6 animate-fade-in">
        <div className="flex flex-wrap items-start gap-5">
          {/* left: identity */}
          <Avatar email={profile.user_id} tenant={profile.client_name} size={48} />
          <div className="flex-1 min-w-0">
            <div className="text-xl font-bold break-all">{displayName}</div>
            {(ep.job_title || ep.department) && (
              <div className="text-[12px] text-white/50 mt-0.5">
                {[ep.job_title, ep.department].filter(Boolean).join(" · ")}
              </div>
            )}
            <div className="mt-1 font-mono text-[11px] text-white/40 break-all">
              {profile.user_id}
            </div>
            <div className="mt-2 flex items-center gap-3 flex-wrap">
              <TenantBadge name={profile.client_name} />
              <span className="text-[11px] text-white/40">{profile.tenant_id}</span>
            </div>
          </div>

          {/* right: info pills */}
          <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-[11px]">
            <InfoPill
              label="Account"
              value={
                ep.account_enabled === true
                  ? "ACTIVE"
                  : ep.account_enabled === false
                  ? "DISABLED"
                  : "—"
              }
              color={
                ep.account_enabled === true
                  ? "#10B981"
                  : ep.account_enabled === false
                  ? "#EF4444"
                  : undefined
              }
            />
            <InfoPill
              label="MFA"
              value={
                ep.has_mfa === true
                  ? `ENROLLED (${(ep.mfa_methods || []).join(", ") || "?"})`
                  : ep.has_mfa === false
                  ? "NO MFA"
                  : "—"
              }
              color={
                ep.has_mfa === true
                  ? "#10B981"
                  : ep.has_mfa === false
                  ? "#EF4444"
                  : undefined
              }
            />
            <InfoPill
              label="Password"
              value={ep.last_password_change ? fmtRelative(ep.last_password_change) : "—"}
              color={passwordAgeColor(ep.last_password_change)}
            />
            <InfoPill
              label="Manager"
              value={ep.manager_name || "—"}
            />
            <InfoPill
              label="Phone"
              value={ep.mobile_phone || "—"}
            />
            <InfoPill
              label="Member since"
              value={
                ep.created_datetime
                  ? fmtRelative(ep.created_datetime)
                  : profile.first_seen
                  ? fmtRelative(profile.first_seen)
                  : "—"
              }
            />
          </div>
        </div>

        {/* ---- quick stats ------------------------------------------- */}
        <div className="mt-6 grid grid-cols-2 md:grid-cols-4 gap-3">
          <MiniStat
            label="Logins (30d)"
            value={ep.login_total_30d ?? profile.total_events}
            color="#2563EB"
            sub={ep.login_failed_30d ? `${fmtNumber(ep.login_failed_30d)} failed` : undefined}
          />
          <MiniStat
            label="Emails"
            value={ep.email_count ?? 0}
            color="#8B5CF6"
          />
          <MiniStat
            label="Files (30d)"
            value={ep.file_count_30d ?? 0}
            color="#F97316"
          />
          <MiniStat
            label="Incidents"
            value={ep.open_incidents ?? 0}
            color={ep.open_incidents > 0 ? "#EF4444" : "#10B981"}
          />
        </div>
      </div>

      {/* ---- risk indicators strip (only if any exist) --------------- */}
      <RiskIndicators
        iocMatches={iocMatches}
        enriched={ep}
        entityKey={entityKey}
        userId={profile.user_id}
      />

      <IocMatchBanner matches={iocMatches} />

      {/* ---- tabs (with optional count badges) ------------------------ */}
      <div className="border-b border-white/5 flex items-center gap-1 flex-wrap overflow-x-auto">
        {TABS.map((t) => {
          const active = tab === t.id;
          // Derive count from already-loaded data so we don't add
          // new API calls. Only specific tabs get a badge; the rest
          // render just the label.
          let count = null;
          if (t.id === "email" && ep.email_count > 0) count = ep.email_count;
          else if (t.id === "files" && ep.file_count_30d > 0) count = ep.file_count_30d;
          else if (t.id === "timeline" && profile?.total_events > 0) count = profile.total_events;
          else if (t.id === "logins" && ep.login_total_30d > 0) count = ep.login_total_30d;
          return (
            <button
              key={t.id}
              type="button"
              onClick={() => setTab(t.id)}
              className={`px-4 py-2 text-xs font-medium border-b-2 -mb-px whitespace-nowrap transition-colors flex items-center gap-1.5 ${
                active
                  ? "border-primary text-primary-light"
                  : "border-transparent text-white/50 hover:text-white"
              }`}
            >
              {t.label}
              {count != null && (
                <span
                  className="inline-flex items-center justify-center tabular-nums font-bold"
                  style={{
                    fontSize: "10px",
                    padding: "1px 6px",
                    borderRadius: "10px",
                    minWidth: "18px",
                    ...(active
                      ? { background: "rgba(37,99,235,0.2)", color: "#2563EB" }
                      : { background: "rgba(255,255,255,0.1)", color: "rgba(255,255,255,0.6)" }),
                  }}
                >
                  {count > 999 ? `${Math.round(count / 1000)}k` : count}
                </span>
              )}
            </button>
          );
        })}
      </div>

      {tab === "timeline"     && <ServerTab entityKey={entityKey} kind="timeline" />}
      {tab === "files"        && <FilesTab    entityKey={entityKey} />}
      {tab === "email"        && <EmailTraceTab entityKey={entityKey} />}
      {tab === "logins"       && <ServerTab entityKey={entityKey} kind="logins" />}
      {tab === "endpoint"     && <EndpointTab entityKey={entityKey} />}
      {tab === "threatlocker" && <ThreatLockerTab entityKey={entityKey} />}
      {tab === "raw"          && <ServerTab entityKey={entityKey} kind="raw" />}
      {tab === "auth"         && <AuthMethodsTab enriched={ep} />}
      {tab === "permissions"  && <PermissionsTab entityKey={entityKey} />}
    </div>
  );
}

// ---------------------------------------------------------------------------
// breadcrumb
// ---------------------------------------------------------------------------

function BreadcrumbBar({ entityKey }) {
  return (
    <div className="text-[11px] text-white/40">
      <Link to="/users" className="hover:text-primary-light">users</Link>
      <span className="mx-2">/</span>
      <span className="text-white/70 break-all">{entityKey}</span>
    </div>
  );
}

// ---------------------------------------------------------------------------
// profile card helpers
// ---------------------------------------------------------------------------

function InfoPill({ label, value, color }) {
  return (
    <div>
      <div className="text-[9px] uppercase tracking-wider text-white/40">{label}</div>
      <div
        className="mt-0.5 font-semibold truncate max-w-[260px]"
        style={color ? { color } : undefined}
        title={typeof value === "string" ? value : undefined}
      >
        {value}
      </div>
    </div>
  );
}

function passwordAgeColor(iso) {
  if (!iso) return undefined;
  const diff = Date.now() - new Date(iso).getTime();
  const days = diff / (1000 * 60 * 60 * 24);
  if (days < 90) return "#10B981";
  if (days < 180) return "#EAB308";
  return "#EF4444";
}

// ---------------------------------------------------------------------------
// risk indicators strip
// ---------------------------------------------------------------------------

function RiskIndicators({ iocMatches, enriched, entityKey, userId }) {
  const ep = enriched || {};
  const items = [];
  if (ep.open_incidents > 0) {
    items.push({
      label: "Open incidents",
      value: fmtNumber(ep.open_incidents),
      color: "#EF4444",
      link: `/incidents?user=${encodeURIComponent(userId)}`,
    });
  }
  if (iocMatches && iocMatches.length > 0) {
    items.push({
      label: "IOC matches",
      value: `${iocMatches.length} match${iocMatches.length === 1 ? "" : "es"}`,
      color: "#EF4444",
    });
  }
  if (ep.watchlist_status) {
    items.push({
      label: "Watchlist",
      value: ep.watchlist_status,
      color: ep.watchlist_status === "escalated" ? "#EF4444" : "#EAB308",
    });
  }
  if (ep.off_hours_logins_30d > 0) {
    items.push({
      label: "Off-hours logins (30d)",
      value: fmtNumber(ep.off_hours_logins_30d),
      color: "#EAB308",
    });
  }

  if (items.length === 0) return null;
  return (
    <div
      className="card p-4 animate-fade-in flex flex-wrap items-center gap-4"
      style={{
        borderLeft: "3px solid #EF4444",
        backgroundColor: "rgba(239,68,68,0.04)",
      }}
    >
      <div className="text-[10px] uppercase tracking-wider text-white/40 font-semibold mr-1">
        Risk indicators
      </div>
      {items.map((it) => (
        <span
          key={it.label}
          className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[10px] font-semibold uppercase tracking-wider border whitespace-nowrap"
          style={{
            color: it.color,
            borderColor: it.color + "55",
            backgroundColor: it.color + "14",
          }}
        >
          {it.link ? (
            <Link
              to={it.link}
              className="hover:underline"
              style={{ color: it.color }}
            >
              {it.label}: {it.value}
            </Link>
          ) : (
            <>{it.label}: {it.value}</>
          )}
        </span>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Auth Methods tab
// ---------------------------------------------------------------------------

function AuthMethodsTab({ enriched }) {
  const methods = enriched?.mfa_methods || [];
  return (
    <div className="bg-surface border border-white/5 rounded-card overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full text-[11px]">
          <thead>
            <tr>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Method</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Type</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {methods.map((m) => (
              <tr key={m} className="hover:bg-white/[0.03]">
                <td className="px-3 py-2 text-white/80">{m}</td>
                <td className="px-3 py-2">
                  <span
                    className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
                    style={{
                      color: "#10B981",
                      borderColor: "#10B98155",
                      backgroundColor: "#10B98114",
                    }}
                  >
                    MFA
                  </span>
                </td>
              </tr>
            ))}
            {methods.length === 0 && (
              <tr>
                <td colSpan={2} className="px-3 py-10 text-center text-white/40">
                  {enriched ? "No MFA methods registered or data unavailable" : "loading…"}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="px-4 py-2 border-t border-white/5 text-[10px] text-white/40">
        Authentication methods registered in Azure AD for this user.
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Permissions tab
// ---------------------------------------------------------------------------

function PermissionsTab({ entityKey }) {
  const [rows, setRows] = useState(null);

  useEffect(() => {
    let cancel = false;
    fetch(`/api/users/${encodeURIComponent(entityKey)}/memberships`, {
      credentials: "same-origin",
      headers: {
        Accept: "application/json",
        ...(localStorage.getItem("vector_token")
          ? { Authorization: `Bearer ${localStorage.getItem("vector_token")}` }
          : {}),
      },
    })
      .then((r) => (r.ok ? r.json() : []))
      .then((d) => { if (!cancel) setRows(d || []); })
      .catch(() => { if (!cancel) setRows([]); });
    return () => { cancel = true; };
  }, [entityKey]);

  return (
    <div className="bg-surface border border-white/5 rounded-card overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full text-[11px]">
          <thead>
            <tr>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Name</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Type</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Description</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {rows === null && (
              <tr>
                <td colSpan={3} className="px-3 py-10 text-center text-white/40">loading…</td>
              </tr>
            )}
            {rows && rows.length === 0 && (
              <tr>
                <td colSpan={3} className="px-3 py-10 text-center text-white/40">
                  No directory roles or group memberships found
                </td>
              </tr>
            )}
            {rows && rows.map((r, i) => {
              const isRole = r.type === "Role";
              const color = isRole ? "#8B5CF6" : "#3B82F6";
              return (
                <tr key={`${r.display_name}-${i}`} className="hover:bg-white/[0.03]">
                  <td className="px-3 py-2 text-white/80 truncate max-w-[300px]">
                    {r.display_name || <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-3 py-2">
                    <span
                      className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
                      style={{
                        color,
                        borderColor: color + "55",
                        backgroundColor: color + "14",
                      }}
                    >
                      {r.type}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-white/50 truncate max-w-[400px]">
                    {r.description || <span className="text-white/30">—</span>}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="px-4 py-2 border-t border-white/5 text-[10px] text-white/40">
        Directory roles and security group memberships from Azure AD.
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------

function IocMatchBanner({ matches }) {
  if (!matches || matches.length === 0) return null;
  const count = matches.length;
  const maxConfidence = matches.reduce(
    (acc, m) => Math.max(acc, Number(m.confidence) || 0),
    0,
  );
  return (
    <div
      className="card p-4 animate-fade-in flex items-center gap-3 flex-wrap"
      style={{
        borderLeft: "3px solid #EF4444",
        backgroundColor: "rgba(239,68,68,0.06)",
      }}
    >
      <div
        className="h-9 w-9 rounded-full flex items-center justify-center text-lg shrink-0"
        style={{
          background: "rgba(239,68,68,0.15)",
          color: "#EF4444",
        }}
        aria-hidden="true"
      >
        ⚠
      </div>
      <div className="min-w-0 flex-1">
        <div className="text-sm font-semibold text-white">
          IOC match detected — this user's activity matches known threat
          indicators
        </div>
        <div className="mt-0.5 text-[11px] text-white/60">
          {count} match{count === 1 ? "" : "es"} · max confidence{" "}
          <span className="font-semibold text-white">
            {maxConfidence || "?"}
          </span>
        </div>
      </div>
      <span
        className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wider rounded-full border whitespace-nowrap"
        style={{
          color: "#EF4444",
          borderColor: "#EF444455",
          backgroundColor: "#EF444414",
        }}
      >
        IOC Match
      </span>
    </div>
  );
}

function MiniStat({ label, value, color, sub }) {
  return (
    <div className="bg-white/[0.03] border border-white/5 rounded-xl px-4 py-3">
      <div className="text-[10px] uppercase tracking-wider text-white/40">{label}</div>
      <div
        className="text-2xl font-bold mt-1 tabular-nums leading-none"
        style={{ color }}
      >
        {fmtNumber(value)}
      </div>
      {sub && (
        <div className="text-[10px] text-white/40 mt-1">{sub}</div>
      )}
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
  const [attachmentInput, setAttachmentInput] = useState("");
  const [attachment, setAttachment] = useState("");
  const [offset, setOffset] = useState(0);
  const [expandedId, setExpandedId] = useState(null);

  // Debounce both search inputs -> committed filter values.
  useEffect(() => {
    const t = setTimeout(() => setSearch(searchInput.trim()), 300);
    return () => clearTimeout(t);
  }, [searchInput]);

  useEffect(() => {
    const t = setTimeout(() => setAttachment(attachmentInput.trim()), 300);
    return () => clearTimeout(t);
  }, [attachmentInput]);

  // Reset pagination whenever the filter changes.
  useEffect(() => {
    setOffset(0);
    setExpandedId(null);
  }, [entityKey, direction, search, attachment]);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    // Build the URL directly: the shared api.userEmails() helper
    // destructures a fixed set of keys so the attachment filter
    // would be dropped silently if routed through it. Keep auth
    // token handling identical to the api.js get() wrapper (401
    // goes back through PKCE, Bearer auth, same-origin credentials).
    const sp = new URLSearchParams();
    if (direction) sp.set("direction", direction);
    if (search) sp.set("search", search);
    if (attachment) sp.set("attachment", attachment);
    sp.set("limit", String(EMAIL_PAGE));
    sp.set("offset", String(offset));
    const url =
      `/api/users/${encodeURIComponent(entityKey)}/emails?${sp.toString()}`;
    const token = localStorage.getItem("vector_token");
    fetch(url, {
      credentials: "same-origin",
      headers: {
        Accept: "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
    })
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status}`);
        return r.json();
      })
      .then((data) => {
        if (!cancel) setRows(data || []);
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
  }, [entityKey, direction, search, attachment, offset]);

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
          className="bg-white/5 border border-white/10 rounded-xl px-3 py-1.5 text-xs text-white placeholder:text-white/40 focus:outline-none focus:border-primary-light w-48"
        />
        <input
          type="search"
          placeholder="search attachment…"
          value={attachmentInput}
          onChange={(e) => setAttachmentInput(e.target.value)}
          className="bg-white/5 border border-white/10 rounded-xl px-3 py-1.5 text-xs text-white placeholder:text-white/40 focus:outline-none focus:border-primary-light w-48"
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
                <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">From</th>
                <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">To</th>
                <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Subject</th>
                <th className="text-right px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Size</th>
                <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {rows.map((r) => {
                const isOpen = expandedId === r.id;
                // Prefer the authoritative has_attachments flag populated
                // by the MessageTraceIngestor's Defender/Graph backfill.
                // Fall back to a size-based heuristic for legacy rows
                // that pre-date the column.
                const hasAttachment =
                  r.has_attachments === true ||
                  (r.has_attachments == null && Number(r.size_bytes) > 50000);
                return (
                  <Fragment key={r.id}>
                    <tr
                      onClick={() =>
                        setExpandedId(isOpen ? null : r.id)
                      }
                      className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                    >
                      <td className="px-3 py-2 text-white/50 whitespace-nowrap tabular-nums">
                        {fmtTime(r.received)}
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
                        {hasAttachment && (
                          <span
                            className="mr-1"
                            title={
                              Array.isArray(r.attachment_names) && r.attachment_names.length
                                ? r.attachment_names.join(", ")
                                : "has attachments"
                            }
                          >
                            📎
                          </span>
                        )}
                        {r.subject || <span className="text-white/30">(no subject)</span>}
                      </td>
                      <td className="px-3 py-2 text-right text-white/50 tabular-nums whitespace-nowrap">
                        {fmtBytes(r.size_bytes)}
                      </td>
                      <td className="px-3 py-2 text-white/60 whitespace-nowrap">
                        {r.status || <span className="text-white/30">—</span>}
                      </td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td
                          colSpan={6}
                          className="p-0 border-t border-white/5"
                          style={{ backgroundColor: "#0D1428" }}
                        >
                          <EmailAttachmentPanel
                            entityKey={entityKey}
                            messageId={r.message_id}
                          />
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
              {!loading && rows.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-3 py-10 text-center text-white/40">
                    Email trace data populates within 15 minutes
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <div className="px-4 py-2 border-t border-white/5 text-[10px] text-white/40">
          Email metadata only. Content not stored. Click a row to load attachments.
        </div>
      </div>
    </div>
  );
}

function EmailAttachmentPanel({ entityKey, messageId }) {
  const [attachments, setAttachments] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    if (!entityKey || !messageId) {
      setAttachments([]);
      return;
    }
    let cancel = false;
    const url =
      `/api/users/${encodeURIComponent(entityKey)}/emails/` +
      `${encodeURIComponent(messageId)}/attachments`;
    fetch(url, {
      credentials: "same-origin",
      headers: {
        Accept: "application/json",
        ...(localStorage.getItem("vector_token")
          ? { Authorization: `Bearer ${localStorage.getItem("vector_token")}` }
          : {}),
      },
    })
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status}`);
        return r.json();
      })
      .then((data) => {
        if (!cancel) setAttachments(data || []);
      })
      .catch((e) => {
        if (!cancel) {
          setAttachments([]);
          setErr(String(e.message || e));
        }
      });
    return () => {
      cancel = true;
    };
  }, [entityKey, messageId]);

  return (
    <div className="px-4 py-3 animate-fade-in">
      <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
        Attachments
      </div>
      {attachments === null ? (
        <div className="text-[11px] text-white/40 py-1">loading…</div>
      ) : err ? (
        <div className="text-[11px] text-white/40 py-1">
          could not load attachments
        </div>
      ) : attachments.length === 0 ? (
        <div className="text-[11px] text-white/40 py-1">
          no attachments found
        </div>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-white/5">
          <table className="min-w-full text-[10px]">
            <thead>
              <tr className="bg-white/[0.02]">
                <th className="text-left px-2 py-1.5 text-[9px] uppercase tracking-wider text-white/40 font-semibold">
                  Name
                </th>
                <th className="text-right px-2 py-1.5 text-[9px] uppercase tracking-wider text-white/40 font-semibold">
                  Size
                </th>
                <th className="text-left px-2 py-1.5 text-[9px] uppercase tracking-wider text-white/40 font-semibold">
                  Type
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {attachments.map((a, i) => (
                <tr key={a.name || i} className="hover:bg-white/[0.02]">
                  <td className="px-2 py-1.5 text-white/80 truncate max-w-[300px]">
                    📎 {a.name || <span className="text-white/30">(unnamed)</span>}
                  </td>
                  <td className="px-2 py-1.5 text-right text-white/50 tabular-nums whitespace-nowrap">
                    {fmtBytes(a.size_bytes)}
                  </td>
                  <td className="px-2 py-1.5 text-white/50 truncate max-w-[200px]">
                    {a.content_type || <span className="text-white/30">—</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
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

// ---- Endpoint (Datto EDR) ------------------------------------------------
//
// Backed by vector_edr_events via /api/users/{entity_key}/edr. Matching is
// either user_account (case-insensitive) or host_name against any device
// seen in UAL for this user. Rows are click-to-expand like the other tabs.

const EDR_SEVERITY_STYLE = {
  high:          { label: "High",          color: "#EF4444" },
  critical:      { label: "Critical",      color: "#EF4444" },
  medium:        { label: "Medium",        color: "#F97316" },
  moderate:      { label: "Medium",        color: "#F97316" },
  low:           { label: "Low",           color: "#EAB308" },
  informational: { label: "Informational", color: "rgba(255,255,255,0.5)" },
  info:          { label: "Informational", color: "rgba(255,255,255,0.5)" },
};

function EdrSeverityPill({ severity }) {
  const key = String(severity || "").trim().toLowerCase();
  const cfg = EDR_SEVERITY_STYLE[key] || {
    label: severity || "—",
    color: "rgba(255,255,255,0.5)",
  };
  return (
    <span
      className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-full border whitespace-nowrap"
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

function EventTypeBadgeEdr({ type }) {
  const label = String(type || "alert").toUpperCase();
  const color = "#F97316";
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

function EndpointTab({ entityKey }) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState(null);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    setExpanded(null);
    api
      .userEdr(entityKey)
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
  }, [entityKey]);

  return (
    <div className="bg-surface border border-white/5 rounded-card overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full text-[11px]">
          <thead>
            <tr>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Timestamp</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Type</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Severity</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Host</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Threat</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Process</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Action</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {rows.map((r) => (
              <Fragment key={r.id}>
                <tr
                  onClick={() =>
                    setExpanded((prev) => (prev === r.id ? null : r.id))
                  }
                  className="hover:bg-white/[0.03] cursor-pointer"
                  style={{
                    borderLeft: "3px solid #F97316",
                  }}
                >
                  <td className="px-3 py-2 text-white/50 whitespace-nowrap tabular-nums">
                    {fmtTime(r.timestamp)}
                  </td>
                  <td className="px-3 py-2">
                    <EventTypeBadgeEdr type={r.event_type} />
                  </td>
                  <td className="px-3 py-2">
                    <EdrSeverityPill severity={r.severity} />
                  </td>
                  <td
                    className="px-3 py-2 text-white/80 truncate max-w-[200px]"
                    title={r.host_name || ""}
                  >
                    {r.host_name || <span className="text-white/30">—</span>}
                  </td>
                  <td
                    className="px-3 py-2 text-white/80 truncate max-w-[220px]"
                    title={r.threat_name || ""}
                  >
                    {r.threat_name || <span className="text-white/30">—</span>}
                  </td>
                  <td
                    className="px-3 py-2 text-white/70 truncate max-w-[200px]"
                    title={r.process_name || ""}
                  >
                    {r.process_name || <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-3 py-2 text-white/70 whitespace-nowrap">
                    {r.action_taken || <span className="text-white/30">—</span>}
                  </td>
                </tr>
                {expanded === r.id && (
                  <tr className="bg-white/[0.02]">
                    <td colSpan={7} className="px-3 py-3">
                      <JsonBlock data={r.raw_json} />
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
            {!loading && rows.length === 0 && (
              <tr>
                <td colSpan={7} className="px-3 py-10 text-center text-white/40">
                  no endpoint alerts
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="px-4 py-2 border-t border-white/5 text-[10px] text-white/40">
        Datto EDR alerts matched by user account or device name.
      </div>
    </div>
  );
}

// ---- ThreatLocker --------------------------------------------------------
//
// Backed by vector_threatlocker_events via
// /api/users/{entity_key}/threatlocker. Matches by username (exact or
// local-part substring) or by hostname against any device seen in UAL
// for this user. Click-to-expand raw_json like the other tabs.

const THREATLOCKER_COLOR = "#3B82F6";

function threatLockerActionStyle(row) {
  const action = String(row?.action || "").trim().toLowerCase();
  const actionType = String(row?.action_type || "").trim().toLowerCase();
  const id = Number(row?.action_id) || 0;
  if (action === "deny" || actionType === "deny" || id === 2) {
    return { label: "DENY", color: "#EF4444" };
  }
  if (action === "ringfenced" || actionType === "ringfenced" || id === 3) {
    return { label: "RINGFENCED", color: "#F97316" };
  }
  if (id === 6) {
    return { label: "ELEVATED", color: "#EAB308" };
  }
  if (id === 1 || action === "permit" || actionType === "permit") {
    return { label: "PERMIT", color: "#10B981" };
  }
  const fallback = (row?.action || row?.action_type || "—").toString().toUpperCase();
  return { label: fallback, color: "rgba(255,255,255,0.5)" };
}

function ThreatLockerActionBadge({ row }) {
  const cfg = threatLockerActionStyle(row);
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

function ThreatLockerTab({ entityKey }) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState(null);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    setExpanded(null);
    api
      .userThreatLocker(entityKey)
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
  }, [entityKey]);

  return (
    <div className="bg-surface border border-white/5 rounded-card overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full text-[11px]">
          <thead>
            <tr>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Timestamp</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Action</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Type</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Full Path</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Process</th>
              <th className="text-left px-3 py-2 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Policy</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {rows.map((r) => (
              <Fragment key={r.id}>
                <tr
                  onClick={() =>
                    setExpanded((prev) => (prev === r.id ? null : r.id))
                  }
                  className="hover:bg-white/[0.03] cursor-pointer"
                  style={{
                    borderLeft: `3px solid ${THREATLOCKER_COLOR}`,
                  }}
                >
                  <td className="px-3 py-2 text-white/50 whitespace-nowrap tabular-nums">
                    {fmtTime(r.event_time)}
                  </td>
                  <td className="px-3 py-2">
                    <ThreatLockerActionBadge row={r} />
                  </td>
                  <td
                    className="px-3 py-2 text-white/80 truncate max-w-[160px]"
                    title={r.action_type || ""}
                  >
                    {r.action_type || <span className="text-white/30">—</span>}
                  </td>
                  <td
                    className="px-3 py-2 text-white/80 truncate max-w-[280px] font-mono text-[10px]"
                    title={r.full_path || ""}
                  >
                    {r.full_path || <span className="text-white/30">—</span>}
                  </td>
                  <td
                    className="px-3 py-2 text-white/70 truncate max-w-[220px] font-mono text-[10px]"
                    title={r.process_path || ""}
                  >
                    {r.process_path || <span className="text-white/30">—</span>}
                  </td>
                  <td
                    className="px-3 py-2 text-white/70 truncate max-w-[200px]"
                    title={r.policy_name || ""}
                  >
                    {r.policy_name || <span className="text-white/30">—</span>}
                  </td>
                </tr>
                {expanded === r.id && (
                  <tr className="bg-white/[0.02]">
                    <td colSpan={6} className="px-3 py-3">
                      <JsonBlock data={r.raw_json} />
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
            {!loading && rows.length === 0 && (
              <tr>
                <td colSpan={6} className="px-3 py-10 text-center text-white/40">
                  no ThreatLocker events
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="px-4 py-2 border-t border-white/5 text-[10px] text-white/40">
        ThreatLocker ActionLog rows matched by username or device name.
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
