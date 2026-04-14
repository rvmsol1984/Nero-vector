import { Fragment, useEffect, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "../components/Avatar.jsx";
import JsonBlock from "../components/JsonBlock.jsx";
import TenantBadge from "../components/TenantBadge.jsx";
import { api } from "../api.js";
import { getEventLabel } from "../utils/eventLabels.js";
import { filenameFromObjectId, fmtNumber, fmtRelative, fmtTime } from "../utils/format.js";

const TENANT = "GameChange Solar";

// Each tab owns its own endpoint, its default severity pill, and an
// intrinsic severity (what the empty-vs-finding story looks like).
const TABS = [
  { id: "dlp",               label: "DLP Risk",          endpoint: "govDlp",               severity: "review",   withTenant: true },
  { id: "sharing",           label: "External Sharing",  endpoint: "govSharing",           severity: "monitor",  withTenant: true },
  { id: "downloads",         label: "Bulk Downloads",    endpoint: "govDownloads",         severity: "monitor",  withTenant: true },
  { id: "brokenInheritance", label: "Broken Inheritance",endpoint: "govBrokenInheritance", severity: "review",   withTenant: false },
  { id: "oauthApps",         label: "OAuth Apps",        endpoint: "govOauthApps",         severity: "review",   withTenant: false },
  { id: "passwordSpray",     label: "Password Spray",    endpoint: "govPasswordSpray",     severity: "critical", withTenant: false },
  { id: "staleAccounts",     label: "Stale Accounts",    endpoint: "govStaleAccounts",     severity: "monitor",  withTenant: false },
  { id: "mfaChanges",        label: "MFA Changes",       endpoint: "govMfaChanges",        severity: "review",   withTenant: false },
  { id: "privilegedRoles",   label: "Privileged Roles",  endpoint: "govPrivilegedRoles",   severity: "critical", withTenant: false },
  { id: "guestUsers",        label: "Guest Users",       endpoint: "govGuestUsers",        severity: "monitor",  withTenant: false },
  { id: "unmanagedDevices",  label: "Unmanaged Devices", endpoint: "govUnmanagedDevices",  severity: "review",   withTenant: false },
  { id: "intuneDevices",     label: "Intune Devices",    endpoint: "govIntuneDevices",     severity: "review",   withTenant: false },
  { id: "aiActivity",        label: "AI Activity",       endpoint: "govAiActivity",        severity: "monitor",  withTenant: false },
  { id: "edrAlerts",         label: "EDR Alerts",        endpoint: "govEdrAlerts",         severity: "critical", withTenant: false },
  { id: "threatLocker",      label: "ThreatLocker",      endpoint: "govThreatLocker",      severity: "critical", withTenant: false },
  { id: "iocMatches",        label: "IOC Matches",       endpoint: "govIocMatches",        severity: "critical", withTenant: false },
];

// GCS tenant -- hardcoded here because the Governance board is GCS-only and
// we need to synthesize entity_key for the Intune user detail link.
const GCS_TENANT_ID = "07b4c47a-e461-493e-91c4-90df73e2ebc6";

// ---------------------------------------------------------------------------

export default function Governance() {
  // Per-tab cache. data[tabId] === undefined means "never fetched";
  // data[tabId] === [] is a real empty result. That distinction is
  // what drives CountBadge's "unvisited" vs "no findings" states.
  const [data, setData] = useState({});
  const [errors, setErrors] = useState({});
  const [loadingTabs, setLoadingTabs] = useState(() => new Set());
  const [activeTab, setActiveTab] = useState("dlp");

  // Lazy-load the active tab on first visit only. Switching back to
  // a previously-viewed tab uses the cached rows and fires no new
  // request, which keeps the Postgres pool from getting hammered by
  // 11 simultaneous queries on mount.
  useEffect(() => {
    // Already cached? nothing to do.
    if (data[activeTab] !== undefined) return;

    const tab = TABS.find((t) => t.id === activeTab);
    if (!tab) return;
    const fn = api[tab.endpoint];
    if (!fn) return;

    let cancel = false;
    setLoadingTabs((prev) => {
      const next = new Set(prev);
      next.add(activeTab);
      return next;
    });

    const promise = tab.withTenant ? fn(TENANT) : fn();
    promise
      .then((rows) => {
        if (cancel) return;
        setData((prev) => ({ ...prev, [activeTab]: rows || [] }));
        setErrors((prev) => {
          if (!(activeTab in prev)) return prev;
          const { [activeTab]: _, ...rest } = prev;
          return rest;
        });
      })
      .catch((err) => {
        if (cancel) return;
        setData((prev) => ({ ...prev, [activeTab]: [] }));
        setErrors((prev) => ({
          ...prev,
          [activeTab]: String(err?.message || err),
        }));
      })
      .finally(() => {
        if (cancel) return;
        setLoadingTabs((prev) => {
          const next = new Set(prev);
          next.delete(activeTab);
          return next;
        });
      });

    return () => {
      cancel = true;
    };
    // The effect intentionally only re-runs on activeTab change; the
    // in-effect `data[activeTab]` check reads the live snapshot and
    // bails early if the tab is already cached.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab]);

  return (
    <div className="space-y-5 animate-fade-in">
      {/* ----- header ----- */}
      <div className="flex items-center gap-3 flex-wrap">
        <h1 className="text-2xl font-bold">Governance</h1>
        <TenantBadge name={TENANT} />
      </div>
      <p className="text-white/50 text-sm -mt-3">
        UAL-derived policy findings and identity hygiene signals.
      </p>

      {/* ----- wrapping tab bar (2-row on narrow screens) ----- */}
      <div
        className="flex flex-wrap gap-1 border-b border-white/5 mb-4"
      >
        {TABS.map((t) => {
          const rows = data[t.id];
          const visited = rows !== undefined;
          const isLoading = loadingTabs.has(t.id);
          // Count: arrays count directly; stale-accounts envelope reads
          // .rows; AI Activity sums its two sub-lists.
          let count = 0;
          if (visited) {
            if (Array.isArray(rows)) {
              count = rows.length;
            } else if (rows && typeof rows === "object") {
              if (t.id === "aiActivity") {
                count =
                  (rows.copilot?.length || 0) +
                  (rows.external_ai?.length || 0);
              } else if (Array.isArray(rows.rows)) {
                count = rows.rows.length;
              }
            }
          }
          const active = activeTab === t.id;
          return (
            <button
              key={t.id}
              type="button"
              onClick={() => setActiveTab(t.id)}
              className={`flex items-center gap-2 whitespace-nowrap cursor-pointer transition-colors -mb-px ${
                active ? "text-primary-light" : "text-white/50 hover:bg-white/[0.05]"
              }`}
              style={{
                padding: "6px 14px",
                fontSize: "12px",
                fontWeight: 500,
                borderRadius: "6px 6px 0 0",
                borderBottom: active
                  ? "2px solid #2563EB"
                  : "2px solid transparent",
                backgroundColor: active
                  ? "rgba(37,99,235,0.15)"
                  : undefined,
              }}
            >
              <span>{t.label}</span>
              <CountBadge
                count={count}
                active={active}
                loading={isLoading}
                visited={visited}
              />
            </button>
          );
        })}
      </div>

      {/* ----- active tab panel ----- */}
      <TabPanel
        tabId={activeTab}
        rows={data[activeTab]}
        loading={loadingTabs.has(activeTab)}
        error={errors[activeTab]}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// tab dispatch
// ---------------------------------------------------------------------------

function TabPanel({ tabId, rows: raw, loading, error }) {
  if (raw === undefined || loading) {
    return (
      <div className="card py-12 text-center text-white/40 text-sm">
        loading…
      </div>
    );
  }
  if (error) {
    return (
      <div className="card py-6 px-4 border-critical/30 text-critical text-sm">
        load error: {error}
      </div>
    );
  }

  // Endpoints may return an array OR an envelope ({rows, ...meta}).
  // Today only /api/governance/stale-accounts uses the envelope to
  // signal "not enough monitoring history yet".
  let rows = raw;
  let meta = null;
  if (!Array.isArray(raw) && raw && typeof raw === "object") {
    rows = Array.isArray(raw.rows) ? raw.rows : [];
    meta = raw;
  }

  // AI Activity: render both sub-sections together, and only fall through
  // to the generic empty state when both are empty.
  if (tabId === "aiActivity") {
    const copilot = Array.isArray(raw?.copilot) ? raw.copilot : [];
    const external = Array.isArray(raw?.external_ai) ? raw.external_ai : [];
    if (copilot.length === 0 && external.length === 0 && !raw?.external_error) {
      return (
        <div className="card">
          <EmptyState message="No AI activity detected" />
        </div>
      );
    }
    return (
      <AiActivityTab
        copilot={copilot}
        external={external}
        externalError={raw?.external_error}
      />
    );
  }

  // Special case: stale accounts with insufficient data -> informational
  // state instead of the generic green-check empty state.
  if (
    tabId === "staleAccounts" &&
    meta &&
    meta.sufficient_data === false
  ) {
    return <InsufficientDataCard meta={meta} />;
  }

  if (!rows.length) {
    // Intune Devices tab gets a tab-specific empty message so the
    // operator knows the Graph call succeeded and the fleet is clean.
    if (tabId === "intuneDevices") {
      return (
        <div className="card">
          <EmptyState message="All managed devices are compliant" />
        </div>
      );
    }
    if (tabId === "iocMatches") {
      return (
        <div className="card">
          <EmptyState message="No IOC matches detected" />
        </div>
      );
    }
    if (tabId === "threatLocker") {
      return (
        <div className="card">
          <EmptyState message="No blocked events detected" />
        </div>
      );
    }
    return (
      <div className="card">
        <EmptyState />
      </div>
    );
  }

  switch (tabId) {
    case "dlp":               return <DlpTable rows={rows} />;
    case "sharing":           return <SharingTable rows={rows} />;
    case "downloads":         return <DownloadsTable rows={rows} />;
    case "brokenInheritance": return <BrokenInheritanceTable rows={rows} />;
    case "oauthApps":         return <OauthAppsTable rows={rows} />;
    case "passwordSpray":     return <PasswordSprayTable rows={rows} />;
    case "staleAccounts":     return <StaleAccountsTable rows={rows} />;
    case "mfaChanges":        return <MfaChangesTable rows={rows} />;
    case "privilegedRoles":   return <PrivilegedRolesTable rows={rows} />;
    case "guestUsers":        return <GuestUsersTable rows={rows} />;
    case "unmanagedDevices":  return <UnmanagedDevicesTable rows={rows} />;
    case "intuneDevices":     return <IntuneDevicesTable rows={rows} />;
    case "edrAlerts":         return <EdrAlertsTable rows={rows} />;
    case "threatLocker":      return <ThreatLockerTable rows={rows} />;
    case "iocMatches":        return <IocMatchesTable rows={rows} />;
    case "aiActivity":
      // Handled above -- this branch is unreachable because TabPanel
      // short-circuits on tabId === "aiActivity" before the switch.
      return null;
    default: return null;
  }
}

// ---------------------------------------------------------------------------
// shared bits
// ---------------------------------------------------------------------------

const SEVERITY = {
  critical: { label: "CRITICAL",        color: "#EF4444" },
  review:   { label: "REVIEW REQUIRED", color: "#F97316" },
  monitor:  { label: "MONITOR",         color: "#EAB308" },
  clean:    { label: "CLEAN",           color: "#10B981" },
  active:   { label: "ACTIVE",          color: "#3B82F6" },
};

function SeverityPill({ severity }) {
  const cfg = SEVERITY[severity];
  if (!cfg) return null;
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

function CountBadge({ count, active, loading, visited }) {
  if (loading) {
    return (
      <span
        className="inline-flex items-center justify-center tabular-nums"
        style={{
          background: "rgba(255,255,255,0.05)",
          color: "rgba(255,255,255,0.3)",
          fontSize: "10px",
          padding: "1px 6px",
          borderRadius: "10px",
          minWidth: "18px",
        }}
      >
        …
      </span>
    );
  }
  // Unvisited tabs render nothing in place of a count so the strip
  // doesn't look like every tab reports zero findings at page load.
  if (!visited) return null;
  const activeStyle = {
    background: "rgba(37,99,235,0.2)",
    color: "#2563EB",
  };
  const inactiveStyle = {
    background: "rgba(255,255,255,0.1)",
    color: count === 0 ? "rgba(255,255,255,0.4)" : "rgba(255,255,255,0.75)",
  };
  return (
    <span
      className="inline-flex items-center justify-center tabular-nums font-bold"
      style={{
        fontSize: "10px",
        padding: "1px 6px",
        borderRadius: "10px",
        minWidth: "18px",
        ...(active ? activeStyle : inactiveStyle),
      }}
    >
      {count}
    </span>
  );
}

function EmptyState({ message = "No findings detected" }) {
  return (
    <div className="py-16 flex flex-col items-center text-white/50 text-sm gap-3">
      <svg width="48" height="48" viewBox="0 0 48 48" fill="none">
        <circle cx="24" cy="24" r="22" stroke="#10B981" strokeWidth="1.5" />
        <path
          d="M16 24 l6 6 l10-12"
          stroke="#10B981"
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          fill="none"
        />
      </svg>
      <div>{message}</div>
      <SeverityPill severity="clean" />
    </div>
  );
}

function InsufficientDataCard({ meta }) {
  const days = meta?.days_available ?? 0;
  const required = meta?.required_days ?? 14;
  return (
    <div className="card py-14 px-6 flex flex-col items-center text-center gap-3">
      <svg width="48" height="48" viewBox="0 0 48 48" fill="none">
        <circle cx="24" cy="24" r="22" stroke="#EAB308" strokeWidth="1.5" />
        <path
          d="M24 14 v12"
          stroke="#EAB308"
          strokeWidth="2.5"
          strokeLinecap="round"
        />
        <circle cx="24" cy="32" r="1.75" fill="#EAB308" />
      </svg>
      <div className="text-white/70 text-sm font-medium">
        Insufficient data — check back after {required} days of monitoring
      </div>
      <div className="text-white/40 text-[11px]">
        {days.toLocaleString(undefined, { maximumFractionDigits: 1 })} /{" "}
        {required} days of tenant history collected
      </div>
      <SeverityPill severity="monitor" />
    </div>
  );
}

function TableCard({ children }) {
  return (
    <div className="card overflow-hidden">
      <div className="overflow-x-auto">{children}</div>
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

function UserCell({ entityKey, userId, clientName = TENANT }) {
  if (!entityKey) {
    return (
      <div className="flex items-center gap-2">
        <Avatar email={userId} tenant={clientName} size={28} />
        <span className="truncate max-w-[260px]">{userId || "—"}</span>
      </div>
    );
  }
  return (
    <Link
      to={`/users/${encodeURIComponent(entityKey)}`}
      onClick={(e) => e.stopPropagation()}
      className="flex items-center gap-2 hover:text-primary-light"
      title={userId || entityKey}
    >
      <Avatar email={userId} tenant={clientName} size={28} />
      <span className="truncate max-w-[260px]">{userId || entityKey}</span>
    </Link>
  );
}

// ---------------------------------------------------------------------------
// tables — existing 3 (moved into tabs)
// ---------------------------------------------------------------------------

const DLP_EXPAND_COLUMNS = [
  UAL_COL_TIMESTAMP,
  {
    key: "file",
    label: "File",
    render: (r) => (
      <span
        className="truncate max-w-[280px] inline-block align-middle"
        title={r.raw_json?.ObjectId || ""}
      >
        {_fileCell(r)}
      </span>
    ),
  },
  {
    key: "device",
    label: "Device",
    render: (r) => {
      const obj = r.raw_json?.ObjectId || "";
      const drive = obj.match(/^([A-Z]):\\/i);
      if (drive) return <span className="font-mono">{drive[1]}:\\</span>;
      return <span className="text-white/30">—</span>;
    },
  },
  UAL_COL_IP,
];

function DlpTable({ rows }) {
  const [openId, setOpenId] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>User</Th>
            <Th align="right">Events</Th>
            <Th>Last Seen</Th>
            <Th>Files Copied</Th>
            <Th>Status</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const isOpen = openId === row.entity_key;
            return (
              <Fragment key={row.entity_key}>
                <tr
                  onClick={() =>
                    setOpenId(isOpen ? null : row.entity_key)
                  }
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td className="px-4 py-2.5">
                    <UserCell entityKey={row.entity_key} userId={row.user_id} clientName={row.client_name} />
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.event_count)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.last_seen)}
                  </td>
                  <td className="px-4 py-2.5 text-white/60 truncate max-w-[340px]">
                    <FilenameList ids={row.files || row.object_ids} />
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity="review" />
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={6}>
                    <UserEventsExpand
                      entityKey={row.entity_key}
                      eventTypes="FileCreatedOnRemovableMedia"
                      columns={DLP_EXPAND_COLUMNS}
                      eventsParams={{
                        user: row.user_id,
                        event_type: "FileCreatedOnRemovableMedia",
                      }}
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

function FilenameList({ ids }) {
  if (!ids || ids.length === 0) return <span className="text-white/30">—</span>;
  const first = ids.slice(0, 2).map(filenameFromObjectId).filter(Boolean);
  const extra = ids.length - first.length;
  return (
    <span title={ids.join("\n")}>
      {first.join(", ")}
      {extra > 0 && <span className="text-white/30"> · +{extra}</span>}
    </span>
  );
}

const SHARING_EXPAND_COLUMNS = [
  UAL_COL_TIMESTAMP,
  {
    key: "file",
    label: "File / URL",
    render: (r) => {
      const fn = filenameFromObjectId(r.raw_json?.ObjectId);
      const display = fn || r.raw_json?.ObjectId || "—";
      return (
        <span
          className="truncate max-w-[300px] inline-block align-middle"
          title={r.raw_json?.ObjectId || ""}
        >
          {display}
        </span>
      );
    },
  },
  UAL_COL_EVENT_TYPE,
  UAL_COL_IP,
];

function SharingTable({ rows }) {
  const [openId, setOpenId] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>User</Th>
            <Th align="right">Events</Th>
            <Th>Event Type</Th>
            <Th>Last Seen</Th>
            <Th>Status</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const rowKey = `${row.entity_key}-${row.event_type}`;
            const isOpen = openId === rowKey;
            return (
              <Fragment key={rowKey}>
                <tr
                  onClick={() => setOpenId(isOpen ? null : rowKey)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td className="px-4 py-2.5">
                    <UserCell entityKey={row.entity_key} userId={row.user_id} clientName={row.client_name} />
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.event_count)}
                  </td>
                  <td className="px-4 py-2.5 text-white/60" title={row.event_type}>
                    {getEventLabel(row.event_type)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.last_seen)}
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity="monitor" />
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={6}>
                    <UserEventsExpand
                      entityKey={row.entity_key}
                      eventTypes={row.event_type}
                      columns={SHARING_EXPAND_COLUMNS}
                      eventsParams={{
                        user: row.user_id,
                        event_type: row.event_type,
                      }}
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

const DOWNLOADS_EXPAND_COLUMNS = [
  UAL_COL_TIMESTAMP,
  {
    key: "file",
    label: "File",
    render: (r) => (
      <span
        className="truncate max-w-[300px] inline-block align-middle"
        title={r.raw_json?.ObjectId || ""}
      >
        {_fileCell(r)}
      </span>
    ),
  },
  UAL_COL_IP,
];

function DownloadsTable({ rows }) {
  const [openId, setOpenId] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>User</Th>
            <Th align="right">Downloads</Th>
            <Th>Last Seen</Th>
            <Th>Status</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const isOpen = openId === row.entity_key;
            return (
              <Fragment key={row.entity_key}>
                <tr
                  onClick={() => setOpenId(isOpen ? null : row.entity_key)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td className="px-4 py-2.5">
                    <UserCell entityKey={row.entity_key} userId={row.user_id} clientName={row.client_name} />
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.download_count)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.last_seen)}
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity="monitor" />
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={5}>
                    <UserEventsExpand
                      entityKey={row.entity_key}
                      eventTypes="FileDownloadedFromBrowser"
                      columns={DOWNLOADS_EXPAND_COLUMNS}
                      eventsParams={{
                        user: row.user_id,
                        event_type: "FileDownloadedFromBrowser",
                      }}
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

// ---------------------------------------------------------------------------
// tables — new 8
// ---------------------------------------------------------------------------

const BROKEN_INHERITANCE_EXPAND_COLUMNS = [
  UAL_COL_TIMESTAMP,
  {
    key: "file",
    label: "File",
    render: (r) => (
      <span
        className="truncate max-w-[260px] inline-block align-middle"
        title={r.raw_json?.ObjectId || ""}
      >
        {_fileCell(r)}
      </span>
    ),
  },
  {
    key: "site",
    label: "Site",
    render: (r) => {
      const site = r.raw_json?.SiteUrl || r.raw_json?.Site || r.raw_json?.SourceRelativeUrl;
      if (!site) return <span className="text-white/30">—</span>;
      return (
        <span
          className="truncate max-w-[220px] inline-block align-middle"
          title={site}
        >
          {site}
        </span>
      );
    },
  },
  UAL_COL_IP,
];

function BrokenInheritanceTable({ rows }) {
  const [openId, setOpenId] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>User</Th>
            <Th align="right">Events</Th>
            <Th>Last Seen</Th>
            <Th>Files</Th>
            <Th>Status</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const isOpen = openId === row.entity_key;
            return (
              <Fragment key={row.entity_key}>
                <tr
                  onClick={() => setOpenId(isOpen ? null : row.entity_key)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td className="px-4 py-2.5">
                    <UserCell entityKey={row.entity_key} userId={row.user_id} clientName={row.client_name} />
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.event_count)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.last_seen)}
                  </td>
                  <td className="px-4 py-2.5 text-white/60 truncate max-w-[340px]">
                    <FilenameList ids={row.files} />
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity="review" />
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={6}>
                    <UserEventsExpand
                      entityKey={row.entity_key}
                      eventTypes="SharingInheritanceBroken"
                      columns={BROKEN_INHERITANCE_EXPAND_COLUMNS}
                      eventsParams={{
                        user: row.user_id,
                        event_type: "SharingInheritanceBroken",
                      }}
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

const OAUTH_EXPAND_COLUMNS = [
  UAL_COL_TIMESTAMP,
  UAL_COL_USER,
  UAL_COL_IP,
];

function OauthAppsTable({ rows }) {
  const [open, setOpen] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>Application</Th>
            <Th align="right">Users</Th>
            <Th>Last Consent</Th>
            <Th>Status</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const appId = row.app_id || row.app_name || "";
            const displayName = row.display_name || appId || "(unknown)";
            const showGuid = !!appId && displayName !== appId;
            const key = appId || displayName;
            const isOpen = open === key;
            return (
              <Fragment key={key}>
                <tr
                  onClick={() => setOpen(isOpen ? null : key)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td
                    className="px-4 py-2.5 max-w-[420px]"
                    title={appId || displayName}
                  >
                    <div className="font-medium truncate">{displayName}</div>
                    {showGuid && (
                      <div className="text-[10px] text-white/40 font-mono truncate">
                        {appId}
                      </div>
                    )}
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.user_count)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.last_consent)}
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity="review" />
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={5}>
                    <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
                      Users who consented
                    </div>
                    <div className="flex flex-wrap gap-2 mb-3">
                      {(row.users || []).map((u) => (
                        <span
                          key={u}
                          className="text-[11px] px-2 py-1 rounded-md bg-white/5 border border-white/10 text-white/80"
                        >
                          {u}
                        </span>
                      ))}
                      {(!row.users || row.users.length === 0) && (
                        <span className="text-white/30 text-[11px]">
                          no users recorded
                        </span>
                      )}
                    </div>
                    <AsyncEventsExpand
                      fetcher={() =>
                        appId
                          ? api.govOauthAppsEvents(appId, 10)
                          : Promise.resolve([])
                      }
                      depKey={key}
                      columns={OAUTH_EXPAND_COLUMNS}
                      title="Consent events"
                      eventsParams={{
                        event_type: "Consent to application.",
                      }}
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

const PASSWORD_SPRAY_EXPAND_COLUMNS = [
  UAL_COL_TIMESTAMP,
  {
    key: "user_id",
    label: "Targeted User",
    render: (r) => (
      <span
        className="truncate max-w-[240px] inline-block align-middle"
        title={r.user_id}
      >
        {r.user_id || <span className="text-white/30">—</span>}
      </span>
    ),
  },
  UAL_COL_IP,
  {
    key: "result_status",
    label: "Result",
    render: (r) => (
      <span className="text-white/70">
        {r.result_status || <span className="text-white/30">—</span>}
      </span>
    ),
  },
];

function PasswordSprayTable({ rows }) {
  const [open, setOpen] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>Client IP</Th>
            <Th align="right">Targeted</Th>
            <Th align="right">Attempts</Th>
            <Th>First Seen</Th>
            <Th>Last Seen</Th>
            <Th>Status</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const key = row.client_ip || "";
            const isOpen = open === key;
            return (
              <Fragment key={key}>
                <tr
                  onClick={() => setOpen(isOpen ? null : key)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td className="px-4 py-2.5 font-mono tabular-nums">
                    {row.client_ip}
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.targeted_users)}
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.total_attempts)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtTime(row.first_seen)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.last_seen)}
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity="critical" />
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={7}>
                    <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
                      Targeted users ({(row.targets || []).length})
                    </div>
                    <div className="flex flex-wrap gap-2 mb-3">
                      {(row.targets || []).slice(0, 12).map((u) => (
                        <span
                          key={u}
                          className="text-[11px] px-2 py-1 rounded-md bg-white/5 border border-white/10 text-white/80"
                        >
                          {u}
                        </span>
                      ))}
                      {(row.targets || []).length > 12 && (
                        <span className="text-[11px] text-white/40">
                          +{row.targets.length - 12}
                        </span>
                      )}
                      {(!row.targets || row.targets.length === 0) && (
                        <span className="text-white/30 text-[11px]">
                          no targets recorded
                        </span>
                      )}
                    </div>
                    <AsyncEventsExpand
                      fetcher={() =>
                        row.client_ip
                          ? api.govEventsByIp({
                              ip: row.client_ip,
                              event_type: "UserLoginFailed",
                              limit: 10,
                            })
                          : Promise.resolve([])
                      }
                      depKey={key}
                      columns={PASSWORD_SPRAY_EXPAND_COLUMNS}
                      title="Recent UserLoginFailed events from this IP"
                      eventsParams={{ event_type: "UserLoginFailed" }}
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

const STALE_EXPAND_COLUMNS = [
  UAL_COL_TIMESTAMP,
  UAL_COL_EVENT_TYPE,
  UAL_COL_WORKLOAD,
  UAL_COL_IP,
];

function StaleAccountsTable({ rows }) {
  const [openId, setOpenId] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>User</Th>
            <Th align="right">Events</Th>
            <Th>Last Activity</Th>
            <Th>Event Types</Th>
            <Th>Status</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const isOpen = openId === row.entity_key;
            return (
              <Fragment key={row.entity_key}>
                <tr
                  onClick={() => setOpenId(isOpen ? null : row.entity_key)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td className="px-4 py-2.5">
                    <UserCell entityKey={row.entity_key} userId={row.user_id} clientName={row.client_name} />
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.total_events)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.last_activity)}
                  </td>
                  <td
                    className="px-4 py-2.5 text-white/60 truncate max-w-[340px]"
                    title={(row.event_types || []).join(", ")}
                  >
                    {(row.event_types || []).slice(0, 3).map(getEventLabel).join(", ") || "—"}
                    {(row.event_types || []).length > 3 && (
                      <span className="text-white/30"> · +{row.event_types.length - 3}</span>
                    )}
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity="monitor" />
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={6}>
                    <UserEventsExpand
                      entityKey={row.entity_key}
                      columns={STALE_EXPAND_COLUMNS}
                      eventsParams={{ user: row.user_id }}
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

const MFA_EXPAND_COLUMNS = [
  UAL_COL_TIMESTAMP,
  {
    key: "operation",
    label: "Operation",
    render: (r) => (
      <span
        className="truncate max-w-[260px] inline-block align-middle"
        title={r.event_type}
      >
        {getEventLabel(r.event_type) || r.event_type || "—"}
      </span>
    ),
  },
  UAL_COL_IP,
];

function MfaChangesTable({ rows }) {
  const [openId, setOpenId] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>User</Th>
            <Th align="right">Changes</Th>
            <Th>Last Seen</Th>
            <Th>Operations</Th>
            <Th>Status</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const isOpen = openId === row.entity_key;
            return (
              <Fragment key={row.entity_key}>
                <tr
                  onClick={() => setOpenId(isOpen ? null : row.entity_key)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td className="px-4 py-2.5">
                    <UserCell entityKey={row.entity_key} userId={row.user_id} clientName={row.client_name} />
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.change_count)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.last_seen)}
                  </td>
                  <td
                    className="px-4 py-2.5 text-white/60 truncate max-w-[340px]"
                    title={(row.operations || []).join(", ")}
                  >
                    {(row.operations || []).slice(0, 2).join(", ") || "—"}
                    {(row.operations || []).length > 2 && (
                      <span className="text-white/30"> · +{row.operations.length - 2}</span>
                    )}
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity="review" />
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={6}>
                    <UserEventsExpand
                      entityKey={row.entity_key}
                      eventTypes={(row.operations || []).join(",") || undefined}
                      columns={MFA_EXPAND_COLUMNS}
                      eventsParams={{ user: row.user_id }}
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

const PRIV_ROLE_EXPAND_COLUMNS = [
  UAL_COL_TIMESTAMP,
  {
    key: "operation",
    label: "Operation",
    render: (r) => (
      <span
        className="truncate max-w-[220px] inline-block align-middle"
        title={r.event_type}
      >
        {getEventLabel(r.event_type) || r.event_type || "—"}
      </span>
    ),
  },
  {
    key: "role",
    label: "Role",
    render: (r) => {
      // Pull the first ModifiedProperty NewValue if the backend didn't
      // pre-flatten it onto the row.
      const rp = r.raw_json || {};
      const modified = rp.ModifiedProperties || [];
      const roleProp =
        Array.isArray(modified) &&
        modified.find((p) => (p?.Name || "").toLowerCase().includes("role"));
      const role = roleProp?.NewValue || rp.Role || "";
      return role ? (
        <span className="truncate max-w-[220px] inline-block align-middle" title={role}>
          {role}
        </span>
      ) : (
        <span className="text-white/30">—</span>
      );
    },
  },
  UAL_COL_IP,
];

function PrivilegedRolesTable({ rows }) {
  const [openId, setOpenId] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>When</Th>
            <Th>Operation</Th>
            <Th>Role</Th>
            <Th>User</Th>
            <Th>Actor</Th>
            <Th>Status</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row, i) => {
            const rowKey = `${row.entity_key}-${row.timestamp}-${i}`;
            const isOpen = openId === rowKey;
            return (
              <Fragment key={rowKey}>
                <tr
                  onClick={() => setOpenId(isOpen ? null : rowKey)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtTime(row.timestamp)}
                  </td>
                  <td className="px-4 py-2.5 text-white/80" title={row.event_type}>
                    {getEventLabel(row.operation || row.event_type)}
                  </td>
                  <td className="px-4 py-2.5 truncate max-w-[260px]" title={row.role}>
                    {row.role || <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-4 py-2.5">
                    <UserCell entityKey={row.entity_key} userId={row.user_id} clientName={row.client_name} />
                  </td>
                  <td className="px-4 py-2.5 text-white/60 truncate max-w-[220px]">
                    {row.actor || <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity="critical" />
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={7}>
                    <UserEventsExpand
                      entityKey={row.entity_key}
                      eventTypes={row.event_type || row.operation}
                      columns={PRIV_ROLE_EXPAND_COLUMNS}
                      eventsParams={{
                        user: row.user_id,
                        event_type: row.event_type || row.operation,
                      }}
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

const GUEST_EXPAND_COLUMNS = [
  UAL_COL_TIMESTAMP,
  UAL_COL_EVENT_TYPE,
  UAL_COL_WORKLOAD,
  {
    key: "resource",
    label: "Resource",
    render: (r) => {
      const obj = r.raw_json?.ObjectId || r.raw_json?.TargetResources?.[0]?.displayName;
      if (!obj) return <span className="text-white/30">—</span>;
      const fn = filenameFromObjectId(obj) || obj;
      return (
        <span className="truncate max-w-[260px] inline-block align-middle" title={obj}>
          {fn}
        </span>
      );
    },
  },
];

function GuestUsersTable({ rows }) {
  const [openId, setOpenId] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>Display Name</Th>
            <Th>Email</Th>
            <Th>Created</Th>
            <Th>Last Sign-In</Th>
            <Th>Status</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const rowKey = row.id || row.userPrincipalName || row.mail;
            const isOpen = openId === rowKey;
            const upn = row.userPrincipalName || row.mail || "";
            // Guest users don't carry an entity_key on the row, so
            // synthesize one from the GCS tenant id + UPN for the
            // expand links and the per-user events query.
            const entityKey = upn ? `${GCS_TENANT_ID}::${upn}` : null;
            return (
              <Fragment key={rowKey}>
                <tr
                  onClick={() => setOpenId(isOpen ? null : rowKey)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td className="px-4 py-2.5">
                    <div className="flex items-center gap-2">
                      <Avatar email={row.mail || row.displayName} tenant={TENANT} size={28} />
                      <span className="font-medium truncate max-w-[220px]">
                        {row.displayName || <span className="text-white/40">—</span>}
                      </span>
                    </div>
                  </td>
                  <td
                    className="px-4 py-2.5 text-white/70 truncate max-w-[280px]"
                    title={row.mail || row.userPrincipalName}
                  >
                    {row.mail || row.userPrincipalName || <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {row.createdDateTime ? fmtTime(row.createdDateTime) : <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {row.lastSignIn ? fmtRelative(row.lastSignIn) : <span className="text-white/30">never</span>}
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity="monitor" />
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={6}>
                    <UserEventsExpand
                      entityKey={entityKey}
                      columns={GUEST_EXPAND_COLUMNS}
                      eventsParams={{ user: upn }}
                      emptyMessage={
                        entityKey
                          ? "no matching events"
                          : "no UPN recorded for this guest"
                      }
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
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
// shared expand helpers
// ---------------------------------------------------------------------------
//
// Every governance table expands its rows in-place using the same
// pattern: a trailing chevron cell on every row, a dark inner card
// (#0D1428) rendered as a full-width <tr> below the clicked row, and
// a footer with "View User" / "View in Events" shortcuts. Click
// handlers live on the outer row so clicking anywhere on it toggles.
// Any inner link still needs `onClick={(e) => e.stopPropagation()}`
// so it navigates without collapsing the row first.

function ChevronCell({ open }) {
  return (
    <td className="px-3 py-2.5 w-8 text-white/30">
      <Chevron open={open} />
    </td>
  );
}

function ExpandedPanel({ colSpan, children }) {
  return (
    <tr>
      <td
        colSpan={colSpan}
        className="p-0 border-t border-white/5"
        style={{ backgroundColor: "#0D1428" }}
      >
        <div className="px-4 py-4">{children}</div>
      </td>
    </tr>
  );
}

// Footer links rendered at the bottom of every expand panel. `entityKey`
// drives the "View User" link; the Events link takes an explicit
// query-param object so each tab can point the filter at the most
// useful slice (workload, event_type, user, client_ip, ...).
function ExpandedFooterLinks({ entityKey, eventsParams }) {
  const params = new URLSearchParams();
  Object.entries(eventsParams || {}).forEach(([k, v]) => {
    if (v != null && v !== "") params.set(k, v);
  });
  const search = params.toString();
  return (
    <div className="mt-3 pt-3 border-t border-white/10 flex items-center gap-4 text-[11px]">
      {entityKey && (
        <Link
          to={`/users/${encodeURIComponent(entityKey)}`}
          onClick={(e) => e.stopPropagation()}
          className="text-primary-light hover:underline"
        >
          View User →
        </Link>
      )}
      <Link
        to={`/events${search ? `?${search}` : ""}`}
        onClick={(e) => e.stopPropagation()}
        className="text-primary-light hover:underline"
      >
        View in Events →
      </Link>
    </div>
  );
}

// Low-level renderer for the inner event table. `columns` is an array
// of { key, label, render?(row) } cell definitions; `render` can return
// any ReactNode.
function InnerEventsTable({ rows, columns, emptyMessage = "no matching events" }) {
  if (rows == null) {
    return <div className="text-white/40 text-[11px] py-2">loading events…</div>;
  }
  if (rows.length === 0) {
    return <div className="text-white/40 text-[11px] py-2">{emptyMessage}</div>;
  }
  return (
    <div className="overflow-x-auto rounded-lg border border-white/5">
      <table className="min-w-full text-[10px]">
        <thead>
          <tr>
            {columns.map((c) => (
              <th
                key={c.key}
                className="text-left px-2 py-1.5 text-[9px] uppercase tracking-wider text-white/40 font-semibold bg-white/[0.02]"
              >
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((r, i) => (
            <tr key={r.id || i} className="hover:bg-white/[0.02]">
              {columns.map((c) => (
                <td
                  key={c.key}
                  className="px-2 py-1.5 text-white/70 align-top"
                >
                  {c.render ? c.render(r) : (r[c.key] ?? <span className="text-white/30">—</span>)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// Fetch + render events for an entity_key using /api/users/{key}/events
// and the supplied event_types / workloads filter. Every UAL-derived
// governance expand goes through this helper.
function UserEventsExpand({
  entityKey,
  eventTypes,
  workloads,
  columns,
  eventsParams,
  title = "Recent matching events",
  emptyMessage,
}) {
  const [rows, setRows] = useState(null);

  useEffect(() => {
    if (!entityKey) {
      setRows([]);
      return;
    }
    let cancel = false;
    api
      .userEvents(entityKey, {
        event_types: eventTypes,
        workloads,
        limit: 10,
      })
      .then((r) => !cancel && setRows(r || []))
      .catch(() => !cancel && setRows([]));
    return () => {
      cancel = true;
    };
  }, [entityKey, eventTypes, workloads]);

  return (
    <>
      <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
        {title}
      </div>
      <InnerEventsTable rows={rows} columns={columns} emptyMessage={emptyMessage} />
      <ExpandedFooterLinks entityKey={entityKey} eventsParams={eventsParams} />
    </>
  );
}

// Generic wrapper for an endpoint-driven expand (EDR, ThreatLocker,
// OAuth Apps, Password Spray). Takes an async fetcher that returns the
// inner rows directly so each call site keeps its own param shape.
function AsyncEventsExpand({
  fetcher,
  depKey,
  columns,
  entityKey,
  eventsParams,
  title = "Recent matching events",
  emptyMessage,
}) {
  const [rows, setRows] = useState(null);

  useEffect(() => {
    let cancel = false;
    setRows(null);
    Promise.resolve()
      .then(() => fetcher())
      .then((r) => !cancel && setRows(r || []))
      .catch(() => !cancel && setRows([]));
    return () => {
      cancel = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [depKey]);

  return (
    <>
      <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
        {title}
      </div>
      <InnerEventsTable rows={rows} columns={columns} emptyMessage={emptyMessage} />
      <ExpandedFooterLinks entityKey={entityKey} eventsParams={eventsParams} />
    </>
  );
}

// Shorthand helpers for some very common UAL event shapes so each
// table can reuse the same column specs.
const _ipCell = (r) =>
  r.client_ip ? (
    <span className="font-mono tabular-nums">{r.client_ip}</span>
  ) : (
    <span className="text-white/30">—</span>
  );

const _fileCell = (r) => {
  const fn = filenameFromObjectId(r.raw_json?.ObjectId);
  return fn || <span className="text-white/30">—</span>;
};

const UAL_COL_TIMESTAMP = {
  key: "timestamp",
  label: "Time",
  render: (r) => (
    <span className="text-white/60 whitespace-nowrap tabular-nums">
      {fmtTime(r.timestamp)}
    </span>
  ),
};
const UAL_COL_IP = { key: "client_ip", label: "IP", render: _ipCell };
const UAL_COL_EVENT_TYPE = {
  key: "event_type",
  label: "Type",
  render: (r) => (
    <span className="truncate max-w-[200px] inline-block align-middle" title={r.event_type}>
      {getEventLabel(r.event_type) || "—"}
    </span>
  ),
};
const UAL_COL_WORKLOAD = {
  key: "workload",
  label: "Workload",
  render: (r) => r.workload || <span className="text-white/30">—</span>,
};
const UAL_COL_USER = {
  key: "user_id",
  label: "User",
  render: (r) => (
    <span className="truncate max-w-[200px] inline-block align-middle" title={r.user_id}>
      {r.user_id || <span className="text-white/30">—</span>}
    </span>
  ),
};

// ---- AI Activity tab -------------------------------------------------------

// Friendly names for the AI tool domains the Defender KQL query filters on.
// Keys are the hostnames we expect RemoteUrl to resolve to; anything not in
// the map falls back to the raw hostname.
const AI_TOOL_NAMES = {
  "chat.openai.com":      "ChatGPT",
  "chatgpt.com":          "ChatGPT",
  "api.openai.com":       "ChatGPT (API)",
  "claude.ai":            "Claude",
  "anthropic.com":        "Anthropic",
  "gemini.google.com":    "Gemini",
  "bard.google.com":      "Bard",
  "deepseek.com":         "DeepSeek",
  "perplexity.ai":        "Perplexity",
  "copilot.microsoft.com":"Copilot",
  "huggingface.co":       "HuggingFace",
  "mistral.ai":           "Mistral",
  "grok.x.ai":            "Grok",
  "you.com":              "You.com",
  "poe.com":              "Poe",
};

function aiToolDisplay(remoteUrl) {
  if (!remoteUrl) return { label: "—", host: "" };
  const raw = String(remoteUrl).trim();
  // Defender's RemoteUrl is usually just a hostname; if it's a full URL,
  // pull the hostname out. Fall back to the raw string on any parse error.
  let host = raw;
  try {
    const parsed = new URL(raw.startsWith("http") ? raw : `https://${raw}`);
    host = parsed.hostname || raw;
  } catch {
    host = raw;
  }
  host = host.toLowerCase();
  // Try exact match, then progressively shorter suffixes (so "www.claude.ai"
  // still resolves to "Claude").
  if (AI_TOOL_NAMES[host]) return { label: AI_TOOL_NAMES[host], host };
  for (const key of Object.keys(AI_TOOL_NAMES)) {
    if (host === key || host.endsWith(`.${key}`)) {
      return { label: AI_TOOL_NAMES[key], host };
    }
  }
  return { label: host, host };
}

function AiActivityTab({ copilot, external, externalError }) {
  const [subTab, setSubTab] = useState("copilot");
  const SUB_TABS = [
    { id: "copilot",  label: "Microsoft Copilot", count: copilot.length },
    { id: "external", label: "External AI Tools", count: external.length },
  ];
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-0 border-b border-white/5">
        {SUB_TABS.map((t) => {
          const active = subTab === t.id;
          return (
            <button
              key={t.id}
              type="button"
              onClick={() => setSubTab(t.id)}
              className={`-mb-px transition-colors whitespace-nowrap ${
                active ? "text-primary-light" : "text-white/50 hover:text-white/80"
              }`}
              style={{
                padding: "8px 16px",
                fontSize: "12px",
                fontWeight: 500,
                borderBottom: active
                  ? "2px solid #2563EB"
                  : "2px solid transparent",
              }}
            >
              {t.label}{" "}
              <span
                className="ml-1 tabular-nums"
                style={{
                  color: active ? "#2563EB" : "rgba(255,255,255,0.4)",
                }}
              >
                ({t.count})
              </span>
            </button>
          );
        })}
      </div>

      {subTab === "copilot" ? (
        <AiCopilotSection rows={copilot} />
      ) : (
        <AiExternalSection rows={external} error={externalError} />
      )}
    </div>
  );
}

function AiCopilotSection({ rows }) {
  return (
    <div className="card overflow-hidden">
      <div className="px-5 py-4 border-b border-white/5 flex items-center justify-between gap-4">
        <div>
          <div className="text-base font-bold">Microsoft Copilot</div>
          <div className="text-[11px] text-white/50 mt-0.5">
            Copilot workload usage from UAL for GameChange Solar
          </div>
        </div>
        <span className="inline-flex items-center px-2.5 py-1 rounded-full text-[10px] uppercase tracking-wider font-semibold bg-primary/15 border border-primary/40 text-primary-light tabular-nums">
          {rows.length} {rows.length === 1 ? "user" : "users"}
        </span>
      </div>
      {rows.length === 0 ? (
        <div className="px-5 py-8 text-white/40 text-sm text-center">
          No Copilot activity detected
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-[11px]">
            <thead>
              <tr>
                <Th>User</Th>
                <Th align="right">Events</Th>
                <Th>Event Types</Th>
                <Th>Last Active</Th>
                <Th>Status</Th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {rows.map((row) => {
                const entityKey = `${GCS_TENANT_ID}::${row.user_id}`;
                const types = Array.isArray(row.event_types) ? row.event_types : [];
                return (
                  <tr key={row.user_id} className="hover:bg-white/[0.03]">
                    <td className="px-4 py-2.5">
                      <UserCell
                        entityKey={entityKey}
                        userId={row.user_id}
                        clientName="GameChange Solar"
                      />
                    </td>
                    <td className="px-4 py-2.5 text-right tabular-nums">
                      {fmtNumber(row.event_count)}
                    </td>
                    <td className="px-4 py-2.5">
                      <div className="flex flex-wrap gap-1">
                        {types.slice(0, 4).map((t) => (
                          <span
                            key={t}
                            className="inline-flex items-center px-1.5 py-[2px] text-[9px] font-semibold uppercase tracking-wide rounded border border-primary-light/30 bg-primary-light/10 text-primary-light whitespace-nowrap"
                            title={t}
                          >
                            {getEventLabel(t)}
                          </span>
                        ))}
                        {types.length > 4 && (
                          <span className="text-[10px] text-white/40">
                            +{types.length - 4}
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                      {fmtRelative(row.last_seen)}
                    </td>
                    <td className="px-4 py-2.5">
                      <SeverityPill severity="active" />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function AiExternalSection({ rows, error }) {
  return (
    <div className="card overflow-hidden">
      <div className="px-5 py-4 border-b border-white/5 flex items-center justify-between gap-4">
        <div>
          <div className="text-base font-bold">External AI Tools</div>
          <div className="text-[11px] text-white/50 mt-0.5">
            Users hitting third-party AI domains from managed devices
          </div>
        </div>
        <span className="inline-flex items-center px-2.5 py-1 rounded-full text-[10px] uppercase tracking-wider font-semibold bg-primary/15 border border-primary/40 text-primary-light tabular-nums">
          {rows.length} {rows.length === 1 ? "user" : "users"}
        </span>
      </div>

      <div
        className="px-5 py-2 text-[11px] text-white/50 border-b border-white/5"
        style={{ backgroundColor: "rgba(59,130,246,0.05)" }}
      >
        Detected via Defender endpoint telemetry. Shows AI tool domains
        accessed from managed devices.
      </div>

      {error ? (
        <div className="px-5 py-8 text-[11px] text-critical text-center">
          Defender hunting query failed: {error}
        </div>
      ) : rows.length === 0 ? (
        <div className="px-5 py-8 text-white/40 text-sm text-center">
          No external AI tool access detected on managed devices in the last 7 days
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-[11px]">
            <thead>
              <tr>
                <Th>User</Th>
                <Th>Tool</Th>
                <Th align="right">Visits</Th>
                <Th>Devices</Th>
                <Th>Last Access</Th>
                <Th>Status</Th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {rows.map((row, i) => {
                const entityKey = row.user
                  ? `${GCS_TENANT_ID}::${row.user}`
                  : null;
                const { label: toolLabel, host } = aiToolDisplay(row.tool);
                const devices = Array.isArray(row.devices) ? row.devices : [];
                return (
                  <tr key={`${row.user}-${row.tool}-${i}`} className="hover:bg-white/[0.03]">
                    <td className="px-4 py-2.5">
                      {entityKey ? (
                        <UserCell
                          entityKey={entityKey}
                          userId={row.user}
                          clientName="GameChange Solar"
                        />
                      ) : (
                        <span className="text-white/40">—</span>
                      )}
                    </td>
                    <td className="px-4 py-2.5">
                      <div className="font-medium" title={host}>
                        {toolLabel}
                      </div>
                      {host && host !== toolLabel && (
                        <div
                          className="text-[10px] text-white/40 font-mono truncate max-w-[240px]"
                          title={host}
                        >
                          {host}
                        </div>
                      )}
                    </td>
                    <td className="px-4 py-2.5 text-right tabular-nums">
                      {fmtNumber(row.visit_count)}
                    </td>
                    <td
                      className="px-4 py-2.5 text-white/60 truncate max-w-[260px]"
                      title={devices.join(", ")}
                    >
                      {devices.length === 0 ? (
                        <span className="text-white/30">—</span>
                      ) : devices.length === 1 ? (
                        <span className="font-mono text-[10px]">{devices[0]}</span>
                      ) : (
                        <>
                          <span className="font-mono text-[10px]">{devices[0]}</span>
                          <span className="text-white/30">
                            {" "}· +{devices.length - 1}
                          </span>
                        </>
                      )}
                    </td>
                    <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                      {fmtRelative(row.last_visit)}
                    </td>
                    <td className="px-4 py-2.5">
                      <SeverityPill severity="monitor" />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function UnmanagedDevicesTable({ rows }) {
  const [expanded, setExpanded] = useState(null);
  // Per-user cache so re-opening doesn't refetch.
  const [detailCache, setDetailCache] = useState({});
  const [loadingUser, setLoadingUser] = useState(null);

  async function toggle(userId) {
    if (expanded === userId) {
      setExpanded(null);
      return;
    }
    setExpanded(userId);
    if (detailCache[userId] !== undefined) return;
    setLoadingUser(userId);
    try {
      const result = await api.govUnmanagedDevicesDetail(userId);
      setDetailCache((prev) => ({ ...prev, [userId]: result || [] }));
    } catch (e) {
      setDetailCache((prev) => ({ ...prev, [userId]: { error: e.message } }));
    } finally {
      setLoadingUser((prev) => (prev === userId ? null : prev));
    }
  }

  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>User</Th>
            <Th align="right">Events</Th>
            <Th>Last Seen</Th>
            <Th>Devices</Th>
            <Th>Status</Th>
            <Th>&nbsp;</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const isOpen = expanded === row.user_id;
            const isLoading = loadingUser === row.user_id;
            const detail = detailCache[row.user_id];
            return (
              <Fragment key={row.entity_key}>
                <tr
                  onClick={() => toggle(row.user_id)}
                  className={`cursor-pointer ${
                    isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"
                  }`}
                >
                  <td className="px-4 py-2.5">
                    <UserCell
                      entityKey={row.entity_key}
                      userId={row.user_id}
                      clientName={row.client_name}
                    />
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.event_count)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.last_seen)}
                  </td>
                  <td className="px-4 py-2.5 text-white/60">
                    <DeviceList devices={row.devices} />
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity="review" />
                  </td>
                  <td className="px-4 py-2.5 w-8 text-right">
                    <Chevron open={isOpen} />
                  </td>
                </tr>
                {isOpen && (
                  <tr className="bg-black/30">
                    <td
                      colSpan={6}
                      className="px-4 py-3 border-t border-white/5 animate-slide-up"
                    >
                      <DeviceDetailTable detail={detail} loading={isLoading} />
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

// ---- Intune Devices tab ----------------------------------------------------

function IssuePill({ tone, children }) {
  const palette = {
    red:    "#EF4444",
    orange: "#F97316",
    yellow: "#EAB308",
    green:  "#10B981",
  };
  const color = palette[tone] || "#8b949e";
  return (
    <span
      className="inline-flex items-center px-1.5 py-[2px] text-[9px] font-bold uppercase tracking-wide rounded border whitespace-nowrap"
      style={{
        color,
        borderColor: `${color}55`,
        backgroundColor: `${color}14`,
      }}
    >
      {children}
    </span>
  );
}

function intuneOldestSync(devices) {
  const times = (devices || [])
    .map((d) => d.lastSyncDateTime)
    .filter(Boolean);
  if (times.length === 0) return null;
  return times.reduce((a, b) => (a < b ? a : b));
}

function intuneDeviceStatus(device) {
  const state = String(device.complianceState || "").toLowerCase();
  const noncompliant = state && state !== "compliant" && state !== "unknown";
  const unencrypted = device.isEncrypted === false;
  const last = device.lastSyncDateTime ? new Date(device.lastSyncDateTime) : null;
  const stale =
    last && !Number.isNaN(last.getTime())
      ? Date.now() - last.getTime() > 30 * 24 * 60 * 60 * 1000
      : false;
  // Red takes precedence over orange, orange over yellow, else green.
  let dot = "#10B981";
  if (noncompliant || unencrypted) dot = "#EF4444";
  else if (stale) dot = "#EAB308";
  return { noncompliant, unencrypted, stale, dot };
}

function IntuneDevicesTable({ rows }) {
  const [expanded, setExpanded] = useState(null);

  function toggle(user) {
    setExpanded(expanded === user ? null : user);
  }

  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>User</Th>
            <Th align="right">Devices</Th>
            <Th>Issues</Th>
            <Th>Oldest Sync</Th>
            <Th>&nbsp;</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const isOpen = expanded === row.user;
            const entityKey = `${GCS_TENANT_ID}::${row.user}`;
            return (
              <Fragment key={row.user}>
                <tr
                  onClick={() => toggle(row.user)}
                  className={`cursor-pointer ${
                    isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"
                  }`}
                >
                  <td
                    className="px-4 py-2.5"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <UserCell
                      entityKey={entityKey}
                      userId={row.user}
                      clientName="GameChange Solar"
                    />
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.device_count)}
                  </td>
                  <td className="px-4 py-2.5">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      {row.unencrypted_count > 0 && (
                        <IssuePill tone="red">
                          NOT ENCRYPTED · {row.unencrypted_count}
                        </IssuePill>
                      )}
                      {row.noncompliant_count > 0 && (
                        <IssuePill tone="orange">
                          NON-COMPLIANT · {row.noncompliant_count}
                        </IssuePill>
                      )}
                      {row.stale_count > 0 && (
                        <IssuePill tone="yellow">
                          STALE · {row.stale_count}
                        </IssuePill>
                      )}
                    </div>
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(intuneOldestSync(row.devices))}
                  </td>
                  <td className="px-4 py-2.5 w-8 text-right">
                    <Chevron open={isOpen} />
                  </td>
                </tr>
                {isOpen && (
                  <tr className="bg-black/30">
                    <td
                      colSpan={5}
                      className="px-4 py-3 border-t border-white/5 animate-slide-up"
                    >
                      <IntuneDeviceDetailTable devices={row.devices} />
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

function IntuneDeviceDetailTable({ devices }) {
  if (!devices || devices.length === 0) {
    return (
      <div className="text-white/40 text-xs py-2">
        No devices for this user
      </div>
    );
  }
  return (
    <div>
      <div className="text-[10px] uppercase tracking-[0.15em] text-white/40 mb-2">
        Intune Devices
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-[11px]">
          <thead>
            <tr>
              <th className="w-5"></th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                Device Name
              </th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                OS
              </th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                Compliance
              </th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                Encrypted
              </th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                Owner
              </th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                Last Sync
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {devices.map((d, i) => {
              const st = intuneDeviceStatus(d);
              const compLabel =
                String(d.complianceState || "unknown").toLowerCase();
              const compColor =
                compLabel === "compliant"
                  ? "text-status-resolved"
                  : compLabel === "unknown"
                  ? "text-white/40"
                  : "text-critical";
              return (
                <tr key={`${d.deviceName || "dev"}-${i}`}>
                  <td className="px-1.5 py-2">
                    <span
                      className="inline-block h-2 w-2 rounded-full"
                      style={{ background: st.dot }}
                      title={
                        st.noncompliant
                          ? "Non-compliant"
                          : st.unencrypted
                          ? "Not encrypted"
                          : st.stale
                          ? `Stale — last sync > 30d`
                          : "Clean"
                      }
                    />
                  </td>
                  <td
                    className="px-2 py-2 font-mono truncate max-w-[240px]"
                    title={d.deviceName || ""}
                  >
                    {d.deviceName || (
                      <span className="text-white/30">—</span>
                    )}
                  </td>
                  <td className="px-2 py-2 text-white/60 whitespace-nowrap">
                    {d.operatingSystem || "—"}
                    {d.osVersion && (
                      <span className="text-white/30"> {d.osVersion}</span>
                    )}
                  </td>
                  <td className={`px-2 py-2 font-medium ${compColor}`}>
                    {d.complianceState || "unknown"}
                  </td>
                  <td className="px-2 py-2">
                    {d.isEncrypted === true ? (
                      <span className="text-status-resolved font-medium">
                        yes
                      </span>
                    ) : d.isEncrypted === false ? (
                      <span className="text-critical font-medium">no</span>
                    ) : (
                      <span className="text-white/30">—</span>
                    )}
                  </td>
                  <td className="px-2 py-2 text-white/60">
                    {d.managedDeviceOwnerType || (
                      <span className="text-white/30">—</span>
                    )}
                  </td>
                  <td className="px-2 py-2 text-white/50 whitespace-nowrap">
                    {d.lastSyncDateTime ? fmtTime(d.lastSyncDateTime) : "—"}
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

function DeviceDetailTable({ detail, loading }) {
  if (loading) {
    return (
      <div className="text-white/40 text-xs py-2">loading device details…</div>
    );
  }
  if (detail && detail.error) {
    return (
      <div className="text-critical text-xs py-2">
        load error: {detail.error}
      </div>
    );
  }
  const devices = Array.isArray(detail) ? detail : [];
  if (devices.length === 0) {
    return (
      <div className="text-white/40 text-xs py-2">
        No device details available
      </div>
    );
  }

  // If any row came from Intune, surface a small corner tag so the
  // operator knows the enrichment happened.
  const hasIntuneRows = devices.some((d) => d.source === "intune");

  return (
    <div>
      <div className="flex items-center gap-2 mb-2">
        <div className="text-[10px] uppercase tracking-[0.15em] text-white/40">
          Devices
        </div>
        {hasIntuneRows && (
          <span
            className="inline-flex items-center px-1.5 py-[2px] text-[9px] font-bold uppercase tracking-wide rounded border"
            style={{
              color: "#3B82F6",
              borderColor: "#3B82F655",
              backgroundColor: "#3B82F614",
            }}
          >
            enriched · Intune
          </span>
        )}
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-[11px]">
          <thead>
            <tr>
              <th className="w-5"></th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                Device Name
              </th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                OS
              </th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                Compliance
              </th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                Encrypted
              </th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                Managed
              </th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                Last Intune Sync
              </th>
              <th className="text-left px-2 py-1 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
                Last UAL Event
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {devices.map((d, i) => {
              const notCompliant = d.is_compliant === false;
              const notManaged = d.is_managed === false;
              const notEncrypted = d.is_encrypted === false;
              // Red for non-compliant or unencrypted (hard failures);
              // orange for unmanaged-only; green for clean.
              let dotColor;
              let dotTitle;
              if (notCompliant) {
                dotColor = "#EF4444";
                dotTitle = "Non-compliant";
              } else if (notEncrypted) {
                dotColor = "#EF4444";
                dotTitle = "Not encrypted";
              } else if (notManaged) {
                dotColor = "#F97316";
                dotTitle = "Not managed";
              } else {
                dotColor = "#10B981";
                dotTitle = "Clean";
              }

              // Prefer Intune complianceState over the UAL boolean so
              // we render the real Graph string (e.g. "noncompliant",
              // "inGracePeriod", "error") when available.
              const complianceLabel = d.compliance_state
                ? String(d.compliance_state)
                : d.is_compliant === true
                ? "compliant"
                : d.is_compliant === false
                ? "noncompliant"
                : null;
              const complianceColor =
                complianceLabel === "compliant"
                  ? "text-status-resolved"
                  : complianceLabel && complianceLabel !== "unknown"
                  ? "text-critical"
                  : "text-white/30";

              return (
                <tr key={`${d.display_name || d.name || d.device_id || "dev"}-${i}`}>
                  <td className="px-1.5 py-2">
                    <span
                      className="inline-block h-2 w-2 rounded-full"
                      style={{ background: dotColor }}
                      title={dotTitle}
                    />
                  </td>
                  <td
                    className="px-2 py-2 font-mono truncate max-w-[260px]"
                    title={d.display_name || d.name || d.device_id || ""}
                  >
                    {d.display_name || d.name || d.device_id || (
                      <span className="text-white/30">—</span>
                    )}
                  </td>
                  <td className="px-2 py-2 text-white/60 whitespace-nowrap">
                    {d.os || <span className="text-white/30">—</span>}
                  </td>
                  <td className={`px-2 py-2 font-medium ${complianceColor}`}>
                    {complianceLabel || <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-2 py-2">
                    {d.is_encrypted === true ? (
                      <span className="text-status-resolved font-medium">yes</span>
                    ) : d.is_encrypted === false ? (
                      <span className="text-critical font-medium">no</span>
                    ) : (
                      <span className="text-white/30">—</span>
                    )}
                  </td>
                  <td className="px-2 py-2">
                    {d.is_managed === true ? (
                      <span className="text-status-resolved font-medium">yes</span>
                    ) : d.is_managed === false ? (
                      <span className="text-high font-medium">no</span>
                    ) : (
                      <span className="text-white/30">—</span>
                    )}
                  </td>
                  <td className="px-2 py-2 text-white/50 whitespace-nowrap">
                    {d.last_sync_date_time ? (
                      fmtTime(d.last_sync_date_time)
                    ) : (
                      <span className="text-white/30">—</span>
                    )}
                  </td>
                  <td className="px-2 py-2 text-white/50 whitespace-nowrap">
                    {d.last_seen ? (
                      fmtTime(d.last_seen)
                    ) : (
                      <span className="text-white/30">—</span>
                    )}
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

// ---------------------------------------------------------------------------
// EDR Alerts (Datto EDR / Infocyte)
// ---------------------------------------------------------------------------

// Map raw EDR severity strings (High/Medium/Low/Informational/…) onto our
// governance severity pills. High folds into CRITICAL (red), Medium into
// REVIEW REQUIRED (orange), everything else into MONITOR.
function edrSeverityToPill(raw) {
  const key = String(raw || "").trim().toLowerCase();
  if (key === "high" || key === "critical") return "critical";
  if (key === "medium" || key === "moderate") return "review";
  return "monitor";
}

const EDR_EXPAND_COLUMNS = [
  {
    key: "timestamp",
    label: "Time",
    render: (r) => (
      <span className="text-white/60 whitespace-nowrap tabular-nums">
        {fmtTime(r.timestamp)}
      </span>
    ),
  },
  {
    key: "action_taken",
    label: "Action",
    render: (r) => r.action_taken || <span className="text-white/30">—</span>,
  },
  {
    key: "event_type",
    label: "Type",
    render: (r) => r.event_type || <span className="text-white/30">—</span>,
  },
  {
    key: "process_name",
    label: "Process",
    render: (r) => (
      <span className="font-mono text-[10px] truncate max-w-[220px] inline-block align-middle" title={r.process_name || ""}>
        {r.process_name || "—"}
      </span>
    ),
  },
  {
    key: "threat_name",
    label: "Threat",
    render: (r) => (
      <span className="truncate max-w-[220px] inline-block align-middle" title={r.threat_name || ""}>
        {r.threat_name || <span className="text-white/30">—</span>}
      </span>
    ),
  },
];

function EdrAlertsTable({ rows }) {
  const [openId, setOpenId] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>Host</Th>
            <Th>User</Th>
            <Th>Threat</Th>
            <Th>Severity</Th>
            <Th align="right">Count</Th>
            <Th>Last Seen</Th>
            <Th>Action</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row, idx) => {
            const key = `${row.host_name || ""}|${row.user_account || ""}|${row.threat_name || ""}|${row.severity || ""}|${idx}`;
            const actions = (row.actions || []).filter(Boolean);
            const isOpen = openId === key;
            return (
              <Fragment key={key}>
                <tr
                  onClick={() => setOpenId(isOpen ? null : key)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td
                    className="px-4 py-2.5 text-white/80 truncate max-w-[200px]"
                    title={row.host_name || ""}
                  >
                    {row.host_name || <span className="text-white/30">—</span>}
                  </td>
                  <td
                    className="px-4 py-2.5 text-white/70 truncate max-w-[220px]"
                    title={row.user_account || ""}
                  >
                    {row.user_account || <span className="text-white/30">—</span>}
                  </td>
                  <td
                    className="px-4 py-2.5 text-white/80 truncate max-w-[240px]"
                    title={row.threat_name || ""}
                  >
                    {row.threat_name || <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity={edrSeverityToPill(row.severity)} />
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.alert_count)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.last_seen)}
                  </td>
                  <td
                    className="px-4 py-2.5 text-white/60 truncate max-w-[200px]"
                    title={actions.join(", ")}
                  >
                    {actions.length > 0 ? (
                      actions.join(", ")
                    ) : (
                      <span className="text-white/30">—</span>
                    )}
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={8}>
                    <AsyncEventsExpand
                      fetcher={() =>
                        api.govEdrAlertsEvents({
                          hostname: row.host_name || undefined,
                          username: row.user_account || undefined,
                          threat_name: row.threat_name || undefined,
                          severity: row.severity || undefined,
                          limit: 10,
                        })
                      }
                      depKey={key}
                      columns={EDR_EXPAND_COLUMNS}
                      eventsParams={{ user: row.user_account || undefined }}
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

// ---------------------------------------------------------------------------
// ThreatLocker ActionLog
// ---------------------------------------------------------------------------

// Map ThreatLocker action strings onto our severity pills. Deny is the
// hard block (CRITICAL red), Ringfenced is a policy-scoped block
// (REVIEW orange), elevations and anything else fall through to
// MONITOR so they still render without shouting.
function threatLockerActionPill(row) {
  const explicit = String(row?.action || "").trim().toLowerCase();
  const actionType = String(row?.action_type || "").trim().toLowerCase();
  const id = Number(row?.action_id) || 0;
  if (explicit === "deny" || actionType === "deny" || id === 2) return "critical";
  if (
    explicit === "ringfenced" ||
    actionType === "ringfenced" ||
    id === 3
  ) return "review";
  return "monitor";
}

function ThreatLockerActionBadge({ row }) {
  const tier = threatLockerActionPill(row);
  const label =
    tier === "critical"
      ? "DENY"
      : tier === "review"
      ? "RINGFENCED"
      : (row?.action || row?.action_type || "—").toString().toUpperCase();
  const color =
    tier === "critical"
      ? "#EF4444"
      : tier === "review"
      ? "#F97316"
      : "rgba(255,255,255,0.5)";
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

const THREATLOCKER_EXPAND_COLUMNS = [
  {
    key: "event_time",
    label: "Time",
    render: (r) => (
      <span className="text-white/60 whitespace-nowrap tabular-nums">
        {fmtTime(r.event_time)}
      </span>
    ),
  },
  {
    key: "action",
    label: "Action",
    render: (r) => <ThreatLockerActionBadge row={r} />,
  },
  {
    key: "action_type",
    label: "Type",
    render: (r) => r.action_type || <span className="text-white/30">—</span>,
  },
  {
    key: "full_path",
    label: "Full Path",
    render: (r) => (
      <span
        className="font-mono text-[10px] truncate max-w-[260px] inline-block align-middle"
        title={r.full_path || ""}
      >
        {r.full_path || "—"}
      </span>
    ),
  },
  {
    key: "policy_name",
    label: "Policy",
    render: (r) => (
      <span
        className="truncate max-w-[200px] inline-block align-middle"
        title={r.policy_name || ""}
      >
        {r.policy_name || <span className="text-white/30">—</span>}
      </span>
    ),
  },
];

function ThreatLockerTable({ rows }) {
  const [openId, setOpenId] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>Host</Th>
            <Th>User</Th>
            <Th>Action</Th>
            <Th>Action Type</Th>
            <Th>Policy</Th>
            <Th align="right">Count</Th>
            <Th>Last Seen</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row, idx) => {
            const key = `${row.hostname || ""}|${row.username || ""}|${row.action || ""}|${row.action_type || ""}|${row.policy_name || ""}|${idx}`;
            const isOpen = openId === key;
            return (
              <Fragment key={key}>
                <tr
                  onClick={() => setOpenId(isOpen ? null : key)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td
                    className="px-4 py-2.5 text-white/80 truncate max-w-[200px]"
                    title={row.hostname || ""}
                  >
                    {row.hostname || <span className="text-white/30">—</span>}
                  </td>
                  <td
                    className="px-4 py-2.5 text-white/70 truncate max-w-[220px]"
                    title={row.username || ""}
                  >
                    {row.username || <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-4 py-2.5 space-x-2">
                    <ThreatLockerActionBadge row={row} />
                    <SeverityPill severity={threatLockerActionPill(row)} />
                  </td>
                  <td
                    className="px-4 py-2.5 text-white/70 truncate max-w-[180px]"
                    title={row.action_type || ""}
                  >
                    {row.action_type || <span className="text-white/30">—</span>}
                  </td>
                  <td
                    className="px-4 py-2.5 text-white/60 truncate max-w-[220px]"
                    title={row.policy_name || ""}
                  >
                    {row.policy_name || <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-4 py-2.5 text-right tabular-nums">
                    {fmtNumber(row.event_count)}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.last_seen)}
                  </td>
                  <ChevronCell open={isOpen} />
                </tr>
                {isOpen && (
                  <ExpandedPanel colSpan={8}>
                    <AsyncEventsExpand
                      fetcher={() =>
                        api.govThreatLockerEvents({
                          hostname: row.hostname || undefined,
                          username: row.username || undefined,
                          action: row.action || undefined,
                          action_type: row.action_type || undefined,
                          policy_name: row.policy_name || undefined,
                          limit: 10,
                        })
                      }
                      depKey={key}
                      columns={THREATLOCKER_EXPAND_COLUMNS}
                      eventsParams={{ user: row.username || undefined }}
                    />
                  </ExpandedPanel>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

// ---------------------------------------------------------------------------
// ThreatLocker ActionLog
// ---------------------------------------------------------------------------

// Map ThreatLocker action strings onto our severity pills. Deny is the
// hard block (CRITICAL red), Ringfenced is a policy-scoped block
// (REVIEW orange), elevations and anything else fall through to
// MONITOR so they still render without shouting.
