import { Fragment, useEffect, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "../components/Avatar.jsx";
import JsonBlock from "../components/JsonBlock.jsx";
import TenantBadge from "../components/TenantBadge.jsx";
import { api } from "../api.js";
import { getEventLabel } from "../utils/eventLabels.js";
import { filenameFromObjectId, fmtNumber, fmtRelative, fmtTime } from "../utils/format.js";

const DEFAULT_TENANT = "GameChange Solar";
const DEFAULT_TENANT_ID = "07b4c47a-e461-493e-91c4-90df73e2ebc6";
let _currentTenantId = DEFAULT_TENANT_ID;

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


// ---------------------------------------------------------------------------

export default function Governance() {
  // Per-tab cache. data[tabId] === undefined means "never fetched";
  // data[tabId] === [] is a real empty result. That distinction is
  // what drives CountBadge's "unvisited" vs "no findings" states.
  const [data, setData] = useState({});
  const [errors, setErrors] = useState({});
  const [loadingTabs, setLoadingTabs] = useState(() => new Set());
  const [activeTab, setActiveTab] = useState("dlp");
  const [tenants, setTenants] = useState([]);
  const [selectedTenant, setSelectedTenant] = useState(DEFAULT_TENANT);
  const [selectedTenantId, setSelectedTenantId] = useState(DEFAULT_TENANT_ID);
  useEffect(() => {
    api.byTenant().then((rows) => {
      setTenants(rows || []);
    }).catch(() => {});
  }, []);

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

    const promise = fn(selectedTenant);
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
  }, [activeTab, selectedTenant]);

  return (
    <div className="space-y-5 animate-fade-in">
      {/* ----- header ----- */}
      <div className="flex items-center gap-3 flex-wrap">
        <h1 className="text-2xl font-bold">Governance</h1>
        <select
          value={selectedTenant}
          onChange={(e) => {
            const t = tenants.find((x) => x.client_name === e.target.value);
            if (t) {
              setSelectedTenant(t.client_name);
              setSelectedTenantId(t.tenant_id || DEFAULT_TENANT_ID);
              _currentTenantId = t.tenant_id || DEFAULT_TENANT_ID;
              setData({});
            }
          }}
          className="bg-white/5 border border-white/10 rounded-xl px-3 py-1.5 text-xs text-white focus:outline-none focus:border-primary-light"
        >
          {tenants.map((t) => (
            <option key={t.client_name} value={t.client_name}>
              {t.client_name}
            </option>
          ))}
        </select>
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
        tenantId={selectedTenantId}
        tenantName={selectedTenant}
        loading={loadingTabs.has(activeTab)}
        error={errors[activeTab]}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// tab dispatch
// ---------------------------------------------------------------------------

function TabPanel({ tabId, rows: raw, loading, error, tenantId, tenantName }) {
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
  const GCS_ONLY_TABS = ["intuneDevices"];
  const GCS_NAME = "GameChange Solar";
  if (GCS_ONLY_TABS.includes(tabId) && tenantName && tenantName !== GCS_NAME) {
    return (
      <div className="card py-14 px-6 flex flex-col items-center text-center gap-3">
        <div className="text-white/50 text-sm">This view requires Microsoft Graph API access</div>
        <div className="text-white/30 text-xs">Currently only available for GameChange Solar (E5)</div>
      </div>
    );
  }
  if (tabId === "aiActivity") {
    const copilot = Array.isArray(raw?.copilot) ? raw.copilot : [];
    const external = Array.isArray(raw?.external_ai) ? raw.external_ai : [];
    const claudeConnector = raw?.claude_connector || { admin_grants: [], shadow_it_attempts: [], total: 0 };
    if (copilot.length === 0 && external.length === 0 && claudeConnector.total === 0 && !raw?.external_error) {
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
        claudeConnector={claudeConnector}
        tenantId={tenantId}
        tenantName={tenantName}
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
    case "guestUsers":        return <GuestUsersTable rows={rows} tenantName={tenantName} />;
    case "unmanagedDevices":  return <UnmanagedDevicesTable rows={rows} tenantId={tenantId} tenantName={tenantName} />;
    case "intuneDevices":     return <IntuneDevicesTable rows={rows} tenantId={tenantId} />;
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

function UserCell({ entityKey, userId, clientName = DEFAULT_TENANT }) {
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

function DlpTable({ rows }) {
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
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => (
            <tr key={row.entity_key} className="hover:bg-white/[0.03]">
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
            </tr>
          ))}
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

function SharingTable({ rows }) {
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
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => (
            <tr key={`${row.entity_key}-${row.event_type}`} className="hover:bg-white/[0.03]">
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
            </tr>
          ))}
        </tbody>
      </table>
    </TableCard>
  );
}

function DownloadsTable({ rows }) {
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>User</Th>
            <Th align="right">Downloads</Th>
            <Th>Last Seen</Th>
            <Th>Status</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => (
            <tr key={row.entity_key} className="hover:bg-white/[0.03]">
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
            </tr>
          ))}
        </tbody>
      </table>
    </TableCard>
  );
}

// ---------------------------------------------------------------------------
// tables — new 8
// ---------------------------------------------------------------------------

function BrokenInheritanceTable({ rows }) {
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
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => (
            <tr key={row.entity_key} className="hover:bg-white/[0.03]">
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
            </tr>
          ))}
        </tbody>
      </table>
    </TableCard>
  );
}

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
                </tr>
                {isOpen && (
                  <tr className="bg-black/30">
                    <td colSpan={4} className="px-4 py-3 border-t border-white/5">
                      <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
                        Users who consented
                      </div>
                      <div className="flex flex-wrap gap-2">
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
                </tr>
                {isOpen && (
                  <tr className="bg-black/30">
                    <td colSpan={6} className="px-4 py-3 border-t border-white/5">
                      <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
                        Targeted users
                      </div>
                      <div className="flex flex-wrap gap-2">
                        {(row.targets || []).map((u) => (
                          <span
                            key={u}
                            className="text-[11px] px-2 py-1 rounded-md bg-white/5 border border-white/10 text-white/80"
                          >
                            {u}
                          </span>
                        ))}
                        {(!row.targets || row.targets.length === 0) && (
                          <span className="text-white/30 text-[11px]">
                            no targets recorded
                          </span>
                        )}
                      </div>
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

function StaleAccountsTable({ rows }) {
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
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => (
            <tr key={row.entity_key} className="hover:bg-white/[0.03]">
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
            </tr>
          ))}
        </tbody>
      </table>
    </TableCard>
  );
}

function MfaChangesTable({ rows }) {
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
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => (
            <tr key={row.entity_key} className="hover:bg-white/[0.03]">
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
            </tr>
          ))}
        </tbody>
      </table>
    </TableCard>
  );
}

function PrivilegedRolesTable({ rows }) {
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
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row, i) => (
            <tr key={`${row.entity_key}-${row.timestamp}-${i}`} className="hover:bg-white/[0.03]">
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
            </tr>
          ))}
        </tbody>
      </table>
    </TableCard>
  );
}

function GuestUsersTable({ rows, tenantName }) {
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
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => (
            <tr key={row.id || row.userPrincipalName} className="hover:bg-white/[0.03]">
              <td className="px-4 py-2.5">
                <div className="flex items-center gap-2">
                  <Avatar email={row.mail || row.displayName} tenant={tenantName || DEFAULT_TENANT} size={28} />
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
            </tr>
          ))}
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

function AiActivityTab({ copilot, external, externalError, claudeConnector = {}, tenantId, tenantName }) {
  const [subTab, setSubTab] = useState("copilot");
  const claudeTotal = (claudeConnector?.admin_grants?.length || 0) + (claudeConnector?.shadow_it_attempts?.length || 0);
  const SUB_TABS = [
    { id: "copilot",  label: "Microsoft Copilot", count: copilot.length },
    { id: "external", label: "External AI Tools", count: external.length },
    { id: "claude",   label: "Claude Connector",  count: claudeTotal },
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
        <AiCopilotSection rows={copilot} tenantId={tenantId} tenantName={tenantName} />
      ) : subTab === "claude" ? (
        <AiClaudeConnectorSection data={claudeConnector} />
      ) : (
        <AiExternalSection rows={external} error={externalError} tenantId={tenantId} tenantName={tenantName} />
      )}
    </div>
  );
}

function AiCopilotSection({ rows, tenantId, tenantName }) {
  return (
    <div className="card overflow-hidden">
      <div className="px-5 py-4 border-b border-white/5 flex items-center justify-between gap-4">
        <div>
          <div className="text-base font-bold">Microsoft Copilot</div>
          <div className="text-[11px] text-white/50 mt-0.5">
            Copilot workload usage from UAL
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
                const entityKey = row.entity_key || `${tenantId}::${row.user_id}`;
                const types = Array.isArray(row.event_types) ? row.event_types : [];
                return (
                  <tr key={row.user_id} className="hover:bg-white/[0.03]">
                    <td className="px-4 py-2.5">
                      <UserCell
                        entityKey={entityKey}
                        userId={row.user_id}
                        clientName={row.client_name || DEFAULT_TENANT}
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

function AiExternalSection({ rows, error, tenantId, tenantName }) {
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
                  ? (row.entity_key || `${tenantId}::${row.user}`)
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
                          clientName={row.client_name || DEFAULT_TENANT}
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

function AiClaudeConnectorSection({ data = {} }) {
  const adminGrants = data?.admin_grants || [];
  const shadowIt = data?.shadow_it_attempts || [];
  const all = [
    ...adminGrants.map(r => ({ ...r, _type: "ADMIN GRANT" })),
    ...shadowIt.map(r => ({ ...r, _type: "SHADOW IT" })),
  ];
  return (
    <div className="card overflow-hidden">
      <div className="px-5 py-4 border-b border-white/5">
        <div className="text-base font-bold">Claude M365 Connector</div>
        <div className="text-[11px] text-white/50 mt-0.5">
          Admin grants (ResultType=0) and shadow IT attempts (ResultType=90095) for the Claude M365 Connector
        </div>
      </div>
      {all.length === 0 ? (
        <div className="px-5 py-8 text-white/40 text-sm text-center">
          No Claude Connector activity detected
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-[11px]">
            <thead>
              <tr>
                <Th>User</Th>
                <Th>Client</Th>
                <Th>Type</Th>
                <Th>IP</Th>
                <Th>Time</Th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {all.map((row, i) => (
                <tr key={i} className="hover:bg-white/[0.03]">
                  <td className="px-4 py-2.5 font-mono text-[10px]">{row.user_id || "—"}</td>
                  <td className="px-4 py-2.5 text-white/60">{row.client_name || "—"}</td>
                  <td className="px-4 py-2.5">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-semibold uppercase ${
                      row._type === "ADMIN GRANT"
                        ? "bg-critical/15 text-critical border border-critical/30"
                        : "bg-warning/15 text-warning border border-warning/30"
                    }`}>{row._type}</span>
                  </td>
                  <td className="px-4 py-2.5 font-mono text-[10px] text-white/50">{row.client_ip || "—"}</td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">{fmtRelative(row.timestamp)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
function UnmanagedDevicesTable({ rows, tenantId, tenantName }) {
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

function IntuneDevicesTable({ rows, tenantId }) {
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
            const entityKey = row.entity_key || `${tenantId}::${row.user}`;
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
                      clientName={row.client_name || DEFAULT_TENANT}
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

function EdrAlertsTable({ rows }) {
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
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row, idx) => {
            const key = `${row.host_name || ""}|${row.user_account || ""}|${row.threat_name || ""}|${row.severity || ""}|${idx}`;
            const actions = (row.actions || []).filter(Boolean);
            return (
              <tr key={key} className="hover:bg-white/[0.03]">
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
              </tr>
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

function ThreatLockerTable({ rows }) {
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
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row, idx) => {
            const key = `${row.hostname || ""}|${row.username || ""}|${row.action || ""}|${row.action_type || ""}|${row.policy_name || ""}|${idx}`;
            return (
              <tr key={key} className="hover:bg-white/[0.03]">
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
              </tr>
            );
          })}
        </tbody>
      </table>
    </TableCard>
  );
}

// ---------------------------------------------------------------------------
// IOC Matches (OpenCTI enrichment)
// ---------------------------------------------------------------------------

// Map raw OpenCTI confidence (0-100) onto our governance severity pills.
// >= 90 -> CRITICAL red, 75-89 -> review (orange), 50-74 -> monitor (yellow).
function iocConfidencePill(confidence) {
  const c = Number(confidence) || 0;
  if (c >= 90) return "critical";
  if (c >= 75) return "review";
  return "monitor";
}

function IocTypeBadge({ type }) {
  const key = String(type || "").toLowerCase();
  const color = {
    ipv4:   "#3B82F6",
    ipv6:   "#3B82F6",
    domain: "#8B5CF6",
    url:    "#A855F7",
    email:  "#F97316",
    sha256: "#10B981",
  }[key] || "rgba(255,255,255,0.4)";
  return (
    <span
      className="inline-flex items-center px-2 py-[2px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
      style={{
        color,
        borderColor: color + "55",
        backgroundColor: color + "14",
      }}
    >
      {key || "—"}
    </span>
  );
}

function IocMatchesTable({ rows }) {
  const [open, setOpen] = useState(null);
  return (
    <TableCard>
      <table className="min-w-full text-[11px]">
        <thead>
          <tr>
            <Th>IOC Value</Th>
            <Th>Type</Th>
            <Th>Confidence</Th>
            <Th>Indicator</Th>
            <Th>Client</Th>
            <Th>User</Th>
            <Th>Matched</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => {
            const isOpen = open === row.id;
            return (
              <Fragment key={row.id}>
                <tr
                  onClick={() => setOpen(isOpen ? null : row.id)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td
                    className="px-4 py-2.5 font-mono text-[11px] text-white truncate max-w-[260px]"
                    title={row.ioc_value}
                  >
                    {row.ioc_value}
                  </td>
                  <td className="px-4 py-2.5">
                    <IocTypeBadge type={row.ioc_type} />
                  </td>
                  <td className="px-4 py-2.5">
                    <SeverityPill severity={iocConfidencePill(row.confidence)} />
                    <span className="ml-2 text-[10px] text-white/50 tabular-nums">
                      {Number(row.confidence) || 0}
                    </span>
                  </td>
                  <td
                    className="px-4 py-2.5 text-white/80 truncate max-w-[260px]"
                    title={row.indicator_name || ""}
                  >
                    {row.indicator_name || <span className="text-white/30">—</span>}
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
                        className="text-primary-light hover:underline truncate max-w-[220px] inline-block align-middle"
                        title={row.user_id || row.entity_key}
                      >
                        {row.user_id || row.entity_key}
                      </Link>
                    ) : (
                      <span className="text-white/30">—</span>
                    )}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.matched_at)}
                  </td>
                </tr>
                {isOpen && (
                  <tr className="bg-black/30">
                    <td colSpan={7} className="px-4 py-3 border-t border-white/5">
                      <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
                        OpenCTI indicator details
                      </div>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-[11px] mb-3">
                        <div>
                          <div className="text-white/40 uppercase tracking-wider text-[9px]">
                            OpenCTI id
                          </div>
                          <div className="font-mono text-white/80 break-all">
                            {row.opencti_id || "—"}
                          </div>
                        </div>
                        <div>
                          <div className="text-white/40 uppercase tracking-wider text-[9px]">
                            Matched at
                          </div>
                          <div className="text-white/80">
                            {fmtTime(row.matched_at)}
                          </div>
                        </div>
                      </div>
                      <JsonBlock data={row.raw_json} />
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

// Pull the DisplayName out of DeviceProperties array-of-{Name,Value} blobs.
function DeviceList({ devices }) {
  if (!devices || devices.length === 0) return <span className="text-white/30">—</span>;
  const names = devices
    .map((dp) => {
      if (!Array.isArray(dp)) return "";
      const hit = dp.find((p) => p && p.Name === "DisplayName");
      return hit ? hit.Value || "" : "";
    })
    .filter(Boolean);
  if (names.length === 0) {
    return <span className="text-white/40">{devices.length} device{devices.length > 1 ? "s" : ""}</span>;
  }
  const shown = names.slice(0, 2);
  const extra = names.length - shown.length;
  return (
    <span title={names.join("\n")}>
      {shown.join(", ")}
      {extra > 0 && <span className="text-white/30"> · +{extra}</span>}
    </span>
  );
}
