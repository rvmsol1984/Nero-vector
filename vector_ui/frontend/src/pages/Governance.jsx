import { Fragment, useEffect, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "../components/Avatar.jsx";
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
];

// ---------------------------------------------------------------------------

export default function Governance() {
  const [data, setData] = useState({});
  const [errors, setErrors] = useState({});
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState("dlp");

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    const calls = TABS.map((t) => {
      const fn = api[t.endpoint];
      const promise = t.withTenant ? fn(TENANT) : fn();
      return { id: t.id, promise };
    });

    Promise.allSettled(calls.map((c) => c.promise)).then((results) => {
      if (cancel) return;
      const nextData = {};
      const nextErrors = {};
      results.forEach((r, i) => {
        const id = calls[i].id;
        if (r.status === "fulfilled") {
          nextData[id] = r.value || [];
        } else {
          nextData[id] = [];
          nextErrors[id] = String(r.reason?.message || r.reason);
        }
      });
      setData(nextData);
      setErrors(nextErrors);
      setLoading(false);
    });

    return () => {
      cancel = true;
    };
  }, []);

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

      {/* ----- horizontal tab bar ----- */}
      <div className="border-b border-white/5 overflow-x-auto">
        <div className="flex items-center gap-0 min-w-max">
          {TABS.map((t) => {
            const count = (data[t.id] || []).length;
            const active = activeTab === t.id;
            return (
              <button
                key={t.id}
                type="button"
                onClick={() => setActiveTab(t.id)}
                className={`px-4 py-2.5 text-[12px] font-medium border-b-2 -mb-px whitespace-nowrap flex items-center gap-2 transition-colors ${
                  active
                    ? "border-primary text-primary-light"
                    : "border-transparent text-white/55 hover:text-white"
                }`}
              >
                <span>{t.label}</span>
                <CountBadge count={count} active={active} loading={loading} />
              </button>
            );
          })}
        </div>
      </div>

      {/* ----- active tab panel ----- */}
      <TabPanel
        tabId={activeTab}
        rows={data[activeTab] || []}
        loading={loading}
        error={errors[activeTab]}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// tab dispatch
// ---------------------------------------------------------------------------

function TabPanel({ tabId, rows, loading, error }) {
  if (loading) {
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
  if (!rows.length) {
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

function CountBadge({ count, active, loading }) {
  if (loading) {
    return (
      <span className="inline-flex items-center justify-center min-w-[20px] px-1.5 py-0.5 rounded-full text-[9px] font-bold text-white/30 bg-white/5 border border-white/10">
        …
      </span>
    );
  }
  const cls = active
    ? "text-primary-light bg-primary/15 border-primary/40"
    : count === 0
    ? "text-white/40 bg-white/[0.03] border-white/10"
    : "text-white/70 bg-white/10 border-white/15";
  return (
    <span
      className={`inline-flex items-center justify-center min-w-[20px] px-1.5 py-0.5 rounded-full text-[9px] font-bold border tabular-nums ${cls}`}
    >
      {count}
    </span>
  );
}

function EmptyState() {
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
      <div>No findings detected</div>
      <SeverityPill severity="clean" />
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
            const key = row.app_name || "(unknown)";
            const isOpen = open === key;
            return (
              <Fragment key={key}>
                <tr
                  onClick={() => setOpen(isOpen ? null : key)}
                  className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  <td className="px-4 py-2.5 truncate max-w-[420px] font-medium" title={row.app_name}>
                    {row.app_name || <span className="text-white/40">(unnamed)</span>}
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

function GuestUsersTable({ rows }) {
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
            </tr>
          ))}
        </tbody>
      </table>
    </TableCard>
  );
}

function UnmanagedDevicesTable({ rows }) {
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
              <td className="px-4 py-2.5 text-white/60">
                <DeviceList devices={row.devices} />
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
