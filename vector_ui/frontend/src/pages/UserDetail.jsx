import { Fragment, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";

import { api } from "../api.js";
import EventTypeBadge from "../components/EventTypeBadge.jsx";
import JsonBlock from "../components/JsonBlock.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import TenantBadge from "../components/TenantBadge.jsx";
import {
  deviceBrowser,
  deviceName,
  deviceOs,
  emailEventLabel,
  extractMailFolder,
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

// Backend filter per tab (comma-separated lists honoured by
// /api/users/{key}/events).
const TAB_QUERY = {
  timeline: {},
  files:    { workloads: "OneDrive,SharePoint" },
  email:    { workloads: "Exchange" },
  logins:   {
    workloads: "AzureActiveDirectory",
    event_types: "UserLoggedIn,UserLoginFailed,UserLoggedOut",
  },
  raw:      {},
};

// Raw tab gets a tighter page size per spec; everything else loads 100.
const TAB_PAGE = { raw: 25 };

export default function UserDetail() {
  const { entityKey: rawKey } = useParams();
  const entityKey = decodeURIComponent(rawKey);

  const [profile, setProfile] = useState(null);
  const [profileErr, setProfileErr] = useState(null);

  const [tab, setTab] = useState("timeline");
  const [offset, setOffset] = useState(0);
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState(null);

  const pageSize = TAB_PAGE[tab] || 100;

  useEffect(() => {
    setProfile(null);
    setProfileErr(null);
    api
      .userProfile(entityKey)
      .then(setProfile)
      .catch((e) => setProfileErr(e.message));
  }, [entityKey]);

  useEffect(() => {
    setOffset(0);
    setExpanded(null);
  }, [tab, entityKey]);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    const q = TAB_QUERY[tab] || {};
    api
      .userEvents(entityKey, { ...q, limit: pageSize, offset })
      .then((r) => {
        if (!cancel) setEvents(r);
      })
      .catch(() => {
        if (!cancel) setEvents([]);
      })
      .finally(() => {
        if (!cancel) setLoading(false);
      });
    return () => {
      cancel = true;
    };
  }, [entityKey, tab, offset, pageSize]);

  function toggleExpand(id) {
    setExpanded((prev) => (prev === id ? null : id));
  }

  const header = useMemo(() => {
    if (profileErr) {
      return (
        <div className="border border-critical/40 bg-critical/10 text-critical text-xs px-3 py-2">
          {profileErr}
        </div>
      );
    }
    if (!profile) {
      return <div className="text-muted text-xs">loading user…</div>;
    }
    return (
      <div className="bg-surface border border-border p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <div className="text-[10px] uppercase tracking-[0.25em] text-muted">
              identity
            </div>
            <div className="font-display text-2xl mt-1 break-all">
              {profile.user_id}
            </div>
            <div className="mt-2 flex items-center gap-3 text-[11px]">
              <TenantBadge name={profile.client_name} />
              <span className="text-muted">{profile.tenant_id}</span>
            </div>
          </div>
          <div className="text-right text-[11px]">
            <div className="text-muted uppercase tracking-[0.2em] text-[10px]">
              first seen
            </div>
            <div className="mt-1">{fmtTime(profile.first_seen)}</div>
            <div className="text-muted uppercase tracking-[0.2em] text-[10px] mt-3">
              last seen
            </div>
            <div className="mt-1">{fmtTime(profile.last_seen)}</div>
          </div>
        </div>

        <div className="mt-5 grid grid-cols-2 md:grid-cols-4 gap-4">
          <Stat label="Total Events"       value={fmtNumber(profile.total_events)} />
          <Stat label="Unique Event Types" value={fmtNumber(profile.unique_event_types)} />
          <Stat label="Unique IPs"         value={fmtNumber(profile.unique_ips)} />
          <Stat label="Unique Devices"     value={fmtNumber(profile.unique_devices)} />
        </div>
      </div>
    );
  }, [profile, profileErr]);

  return (
    <div className="space-y-4">
      <div className="text-[10px] uppercase tracking-[0.25em] text-muted">
        <Link to="/users" className="hover:text-accent">users</Link>
        <span className="mx-2">/</span>
        <span className="text-slate-300 normal-case tracking-normal break-all">
          {entityKey}
        </span>
      </div>

      {header}

      <div className="border-b border-border flex items-center gap-1 flex-wrap">
        {TABS.map((t) => {
          const active = tab === t.id;
          return (
            <button
              key={t.id}
              type="button"
              onClick={() => setTab(t.id)}
              className={`px-4 py-2 text-[11px] uppercase tracking-[0.22em] border-b-2 -mb-px transition-colors ${
                active
                  ? "border-accent text-accent"
                  : "border-transparent text-muted hover:text-slate-100"
              }`}
            >
              {t.label}
            </button>
          );
        })}
        <div className="ml-auto flex items-center gap-2 text-[11px] pb-1">
          <button
            type="button"
            disabled={offset === 0 || loading}
            onClick={() => setOffset(Math.max(0, offset - pageSize))}
            className="border border-border px-3 py-1 disabled:opacity-30 hover:border-accent hover:text-accent"
          >
            prev
          </button>
          <span className="text-muted tabular-nums">
            offset {offset.toLocaleString("en-US")}
          </span>
          <button
            type="button"
            disabled={events.length < pageSize || loading}
            onClick={() => setOffset(offset + pageSize)}
            className="border border-border px-3 py-1 disabled:opacity-30 hover:border-accent hover:text-accent"
          >
            next
          </button>
        </div>
      </div>

      {tab === "timeline" && (
        <TimelineTable events={events} expanded={expanded} onToggle={toggleExpand} loading={loading} />
      )}
      {tab === "files" && (
        <FilesTable events={events} expanded={expanded} onToggle={toggleExpand} loading={loading} />
      )}
      {tab === "email" && (
        <EmailTable events={events} expanded={expanded} onToggle={toggleExpand} loading={loading} />
      )}
      {tab === "logins" && (
        <LoginsTable events={events} expanded={expanded} onToggle={toggleExpand} loading={loading} />
      )}
      {tab === "raw" && (
        <RawTable events={events} expanded={expanded} onToggle={toggleExpand} loading={loading} />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function Stat({ label, value }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-[0.25em] text-muted">{label}</div>
      <div className="font-display text-2xl text-accent mt-1 leading-none">{value}</div>
    </div>
  );
}

function RemovableMediaBadge() {
  return (
    <span
      className="inline-block px-2 py-0.5 text-[9px] uppercase tracking-[0.15em] border"
      style={{
        color: "#d29922",
        borderColor: "#d2992255",
        backgroundColor: "#d2992214",
      }}
    >
      Removable Media
    </span>
  );
}

function ExpandableTable({ columns, rows, expanded, onToggle, loading, renderRow, emptyLabel }) {
  return (
    <div className="bg-surface border border-border overflow-x-auto">
      <table className="min-w-full text-[11px]">
        <thead className="text-muted uppercase text-[10px] tracking-[0.2em]">
          <tr>
            {columns.map((c) => (
              <th key={c} className="text-left px-3 py-2">{c}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {rows.map((r) => {
            const open = expanded === r.id;
            return (
              <Fragment key={r.id}>
                <tr
                  onClick={() => onToggle(r.id)}
                  className={`cursor-pointer ${open ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                >
                  {renderRow(r)}
                </tr>
                {open && (
                  <tr className="bg-black/30">
                    <td colSpan={columns.length} className="px-3 py-3 border-t border-border">
                      <JsonBlock data={r.raw_json} />
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
          {!loading && rows.length === 0 && (
            <tr>
              <td colSpan={columns.length} className="px-3 py-6 text-muted text-center">
                {emptyLabel}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

// ---- Timeline --------------------------------------------------------------

function TimelineTable({ events, expanded, onToggle, loading }) {
  return (
    <ExpandableTable
      columns={["Timestamp", "Event Type", "Workload", "Result", "Client IP"]}
      rows={events}
      expanded={expanded}
      onToggle={onToggle}
      loading={loading}
      emptyLabel="no events"
      renderRow={(r) => (
        <>
          <td className="px-3 py-1.5 text-muted whitespace-nowrap">{fmtTime(r.timestamp)}</td>
          <td className="px-3 py-1.5"><EventTypeBadge type={r.event_type} workload={r.workload} /></td>
          <td className="px-3 py-1.5 text-muted">{r.workload}</td>
          <td className="px-3 py-1.5"><StatusBadge status={r.result_status} /></td>
          <td className="px-3 py-1.5 text-muted">{r.client_ip ?? ""}</td>
        </>
      )}
    />
  );
}

// ---- Files (OneDrive + SharePoint) ----------------------------------------

function FilesTable({ events, expanded, onToggle, loading }) {
  return (
    <ExpandableTable
      columns={["Timestamp", "Event", "Filename", "Site", "Client IP"]}
      rows={events}
      expanded={expanded}
      onToggle={onToggle}
      loading={loading}
      emptyLabel="no file activity"
      renderRow={(r) => {
        const filename = filenameFromObjectId(r.raw_json?.ObjectId);
        const site = siteDomain(r.raw_json);
        const isRemovable = r.event_type === "FileCreatedOnRemovableMedia";
        return (
          <>
            <td className="px-3 py-1.5 text-muted whitespace-nowrap">{fmtTime(r.timestamp)}</td>
            <td className="px-3 py-1.5">
              <div className="flex items-center gap-2">
                <EventTypeBadge type={r.event_type} workload={r.workload} />
                {isRemovable && <RemovableMediaBadge />}
              </div>
            </td>
            <td
              className="px-3 py-1.5 truncate max-w-[360px]"
              title={r.raw_json?.ObjectId || ""}
            >
              {filename || <span className="text-muted">—</span>}
            </td>
            <td className="px-3 py-1.5 text-muted truncate max-w-[260px]">
              {site || <span className="text-muted">—</span>}
            </td>
            <td className="px-3 py-1.5 text-muted">{r.client_ip ?? ""}</td>
          </>
        );
      }}
    />
  );
}

// ---- Email (Exchange) ------------------------------------------------------

function EmailTable({ events, expanded, onToggle, loading }) {
  return (
    <div className="space-y-3">
      <div className="text-[11px] text-muted border border-border bg-black/20 px-3 py-2">
        Send/attachment data available after 24h — mailbox audit enabled 2026-04-12
      </div>
      <ExpandableTable
        columns={["Timestamp", "Event", "Folder", "Client IP"]}
        rows={events}
        expanded={expanded}
        onToggle={onToggle}
        loading={loading}
        emptyLabel="no mailbox activity"
        renderRow={(r) => (
          <>
            <td className="px-3 py-1.5 text-muted whitespace-nowrap">{fmtTime(r.timestamp)}</td>
            <td className="px-3 py-1.5">
              <EventTypeBadge type={emailEventLabel(r.event_type)} workload={r.workload} />
            </td>
            <td
              className="px-3 py-1.5 truncate max-w-[420px]"
              title={extractMailFolder(r.raw_json)}
            >
              {extractMailFolder(r.raw_json) || <span className="text-muted">—</span>}
            </td>
            <td className="px-3 py-1.5 text-muted">{r.client_ip ?? ""}</td>
          </>
        )}
      />
    </div>
  );
}

// ---- Logins (Azure AD) -----------------------------------------------------

function LoginsTable({ events, expanded, onToggle, loading }) {
  return (
    <ExpandableTable
      columns={["Timestamp", "Result", "Client IP", "Device", "OS", "Browser"]}
      rows={events}
      expanded={expanded}
      onToggle={onToggle}
      loading={loading}
      emptyLabel="no login events"
      renderRow={(r) => {
        // Spec: Succeeded/Failed/LoggedOut -> green / red / muted
        let status;
        if (r.event_type === "UserLoggedIn") status = "Succeeded";
        else if (r.event_type === "UserLoginFailed") status = "Failed";
        else status = "Logged Out";
        return (
          <>
            <td className="px-3 py-1.5 text-muted whitespace-nowrap">{fmtTime(r.timestamp)}</td>
            <td className="px-3 py-1.5"><StatusBadge status={status} /></td>
            <td className="px-3 py-1.5 text-muted">{r.client_ip ?? ""}</td>
            <td className="px-3 py-1.5">
              {deviceName(r.raw_json) || <span className="text-muted">—</span>}
            </td>
            <td className="px-3 py-1.5 text-muted">
              {deviceOs(r.raw_json) || "—"}
            </td>
            <td className="px-3 py-1.5 text-muted">
              {deviceBrowser(r.raw_json, r.user_agent) || "—"}
            </td>
          </>
        );
      }}
    />
  );
}

// ---- Raw --------------------------------------------------------------------

function RawTable({ events, expanded, onToggle, loading }) {
  return (
    <ExpandableTable
      columns={["Timestamp", "Event Type", "Workload", "Result"]}
      rows={events}
      expanded={expanded}
      onToggle={onToggle}
      loading={loading}
      emptyLabel="no events"
      renderRow={(r) => (
        <>
          <td className="px-3 py-1.5 text-muted whitespace-nowrap">{fmtTime(r.timestamp)}</td>
          <td className="px-3 py-1.5">
            <EventTypeBadge type={r.event_type} workload={r.workload} />
          </td>
          <td className="px-3 py-1.5 text-muted">{r.workload}</td>
          <td className="px-3 py-1.5"><StatusBadge status={r.result_status} /></td>
        </>
      )}
    />
  );
}
