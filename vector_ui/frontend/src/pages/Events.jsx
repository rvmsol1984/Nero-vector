import { useEffect, useState } from "react";

import { api } from "../api.js";

const PAGE = 50;

function fmtTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().replace("T", " ").slice(0, 19) + "Z";
}

function resultClass(r) {
  if (r === "Succeeded") return "text-success";
  if (r === "Failed") return "text-critical";
  return "text-muted";
}

export default function Events() {
  const [rows, setRows] = useState([]);
  const [offset, setOffset] = useState(0);
  const [tenant, setTenant] = useState("");
  const [eventType, setEventType] = useState("");
  const [byTenant, setByTenant] = useState([]);
  const [byType, setByType] = useState([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  useEffect(() => {
    api.byTenant().then(setByTenant).catch(() => {});
    api.byType(100).then(setByType).catch(() => {});
  }, []);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    setErr(null);
    api
      .events({
        limit: PAGE,
        offset,
        tenant: tenant || undefined,
        event_type: eventType || undefined,
      })
      .then((r) => {
        if (!cancel) setRows(r);
      })
      .catch((e) => {
        if (!cancel) setErr(e.message);
      })
      .finally(() => {
        if (!cancel) setLoading(false);
      });
    return () => {
      cancel = true;
    };
  }, [offset, tenant, eventType]);

  return (
    <div className="space-y-4">
      <div>
        <h1 className="font-display text-2xl tracking-[0.2em]">EVENTS</h1>
        <p className="text-muted text-xs mt-1">
          Raw audit stream across all connected tenants.
        </p>
      </div>

      <div className="flex flex-wrap items-center gap-3 text-xs">
        <select
          value={tenant}
          onChange={(e) => {
            setOffset(0);
            setTenant(e.target.value);
          }}
          className="bg-surface border border-border px-2 py-1 text-slate-100 focus:outline-none focus:border-accent"
        >
          <option value="">all tenants</option>
          {byTenant.map((t) => (
            <option key={t.client_name} value={t.client_name}>
              {t.client_name}
            </option>
          ))}
        </select>

        <select
          value={eventType}
          onChange={(e) => {
            setOffset(0);
            setEventType(e.target.value);
          }}
          className="bg-surface border border-border px-2 py-1 text-slate-100 focus:outline-none focus:border-accent"
        >
          <option value="">all event types</option>
          {byType.map((t) => (
            <option key={t.event_type ?? ""} value={t.event_type ?? ""}>
              {t.event_type ?? "(none)"}
            </option>
          ))}
        </select>

        <div className="ml-auto flex items-center gap-2">
          <button
            type="button"
            disabled={offset === 0 || loading}
            onClick={() => setOffset(Math.max(0, offset - PAGE))}
            className="border border-border px-3 py-1 disabled:opacity-30 hover:border-accent hover:text-accent"
          >
            prev
          </button>
          <span className="text-muted tabular-nums">
            offset {offset.toLocaleString("en-US")}
          </span>
          <button
            type="button"
            disabled={rows.length < PAGE || loading}
            onClick={() => setOffset(offset + PAGE)}
            className="border border-border px-3 py-1 disabled:opacity-30 hover:border-accent hover:text-accent"
          >
            next
          </button>
        </div>
      </div>

      {err && (
        <div className="border border-critical/40 bg-critical/10 text-critical text-xs px-3 py-2">
          load error: {err}
        </div>
      )}

      <div className="bg-surface border border-border overflow-x-auto">
        <table className="min-w-full text-[11px]">
          <thead className="text-muted uppercase text-[10px] tracking-[0.2em]">
            <tr>
              <th className="text-left px-3 py-2">Timestamp</th>
              <th className="text-left px-3 py-2">Client</th>
              <th className="text-left px-3 py-2">User</th>
              <th className="text-left px-3 py-2">Event Type</th>
              <th className="text-left px-3 py-2">Workload</th>
              <th className="text-left px-3 py-2">Result</th>
              <th className="text-left px-3 py-2">Client IP</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {rows.map((r) => (
              <tr key={r.id} className="hover:bg-black/30">
                <td className="px-3 py-1.5 text-muted whitespace-nowrap">
                  {fmtTime(r.timestamp)}
                </td>
                <td className="px-3 py-1.5 text-accent whitespace-nowrap">
                  {r.client_name}
                </td>
                <td className="px-3 py-1.5 truncate max-w-[260px]" title={r.user_id}>
                  {r.user_id}
                </td>
                <td className="px-3 py-1.5">{r.event_type}</td>
                <td className="px-3 py-1.5 text-muted">{r.workload}</td>
                <td className={`px-3 py-1.5 ${resultClass(r.result_status)}`}>
                  {r.result_status ?? ""}
                </td>
                <td className="px-3 py-1.5 text-muted">{r.client_ip ?? ""}</td>
              </tr>
            ))}
            {!loading && rows.length === 0 && (
              <tr>
                <td colSpan={7} className="px-3 py-6 text-muted text-center">
                  no events match current filter
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
