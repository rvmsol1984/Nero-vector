import { useEffect, useState } from "react";

import { api } from "../api.js";

function fmtTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().replace("T", " ").slice(0, 19) + "Z";
}

function fmtNumber(n) {
  if (n === null || n === undefined) return "—";
  return Number(n).toLocaleString("en-US");
}

export default function Users() {
  const [tenant, setTenant] = useState("");
  const [byTenant, setByTenant] = useState([]);
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  useEffect(() => {
    api.byTenant().then(setByTenant).catch(() => {});
  }, []);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    setErr(null);
    api
      .users(tenant || undefined)
      .then((r) => {
        if (!cancel) setUsers(r);
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
  }, [tenant]);

  return (
    <div className="space-y-4">
      <div>
        <h1 className="font-display text-2xl tracking-[0.2em]">USERS</h1>
        <p className="text-muted text-xs mt-1">
          Per-identity activity rollup across the audit stream.
        </p>
      </div>

      <div className="flex items-center gap-3 text-xs">
        <select
          value={tenant}
          onChange={(e) => setTenant(e.target.value)}
          className="bg-surface border border-border px-2 py-1 text-slate-100 focus:outline-none focus:border-accent"
        >
          <option value="">all tenants</option>
          {byTenant.map((t) => (
            <option key={t.client_name} value={t.client_name}>
              {t.client_name}
            </option>
          ))}
        </select>
        <span className="text-muted">{users.length} identities</span>
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
              <th className="text-left px-3 py-2">Entity Key</th>
              <th className="text-left px-3 py-2">Client</th>
              <th className="text-right px-3 py-2">Events</th>
              <th className="text-left px-3 py-2">Top Event Type</th>
              <th className="text-left px-3 py-2">Last Seen</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {users.map((u) => (
              <tr key={u.entity_key} className="hover:bg-black/30">
                <td
                  className="px-3 py-1.5 truncate max-w-[460px]"
                  title={u.entity_key}
                >
                  {u.entity_key}
                </td>
                <td className="px-3 py-1.5 text-accent whitespace-nowrap">
                  {u.client_name}
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums">
                  {fmtNumber(u.event_count)}
                </td>
                <td className="px-3 py-1.5 text-muted">{u.top_event_type}</td>
                <td className="px-3 py-1.5 text-muted whitespace-nowrap">
                  {fmtTime(u.last_seen)}
                </td>
              </tr>
            ))}
            {!loading && users.length === 0 && (
              <tr>
                <td colSpan={5} className="px-3 py-6 text-muted text-center">
                  no users
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
