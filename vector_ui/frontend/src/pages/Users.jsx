import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "../components/Avatar.jsx";
import TenantBadge from "../components/TenantBadge.jsx";
import { api } from "../api.js";
import { fmtNumber, fmtRelative } from "../utils/format.js";

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
    <div className="space-y-4 animate-fade-in">
      <div>
        <h1 className="text-2xl font-bold">Users</h1>
        <p className="text-white/50 text-sm mt-1">
          Per-identity activity rollup across the audit stream.
        </p>
      </div>

      <div className="flex items-center gap-2 flex-wrap">
        <button
          type="button"
          onClick={() => setTenant("")}
          className={`px-3 py-1.5 rounded-xl text-xs font-medium transition-all active:scale-95 ${
            !tenant
              ? "bg-primary text-white"
              : "bg-white/10 text-white/70 hover:bg-white/15"
          }`}
        >
          all tenants
        </button>
        {byTenant.map((t) => (
          <button
            key={t.client_name}
            type="button"
            onClick={() => setTenant(t.client_name)}
            className={`px-3 py-1.5 rounded-xl text-xs font-medium transition-all active:scale-95 ${
              tenant === t.client_name
                ? "bg-primary text-white"
                : "bg-white/10 text-white/70 hover:bg-white/15"
            }`}
          >
            {t.client_name}
          </button>
        ))}
        <span className="ml-auto text-xs text-white/50 tabular-nums">
          {users.length} identities
        </span>
      </div>

      {err && (
        <div className="card border-critical/30 text-critical text-sm px-4 py-3">
          load error: {err}
        </div>
      )}

      <div className="space-y-2">
        {users.map((u) => (
          <Link
            key={u.entity_key}
            to={`/users/${encodeURIComponent(u.entity_key)}`}
            className="card flex items-center gap-3 p-4 hover:bg-white/[0.03] active:scale-[0.997] transition-all"
          >
            <Avatar email={u.user_id} tenant={u.client_name} size={40} />
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 flex-wrap">
                <span className="font-semibold text-sm truncate max-w-[280px]">
                  {u.user_id}
                </span>
                <TenantBadge name={u.client_name} />
              </div>
              <div className="flex items-center gap-3 mt-1 text-[11px] text-white/50">
                <span className="tabular-nums">
                  {fmtNumber(u.event_count)} events
                </span>
                <span className="opacity-60">·</span>
                <span className="truncate">{u.top_event_type}</span>
                <span className="opacity-60">·</span>
                <span>last seen {fmtRelative(u.last_seen)}</span>
              </div>
            </div>
          </Link>
        ))}
        {!loading && users.length === 0 && (
          <div className="card text-white/50 text-sm text-center py-10">
            no users
          </div>
        )}
      </div>
    </div>
  );
}
