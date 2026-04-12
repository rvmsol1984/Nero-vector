import { Fragment, useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { api } from "../api.js";
import JsonBlock from "../components/JsonBlock.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import TenantBadge from "../components/TenantBadge.jsx";
import { fmtTime } from "../utils/format.js";

const PAGE = 50;

export default function Events() {
  const [searchParams, setSearchParams] = useSearchParams();

  const tenant = searchParams.get("tenant") || "";
  const eventType = searchParams.get("event_type") || "";
  const workload = searchParams.get("workload") || "";
  const userQuery = searchParams.get("user") || "";
  const offset = Number(searchParams.get("offset") || 0);

  // Debounced copy of the user input so we don't hit the API on every keystroke.
  const [userInput, setUserInput] = useState(userQuery);

  useEffect(() => {
    const h = setTimeout(() => {
      updateFilter("user", userInput);
    }, 300);
    return () => clearTimeout(h);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [userInput]);

  function updateFilter(key, value) {
    const p = new URLSearchParams(searchParams);
    if (value) p.set(key, value);
    else p.delete(key);
    // Any filter change rewinds pagination.
    p.delete("offset");
    setSearchParams(p, { replace: true });
  }

  function setOffset(n) {
    const p = new URLSearchParams(searchParams);
    if (n > 0) p.set("offset", String(n));
    else p.delete("offset");
    setSearchParams(p, { replace: true });
  }

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  const [byTenant, setByTenant] = useState([]);
  const [byType, setByType] = useState([]);
  const [byWorkload, setByWorkload] = useState([]);

  useEffect(() => {
    api.byTenant().then(setByTenant).catch(() => {});
    api.byType(200).then(setByType).catch(() => {});
    api.byWorkload().then(setByWorkload).catch(() => {});
  }, []);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    setErr(null);
    setExpanded(null);
    api
      .events({
        limit: PAGE,
        offset,
        tenant: tenant || undefined,
        event_type: eventType || undefined,
        workload: workload || undefined,
        user: userQuery || undefined,
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
  }, [offset, tenant, eventType, workload, userQuery]);

  // ----- inline row expansion ------------------------------------------------
  const [expanded, setExpanded] = useState(null); // { id, data | null }

  async function toggleExpand(id) {
    if (expanded && expanded.id === id) {
      setExpanded(null);
      return;
    }
    setExpanded({ id, data: null });
    try {
      const full = await api.eventById(id);
      setExpanded({ id, data: full });
    } catch (e) {
      setExpanded({ id, data: { error: e.message } });
    }
  }

  const activeFilters =
    [tenant, eventType, workload, userQuery].filter(Boolean).length;

  return (
    <div className="space-y-4">
      <div className="flex items-end justify-between">
        <div>
          <h1 className="font-display text-2xl tracking-[0.2em]">EVENTS</h1>
          <p className="text-muted text-xs mt-1">
            Raw audit stream across all connected tenants.
          </p>
        </div>
        {activeFilters > 0 && (
          <button
            type="button"
            onClick={() => {
              setUserInput("");
              setSearchParams({}, { replace: true });
            }}
            className="text-[10px] uppercase tracking-[0.2em] text-muted hover:text-accent border border-border px-2 py-1"
          >
            clear filters ({activeFilters})
          </button>
        )}
      </div>

      {/* filter bar */}
      <div className="flex flex-wrap items-center gap-3 text-xs">
        <select
          value={tenant}
          onChange={(e) => updateFilter("tenant", e.target.value)}
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
          onChange={(e) => updateFilter("event_type", e.target.value)}
          className="bg-surface border border-border px-2 py-1 text-slate-100 focus:outline-none focus:border-accent max-w-[220px]"
        >
          <option value="">all event types</option>
          {byType.map((t) => (
            <option key={t.event_type ?? ""} value={t.event_type ?? ""}>
              {t.event_type ?? "(none)"}
            </option>
          ))}
        </select>

        <select
          value={workload}
          onChange={(e) => updateFilter("workload", e.target.value)}
          className="bg-surface border border-border px-2 py-1 text-slate-100 focus:outline-none focus:border-accent"
        >
          <option value="">all workloads</option>
          {byWorkload.map((t) => (
            <option key={t.workload ?? ""} value={t.workload ?? ""}>
              {t.workload ?? "(none)"}
            </option>
          ))}
        </select>

        <input
          type="search"
          placeholder="search user…"
          value={userInput}
          onChange={(e) => setUserInput(e.target.value)}
          className="bg-surface border border-border px-2 py-1 text-slate-100 focus:outline-none focus:border-accent w-56 placeholder:text-muted"
        />

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
            {rows.map((r) => {
              const isOpen = expanded?.id === r.id;
              return (
                <Fragment key={r.id}>
                  <tr
                    onClick={() => toggleExpand(r.id)}
                    className={`cursor-pointer transition-colors ${
                      isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"
                    }`}
                  >
                    <td className="px-3 py-1.5 text-muted whitespace-nowrap">
                      {fmtTime(r.timestamp)}
                    </td>
                    <td className="px-3 py-1.5 whitespace-nowrap">
                      <TenantBadge name={r.client_name} />
                    </td>
                    <td
                      className="px-3 py-1.5 truncate max-w-[260px]"
                      title={r.user_id}
                      onClick={(e) => e.stopPropagation()}
                    >
                      <Link
                        to={`/users/${encodeURIComponent(r.entity_key)}`}
                        className="hover:text-accent"
                      >
                        {r.user_id}
                      </Link>
                    </td>
                    <td className="px-3 py-1.5">{r.event_type}</td>
                    <td className="px-3 py-1.5 text-muted">{r.workload}</td>
                    <td className="px-3 py-1.5">
                      <StatusBadge status={r.result_status} />
                    </td>
                    <td className="px-3 py-1.5 text-muted">{r.client_ip ?? ""}</td>
                  </tr>
                  {isOpen && (
                    <tr className="bg-black/30">
                      <td colSpan={7} className="px-3 py-3 border-t border-border">
                        <JsonBlock
                          data={expanded?.data?.raw_json ?? expanded?.data}
                          loading={expanded?.data === null}
                        />
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
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
