import { useEffect, useMemo, useState } from "react";
import TenantSelect from "../components/TenantSelect.jsx";

const TENANTS = ["NERO", "London Fischer", "GameChange Solar"];

function fetchMailboxRules(tenant, signal) {
  const token = localStorage.getItem("vector_token");
  return fetch(`/api/mailbox-rules?${new URLSearchParams({ tenant })}`, {
    credentials: "same-origin",
    signal,
    headers: {
      Accept: "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  }).then((r) => {
    if (!r.ok) throw new Error(`${r.status}`);
    return r.json();
  });
}

export default function MailboxRules() {
  const [tenant, setTenant]   = useState(TENANTS[0]);
  const [rules, setRules]     = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr]         = useState(null);
  const [showAll, setShowAll] = useState(false);

  useEffect(() => {
    const ctrl = new AbortController();
    setRules(null);
    setErr(null);
    setLoading(true);
    fetchMailboxRules(tenant, ctrl.signal)
      .then((data) => { setRules(data); setLoading(false); })
      .catch((e) => {
        if (e.name === "AbortError") return;
        setErr(String(e.message || e));
        setLoading(false);
      });
    return () => ctrl.abort();
  }, [tenant]);

  const suspiciousCount = rules ? rules.filter((r) => r.is_suspicious).length : 0;
  const totalCount      = rules ? rules.length : 0;

  const visible = useMemo(() => {
    if (!rules) return [];
    return showAll ? rules : rules.filter((r) => r.is_suspicious);
  }, [rules, showAll]);

  return (
    <div className="px-6 py-6 space-y-5 max-w-6xl mx-auto">
      {/* header */}
      <div>
        <h1 className="text-xl font-bold">Mailbox Rules</h1>
        <p className="text-white/50 text-[12px] mt-1">
          Inbox rule inventory across all tenant mailboxes. Flags forwarding to external addresses,
          auto-delete rules, and other suspicious patterns. Cached 10 minutes per tenant.
        </p>
      </div>

      <TenantSelect tenants={TENANTS} value={tenant} onChange={setTenant} />

      {loading && (
        <div className="text-white/40 text-[12px] py-4">
          Loading mailbox rules for{" "}
          <span className="text-white/70">{tenant}</span>…
          <span className="text-white/30 block mt-1 text-[11px]">
            Querying all mailboxes — may take up to 30 seconds on first load.
          </span>
        </div>
      )}

      {err && (
        <div className="text-[12px] text-red-400 bg-red-400/10 border border-red-400/20 rounded-lg px-4 py-3">
          Failed to load mailbox rules: {err}
        </div>
      )}

      {rules && !loading && (
        <>
          {/* summary + toggle */}
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div className="flex items-baseline gap-3 text-[12px]">
              <span>
                <span className="text-xl font-bold tabular-nums" style={{ color: "#EF4444" }}>
                  {suspiciousCount}
                </span>{" "}
                <span style={{ color: "#EF4444CC" }}>suspicious</span>
              </span>
              <span className="text-white/20">·</span>
              <span className="text-white/50">
                <span className="text-white/70 font-semibold tabular-nums">{totalCount}</span> total rules
              </span>
            </div>

            <div className="flex text-[11px] rounded-lg overflow-hidden border border-white/10">
              <button
                type="button"
                onClick={() => setShowAll(false)}
                className={`px-3 py-1.5 font-semibold transition-colors ${
                  !showAll
                    ? "bg-white/10 text-white"
                    : "text-white/50 hover:text-white/70"
                }`}
              >
                Suspicious Only
              </button>
              <button
                type="button"
                onClick={() => setShowAll(true)}
                className={`px-3 py-1.5 font-semibold transition-colors border-l border-white/10 ${
                  showAll
                    ? "bg-white/10 text-white"
                    : "text-white/50 hover:text-white/70"
                }`}
              >
                All Rules
              </button>
            </div>
          </div>

          {/* table */}
          <div
            className="rounded-lg border border-white/5 overflow-hidden"
            style={{ backgroundColor: "rgba(255,255,255,0.015)" }}
          >
            {visible.length === 0 ? (
              <div className="text-white/40 text-[12px] px-4 py-8 text-center">
                {showAll ? "No mailbox rules found." : "No suspicious rules detected."}
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full table-fixed text-[11px]">
                  <colgroup>
                    <col style={{ width: "22%" }} />
                    <col style={{ width: "20%" }} />
                    <col style={{ width: "20%" }} />
                    <col style={{ width: "10%" }} />
                    <col style={{ width: "28%" }} />
                  </colgroup>
                  <thead>
                    <tr className="border-b border-white/8 text-left">
                      <Th>User</Th>
                      <Th>Rule Name</Th>
                      <Th>Actions</Th>
                      <Th>Suspicious</Th>
                      <Th>Reason</Th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/5">
                    {visible.map((row, i) => (
                      <RuleRow key={i} row={row} />
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          <div className="text-white/30 text-[10px]">
            Showing {visible.length} of {rules.length} rule
            {rules.length !== 1 ? "s" : ""} · {tenant}
            {!showAll ? " (suspicious only)" : ""}
          </div>
        </>
      )}
    </div>
  );
}

function Th({ children }) {
  return (
    <th className="px-3 py-2 text-[10px] uppercase tracking-wider text-white/40 font-medium">
      {children}
    </th>
  );
}

function RuleRow({ row }) {
  const suspicious = row.is_suspicious;
  const actions    = row.actions || {};
  const labels     = [];
  if ((actions.forwardTo  || []).length) labels.push("Forward");
  if ((actions.redirectTo || []).length) labels.push("Redirect");
  if (actions.deleteMessage)             labels.push("Delete");
  if (actions.markAsRead)                labels.push("Mark Read");
  if (actions.moveToFolder)              labels.push(`Move → ${actions.moveToFolder}`);
  if (actions.copyToFolder)              labels.push(`Copy → ${actions.copyToFolder}`);

  return (
    <tr
      className={`transition-colors ${
        suspicious ? "hover:bg-amber-500/[0.04]" : "hover:bg-white/[0.03]"
      }`}
      style={suspicious ? { borderLeft: "3px solid #F59E0B" } : {}}
    >
      {/* User */}
      <td className="px-3 py-2 min-w-0">
        <div className="font-medium text-white/90 truncate" title={row.user}>
          {row.display_name || row.user}
        </div>
        {row.display_name && row.display_name !== row.user && (
          <div className="text-white/40 text-[10px] truncate mt-0.5" title={row.user}>
            {row.user}
          </div>
        )}
      </td>

      {/* Rule Name */}
      <td className="px-3 py-2 min-w-0">
        <div
          className={`truncate ${row.rule_enabled ? "text-white/80" : "text-white/35 line-through"}`}
          title={row.rule_name}
        >
          {row.rule_name}
        </div>
        {!row.rule_enabled && (
          <div className="text-white/25 text-[10px]">disabled</div>
        )}
      </td>

      {/* Actions */}
      <td className="px-3 py-2 text-white/55">
        {labels.length > 0 ? (
          <span className="truncate block">{labels.join(", ")}</span>
        ) : (
          <span className="text-white/20">—</span>
        )}
      </td>

      {/* Suspicious badge */}
      <td className="px-3 py-2">
        {suspicious ? (
          <span
            className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider"
            style={{ color: "#F59E0B", backgroundColor: "#F59E0B15" }}
          >
            Yes
          </span>
        ) : (
          <span className="text-white/25 text-[10px]">—</span>
        )}
      </td>

      {/* Reason */}
      <td className="px-3 py-2 text-white/50 text-[10px]">
        {row.suspicion_reason || <span className="text-white/20">—</span>}
      </td>
    </tr>
  );
}
