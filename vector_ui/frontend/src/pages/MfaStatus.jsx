import { useEffect, useState } from "react";

import TenantBadge from "../components/TenantBadge.jsx";

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function fetchMfaStatus(signal) {
  const token = localStorage.getItem("vector_token");
  return fetch("/api/mfa-status", {
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

const STATUS_META = {
  STRONG: { label: "STRONG", color: "#10B981", bg: "#10B98115" },
  WEAK:   { label: "WEAK",   color: "#EAB308", bg: "#EAB30815" },
  NONE:   { label: "NONE",   color: "#EF4444", bg: "#EF444415" },
};

function StatusPill({ status }) {
  const meta = STATUS_META[status] || STATUS_META.NONE;
  return (
    <span
      className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider"
      style={{ color: meta.color, backgroundColor: meta.bg }}
    >
      {meta.label}
    </span>
  );
}

// ---------------------------------------------------------------------------
// page
// ---------------------------------------------------------------------------

export default function MfaStatus() {
  const [rows, setRows]       = useState(null);
  const [err, setErr]         = useState(null);
  const [search, setSearch]   = useState("");

  useEffect(() => {
    const ctrl = new AbortController();
    setRows(null);
    setErr(null);
    fetchMfaStatus(ctrl.signal)
      .then(setRows)
      .catch((e) => { if (e.name !== "AbortError") setErr(String(e.message || e)); });
    return () => ctrl.abort();
  }, []);

  // Summary counts (computed from full dataset, before search filter)
  const noneCount   = rows ? rows.filter((r) => r.status === "NONE").length   : 0;
  const weakCount   = rows ? rows.filter((r) => r.status === "WEAK").length   : 0;
  const strongCount = rows ? rows.filter((r) => r.status === "STRONG").length : 0;

  const q = search.trim().toLowerCase();
  const visible = rows
    ? rows.filter((r) => {
        if (!q) return true;
        return (
          (r.user_id      || "").toLowerCase().includes(q) ||
          (r.display_name || "").toLowerCase().includes(q) ||
          (r.client_name  || "").toLowerCase().includes(q) ||
          (r.mfa_method   || "").toLowerCase().includes(q) ||
          (r.status       || "").toLowerCase().includes(q)
        );
      })
    : [];

  return (
    <div className="px-6 py-6 space-y-5 max-w-6xl mx-auto">
      {/* header */}
      <div>
        <h1 className="text-xl font-bold">MFA Status</h1>
        <p className="text-white/50 text-[12px] mt-1">
          MFA registration state for all users across all tenants. First load
          queries Microsoft Graph per user and may take a moment; results are
          cached for 5 minutes.
        </p>
      </div>

      {/* summary bar */}
      {rows && (
        <div className="flex gap-4 flex-wrap">
          <SummaryChip
            count={noneCount}
            label="no MFA"
            color={STATUS_META.NONE.color}
            bg={STATUS_META.NONE.bg}
          />
          <SummaryChip
            count={weakCount}
            label="weak MFA"
            color={STATUS_META.WEAK.color}
            bg={STATUS_META.WEAK.bg}
          />
          <SummaryChip
            count={strongCount}
            label="strong MFA"
            color={STATUS_META.STRONG.color}
            bg={STATUS_META.STRONG.bg}
          />
        </div>
      )}

      {/* search */}
      <input
        type="search"
        placeholder="Filter by user, tenant, method…"
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        className="w-full max-w-sm bg-white/5 border border-white/10 rounded-lg px-3 py-1.5 text-[12px] text-white placeholder-white/30 outline-none focus:border-white/30"
      />

      {/* states */}
      {err && (
        <div className="text-[12px] text-red-400 bg-red-400/10 border border-red-400/20 rounded-lg px-4 py-3">
          Failed to load MFA status: {err}
        </div>
      )}
      {rows === null && !err && (
        <div className="text-white/40 text-[12px] py-6">
          Loading MFA status from Microsoft Graph…
        </div>
      )}

      {/* table */}
      {rows !== null && (
        <div
          className="rounded-lg border border-white/5 overflow-hidden"
          style={{ backgroundColor: "rgba(255,255,255,0.015)" }}
        >
          {visible.length === 0 ? (
            <div className="text-white/40 text-[12px] px-4 py-6">
              {q ? "No users match the filter." : "No users found."}
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full table-fixed text-[11px]">
                <colgroup>
                  <col style={{ width: "28%" }} />
                  <col style={{ width: "16%" }} />
                  <col style={{ width: "20%" }} />
                  <col style={{ width: "20%" }} />
                  <col style={{ width: "16%" }} />
                </colgroup>
                <thead>
                  <tr className="border-b border-white/8 text-left">
                    <Th>User</Th>
                    <Th>Tenant</Th>
                    <Th>MFA Method</Th>
                    <Th>Phone</Th>
                    <Th>Status</Th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-white/5">
                  {visible.map((row, i) => (
                    <MfaRow key={row.user_id || i} row={row} />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {rows !== null && visible.length > 0 && (
        <div className="text-white/30 text-[10px]">
          Showing {visible.length} of {rows.length} user{rows.length !== 1 ? "s" : ""}
          {q ? " (filtered)" : ""}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// sub-components
// ---------------------------------------------------------------------------

function SummaryChip({ count, label, color, bg }) {
  return (
    <div
      className="flex items-center gap-2 px-4 py-2 rounded-xl border text-[12px]"
      style={{ borderColor: color + "40", backgroundColor: bg }}
    >
      <span className="text-xl font-bold tabular-nums" style={{ color }}>
        {count}
      </span>
      <span style={{ color: color + "CC" }}>{label}</span>
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

function MfaRow({ row }) {
  const displayName = row.display_name || row.user_id || "—";
  const email       = row.user_id || "";
  const showEmail   = row.display_name && row.display_name !== email;

  return (
    <tr className="hover:bg-white/[0.03] transition-colors">
      {/* User */}
      <td className="px-3 py-2 min-w-0">
        <div className="font-medium text-white/90 truncate" title={displayName}>
          {displayName}
        </div>
        {showEmail && (
          <div className="text-white/40 text-[10px] truncate mt-0.5" title={email}>
            {email}
          </div>
        )}
      </td>

      {/* Tenant */}
      <td className="px-3 py-2">
        {row.client_name ? (
          <TenantBadge name={row.client_name} />
        ) : (
          <span className="text-white/30">—</span>
        )}
      </td>

      {/* MFA Method */}
      <td className="px-3 py-2">
        <MethodBadge method={row.mfa_method} status={row.status} />
      </td>

      {/* Phone */}
      <td className="px-3 py-2 text-white/50 tabular-nums">
        {row.phone_number || <span className="text-white/20">—</span>}
      </td>

      {/* Status */}
      <td className="px-3 py-2">
        <StatusPill status={row.status} />
      </td>
    </tr>
  );
}

function MethodBadge({ method, status }) {
  if (!method || method === "None") {
    return <span className="text-white/30">None</span>;
  }
  const meta = STATUS_META[status] || STATUS_META.NONE;
  return (
    <span
      className="inline-flex items-center gap-1 text-[10px]"
      style={{ color: meta.color }}
    >
      <MethodIcon method={method} />
      {method}
    </span>
  );
}

function MethodIcon({ method }) {
  const m = (method || "").toLowerCase();
  if (m.includes("authenticator"))  return <span aria-hidden="true">📱</span>;
  if (m.includes("fido2"))          return <span aria-hidden="true">🔑</span>;
  if (m.includes("windows hello"))  return <span aria-hidden="true">🪟</span>;
  if (m.includes("sms"))            return <span aria-hidden="true">💬</span>;
  if (m.includes("oath"))           return <span aria-hidden="true">🔢</span>;
  return <span aria-hidden="true">🔐</span>;
}
