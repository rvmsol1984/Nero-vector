import { useCallback, useMemo, useState } from "react";

import TenantBadge from "../components/TenantBadge.jsx";

// ---------------------------------------------------------------------------
// constants
// ---------------------------------------------------------------------------

// Tenants available in the MFA status endpoint.  Must match _MFA_TENANTS
// client_name values in the backend.  "All" is intentionally absent —
// fetching every tenant at once is too expensive.
const MFA_TENANTS = ["NERO", "London Fischer"];

// ---------------------------------------------------------------------------
// fetch helper
// ---------------------------------------------------------------------------

function fetchMfaStatus(tenantName, signal) {
  const token = localStorage.getItem("vector_token");
  const params = new URLSearchParams({ tenant: tenantName });
  return fetch(`/api/mfa-status?${params}`, {
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

// ---------------------------------------------------------------------------
// style maps
// ---------------------------------------------------------------------------

const STATUS_META = {
  STRONG: { label: "STRONG", color: "#10B981", bg: "#10B98115" },
  WEAK:   { label: "WEAK",   color: "#EAB308", bg: "#EAB30815" },
  NONE:   { label: "NONE",   color: "#EF4444", bg: "#EF444415" },
};

// ---------------------------------------------------------------------------
// page
// ---------------------------------------------------------------------------

export default function MfaStatus() {
  const [tenant, setTenant]   = useState(null);   // null = none selected
  const [rows, setRows]       = useState(null);   // null = not yet fetched
  const [loading, setLoading] = useState(false);
  const [err, setErr]         = useState(null);
  const [search, setSearch]   = useState("");

  // Stable abort-controller ref so switching tenants cancels in-flight fetch.
  const [ctrl, setCtrl] = useState(null);

  const selectTenant = useCallback((name) => {
    // Cancel any in-flight request for the previous tenant.
    if (ctrl) ctrl.abort();

    setTenant(name);
    setRows(null);
    setErr(null);
    setSearch("");
    setLoading(true);

    const next = new AbortController();
    setCtrl(next);

    fetchMfaStatus(name, next.signal)
      .then((data) => { setRows(data); setLoading(false); })
      .catch((e) => {
        if (e.name === "AbortError") return;
        setErr(String(e.message || e));
        setLoading(false);
      });
  }, [ctrl]);

  // Summary counts — based on full fetched set, before text search.
  const noneCount   = rows ? rows.filter((r) => r.status === "NONE").length   : 0;
  const weakCount   = rows ? rows.filter((r) => r.status === "WEAK").length   : 0;
  const strongCount = rows ? rows.filter((r) => r.status === "STRONG").length : 0;

  const q = search.trim().toLowerCase();
  const visible = useMemo(() => {
    if (!rows) return [];
    if (!q) return rows;
    return rows.filter((r) =>
      (r.user_id      || "").toLowerCase().includes(q) ||
      (r.display_name || "").toLowerCase().includes(q) ||
      (r.mfa_method   || "").toLowerCase().includes(q) ||
      (r.status       || "").toLowerCase().includes(q)
    );
  }, [rows, q]);

  return (
    <div className="px-6 py-6 space-y-5 max-w-6xl mx-auto">
      {/* header */}
      <div>
        <h1 className="text-xl font-bold">MFA Status</h1>
        <p className="text-white/50 text-[12px] mt-1">
          Select a tenant to load MFA registration status from Microsoft Graph.
          Results are cached for 5 minutes per tenant.
        </p>
      </div>

      {/* tenant selector */}
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-[10px] uppercase tracking-wider text-white/40 mr-1">
          Tenant
        </span>
        {MFA_TENANTS.map((name) => (
          <TenantPill
            key={name}
            active={tenant === name}
            onClick={() => selectTenant(name)}
          >
            {name}
          </TenantPill>
        ))}
      </div>

      {/* prompt when nothing selected */}
      {tenant === null && (
        <div className="text-white/40 text-[12px] py-4">
          Select a tenant to load MFA status.
        </div>
      )}

      {/* loading */}
      {loading && (
        <div className="text-white/40 text-[12px] py-4">
          Loading MFA status for <span className="text-white/70">{tenant}</span>
          {" "}from Microsoft Graph…
        </div>
      )}

      {/* error */}
      {err && (
        <div className="text-[12px] text-red-400 bg-red-400/10 border border-red-400/20 rounded-lg px-4 py-3">
          Failed to load MFA status: {err}
        </div>
      )}

      {/* summary bar — only when data is loaded */}
      {rows && !loading && (
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

      {/* search — only when data is loaded */}
      {rows && !loading && (
        <input
          type="search"
          placeholder="Filter by user, method, status…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full max-w-sm bg-white/5 border border-white/10 rounded-lg px-3 py-1.5 text-[12px] text-white placeholder-white/30 outline-none focus:border-white/30"
        />
      )}

      {/* table */}
      {rows && !loading && (
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
                  <col style={{ width: "32%" }} />
                  <col style={{ width: "18%" }} />
                  <col style={{ width: "22%" }} />
                  <col style={{ width: "28%" }} />
                </colgroup>
                <thead>
                  <tr className="border-b border-white/8 text-left">
                    <Th>User</Th>
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

      {rows && !loading && visible.length > 0 && (
        <div className="text-white/30 text-[10px]">
          Showing {visible.length} of {rows.length} user
          {rows.length !== 1 ? "s" : ""}
          {q ? " (filtered)" : ""}
          {" · "}{tenant}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// sub-components
// ---------------------------------------------------------------------------

function TenantPill({ active, onClick, children }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`px-3 py-1.5 rounded-xl text-xs font-medium whitespace-nowrap transition-all duration-200 active:scale-95 ${
        active
          ? "bg-primary text-white"
          : "bg-white/10 text-white/70 hover:bg-white/15"
      }`}
    >
      {children}
    </button>
  );
}

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
