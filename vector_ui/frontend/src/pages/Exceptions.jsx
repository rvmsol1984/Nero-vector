import { useEffect, useState } from "react";
import { api } from "../api.js";
import { fmtRelative, fmtNumber } from "../utils/format.js";

const RULES = [
  "NewCountryLoginRule",
  "OffHoursLoginRule",
  "HighVolumeFileAccessRule",
  "SuspiciousMailboxRule",
  "MalwareDetectedRule",
  "IOCMatchRule",
  "HighRiskCountryLoginRule",
  "VPNLoginRule",
  "ImpossibleTravelRule",
  "InboxRuleCreatedRule",
];

const TYPES = ["country", "ip", "user", "domain", "any"];

function fetchJson(url, opts = {}) {
  const token = localStorage.getItem("vector_token");
  return fetch(url, {
    credentials: "same-origin",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    ...opts,
  }).then((r) => {
    if (!r.ok) throw new Error(`${r.status}`);
    return r.json();
  });
}

export default function Exceptions() {
  const [tenants, setTenants] = useState([]);
  const [selectedTenant, setSelectedTenant] = useState("");
  const [rows, setRows] = useState([]);
  const [err, setErr] = useState(null);

  // form state
  const [formRule, setFormRule] = useState(RULES[0]);
  const [formType, setFormType] = useState(TYPES[0]);
  const [formValue, setFormValue] = useState("");
  const [formNote, setFormNote] = useState("");
  const [saving, setSaving] = useState(false);

  // load tenants on mount
  useEffect(() => {
    api.byTenant().then((t) => {
      setTenants(t || []);
      if (t && t.length) setSelectedTenant(t[0].client_name);
    }).catch(() => {});
  }, []);

  // load exceptions when tenant changes
  useEffect(() => {
    if (!selectedTenant) return;
    let cancel = false;
    setErr(null);
    fetchJson(`/api/exceptions?tenant=${encodeURIComponent(selectedTenant)}`)
      .then((d) => { if (!cancel) setRows(d || []); })
      .catch((e) => { if (!cancel) setErr(String(e.message || e)); });
    return () => { cancel = true; };
  }, [selectedTenant]);

  function reload() {
    if (!selectedTenant) return;
    fetchJson(`/api/exceptions?tenant=${encodeURIComponent(selectedTenant)}`)
      .then(setRows)
      .catch(() => {});
  }

  // Resolve tenant_id for the selected tenant (needed for POST)
  const [tenantId, setTenantId] = useState("");
  useEffect(() => {
    if (!selectedTenant) return;
    let cancel = false;
    api.events({ tenant: selectedTenant, limit: 1 })
      .then((r) => {
        if (!cancel && r && r[0]) setTenantId(r[0].tenant_id || "");
      })
      .catch(() => {});
    return () => { cancel = true; };
  }, [selectedTenant]);

  async function handleAdd(e) {
    e.preventDefault();
    if (!formValue.trim() || !tenantId) return;
    setSaving(true);
    try {
      await fetchJson("/api/exceptions", {
        method: "POST",
        body: JSON.stringify({
          tenant_id: tenantId,
          client_name: selectedTenant,
          rule_name: formRule,
          exception_type: formType,
          exception_value: formValue.trim(),
          note: formNote.trim() || null,
        }),
      });
      setFormValue("");
      setFormNote("");
      reload();
    } catch (ex) {
      setErr(String(ex.message || ex));
    } finally {
      setSaving(false);
    }
  }

  async function handleDelete(id) {
    try {
      await fetchJson(`/api/exceptions/${encodeURIComponent(id)}`, {
        method: "DELETE",
      });
      reload();
    } catch (ex) {
      setErr(String(ex.message || ex));
    }
  }

  return (
    <div className="space-y-5 animate-fade-in">
      <div>
        <h1 className="text-2xl font-bold">Rule Exceptions</h1>
        <p className="text-white/50 text-sm mt-1">
          Suppress specific rule triggers per tenant.
        </p>
      </div>

      {/* tenant selector */}
      {tenants.length > 0 && (
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold mr-1">
            tenant
          </span>
          {tenants.map((t) => (
            <button
              key={t.client_name}
              type="button"
              onClick={() => setSelectedTenant(t.client_name)}
              className={`px-3 py-1.5 rounded-xl text-xs font-medium whitespace-nowrap transition-all active:scale-95 ${
                selectedTenant === t.client_name
                  ? "bg-primary text-white"
                  : "bg-white/10 text-white/70 hover:bg-white/15"
              }`}
            >
              {t.client_name}
            </button>
          ))}
        </div>
      )}

      {err && (
        <div className="card border-critical/30 text-critical text-sm px-4 py-3">
          {err}
        </div>
      )}

      {/* add exception form */}
      <form
        onSubmit={handleAdd}
        className="card p-4 flex flex-wrap items-end gap-3"
      >
        <div>
          <label className="block text-[10px] uppercase tracking-wider text-white/40 mb-1">
            Rule
          </label>
          <select
            value={formRule}
            onChange={(e) => setFormRule(e.target.value)}
            className="bg-white/5 border border-white/10 rounded-xl px-3 py-1.5 text-xs text-white focus:outline-none focus:border-primary-light"
          >
            {RULES.map((r) => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-[10px] uppercase tracking-wider text-white/40 mb-1">
            Type
          </label>
          <select
            value={formType}
            onChange={(e) => setFormType(e.target.value)}
            className="bg-white/5 border border-white/10 rounded-xl px-3 py-1.5 text-xs text-white focus:outline-none focus:border-primary-light"
          >
            {TYPES.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
        </div>
        <div className="flex-1 min-w-[180px]">
          <label className="block text-[10px] uppercase tracking-wider text-white/40 mb-1">
            Value
          </label>
          <input
            type="text"
            value={formValue}
            onChange={(e) => setFormValue(e.target.value)}
            placeholder={formType === "country" ? "e.g. CN" : formType === "ip" ? "e.g. 1.2.3.4" : formType === "any" ? "*" : "e.g. alice@example.com"}
            className="w-full bg-white/5 border border-white/10 rounded-xl px-3 py-1.5 text-xs text-white placeholder:text-white/40 focus:outline-none focus:border-primary-light"
          />
        </div>
        <div className="flex-1 min-w-[140px]">
          <label className="block text-[10px] uppercase tracking-wider text-white/40 mb-1">
            Note
          </label>
          <input
            type="text"
            value={formNote}
            onChange={(e) => setFormNote(e.target.value)}
            placeholder="optional reason"
            className="w-full bg-white/5 border border-white/10 rounded-xl px-3 py-1.5 text-xs text-white placeholder:text-white/40 focus:outline-none focus:border-primary-light"
          />
        </div>
        <button
          type="submit"
          disabled={saving || !formValue.trim() || !tenantId}
          className="px-4 py-1.5 text-xs font-semibold rounded-xl bg-primary text-white hover:bg-primary/90 active:scale-95 transition-all disabled:opacity-40"
        >
          {saving ? "adding…" : "Add Exception"}
        </button>
      </form>

      {/* exceptions table */}
      <div className="card overflow-hidden">
        <div className="overflow-x-hidden">
          <table className="w-full table-fixed text-[11px]">
            <thead>
              <tr>
                <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold" style={{ width: "22%" }}>Rule</th>
                <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold" style={{ width: "10%" }}>Type</th>
                <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold" style={{ width: "20%" }}>Value</th>
                <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">Note</th>
                <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold" style={{ width: "12%" }}>Created</th>
                <th className="text-left px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold" style={{ width: "8%" }}>{""}</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {rows.map((row) => (
                <tr key={row.id} className="hover:bg-white/[0.03]">
                  <td className="px-4 py-2.5 text-white/80 truncate" title={row.rule_name}>
                    {row.rule_name}
                  </td>
                  <td className="px-4 py-2.5">
                    <TypeBadge type={row.exception_type} />
                  </td>
                  <td className="px-4 py-2.5 font-mono text-white/80 truncate" title={row.exception_value}>
                    {row.exception_value}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 truncate" title={row.note || ""}>
                    {row.note || <span className="text-white/30">—</span>}
                  </td>
                  <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                    {fmtRelative(row.created_at)}
                  </td>
                  <td className="px-4 py-2.5">
                    <button
                      type="button"
                      onClick={() => handleDelete(row.id)}
                      className="px-2 py-1 text-[10px] font-semibold rounded-lg border text-critical border-critical/30 bg-critical/10 hover:bg-critical/20 active:scale-95 transition-all"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
              {rows.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-10 text-center text-white/40">
                    No exceptions configured for this tenant.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function TypeBadge({ type }) {
  const colors = {
    country: "#3B82F6",
    ip:      "#F97316",
    user:    "#8B5CF6",
    domain:  "#14B8A6",
    any:     "#EF4444",
  };
  const color = colors[type] || "#6B7280";
  return (
    <span
      className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
      style={{
        color,
        borderColor: color + "55",
        backgroundColor: color + "14",
      }}
    >
      {type}
    </span>
  );
}
