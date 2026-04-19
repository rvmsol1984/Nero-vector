import { useEffect, useMemo, useState } from "react";
import TenantSelect from "../components/TenantSelect.jsx";

const TENANTS = ["NERO", "London Fischer", "GameChange Solar"];

const SEVERITY_META = {
  CRITICAL: { color: "#EF4444", bg: "#EF444415", border: "#EF444430" },
  HIGH:     { color: "#F97316", bg: "#F9731615", border: "#F9731630" },
  MEDIUM:   { color: "#EAB308", bg: "#EAB30815", border: "#EAB30830" },
  LOW:      { color: "#6B7280", bg: "#6B728015", border: "#6B728030" },
};

function fetchFindings(tenant, signal) {
  const token = localStorage.getItem("vector_token");
  return fetch(`/api/security-findings?${new URLSearchParams({ tenant })}`, {
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

const CATEGORY_TYPES = {
  "Forward Rules":    ["EXTERNAL_FORWARD_RULE", "INTERNAL_FORWARD_RULE", "DELETE_INBOX_RULE", "HIDE_INBOX_RULE"],
  "Admin Risk":       ["ADMIN_NO_MFA", "STALE_ADMIN", "UNREGISTERED_DEVICE_ADMIN", "NEW_GLOBAL_ADMIN", "MFA_METHOD_CHANGED"],
  "Sharing":          ["ANONYMOUS_SHARE", "GUEST_WITH_ACCESS", "OAUTH_HIGH_PERMISSION_APP"],
  "Inactive Users":   ["INACTIVE_LICENSED_USER"],
  "Shared Mailboxes": ["SHARED_MAILBOX_SIGNIN_ENABLED"],
  "Legacy Auth":      ["LEGACY_AUTH_LOGIN"],
  "Cross-Tenant":     ["CROSS_TENANT_IOC"],
  "Phishing":         ["PATIENT_ZERO_PHISHING", "IMPOSSIBLE_TRAVEL"],
};

export default function SecurityFindings() {
  const [tenant, setTenant]       = useState(TENANTS[0]);
  const [findings, setFindings]   = useState(null);
  const [loading, setLoading]     = useState(false);
  const [err, setErr]             = useState(null);
  const [sevFilter, setSevFilter] = useState("ALL");
  const [catFilter, setCatFilter] = useState("ALL");
  const [expanded, setExpanded]   = useState({});

  useEffect(() => {
    const ctrl = new AbortController();
    setFindings(null);
    setErr(null);
    setSevFilter("ALL");
    setCatFilter("ALL");
    setExpanded({});
    setLoading(true);
    fetchFindings(tenant, ctrl.signal)
      .then((data) => { setFindings(data); setLoading(false); })
      .catch((e) => {
        if (e.name === "AbortError") return;
        setErr(String(e.message || e));
        setLoading(false);
      });
    return () => ctrl.abort();
  }, [tenant]);

  const filtered = useMemo(() => {
    if (!findings) return [];
    return findings.filter((f) => {
      const sevOk = sevFilter === "ALL" || f.severity === sevFilter;
      const catOk = catFilter === "ALL" || (CATEGORY_TYPES[catFilter] || []).includes(f.finding_type);
      return sevOk && catOk;
    });
  }, [findings, sevFilter, catFilter]);

  const criticalCount = filtered.filter((f) => f.severity === "CRITICAL").length;
  const highCount     = filtered.filter((f) => f.severity === "HIGH").length;
  const mediumCount   = filtered.filter((f) => f.severity === "MEDIUM").length;

  const visible = filtered;

  function toggleExpand(idx) {
    setExpanded((prev) => ({ ...prev, [idx]: !prev[idx] }));
  }

  return (
    <div className="px-6 py-6 space-y-5 max-w-6xl mx-auto">
      {/* header */}
      <div>
        <h1 className="text-xl font-bold">Security Findings</h1>
        <p className="text-white/50 text-[12px] mt-1">
          Proactive posture checks across Graph API and audit log. Results cached 10 minutes per tenant.
        </p>
      </div>

      <TenantSelect tenants={TENANTS} value={tenant} onChange={setTenant} />

      {loading && (
        <div className="text-white/40 text-[12px] py-4">
          Running security checks for{" "}
          <span className="text-white/70">{tenant}</span>…
          <span className="text-white/30 block mt-1 text-[11px]">
            This may take up to 30 seconds on first load (Graph API calls per mailbox).
          </span>
        </div>
      )}

      {err && (
        <div className="text-[12px] text-red-400 bg-red-400/10 border border-red-400/20 rounded-lg px-4 py-3">
          Failed to load security findings: {err}
        </div>
      )}

      {findings && !loading && (
        <>
          {/* summary bar — counts reflect current filters */}
          <div className="flex gap-3 flex-wrap">
            <SummaryChip count={criticalCount} label="critical" color={SEVERITY_META.CRITICAL.color} bg={SEVERITY_META.CRITICAL.bg} />
            <SummaryChip count={highCount}     label="high"     color={SEVERITY_META.HIGH.color}     bg={SEVERITY_META.HIGH.bg} />
            <SummaryChip count={mediumCount}   label="medium"   color={SEVERITY_META.MEDIUM.color}   bg={SEVERITY_META.MEDIUM.bg} />
          </div>

          {/* severity filter pills */}
          <div className="flex gap-2 flex-wrap">
            {[
              { key: "ALL",      label: `All (${findings.length})` },
              { key: "CRITICAL", label: "Critical" },
              { key: "HIGH",     label: "High" },
              { key: "MEDIUM",   label: "Medium" },
            ].map(({ key, label }) => (
              <button
                key={key}
                type="button"
                onClick={() => setSevFilter(key)}
                className={`px-3 py-1 text-[11px] font-semibold rounded-full border transition-colors ${
                  sevFilter === key
                    ? "bg-white/15 border-white/30 text-white"
                    : "bg-transparent border-white/10 text-white/50 hover:border-white/20 hover:text-white/70"
                }`}
              >
                {label}
              </button>
            ))}
          </div>

          {/* category filter tabs */}
          <div className="flex text-[11px] rounded-lg overflow-hidden border border-white/10 w-fit">
            {["ALL", ...Object.keys(CATEGORY_TYPES)].map((cat, i) => (
              <button
                key={cat}
                type="button"
                onClick={() => setCatFilter(cat)}
                className={`px-3 py-1.5 font-semibold transition-colors whitespace-nowrap ${
                  i > 0 ? "border-l border-white/10" : ""
                } ${
                  catFilter === cat
                    ? "bg-white/10 text-white"
                    : "text-white/50 hover:text-white/70"
                }`}
              >
                {cat === "ALL" ? "All" : cat}
              </button>
            ))}
          </div>

          {/* cards */}
          {visible.length === 0 ? (
            <div className="text-white/40 text-[12px] py-10 text-center">
              {sevFilter === "ALL" && catFilter === "ALL"
                ? "No findings detected. Posture looks good."
                : "No findings match the selected filters."}
            </div>
          ) : (
            <div className="space-y-3">
              {visible.map((finding, idx) => (
                <FindingCard
                  key={idx}
                  finding={finding}
                  expanded={!!expanded[idx]}
                  onToggle={() => toggleExpand(idx)}
                />
              ))}
            </div>
          )}

          <div className="text-white/30 text-[10px]">
            Showing {visible.length} of {findings.length} finding
            {findings.length !== 1 ? "s" : ""} · {tenant}
            {sevFilter !== "ALL" ? ` · severity: ${sevFilter}` : ""}
            {catFilter !== "ALL" ? ` · category: ${catFilter}` : ""}
          </div>
        </>
      )}
    </div>
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

function FindingCard({ finding, expanded, onToggle }) {
  const meta = SEVERITY_META[finding.severity] || SEVERITY_META.LOW;
  return (
    <div
      className="rounded-lg border overflow-hidden"
      style={{
        backgroundColor: "rgba(255,255,255,0.015)",
        borderColor: meta.border,
        borderLeftWidth: "3px",
        borderLeftColor: meta.color,
      }}
    >
      <div className="px-4 py-3">
        <div className="flex items-start gap-2 flex-wrap">
          <span
            className="shrink-0 inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider"
            style={{ color: meta.color, backgroundColor: meta.bg }}
          >
            {finding.severity}
          </span>
          <span className="shrink-0 inline-flex items-center px-2 py-0.5 rounded text-[10px] font-medium bg-white/5 text-white/50 border border-white/10">
            {finding.finding_type}
          </span>
          <span className="font-semibold text-[13px] text-white/90 leading-tight">
            {finding.title}
          </span>
        </div>

        <div className="mt-2 space-y-1">
          <div className="text-[11px]">
            <span className="text-white/40">Affected: </span>
            <span className="font-mono text-white/70">{finding.affected}</span>
          </div>
          <div className="text-[11px] text-white/60">{finding.description}</div>
        </div>

        <button
          type="button"
          onClick={onToggle}
          className="mt-2 text-[10px] text-white/40 hover:text-white/60 transition-colors flex items-center gap-1"
        >
          {expanded ? "▲ Hide recommendation" : "▼ Show recommendation"}
        </button>

        {expanded && (
          <div
            className="mt-2 px-3 py-2 rounded text-[11px] text-white/70 border border-white/8"
            style={{ backgroundColor: "rgba(255,255,255,0.03)" }}
          >
            <span className="text-white/40 text-[10px] uppercase tracking-wider font-semibold block mb-1">
              Recommendation
            </span>
            {finding.recommendation}
          </div>
        )}
      </div>
    </div>
  );
}
