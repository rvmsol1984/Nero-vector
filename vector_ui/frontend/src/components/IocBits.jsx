// Shared IOC-match display helpers used by the Dashboard alert section,
// the UserDetail banner, and the Governance IOC Matches tab.
//
// A skull icon, an IOC-type badge coloured by observable kind, and a
// confidence pill that maps the 0–100 score to CRITICAL/HIGH/MEDIUM/LOW.

export function SkullIcon({ size = 14 }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M12 2a8 8 0 0 0-8 8v4a4 4 0 0 0 2 3.46V20a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-2.54A4 4 0 0 0 20 14v-4a8 8 0 0 0-8-8z" />
      <circle cx="9" cy="11" r="1.4" fill="currentColor" stroke="none" />
      <circle cx="15" cy="11" r="1.4" fill="currentColor" stroke="none" />
      <path d="M10 17h4" />
    </svg>
  );
}

const IOC_TYPE_COLORS = {
  "ipv4-addr":   "#3B82F6",
  "ipv6-addr":   "#3B82F6",
  "email-addr":  "#c084fc",
  "file-sha256": "#F97316",
  "domain-name": "#10B981",
  "url":         "#10B981",
};

const IOC_TYPE_LABELS = {
  "ipv4-addr":   "IPv4",
  "ipv6-addr":   "IPv6",
  "email-addr":  "EMAIL",
  "file-sha256": "SHA256",
  "domain-name": "DOMAIN",
  "url":         "URL",
};

export function IocTypeBadge({ type }) {
  if (!type) return null;
  const color = IOC_TYPE_COLORS[type] || "rgba(255,255,255,0.5)";
  const label = IOC_TYPE_LABELS[type] || String(type).toUpperCase();
  return (
    <span
      className="inline-flex items-center px-1.5 py-[2px] text-[9px] font-bold uppercase tracking-wide rounded border whitespace-nowrap"
      style={{
        color,
        borderColor: color + "55",
        backgroundColor: color + "14",
      }}
    >
      {label}
    </span>
  );
}

function confidenceBucket(confidence) {
  const n = Number(confidence || 0);
  if (n >= 90) return { label: "CRITICAL", color: "#EF4444" };
  if (n >= 75) return { label: "HIGH",     color: "#F97316" };
  if (n >= 50) return { label: "MEDIUM",   color: "#EAB308" };
  return { label: "LOW", color: "rgba(255,255,255,0.5)" };
}

export function ConfidencePill({ confidence }) {
  const n = Number(confidence || 0);
  const { label, color } = confidenceBucket(n);
  return (
    <span
      className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
      style={{
        color,
        borderColor: color + "55",
        backgroundColor: color + "14",
      }}
    >
      {label} · {n}
    </span>
  );
}
