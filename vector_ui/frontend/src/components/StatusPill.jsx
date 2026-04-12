// Success / Failed / Warning / Info / neutral pill. Static Tailwind class
// literals so the JIT scanner picks up every variant.

const TONES = {
  success: {
    cls: "border-status-resolved/40 bg-status-resolved/10 text-status-resolved",
    dot: "bg-status-resolved",
  },
  critical: {
    cls: "border-critical/40 bg-critical/10 text-critical",
    dot: "bg-critical",
  },
  warning: {
    cls: "border-high/40 bg-high/10 text-high",
    dot: "bg-high",
  },
  info: {
    cls: "border-primary-light/40 bg-primary-light/10 text-primary-light",
    dot: "bg-primary-light",
  },
  muted: {
    cls: "border-white/10 bg-white/5 text-white/60",
    dot: "bg-white/40",
  },
};

function toneFor(status) {
  if (!status) return "muted";
  const s = String(status).toLowerCase();
  if (s === "succeeded" || s === "success" || s === "partialsuccess") return "success";
  if (s === "failed" || s === "failure" || s.includes("error")) return "critical";
  if (s.includes("warn")) return "warning";
  if (s.includes("logged out")) return "muted";
  return "info";
}

export default function StatusPill({ status, tone, dot = false }) {
  if (!status && !tone) return null;
  const t = TONES[tone || toneFor(status)] || TONES.muted;
  return (
    <span className={`pill ${t.cls}`}>
      {dot && <span className={`h-1.5 w-1.5 rounded-full ${t.dot}`} />}
      <span className="whitespace-nowrap">{status}</span>
    </span>
  );
}
