// Deliberately static Tailwind class literals so the JIT content scanner
// can see every variant and not tree-shake them out.
const STYLES = {
  success:  "border-success/40  bg-success/10  text-success",
  critical: "border-critical/40 bg-critical/10 text-critical",
  warning:  "border-warning/40  bg-warning/10  text-warning",
  info:     "border-accent/40   bg-accent/10   text-accent",
  muted:    "border-border      bg-black/30    text-muted",
};

function toneFor(status) {
  if (!status) return "muted";
  const s = String(status).toLowerCase();
  if (s === "succeeded" || s === "success" || s === "partialsuccess") return "success";
  if (s === "failed"    || s === "failure"  || s.includes("error"))    return "critical";
  if (s.includes("warn")) return "warning";
  return "info";
}

export default function StatusBadge({ status, tone }) {
  if (!status && !tone) return null;
  const resolved = tone || toneFor(status);
  const cls = STYLES[resolved] || STYLES.muted;
  return (
    <span
      className={`inline-block px-2 py-0.5 text-[10px] uppercase tracking-[0.15em] border ${cls}`}
    >
      {status}
    </span>
  );
}
