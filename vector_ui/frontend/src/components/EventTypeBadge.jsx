import { workloadColor } from "../utils/format.js";

// Event-type badge, tinted by the workload that produced the event.
// Uses inline styles (hex + hex alpha) because Tailwind can't JIT a
// palette of 4 colors across border/background/text without a theme
// extension, and the workload->color mapping is a product decision.

export default function EventTypeBadge({ type, workload, compact = false }) {
  if (!type) return null;
  const color = workloadColor(workload);
  return (
    <span
      className={`inline-block uppercase tracking-[0.15em] border whitespace-nowrap ${
        compact ? "px-1.5 py-0 text-[9px]" : "px-2 py-0.5 text-[10px]"
      }`}
      style={{
        color,
        borderColor: `${color}55`,
        backgroundColor: `${color}14`,
      }}
    >
      {type}
    </span>
  );
}
