import { workloadColor } from "../utils/format.js";

// Workload-tinted event-type badge. Inline hex styles because the four
// workload colors aren't in the Tailwind theme.

export default function EventTypeBadge({ type, workload }) {
  if (!type) return null;
  const color = workloadColor(workload);
  return (
    <span
      className="inline-flex items-center px-2 py-[3px] text-[10px] font-semibold uppercase tracking-wide rounded-md border whitespace-nowrap"
      style={{
        color,
        borderColor: `${color}55`,
        backgroundColor: `${color}1a`,
      }}
    >
      {type}
    </span>
  );
}
