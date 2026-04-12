import { tenantColor } from "../utils/tenantColor.js";

export default function TenantBadge({ name, dim = false }) {
  if (!name) return <span className="text-muted">—</span>;
  const color = tenantColor(name);
  return (
    <span
      className="inline-flex items-center gap-1.5 whitespace-nowrap"
      style={{ color, opacity: dim ? 0.8 : 1 }}
    >
      <span
        className="h-1.5 w-1.5 rounded-full"
        style={{ backgroundColor: color }}
      />
      <span className="font-medium">{name}</span>
    </span>
  );
}
