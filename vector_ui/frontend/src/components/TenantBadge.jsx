import { tenantColor } from "../utils/tenantColor.js";

// Compact pill showing a tenant's brand color + name.
export default function TenantBadge({ name }) {
  if (!name) return <span className="text-white/40">—</span>;
  const color = tenantColor(name);
  return (
    <span
      className="pill whitespace-nowrap"
      style={{
        color,
        borderColor: `${color}55`,
        backgroundColor: `${color}14`,
        border: "1px solid",
      }}
    >
      <span
        className="h-1.5 w-1.5 rounded-full"
        style={{ backgroundColor: color }}
      />
      <span className="font-medium">{name}</span>
    </span>
  );
}
