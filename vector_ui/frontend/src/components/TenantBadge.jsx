import { tenantColor } from "../utils/tenantColor.js";
// Compact pill showing a tenant's brand color + name.
export default function TenantBadge({ name }) {
  if (!name) return <span className="text-white/40">—</span>;
  const color = tenantColor(name);
  return (
    <span
      style={{
        color,
        borderColor: `${color}55`,
        backgroundColor: `${color}14`,
        border: "1px solid",
        display: "inline-flex",
        alignItems: "center",
        padding: "2px 7px",
        borderRadius: "999px",
        fontSize: "10px",
        fontWeight: 500,
        whiteSpace: "nowrap",
      }}
    >
      {name}
    </span>
  );
}
