import { initialsFrom } from "../utils/format.js";
import { tenantColor } from "../utils/tenantColor.js";

// Colored circle with 2-letter initials. Size defaults to 36 but the user
// detail header uses 64 and tiny inline spots use 28.

export default function Avatar({ email, tenant, size = 36 }) {
  const color = tenantColor(tenant);
  return (
    <div
      className="rounded-full flex items-center justify-center font-semibold text-white shrink-0 select-none"
      style={{
        width: size,
        height: size,
        background: color,
        fontSize: Math.round(size * 0.34),
        boxShadow: `0 0 0 1px ${color}66 inset`,
      }}
      aria-label={email}
    >
      {initialsFrom(email)}
    </div>
  );
}
