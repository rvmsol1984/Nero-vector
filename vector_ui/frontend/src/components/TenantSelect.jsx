/**
 * TenantSelect — reusable "TENANT" label + native <select> dropdown.
 *
 * Props:
 *   tenants     string[]   ordered list of tenant display names
 *   value       string     currently selected tenant name
 *   onChange    function   called with the new tenant name string
 *   placeholder string?    if provided, renders as first disabled option
 */
export default function TenantSelect({ tenants, value, onChange, placeholder }) {
  return (
    <div className="flex items-center gap-2">
      <span className="text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold">
        Tenant
      </span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="bg-white/5 border border-white/10 rounded-lg px-3 py-1.5 text-[12px] text-white outline-none focus:border-white/30 cursor-pointer"
        style={{ minWidth: "160px", colorScheme: "dark" }}
      >
        {placeholder && (
          <option value="" disabled>
            {placeholder}
          </option>
        )}
        {tenants.map((t) => (
          <option key={t} value={t}>
            {t}
          </option>
        ))}
      </select>
    </div>
  );
}
