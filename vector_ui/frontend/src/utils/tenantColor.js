// Tenant color map. GCS is the blue brand, NERO is the green brand;
// any other tenant hashes deterministically onto a small accent cycle
// so new clients never repaint the existing ones.

const FIXED = {
  "GameChange Solar": "#2563EB",
  "NERO":             "#10B981",
};

const CYCLE = [
  "#8B5CF6",
  "#F97316",
  "#EAB308",
  "#EF4444",
  "#3B82F6",
  "#22C55E",
];

export function tenantColor(name) {
  if (!name) return "rgba(255,255,255,0.4)";
  if (FIXED[name]) return FIXED[name];
  let h = 0;
  for (let i = 0; i < name.length; i += 1) {
    h = (h * 31 + name.charCodeAt(i)) >>> 0;
  }
  return CYCLE[h % CYCLE.length];
}
