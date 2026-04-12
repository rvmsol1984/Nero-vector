// Tenant color map.
//
// Two MSP tenants are fixed in the brand so the operator builds muscle
// memory. Any other tenant hashes deterministically onto a small cycle
// of accent tones so adding a client never repaints existing ones.

const FIXED = {
  "GameChange Solar": "#58a6ff",
  "NERO":             "#3fb950",
};

const CYCLE = [
  "#d29922", // warning
  "#f778ba", // pink
  "#a371f7", // purple
  "#79c0ff", // sky
  "#ff7b72", // coral
  "#56d364", // lime
];

export function tenantColor(name) {
  if (!name) return "#8b949e";
  if (FIXED[name]) return FIXED[name];
  let h = 0;
  for (let i = 0; i < name.length; i += 1) {
    h = (h * 31 + name.charCodeAt(i)) >>> 0;
  }
  return CYCLE[h % CYCLE.length];
}
