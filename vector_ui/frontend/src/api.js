// Tiny fetch wrapper used by every page. All endpoints are same-origin
// (the FastAPI backend serves the SPA bundle too), so there are no
// cross-origin concerns and no auth header - Cloudflare Access gates
// the whole app upstream.

async function get(path) {
  const res = await fetch(path, {
    credentials: "same-origin",
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    throw new Error(`${path} -> ${res.status} ${res.statusText}`);
  }
  return res.json();
}

function qs(params) {
  const sp = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") sp.set(k, v);
  });
  const s = sp.toString();
  return s ? `?${s}` : "";
}

export const api = {
  stats: () => get("/api/stats"),

  recent: (limit = 50) => get(`/api/events/recent${qs({ limit })}`),

  events: ({ limit = 50, offset = 0, tenant, event_type } = {}) =>
    get(`/api/events/recent${qs({ limit, offset, tenant, event_type })}`),

  byTenant: () => get("/api/events/by-tenant"),

  byType: (limit = 20) => get(`/api/events/by-type${qs({ limit })}`),

  users: (tenant) => get(`/api/events/users${qs({ tenant })}`),
};
