// Tiny fetch wrapper used by every page. Same-origin (FastAPI serves the
// SPA bundle too) so no cross-origin concerns - Cloudflare Access gates
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

function path(entityKey) {
  return encodeURIComponent(entityKey);
}

export const api = {
  // ----- global ---------------------------------------------------------
  stats:      () => get("/api/stats"),
  recent:     (limit = 50) => get(`/api/events/recent${qs({ limit })}`),
  events:     ({ limit = 50, offset = 0, tenant, event_type, workload, user } = {}) =>
                get(`/api/events/recent${qs({ limit, offset, tenant, event_type, workload, user })}`),
  eventById:  (id) => get(`/api/events/${encodeURIComponent(id)}`),
  byTenant:   () => get("/api/events/by-tenant"),
  byType:     (limit = 100) => get(`/api/events/by-type${qs({ limit })}`),
  byWorkload: () => get("/api/events/by-workload"),
  users:      (tenant) => get(`/api/events/users${qs({ tenant })}`),

  // ----- per-user detail ------------------------------------------------
  userProfile: (entityKey) => get(`/api/users/${path(entityKey)}`),
  userEvents:  (entityKey, { workloads, event_types, workload, event_type, limit = 100, offset = 0 } = {}) =>
                 get(`/api/users/${path(entityKey)}/events${qs({ workloads, event_types, workload, event_type, limit, offset })}`),
  userStats:   (entityKey) => get(`/api/users/${path(entityKey)}/stats`),

  // ----- governance -----------------------------------------------------
  govDlp:       (tenant) => get(`/api/governance/dlp${qs({ tenant })}`),
  govSharing:   (tenant) => get(`/api/governance/sharing${qs({ tenant })}`),
  govDownloads: (tenant) => get(`/api/governance/downloads${qs({ tenant })}`),
};
