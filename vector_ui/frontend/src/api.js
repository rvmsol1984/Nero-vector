// Tiny fetch wrapper used by every page. Every /api/* call carries an
// `Authorization: Bearer <jwt>` header pulled out of localStorage via
// getToken(). A 401 anywhere wipes the token and hard-redirects the
// user back through the PKCE flow.

import { clearToken, getToken, redirectToLogin } from "./auth.jsx";

async function get(path) {
  const token = getToken();
  const res = await fetch(path, {
    credentials: "same-origin",
    headers: {
      Accept: "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });
  if (res.status === 401) {
    clearToken();
    redirectToLogin();
    throw new Error("unauthorized");
  }
  if (!res.ok) {
    throw new Error(`${path} -> ${res.status} ${res.statusText}`);
  }
  return res.json();
}

async function post(path, body) {
  const token = getToken();
  const res = await fetch(path, {
    method: "POST",
    credentials: "same-origin",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
  });
  if (res.status === 401) {
    clearToken();
    redirectToLogin();
    throw new Error("unauthorized");
  }
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
  events:     ({ limit = 50, offset = 0, tenant, event_type, workload, user, ip, source } = {}) =>
                get(`/api/events/recent${qs({ limit, offset, tenant, event_type, workload, user, ip, source })}`),
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
  userEmails:  (entityKey, { direction, search, limit = 50, offset = 0 } = {}) =>
                 get(`/api/users/${path(entityKey)}/emails${qs({ direction, search, limit, offset })}`),
  userEdr:         (entityKey) => get(`/api/users/${path(entityKey)}/edr`),
  userIoc:         (entityKey) => get(`/api/users/${path(entityKey)}/ioc`),
  userThreatLocker: (entityKey) => get(`/api/users/${path(entityKey)}/threatlocker`),

  // ----- IOC enrichment (OpenCTI) ---------------------------------------
  iocMatches:        (limit = 50) => get(`/api/ioc/matches${qs({ limit })}`),
  iocMatchesByValue: (value) => get(`/api/ioc/matches/${encodeURIComponent(value)}`),

  // ----- unified feed + watchlist ---------------------------------------
  feedRecent:  (limit = 25) => get(`/api/feed/recent${qs({ limit })}`),
  dashboardFeed: ({ ual_limit = 50, inky_limit = 20, edr_limit = 20 } = {}) =>
    get(`/api/dashboard/feed${qs({ ual_limit, inky_limit, edr_limit })}`),
  inkyCount:   () => get("/api/sources/inky-count"),
  edrCount:    () => get("/api/sources/edr-count"),
  watchlist:   (status) => get(`/api/watchlist${qs({ status })}`),

  // ----- governance -----------------------------------------------------
  govDlp:       (tenant) => get(`/api/governance/dlp${qs({ tenant })}`),
  govSharing:   (tenant) => get(`/api/governance/sharing${qs({ tenant })}`),
  govDownloads: (tenant) => get(`/api/governance/downloads${qs({ tenant })}`),

  // ----- extended GCS governance (tenant hard-coded server-side) --------
  govExternalForwarding: () => get("/api/governance/external-forwarding"),
  govUnmanagedDevices:   () => get("/api/governance/unmanaged-devices"),
  govUnmanagedDevicesDetail:
    (userId) =>
      get(`/api/governance/unmanaged-devices/${encodeURIComponent(userId)}/devices`),
  govIntuneDevices:      () => get("/api/governance/intune-devices"),
  govIntuneDevicesUser:  (upn) =>
    get(`/api/governance/intune-devices/${encodeURIComponent(upn)}`),
  govAiActivity:         () => get("/api/governance/ai-activity"),
  govAiActivityRaw:      () => get("/api/governance/ai-activity/external-raw"),
  govBrokenInheritance:  () => get("/api/governance/broken-inheritance"),
  govOauthApps:          () => get("/api/governance/oauth-apps"),
  govPasswordSpray:      () => get("/api/governance/password-spray"),
  govStaleAccounts:      () => get("/api/governance/stale-accounts"),
  govMfaChanges:         () => get("/api/governance/mfa-changes"),
  govPrivilegedRoles:    () => get("/api/governance/privileged-roles"),
  govGuestUsers:         () => get("/api/governance/guest-users"),
  govEdrAlerts:          () => get("/api/governance/edr-alerts"),
  govThreatLocker:       () => get("/api/governance/threatlocker"),
  govIocMatches:         () => get("/api/ioc/matches?limit=200"),

  // ----- incidents -----------------------------------------------------
  claudeConnector:    () => get("/api/governance/claude-connector"),
  govClaudeConnector: () => get("/api/governance/claude-connector"),
  incidentStats:    () => get("/api/incidents/stats"),
  incidentList:     ({ limit = 50, status = "" } = {}) =>
                      get(`/api/incidents/list${qs({ limit, status })}`),
  incidents:        ({ limit = 50, status = "" } = {}) =>
                      get(`/api/incidents/list${qs({ limit, status })}`),
  updateIncidentStatus: (id, status) => post(`/api/incidents/${id}/status`, { status }),
  incidentDetail:   (id) => get(`/api/incidents/${id}`),
  incidentUpdate:   (id, data) => post(`/api/incidents/${id}`, data),

  // ----- baseline ------------------------------------------------------
  baselineStats:    () => get("/api/baseline/stats"),
  baselineList:     ({ limit = 100, search = "" } = {}) =>
                      get(`/api/baseline/list${qs({ limit, search })}`),
  baselineDetail:   (entityKey) => get(`/api/baseline/${path(entityKey)}`),
};
