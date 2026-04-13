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
  userEmails:  (entityKey, { direction, search, limit = 50, offset = 0 } = {}) =>
                 get(`/api/users/${path(entityKey)}/emails${qs({ direction, search, limit, offset })}`),

  // ----- unified feed + watchlist ---------------------------------------
  feedRecent:  (limit = 25) => get(`/api/feed/recent${qs({ limit })}`),
  watchlist:   (status) => get(`/api/watchlist${qs({ status })}`),

  // ----- governance -----------------------------------------------------
  govDlp:       (tenant) => get(`/api/governance/dlp${qs({ tenant })}`),
  govSharing:   (tenant) => get(`/api/governance/sharing${qs({ tenant })}`),
  govDownloads: (tenant) => get(`/api/governance/downloads${qs({ tenant })}`),

  // ----- extended GCS governance (tenant hard-coded server-side) --------
  govExternalForwarding: () => get("/api/governance/external-forwarding"),
  govUnmanagedDevices:   () => get("/api/governance/unmanaged-devices"),
  govBrokenInheritance:  () => get("/api/governance/broken-inheritance"),
  govOauthApps:          () => get("/api/governance/oauth-apps"),
  govPasswordSpray:      () => get("/api/governance/password-spray"),
  govStaleAccounts:      () => get("/api/governance/stale-accounts"),
  govMfaChanges:         () => get("/api/governance/mfa-changes"),
  govPrivilegedRoles:    () => get("/api/governance/privileged-roles"),
  govGuestUsers:         () => get("/api/governance/guest-users"),
};
