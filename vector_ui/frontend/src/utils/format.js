// Display + raw_json field extraction helpers for the O365 UAL shape.
// Nothing in here throws: if the payload doesn't match the expected
// shape the extractor returns "" so the UI degrades to a blank cell.

// ---- display ---------------------------------------------------------------

export function fmtNumber(n) {
  if (n === null || n === undefined) return "—";
  return Number(n).toLocaleString("en-US");
}

export function fmtTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().replace("T", " ").slice(0, 19) + "Z";
}

export function fmtRelative(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const delta = (Date.now() - d.getTime()) / 1000;
  if (delta < 60)      return `${Math.floor(delta)}s ago`;
  if (delta < 3600)    return `${Math.floor(delta / 60)}m ago`;
  if (delta < 86400)   return `${Math.floor(delta / 3600)}h ago`;
  if (delta < 604800)  return `${Math.floor(delta / 86400)}d ago`;
  return d.toISOString().slice(0, 10);
}

// Build a 2-letter avatar label out of an email address.
//
//   jane.doe@x.com  -> JD
//   admin@x.com     -> AD
//   (falsy)         -> ??
export function initialsFrom(email) {
  if (!email) return "??";
  const name = String(email).split("@")[0];
  const parts = name.split(/[._\-+]/).filter(Boolean);
  if (parts.length >= 2) {
    return (parts[0][0] + parts[1][0]).toUpperCase();
  }
  return name.slice(0, 2).toUpperCase();
}

// ---- workload -> accent color ---------------------------------------------
// These are the FieldDesk-rebuild colors used as the left-border of event
// cards, as the border/fill of EventTypeBadge, and anywhere else we tint by
// workload.

export const WORKLOAD_COLORS = {
  AzureActiveDirectory: "#8B5CF6",
  Exchange:             "#F97316",
  SharePoint:           "#3B82F6",
  OneDrive:             "#22C55E",
  OneDriveForBusiness:  "#22C55E",
};

export function workloadColor(workload) {
  return WORKLOAD_COLORS[workload] || "rgba(255,255,255,0.3)";
}

// ---- ObjectId / SharePoint helpers ----------------------------------------

// Take everything after the last / or \ and URI-decode it.
export function filenameFromObjectId(id) {
  if (!id) return "";
  const clean = String(id).split("?")[0].split("#")[0];
  const parts = clean.split(/[\\/]/);
  const last = parts[parts.length - 1] || clean;
  try {
    return decodeURIComponent(last);
  } catch {
    return last;
  }
}

export function siteDomain(raw) {
  const url = raw?.SiteUrl || raw?.Site;
  if (!url) return "";
  try {
    return new URL(url).hostname;
  } catch {
    return url;
  }
}

// ---- Exchange / mailbox ---------------------------------------------------

export function extractMailFolder(raw) {
  if (!raw) return "";
  const pf = raw.ParentFolder;
  if (pf && typeof pf === "object" && pf.FolderName) return pf.FolderName;
  if (typeof pf === "string") return pf;
  const f = raw.Folder;
  if (f && typeof f === "object" && (f.Path || f.Name)) return f.Path || f.Name;
  if (Array.isArray(raw.Folders) && raw.Folders.length) {
    return raw.Folders[0]?.Path || raw.Folders[0]?.Name || "";
  }
  return "";
}

// ---- Azure AD / login device info -----------------------------------------

export function deviceProp(raw, name) {
  if (!raw || !Array.isArray(raw.DeviceProperties)) return "";
  const match = raw.DeviceProperties.find((p) => p && p.Name === name);
  return match ? match.Value || "" : "";
}

export function deviceName(raw) {
  return deviceProp(raw, "DisplayName") || deviceProp(raw, "Name");
}

export function deviceOs(raw) {
  return deviceProp(raw, "OS");
}

export function parseBrowser(ua) {
  if (!ua) return "";
  if (/Edg\//.test(ua))   return "Edge";
  if (/OPR\//.test(ua))   return "Opera";
  if (/Chrome\//.test(ua) && !/Chromium/.test(ua)) return "Chrome";
  if (/Firefox\//.test(ua)) return "Firefox";
  if (/Safari\//.test(ua) && !/Chrome/.test(ua))   return "Safari";
  return "";
}

export function deviceBrowser(raw, userAgent) {
  const bt = deviceProp(raw, "BrowserType");
  if (bt) return bt;
  return parseBrowser(userAgent);
}
