// Shared formatting helpers + raw_json field extractors for O365 UAL events.
// Nothing in here throws: if a payload doesn't match the expected shape the
// extractor returns "" so the UI degrades to a blank cell.

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

// ---- workload -> accent color ---------------------------------------------

export const WORKLOAD_COLORS = {
  AzureActiveDirectory: "#c084fc",
  Exchange:             "#f0883e",
  SharePoint:           "#58a6ff",
  OneDrive:             "#3fb950",
};

export function workloadColor(workload) {
  return WORKLOAD_COLORS[workload] || "#8b949e";
}

// ---- ObjectId / SharePoint helpers ----------------------------------------

// ObjectId typically looks like
//   https://<tenant>.sharepoint.com/sites/<site>/Shared%20Documents/path/file.docx
// Strip query + fragment, take the trailing path segment, and URI-decode.
export function filenameFromObjectId(id) {
  if (!id) return "";
  const clean = String(id).split("?")[0].split("#")[0];
  const parts = clean.split("/");
  const last = parts[parts.length - 1] || clean;
  try {
    return decodeURIComponent(last);
  } catch {
    return last;
  }
}

// Domain of the SharePoint / OneDrive site the event touched.
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

export function emailEventLabel(eventType) {
  if (eventType === "FolderBind") return "Mailbox Access";
  return eventType;
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

// ---- legacy compatibility shim --------------------------------------------
// (nothing new calls these, but keeps imports from older pages compiling
// if anything is still around that imports them)

export const extractDeviceName = deviceName;
export const extractFolder = extractMailFolder;
export const extractObjectId = (raw) => raw?.ObjectId ?? "";
export const emailLabel = emailEventLabel;
