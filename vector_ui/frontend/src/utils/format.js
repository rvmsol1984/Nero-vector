// Shared formatting helpers.

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

export function fmtDate(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().slice(0, 10);
}

// ---- raw_json field extractors ---------------------------------------------
// These know about the shapes of O365 UAL events. None of them throw: every
// helper falls back to "" / null if the event payload doesn't match.

export function extractObjectId(raw) {
  if (!raw) return "";
  return raw.ObjectId || raw.SourceFileName || raw.SourceRelativeUrl || "";
}

export function extractFolder(raw) {
  if (!raw) return "";
  const fi = raw.Folder || raw.Folders || raw.FolderInfo;
  if (!fi) return raw.MailboxOwnerUPN || "";
  if (typeof fi === "string") return fi;
  if (Array.isArray(fi) && fi.length) return fi[0]?.Path || fi[0]?.Name || "";
  return fi.Path || fi.Name || "";
}

export function extractDeviceName(raw) {
  if (!raw || !Array.isArray(raw.DeviceProperties)) return "";
  const hit = raw.DeviceProperties.find((p) => p && p.Name === "DisplayName");
  return hit ? hit.Value || "" : "";
}

export function emailLabel(eventType) {
  if (eventType === "FolderBind") return "Mailbox Access";
  return eventType;
}
