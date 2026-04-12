import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../api.js";
import { filenameFromObjectId, fmtNumber, fmtTime } from "../utils/format.js";

const TENANT = "GameChange Solar";

export default function Governance() {
  const [dlp, setDlp] = useState([]);
  const [sharing, setSharing] = useState([]);
  const [downloads, setDownloads] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    setErr(null);
    Promise.all([
      api.govDlp(TENANT),
      api.govExternalSharing(TENANT),
      api.govBulkDownloads(TENANT),
    ])
      .then(([d, s, b]) => {
        if (cancel) return;
        setDlp(d || []);
        setSharing(s || []);
        setDownloads(b || []);
      })
      .catch((e) => {
        if (!cancel) setErr(e.message);
      })
      .finally(() => {
        if (!cancel) setLoading(false);
      });
    return () => {
      cancel = true;
    };
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="font-display text-2xl tracking-[0.2em]">
          GOVERNANCE — GAMECHANGE SOLAR
        </h1>
        <p className="text-muted text-xs mt-1">
          UAL-derived policy findings. Review queue for the operator.
        </p>
      </div>

      {err && (
        <div className="border border-critical/40 bg-critical/10 text-critical text-xs px-3 py-2">
          load error: {err}
        </div>
      )}

      <GovSection
        title="DLP Risk"
        subtitle="Users who copied files onto removable media"
        count={dlp.length}
        loading={loading}
        empty="no removable-media activity detected"
      >
        <table className="min-w-full text-[11px]">
          <thead className="text-muted uppercase text-[10px] tracking-[0.2em]">
            <tr>
              <th className="text-left px-3 py-2">User</th>
              <th className="text-right px-3 py-2">Events</th>
              <th className="text-left px-3 py-2">Last Seen</th>
              <th className="text-left px-3 py-2">Files Copied</th>
              <th className="text-left px-3 py-2">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {dlp.map((row) => (
              <tr key={row.entity_key} className="hover:bg-white/[0.03]">
                <td className="px-3 py-1.5 truncate max-w-[280px]">
                  <Link
                    to={`/users/${encodeURIComponent(row.entity_key)}?tab=files`}
                    className="hover:text-accent"
                  >
                    {row.user_id}
                  </Link>
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums">
                  {fmtNumber(row.event_count)}
                </td>
                <td className="px-3 py-1.5 text-muted whitespace-nowrap">
                  {fmtTime(row.last_seen)}
                </td>
                <td className="px-3 py-1.5 text-muted">
                  <FileList ids={row.object_ids} />
                </td>
                <td className="px-3 py-1.5">
                  <ReviewBadge />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </GovSection>

      <GovSection
        title="External Sharing"
        subtitle="Anonymous or generated share links used"
        count={sharing.length}
        loading={loading}
        empty="no external link usage detected"
      >
        <table className="min-w-full text-[11px]">
          <thead className="text-muted uppercase text-[10px] tracking-[0.2em]">
            <tr>
              <th className="text-left px-3 py-2">User</th>
              <th className="text-right px-3 py-2">Events</th>
              <th className="text-left px-3 py-2">Last Seen</th>
              <th className="text-left px-3 py-2">Event Type</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {sharing.map((row) => (
              <tr key={row.entity_key} className="hover:bg-white/[0.03]">
                <td className="px-3 py-1.5 truncate max-w-[320px]">
                  <Link
                    to={`/users/${encodeURIComponent(row.entity_key)}?tab=files`}
                    className="hover:text-accent"
                  >
                    {row.user_id}
                  </Link>
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums">
                  {fmtNumber(row.event_count)}
                </td>
                <td className="px-3 py-1.5 text-muted whitespace-nowrap">
                  {fmtTime(row.last_seen)}
                </td>
                <td className="px-3 py-1.5 text-muted">{row.top_event_type}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </GovSection>

      <GovSection
        title="Bulk Downloads"
        subtitle="FileDownloadedFromBrowser > 10 in the last 24h"
        count={downloads.length}
        loading={loading}
        empty="no bulk download activity in the last 24h"
      >
        <table className="min-w-full text-[11px]">
          <thead className="text-muted uppercase text-[10px] tracking-[0.2em]">
            <tr>
              <th className="text-left px-3 py-2">User</th>
              <th className="text-right px-3 py-2">Downloads</th>
              <th className="text-left px-3 py-2">Last Seen</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {downloads.map((row) => (
              <tr key={row.entity_key} className="hover:bg-white/[0.03]">
                <td className="px-3 py-1.5 truncate max-w-[320px]">
                  <Link
                    to={`/users/${encodeURIComponent(row.entity_key)}?tab=files`}
                    className="hover:text-accent"
                  >
                    {row.user_id}
                  </Link>
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums">
                  {fmtNumber(row.download_count)}
                </td>
                <td className="px-3 py-1.5 text-muted whitespace-nowrap">
                  {fmtTime(row.last_seen)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </GovSection>
    </div>
  );
}

// ---------------------------------------------------------------------------

function GovSection({ title, subtitle, count, loading, empty, children }) {
  return (
    <div className="bg-surface border border-border">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <div>
          <div className="font-display text-sm tracking-[0.2em]">{title.toUpperCase()}</div>
          <div className="text-[10px] text-muted mt-1 uppercase tracking-[0.15em]">
            {subtitle}
          </div>
        </div>
        <span
          className="inline-block px-2 py-0.5 text-[10px] uppercase tracking-[0.2em] border border-accent/30 bg-accent/10 text-accent tabular-nums"
        >
          {count} {count === 1 ? "finding" : "findings"}
        </span>
      </div>
      {loading ? (
        <div className="px-4 py-6 text-muted text-xs text-center">loading…</div>
      ) : count === 0 ? (
        <div className="px-4 py-6 text-muted text-xs text-center">{empty}</div>
      ) : (
        <div className="overflow-x-auto">{children}</div>
      )}
    </div>
  );
}

function ReviewBadge() {
  return (
    <span
      className="inline-block px-2 py-0.5 text-[10px] uppercase tracking-[0.15em] border whitespace-nowrap"
      style={{
        color: "#d29922",
        borderColor: "#d2992255",
        backgroundColor: "#d2992214",
      }}
    >
      Review Required
    </span>
  );
}

function FileList({ ids }) {
  if (!ids || ids.length === 0) {
    return <span className="text-muted">—</span>;
  }
  const first = ids.slice(0, 3).map(filenameFromObjectId).filter(Boolean);
  const extra = ids.length - first.length;
  const joined = first.join(", ");
  return (
    <span className="truncate inline-block max-w-[440px] align-bottom" title={ids.join("\n")}>
      {joined}
      {extra > 0 && (
        <span className="text-muted"> · +{extra} more</span>
      )}
    </span>
  );
}
