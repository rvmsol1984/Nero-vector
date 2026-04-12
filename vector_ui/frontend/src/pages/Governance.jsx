import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "../components/Avatar.jsx";
import TenantBadge from "../components/TenantBadge.jsx";
import { api } from "../api.js";
import { filenameFromObjectId, fmtNumber, fmtRelative } from "../utils/format.js";

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
      api.govSharing(TENANT),
      api.govDownloads(TENANT),
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
    <div className="space-y-5 animate-fade-in">
      <div className="flex items-center gap-3 flex-wrap">
        <h1 className="text-2xl font-bold">Governance</h1>
        <TenantBadge name={TENANT} />
      </div>
      <p className="text-white/50 text-sm -mt-3">
        UAL-derived policy findings. Review queue for the operator.
      </p>

      {err && (
        <div className="card border-critical/30 text-critical text-sm px-4 py-3">
          load error: {err}
        </div>
      )}

      {/* ----- DLP Risk ----- */}
      <Section
        title="DLP Risk"
        subtitle="Users who copied files onto removable media"
        count={dlp.length}
        loading={loading}
        empty="no removable-media activity detected"
      >
        {dlp.map((row) => (
          <GovRow
            key={row.entity_key}
            row={row}
            rightPill={<ReviewPill />}
          >
            <div className="text-[11px] text-white/50 mt-0.5 flex items-center gap-3 flex-wrap">
              <span className="tabular-nums">
                {fmtNumber(row.event_count)} events
              </span>
              <span className="opacity-60">·</span>
              <span>last seen {fmtRelative(row.last_seen)}</span>
              <FileList ids={row.files} />
            </div>
          </GovRow>
        ))}
      </Section>

      {/* ----- External Sharing ----- */}
      <Section
        title="External Sharing"
        subtitle="Anonymous or generated share links used"
        count={sharing.length}
        loading={loading}
        empty="no external link usage detected"
      >
        {sharing.map((row) => (
          <GovRow
            key={`${row.entity_key}-${row.event_type}`}
            row={row}
            rightPill={<MonitorPill />}
          >
            <div className="text-[11px] text-white/50 mt-0.5 flex items-center gap-3 flex-wrap">
              <span className="tabular-nums">
                {fmtNumber(row.event_count)} events
              </span>
              <span className="opacity-60">·</span>
              <span>{row.event_type}</span>
              <span className="opacity-60">·</span>
              <span>last seen {fmtRelative(row.last_seen)}</span>
            </div>
          </GovRow>
        ))}
      </Section>

      {/* ----- Bulk Downloads ----- */}
      <Section
        title="Bulk Downloads"
        subtitle="FileDownloadedFromBrowser > 5 in the last 24h"
        count={downloads.length}
        loading={loading}
        empty="no bulk download activity in the last 24h"
      >
        {downloads.map((row) => (
          <GovRow
            key={row.entity_key}
            row={{ ...row, event_count: row.download_count }}
            rightPill={<MonitorPill />}
          >
            <div className="text-[11px] text-white/50 mt-0.5 flex items-center gap-3 flex-wrap">
              <span className="tabular-nums">
                {fmtNumber(row.download_count)} downloads
              </span>
              <span className="opacity-60">·</span>
              <span>last seen {fmtRelative(row.last_seen)}</span>
            </div>
          </GovRow>
        ))}
      </Section>
    </div>
  );
}

// ---------------------------------------------------------------------------

function Section({ title, subtitle, count, loading, empty, children }) {
  const isEmpty = !loading && count === 0;
  return (
    <div className="card overflow-hidden">
      <div className="px-5 py-4 border-b border-white/5 flex items-center justify-between gap-4">
        <div>
          <div className="text-base font-bold">{title}</div>
          <div className="text-[11px] text-white/50 mt-0.5">{subtitle}</div>
        </div>
        <span className="inline-flex items-center px-2.5 py-1 rounded-full text-[10px] uppercase tracking-wider font-semibold bg-primary/15 border border-primary/40 text-primary-light tabular-nums">
          {count} {count === 1 ? "finding" : "findings"}
        </span>
      </div>
      {loading ? (
        <div className="px-5 py-8 text-white/40 text-sm text-center">loading…</div>
      ) : isEmpty ? (
        <div className="px-5 py-8 text-white/40 text-sm text-center">{empty}</div>
      ) : (
        <div className="divide-y divide-white/5">{children}</div>
      )}
    </div>
  );
}

function GovRow({ row, rightPill, children }) {
  return (
    <Link
      to={`/users/${encodeURIComponent(row.entity_key)}`}
      className="flex items-center gap-3 px-5 py-4 hover:bg-white/[0.03] active:scale-[0.997] transition-all"
    >
      <Avatar email={row.user_id} tenant={row.client_name} size={36} />
      <div className="flex-1 min-w-0">
        <div className="font-medium text-sm truncate">{row.user_id}</div>
        {children}
      </div>
      {rightPill}
    </Link>
  );
}

function ReviewPill() {
  return (
    <span
      className="inline-flex items-center px-2.5 py-1 rounded-full text-[10px] font-semibold uppercase tracking-wider border whitespace-nowrap"
      style={{
        color: "#F97316",
        borderColor: "#F9731655",
        backgroundColor: "#F9731615",
      }}
    >
      Review Required
    </span>
  );
}

function MonitorPill() {
  return (
    <span
      className="inline-flex items-center px-2.5 py-1 rounded-full text-[10px] font-semibold uppercase tracking-wider border whitespace-nowrap"
      style={{
        color: "#EAB308",
        borderColor: "#EAB30855",
        backgroundColor: "#EAB30815",
      }}
    >
      Monitor
    </span>
  );
}

function FileList({ ids }) {
  if (!ids || ids.length === 0) return null;
  const first = ids.slice(0, 2).map(filenameFromObjectId).filter(Boolean);
  const extra = ids.length - first.length;
  return (
    <>
      <span className="opacity-60">·</span>
      <span className="truncate max-w-[340px]" title={ids.join("\n")}>
        {first.join(", ")}
        {extra > 0 && <span className="text-white/30"> · +{extra}</span>}
      </span>
    </>
  );
}
