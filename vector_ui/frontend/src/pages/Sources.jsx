// Hardcoded telemetry source catalogue for the MVP. Status values are
// driven statically here because only the UAL connector actually exists
// right now; once other integrations ship they should move onto a real
// /api/sources endpoint fed by per-connector health checks.

const SOURCES = [
  {
    key: "UAL",
    name: "Microsoft UAL",
    status: "online",
    desc: "Office 365 Unified Audit Log across all managed tenants (Azure AD, Exchange, SharePoint, General).",
    detail: "vector-ingest · poll 5m",
  },
  {
    key: "SAAS",
    name: "SaaS Alerts",
    status: "planned",
    desc: "SaaS posture and alert ingestion for O365, Google Workspace, Dropbox, and friends.",
    detail: "integration not yet wired",
  },
  {
    key: "INKY",
    name: "INKY",
    status: "planned",
    desc: "Email threat intel and phishing verdicts from INKY's mail-flow inspection.",
    detail: "integration not yet wired",
  },
  {
    key: "DATTO",
    name: "Datto EDR",
    status: "planned",
    desc: "Endpoint detections and response actions from managed Datto EDR fleet.",
    detail: "integration not yet wired",
  },
  {
    key: "FEED",
    name: "FeedLattice",
    status: "planned",
    desc: "External threat intelligence feed normalizer feeding indicator enrichment.",
    detail: "integration not yet wired",
  },
];

const statusStyle = {
  online:   { label: "online",   cls: "text-success", dot: "bg-success" },
  degraded: { label: "degraded", cls: "text-warning", dot: "bg-warning" },
  offline:  { label: "offline",  cls: "text-critical", dot: "bg-critical" },
  planned:  { label: "planned",  cls: "text-muted",   dot: "bg-muted"   },
};

export default function Sources() {
  return (
    <div className="space-y-4">
      <div>
        <h1 className="font-display text-2xl tracking-[0.2em]">SOURCES</h1>
        <p className="text-muted text-xs mt-1">
          Telemetry connectors wired into the correlation graph.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {SOURCES.map((s) => {
          const st = statusStyle[s.status] ?? statusStyle.planned;
          return (
            <div key={s.key} className="bg-surface border border-border p-4">
              <div className="flex items-center justify-between">
                <div className="font-display text-sm tracking-[0.2em]">{s.name}</div>
                <div
                  className={`flex items-center gap-2 text-[10px] uppercase tracking-[0.25em] ${st.cls}`}
                >
                  <span className={`h-1.5 w-1.5 rounded-full ${st.dot}`} />
                  {st.label}
                </div>
              </div>
              <div className="text-xs text-muted mt-3 leading-relaxed">{s.desc}</div>
              <div className="text-[10px] text-muted mt-4 border-t border-border pt-2 uppercase tracking-[0.2em]">
                {s.detail}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
