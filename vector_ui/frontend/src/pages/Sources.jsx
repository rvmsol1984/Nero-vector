// Hardcoded source catalogue for the v0.1 build. Each entry drives a card
// with a status ring + label + description. Once connectors expose real
// health checks this should move onto /api/sources.

const SOURCES = [
  {
    name: "Microsoft UAL",
    status: "live",
    label: "LIVE",
    description:
      "Office 365 Unified Audit Log across Azure AD, Exchange, SharePoint and OneDrive for every managed tenant.",
  },
  {
    name: "SaaS Alerts",
    status: "planned",
    label: "PLANNED",
    description:
      "Identity anomaly alerts and Fortify enforcement actions. Integration planned for Phase 2.",
  },
  {
    name: "INKY MailShield",
    status: "pending",
    label: "PENDING",
    description:
      "Inbound email threat verdicts and AiTM phishing detection. Webhook integration pending — contact Dave Baggett.",
  },
  {
    name: "Datto EDR",
    status: "planned",
    label: "PLANNED",
    description:
      "Endpoint behavioral telemetry and threat detections. REST API integration planned for Phase 2.",
  },
  {
    name: "ThreatLocker",
    status: "planned",
    label: "PLANNED",
    description:
      "Application allowlisting and ringfencing telemetry. Blocked execution events and policy violations feed into Vector for endpoint correlation.",
  },
  {
    name: "FeedLattice / OpenCTI",
    status: "planned",
    label: "PLANNED",
    description:
      "IOC enrichment and threat intelligence correlation. Internal platform available — ingestion pipeline planned.",
  },
  {
    name: "Defender ATP",
    status: "e5",
    label: "E5 TENANTS ONLY",
    description:
      "Advanced Hunting email and endpoint stream. Available on GameChange Solar (E5). Integration planned.",
  },
  {
    name: "KSIEM",
    status: "passthrough",
    label: "PASSTHROUGH",
    description:
      "Legacy Kaseya SIEM. Events mirrored without enrichment for continuity.",
  },
];

const COLOR = {
  live:        "#10B981",
  pending:     "#EAB308",
  e5:          "#3B82F6",
  planned:     "rgba(255,255,255,0.35)",
  passthrough: "rgba(255,255,255,0.35)",
};

function StatusRing({ color }) {
  const size = 56;
  const stroke = 5;
  const r = (size - stroke) / 2;
  const c = size / 2;
  const circ = 2 * Math.PI * r;
  return (
    <div className="relative shrink-0" style={{ width: size, height: size }}>
      <svg width={size} height={size}>
        <circle
          cx={c}
          cy={c}
          r={r}
          fill="none"
          stroke="rgba(255,255,255,0.08)"
          strokeWidth={stroke}
        />
        <circle
          cx={c}
          cy={c}
          r={r}
          fill="none"
          stroke={color}
          strokeWidth={stroke}
          strokeLinecap="round"
          strokeDasharray={`${circ} ${circ}`}
          transform={`rotate(-90 ${c} ${c})`}
        />
      </svg>
      <div
        className="absolute inset-0 flex items-center justify-center"
        style={{ color }}
      >
        <span className="h-2 w-2 rounded-full" style={{ background: color }} />
      </div>
    </div>
  );
}

export default function Sources() {
  return (
    <div className="space-y-5 animate-fade-in">
      <div>
        <h1 className="text-2xl font-bold">Sources</h1>
        <p className="text-white/50 text-sm mt-1">
          Telemetry connectors wired into the correlation graph.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {SOURCES.map((s) => {
          const color = COLOR[s.status] || COLOR.passthrough;
          return (
            <div key={s.name} className="card p-5 flex items-start gap-4">
              <StatusRing color={color} />
              <div className="min-w-0">
                <div className="font-semibold text-base">{s.name}</div>
                <div
                  className="text-[10px] font-semibold uppercase tracking-wider mt-1"
                  style={{ color }}
                >
                  {s.label}
                </div>
                <div className="text-sm text-white/60 mt-3 leading-relaxed">
                  {s.description}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
