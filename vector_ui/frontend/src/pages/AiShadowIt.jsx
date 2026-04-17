import Governance from "./Governance.jsx";

// AI & Shadow IT board -- a single tab today (AI Activity) but
// housed on its own page so the Microsoft Copilot / External AI
// Tools / Claude Connector sub-tab strip has room to breathe
// without competing with the 15+ tabs on the unified Governance
// board. Future shadow-SaaS detections will land here too.
export default function AiShadowIt() {
  return (
    <Governance
      pageTitle="AI & Shadow IT"
      subtitle="Generative-AI usage telemetry: Microsoft Copilot workload activity, external AI domain hits from managed devices, and the Claude Connector audit stream."
      tabIds={["aiActivity"]}
    />
  );
}
