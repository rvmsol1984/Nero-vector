import Governance from "./Governance.jsx";

// Data & Sharing board -- DLP risk, external sharing events, bulk
// download bursts, and broken-inheritance findings. All four tabs
// live inside SharePoint / OneDrive workloads so operators working
// on data exfiltration investigations have one place to pivot.
export default function DataSharing() {
  return (
    <Governance
      pageTitle="Data & Sharing"
      subtitle="SharePoint and OneDrive content signals: DLP events, link sharing, bulk downloads, permission drift."
      tabIds={["dlp", "sharing", "downloads", "brokenInheritance"]}
    />
  );
}
