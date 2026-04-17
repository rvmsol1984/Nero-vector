import Governance from "./Governance.jsx";

// Threat Intelligence board -- OpenCTI IOC matches, Datto EDR
// alerts, and ThreatLocker deny / ringfenced / elevation events.
// The unifying theme is "confirmed malicious activity detected by
// a non-Microsoft security stack", so these three tabs make a
// natural pivot surface for active-incident investigations.
export default function ThreatIntelligence() {
  return (
    <Governance
      pageTitle="Threat Intelligence"
      subtitle="Confirmed malicious activity: OpenCTI indicator matches, Datto EDR alerts, ThreatLocker application-control denies."
      tabIds={["iocMatches", "edrAlerts", "threatLocker"]}
    />
  );
}
