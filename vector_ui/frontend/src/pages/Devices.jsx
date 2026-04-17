import Governance from "./Governance.jsx";

// Devices board -- unmanaged / non-compliant endpoints (UAL-derived)
// and the full Intune fleet (Graph-derived). Kept as a dedicated
// page because device posture investigations rarely cross over
// with identity- or data-level findings.
export default function Devices() {
  return (
    <Governance
      pageTitle="Devices"
      subtitle="Endpoint posture: unmanaged devices in UAL and the full Intune-enrolled fleet with compliance / encryption / stale-sync flags."
      tabIds={["unmanagedDevices", "intuneDevices"]}
    />
  );
}
