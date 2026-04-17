import Governance from "./Governance.jsx";

// Identity & Access board -- reuses the Governance engine with a
// curated subset of tabs focused on who can sign in and what
// privileges they hold. Every tab queries the same APIs as the
// unified Governance page, just filtered by the top-level route.
export default function IdentityAccess() {
  return (
    <Governance
      pageTitle="Identity & Access"
      subtitle="Account-level signals: sign-in anomalies, MFA posture, role escalations, and identity hygiene."
      tabIds={[
        "passwordSpray",
        "mfaChanges",
        "privilegedRoles",
        "staleAccounts",
        "oauthApps",
        "guestUsers",
      ]}
    />
  );
}
