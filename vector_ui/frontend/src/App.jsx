import { Navigate, Route, Routes } from "react-router-dom";

import { AuthProvider } from "./auth.jsx";
import Layout from "./components/Layout.jsx";
import AiShadowIt from "./pages/AiShadowIt.jsx";
import Baseline from "./pages/Baseline.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import Exceptions from "./pages/Exceptions.jsx";
import DataSharing from "./pages/DataSharing.jsx";
import Devices from "./pages/Devices.jsx";
import Events from "./pages/Events.jsx";
import Governance from "./pages/Governance.jsx";
import IdentityAccess from "./pages/IdentityAccess.jsx";
import Incidents from "./pages/Incidents.jsx";
import Sources from "./pages/Sources.jsx";
import ThreatIntelligence from "./pages/ThreatIntelligence.jsx";
import Users from "./pages/Users.jsx";
import UserDetail from "./pages/UserDetail.jsx";
import Watchlist from "./pages/Watchlist.jsx";
import MailboxRules from "./pages/MailboxRules.jsx";
import MfaStatus from "./pages/MfaStatus.jsx";
import SecurityFindings from "./pages/SecurityFindings.jsx";

export default function App() {
  return (
    <AuthProvider>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard"        element={<Dashboard />} />
          <Route path="/incidents" element={<Incidents />} />
          <Route path="/watchlist" element={<Watchlist />} />
          <Route path="/events"           element={<Events />} />
          <Route path="/users"            element={<Users />} />
          <Route path="/users/:entityKey" element={<UserDetail />} />
          <Route path="/baseline" element={<Baseline />} />
          {/* Split-governance pages -- each renders <Governance> */}
          {/* with a curated tabIds subset. /governance is kept as a */}
          {/* legacy route showing every tab at once. */}
          <Route path="/identity"         element={<IdentityAccess />} />
          <Route path="/data"             element={<DataSharing />} />
          <Route path="/devices"          element={<Devices />} />
          <Route path="/threats"          element={<ThreatIntelligence />} />
          <Route path="/ai"               element={<AiShadowIt />} />
          <Route path="/governance"       element={<Governance />} />
          <Route path="/sources"             element={<Sources />} />
          <Route path="/exceptions"          element={<Exceptions />} />
          <Route path="/mfa-status"          element={<MfaStatus />} />
          <Route path="/security-findings"   element={<SecurityFindings />} />
          <Route path="/mailbox-rules"       element={<MailboxRules />} />
          <Route path="*" element={<Navigate to="/dashboard" replace />} />
        </Route>
      </Routes>
    </AuthProvider>
  );
}
