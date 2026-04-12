import { Navigate, Route, Routes } from "react-router-dom";

import Layout from "./components/Layout.jsx";
import Placeholder from "./components/Placeholder.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import Events from "./pages/Events.jsx";
import Users from "./pages/Users.jsx";
import UserDetail from "./pages/UserDetail.jsx";
import Sources from "./pages/Sources.jsx";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route
          path="/incidents"
          element={
            <Placeholder
              title="Incidents"
              blurb="Cross-source correlation graph and triage queue. Fuses UAL, SaaS Alerts, INKY, Datto EDR, and FeedLattice events into deduplicated incidents with severity scoring and analyst assignment."
            />
          }
        />
        <Route
          path="/watchlist"
          element={
            <Placeholder
              title="Watchlist"
              blurb="Pin high-risk identities, assets, and IP ranges. Any new event touching a watchlisted entity surfaces here and pages the on-call analyst."
            />
          }
        />
        <Route path="/events" element={<Events />} />
        <Route path="/users" element={<Users />} />
        <Route path="/users/:entityKey" element={<UserDetail />} />
        <Route
          path="/baseline"
          element={
            <Placeholder
              title="Baseline"
              blurb="Per-tenant behavioural baselines for login geography, login time, data-access volume, and workload mix. Feeds the anomaly scoring inside the correlation engine."
            />
          }
        />
        <Route path="/sources" element={<Sources />} />
        <Route
          path="/governance"
          element={
            <Placeholder
              title="Governance"
              blurb="Retention policies, tenant scoping rules, operator audit log, and evidence export for client reporting."
            />
          }
        />
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Route>
    </Routes>
  );
}
