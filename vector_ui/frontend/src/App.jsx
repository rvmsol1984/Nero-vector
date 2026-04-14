import { Navigate, Route, Routes } from "react-router-dom";

import { AuthProvider } from "./auth.jsx";
import Layout from "./components/Layout.jsx";
import Placeholder from "./components/Placeholder.jsx";
import Incidents from "./pages/Incidents.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import Events from "./pages/Events.jsx";
import Users from "./pages/Users.jsx";
import UserDetail from "./pages/UserDetail.jsx";
import Baseline from "./pages/Baseline.jsx";
import Governance from "./pages/Governance.jsx";
import Sources from "./pages/Sources.jsx";
import Watchlist from "./pages/Watchlist.jsx";

export default function App() {
  return (
    <AuthProvider>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard"        element={<Dashboard />} />
          <Route
            path="/incidents"
            element={<Incidents />}
          />
          <Route path="/watchlist" element={<Watchlist />} />
          <Route path="/events"           element={<Events />} />
          <Route path="/users"            element={<Users />} />
          <Route path="/users/:entityKey" element={<UserDetail />} />
          <Route
            path="/baseline"
            element={<Baseline />}
          />
          <Route path="/governance"       element={<Governance />} />
          <Route path="/sources"          element={<Sources />} />
          <Route path="*" element={<Navigate to="/dashboard" replace />} />
        </Route>
      </Routes>
    </AuthProvider>
  );
}
