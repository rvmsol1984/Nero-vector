import { createContext, useContext, useEffect, useState } from "react";

// Session-cookie auth. On mount we hit the auth sidecar's /auth/me and
// either render the app (when a valid NERO session is found) or do a
// hard navigation to /auth/login. The whole SPA sits inside the gate
// so no page ever renders without a valid session.

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [state, setState] = useState({ status: "loading", user: null });

  useEffect(() => {
    let cancelled = false;
    fetch("/auth/me", { credentials: "same-origin" })
      .then(async (res) => {
        if (cancelled) return;
        if (res.status === 401) {
          // Session missing -> bounce to the PKCE flow.
          window.location.href = "/auth/login";
          return;
        }
        if (res.status === 403) {
          // Signed in but not in USER_MAP.
          window.location.href = "/auth/denied";
          return;
        }
        if (!res.ok) {
          throw new Error(`auth/me returned ${res.status}`);
        }
        const data = await res.json();
        setState({ status: "ready", user: data.user });
      })
      .catch((e) => {
        if (!cancelled) {
          setState({ status: "error", user: null, error: e.message });
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (state.status === "loading") {
    return (
      <div className="flex items-center justify-center h-screen bg-bg text-white/60 text-sm font-sans">
        signing in…
      </div>
    );
  }

  if (state.status === "error") {
    return (
      <div className="flex flex-col items-center justify-center h-screen bg-bg text-sm font-sans gap-3 px-6 text-center">
        <div className="text-critical">auth error: {state.error}</div>
        <a href="/auth/login" className="text-primary-light underline">
          try signing in again
        </a>
      </div>
    );
  }

  return (
    <AuthContext.Provider value={state.user}>{children}</AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}

export function signOut() {
  // Full nav so express-session can destroy the cookie and Cloudflare
  // doesn't serve a stale SPA shell afterwards.
  window.location.href = "/auth/logout";
}
