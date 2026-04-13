import { createContext, useContext, useEffect, useState } from "react";

import Login from "./pages/Login.jsx";

// JWT-based auth — mirrors NERO FieldDesk.
//
// Flow:
//   1. Browser loads the SPA. We look for ?token=... in the URL (set by
//      the auth-server /auth/callback redirect) and stash it in
//      localStorage under "vector_token", then strip it from the URL.
//   2. If we have a token we call /auth/me with Authorization: Bearer …
//      to confirm it's still valid and fetch the user blob.
//   3. If /auth/me is 401/403 or the token is missing, we hard-redirect
//      to /auth/login to start the PKCE bounce.
//
// The API layer in api.js reads the token via getToken() and sends it
// on every /api/* request. A 401 anywhere clears the token and kicks
// the user back to /auth/login.

const AuthContext = createContext(null);

const TOKEN_KEY = "vector_token";

// Compute the URL base for /auth/* calls. When the SPA is served
// through the nginx edge at vector.rvmsol.com (same origin as the
// auth-server), use a relative path. When developers hit the vector-ui
// container directly on port 3005, the auth-server is on :3006 of the
// same host, so we flip the port. Overridable via VITE_AUTH_BASE at
// build time if neither default fits.
export const AUTH_BASE = (() => {
  const envBase = import.meta.env.VITE_AUTH_BASE;
  if (envBase) return String(envBase).replace(/\/+$/, "");
  if (typeof window === "undefined") return "";
  const { protocol, hostname, port } = window.location;
  if (port === "3005") return `${protocol}//${hostname}:3006`;
  return "";
})();

export function getToken() {
  try {
    return localStorage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
}

export function setToken(token) {
  try {
    if (token) localStorage.setItem(TOKEN_KEY, token);
    else localStorage.removeItem(TOKEN_KEY);
  } catch {
    /* storage unavailable */
  }
}

export function clearToken() {
  setToken(null);
}

export function redirectToLogin() {
  clearToken();
  // Land on "/" so the AuthProvider re-mounts and renders the Login
  // page. We never auto-bounce straight to /auth/login any more -- the
  // user has to click the button on the Login page.
  window.location.href = "/";
}

// Hard sign out:
//   1. POST /auth/logout on the auth sidecar (fire-and-forget with
//      keepalive: true so the browser still delivers it after we've
//      navigated away — we don't care about the response)
//   2. clear localStorage["vector_token"]
//   3. window.location.href = "/" (back to the Login page)
export function signOut() {
  const token = getToken();
  try {
    fetch(`${AUTH_BASE}/auth/logout`, {
      method: "POST",
      credentials: "include",
      keepalive: true,
      headers: {
        "Content-Type": "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
    }).catch((err) => {
      // eslint-disable-next-line no-console
      console.warn("[auth] /auth/logout POST failed, continuing", err);
    });
  } catch (err) {
    // eslint-disable-next-line no-console
    console.warn("[auth] /auth/logout threw synchronously, continuing", err);
  }
  try {
    localStorage.removeItem("vector_token");
  } catch {
    /* storage unavailable */
  }
  window.location.href = "/";
}

export function AuthProvider({ children }) {
  const [state, setState] = useState({ status: "loading", user: null });

  useEffect(() => {
    // ---- 1. capture ?token=… from the callback redirect -------------
    try {
      const url = new URL(window.location.href);
      const tokenParam = url.searchParams.get("token");
      if (tokenParam) {
        setToken(tokenParam);
        url.searchParams.delete("token");
        window.history.replaceState(
          {},
          "",
          url.pathname + (url.search || "") + url.hash,
        );
      }
    } catch {
      /* non-browser runtime */
    }

    const token = getToken();
    if (!token) {
      // No session -- show the Login page, do NOT auto-bounce to MS.
      setState({ status: "unauthenticated", user: null });
      return;
    }

    // ---- 2. verify with /auth/me -----------------------------------
    let cancelled = false;
    fetch(`${AUTH_BASE}/auth/me`, {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then(async (res) => {
        if (cancelled) return;
        if (res.status === 401 || res.status === 403) {
          clearToken();
          setState({ status: "unauthenticated", user: null });
          return;
        }
        if (!res.ok) throw new Error(`auth/me returned ${res.status}`);
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

  if (state.status === "unauthenticated") {
    return <Login />;
  }

  if (state.status === "error") {
    return (
      <div className="flex flex-col items-center justify-center h-screen bg-bg text-sm font-sans gap-3 px-6 text-center">
        <div className="text-critical">auth error: {state.error}</div>
        <button
          type="button"
          onClick={() => window.location.reload()}
          className="text-primary-light underline"
        >
          reload
        </button>
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
