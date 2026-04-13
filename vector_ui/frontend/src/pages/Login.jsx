// Unauthenticated landing page. Rendered by AuthProvider when the SPA
// loads without a valid JWT. Clicking "Sign in with Microsoft" starts
// the PKCE flow at /auth/login, which nginx proxies to the auth sidecar.

export default function Login() {
  function handleSignIn() {
    window.location.href = "/auth/login";
  }

  return (
    <div className="min-h-screen bg-bg flex items-center justify-center px-5 py-10 font-sans">
      <div
        className="w-full max-w-[400px] bg-card border border-white/5 rounded-2xl shadow-xl p-10 animate-fade-in"
        style={{ backgroundColor: "#1a2235" }}
      >
        {/* ---- NERO logo row ---- */}
        <div className="flex items-center gap-3 mb-10">
          <div className="text-3xl font-bold tracking-wider text-white leading-none">
            NERO
          </div>
          <div className="border-l border-white/15 pl-3 text-[9px] uppercase tracking-[0.18em] text-white/55 leading-[1.4]">
            Technology
            <br />
            Consulting
            <br />
            Services
          </div>
        </div>

        {/* ---- product title ---- */}
        <div className="mb-8">
          <div className="text-2xl font-bold text-white leading-tight">
            NERO Vector
          </div>
          <div className="text-sm text-white/50 mt-1">
            Security Intelligence Platform
          </div>
        </div>

        {/* ---- sign in ---- */}
        <p className="text-center text-sm text-white/60 mb-5">
          Sign in with your NERO Microsoft account
        </p>

        <button
          type="button"
          onClick={handleSignIn}
          className="w-full flex items-center justify-center gap-3 px-4 py-3 bg-white text-[#0A0F1E] font-semibold rounded-xl shadow-lg hover:bg-white/95 active:scale-[0.98] transition-all"
        >
          <svg
            width="20"
            height="20"
            viewBox="0 0 21 21"
            aria-hidden="true"
          >
            <rect x="1"  y="1"  width="9" height="9" fill="#f25022" />
            <rect x="11" y="1"  width="9" height="9" fill="#7fba00" />
            <rect x="1"  y="11" width="9" height="9" fill="#00a4ef" />
            <rect x="11" y="11" width="9" height="9" fill="#ffb900" />
          </svg>
          Sign in with Microsoft
        </button>

        {/* ---- footer ---- */}
        <div className="text-center text-[11px] text-white/40 mt-8">
          Access restricted to NERO Consulting team
        </div>
      </div>
    </div>
  );
}
