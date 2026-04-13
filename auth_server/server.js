/*
 * NERO Vector auth server.
 *
 * Mirrors the NERO FieldDesk PKCE flow:
 *   GET /auth/login     -- build code_verifier + challenge, stash in
 *                          session, redirect to Microsoft authorize
 *   GET /auth/callback  -- exchange ?code + verifier for tokens,
 *                          decode id_token, enforce USER_MAP, set session
 *   GET /auth/me        -- return the current user (401 if unauth)
 *   GET /auth/logout    -- destroy session and redirect to /
 *   GET /auth/denied    -- 403 page for users outside USER_MAP
 *
 * The process listens on 3006 behind the shared nginx/Cloudflare edge.
 * express-session writes to a SQLite store mounted at /data so sessions
 * survive container restarts.
 */

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const axios = require("axios");
const express = require("express");
const session = require("express-session");
const SQLiteStore = require("connect-sqlite3")(session);

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

const PORT = parseInt(process.env.PORT || "3006", 10);

const CLIENT_ID =
  process.env.AZURE_CLIENT_ID || "4a38585b-ee28-4290-9dc0-fe70cc457fbb";
const TENANT_ID =
  process.env.AZURE_TENANT_ID || "12077321-d5c3-4d90-ad87-d19b58b2f847";
const REDIRECT_URI =
  process.env.AUTH_REDIRECT_URI || "https://vector.rvmsol.com/auth/callback";
const PUBLIC_BASE_URL =
  process.env.PUBLIC_BASE_URL || "https://vector.rvmsol.com";
const SESSION_SECRET = process.env.SESSION_SECRET;
const SESSION_DB_DIR = process.env.SESSION_DB_DIR || "/data";

if (!SESSION_SECRET) {
  console.error(
    "[auth] SESSION_SECRET is required (set it in .env); refusing to start",
  );
  process.exit(1);
}

// Domain allowlist loaded from users.json. The whole file is a tiny
// config blob:
//
//   { "domain": "nero-consulting.com", "defaultRole": "admin" }
//
// Anyone whose id_token email ends with "@<domain>" is admitted with
// role = defaultRole. The role field is still plumbed through to
// /auth/me so the UI can gate containment actions on it later, but for
// the current internal test phase there is no per-user override -- a
// domain match is enough.
const USERS_FILE = process.env.USERS_FILE || path.join(__dirname, "users.json");

let USERS_CONFIG;
try {
  USERS_CONFIG = JSON.parse(fs.readFileSync(USERS_FILE, "utf8"));
} catch (err) {
  console.error(`[auth] failed to load users config at ${USERS_FILE}:`, err.message);
  process.exit(1);
}

const ALLOWED_DOMAIN = String(USERS_CONFIG.domain || "").toLowerCase();
const DEFAULT_ROLE = USERS_CONFIG.defaultRole || "operator";

if (!ALLOWED_DOMAIN) {
  console.error(`[auth] users config at ${USERS_FILE} is missing "domain"`);
  process.exit(1);
}

function resolveRole(email) {
  if (!email) return null;
  const lower = email.toLowerCase();
  if (!lower.endsWith("@" + ALLOWED_DOMAIN)) return null;
  return DEFAULT_ROLE;
}

const SCOPE = "openid email profile";

// ---------------------------------------------------------------------------
// app
// ---------------------------------------------------------------------------

const app = express();
// Trust exactly one hop (nginx -> Cloudflare) so secure cookies work.
app.set("trust proxy", 1);
app.disable("x-powered-by");

app.use(
  session({
    store: new SQLiteStore({
      db: "sessions.sqlite",
      dir: SESSION_DB_DIR,
    }),
    name: "vector.sid",
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: "lax",
      secure: true,
      maxAge: 1000 * 60 * 60 * 24 * 7, // 7 days
    },
  }),
);

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function base64url(buf) {
  return buf
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function randomBase64url(bytes = 32) {
  return base64url(crypto.randomBytes(bytes));
}

function pkceChallenge(verifier) {
  return base64url(crypto.createHash("sha256").update(verifier).digest());
}

function decodeJwtPayload(jwt) {
  if (!jwt) return null;
  const [, payloadB64] = jwt.split(".");
  if (!payloadB64) return null;
  try {
    const json = Buffer.from(
      payloadB64.replace(/-/g, "+").replace(/_/g, "/"),
      "base64",
    ).toString("utf8");
    return JSON.parse(json);
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// routes
// ---------------------------------------------------------------------------

app.get("/auth/login", (req, res) => {
  const verifier = randomBase64url(32);
  const challenge = pkceChallenge(verifier);
  const state = randomBase64url(16);

  req.session.pkce = { verifier, state };
  req.session.save((err) => {
    if (err) {
      console.error("[auth] failed to save session", err);
      return res.status(500).send("session error");
    }
    const url = new URL(
      `https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/authorize`,
    );
    url.searchParams.set("client_id", CLIENT_ID);
    url.searchParams.set("response_type", "code");
    url.searchParams.set("redirect_uri", REDIRECT_URI);
    url.searchParams.set("response_mode", "query");
    url.searchParams.set("scope", SCOPE);
    url.searchParams.set("code_challenge", challenge);
    url.searchParams.set("code_challenge_method", "S256");
    url.searchParams.set("state", state);
    res.redirect(url.toString());
  });
});

app.get("/auth/callback", async (req, res) => {
  const { code, state, error, error_description } = req.query;
  if (error) {
    console.error("[auth] callback error", error, error_description);
    return res
      .status(400)
      .send(`Microsoft returned an error: ${error} — ${error_description || ""}`);
  }
  if (!code) {
    return res.status(400).send("missing ?code");
  }
  if (!req.session.pkce) {
    return res.status(400).send("missing pkce state — start at /auth/login");
  }
  if (state !== req.session.pkce.state) {
    return res.status(400).send("state mismatch");
  }

  const { verifier } = req.session.pkce;
  delete req.session.pkce;

  let tokenResp;
  try {
    tokenResp = await axios.post(
      `https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/token`,
      new URLSearchParams({
        client_id: CLIENT_ID,
        grant_type: "authorization_code",
        code: String(code),
        redirect_uri: REDIRECT_URI,
        code_verifier: verifier,
        scope: SCOPE,
      }).toString(),
      {
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        timeout: 15000,
      },
    );
  } catch (err) {
    console.error(
      "[auth] token exchange failed",
      err?.response?.data || err.message,
    );
    return res.status(500).send("token exchange failed");
  }

  const claims = decodeJwtPayload(tokenResp.data.id_token);
  const rawEmail =
    (claims &&
      (claims.email || claims.preferred_username || claims.upn || "")) ||
    "";
  const email = String(rawEmail).toLowerCase();

  if (!email) {
    return res.status(400).send("id_token missing email / upn claim");
  }

  const role = resolveRole(email);
  if (!role) {
    console.warn("[auth] denied email", email);
    req.session.deniedEmail = email;
    return req.session.save(() => res.redirect("/auth/denied"));
  }

  req.session.user = {
    email,
    role,
    name: (claims && claims.name) || email,
    loginAt: new Date().toISOString(),
  };
  delete req.session.deniedEmail;
  req.session.save((err) => {
    if (err) {
      console.error("[auth] failed to save session after login", err);
      return res.status(500).send("session error");
    }
    console.log("[auth] login", email, "role=" + role);
    res.redirect("/");
  });
});

app.get("/auth/me", (req, res) => {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ authenticated: false });
  }
  return res.json({ authenticated: true, user: req.session.user });
});

app.get("/auth/logout", (req, res) => {
  const email = req.session?.user?.email;
  req.session.destroy(() => {
    res.clearCookie("vector.sid");
    if (email) console.log("[auth] logout", email);
    res.redirect("/");
  });
});

app.get("/auth/denied", (req, res) => {
  const email = (req.session && req.session.deniedEmail) || "";
  res.status(403).set("Content-Type", "text/html").send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Access Restricted — NERO Vector</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
      :root { color-scheme: dark; }
      body {
        background: #0A0F1E;
        color: #fff;
        font-family: Inter, system-ui, sans-serif;
        -webkit-font-smoothing: antialiased;
        display: flex;
        align-items: center;
        justify-content: center;
        min-height: 100vh;
        margin: 0;
      }
      .box {
        background: #1a2235;
        padding: 36px 40px;
        border-radius: 16px;
        max-width: 460px;
        text-align: center;
        border: 1px solid rgba(255,255,255,0.05);
        box-shadow: 0 10px 30px rgba(0,0,0,0.35);
      }
      h1 { margin: 0 0 10px; font-size: 20px; letter-spacing: 0.05em; }
      p  { color: rgba(255,255,255,0.6); font-size: 14px; line-height: 1.6; }
      .email {
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        color: #EF4444;
      }
      a {
        display: inline-block;
        margin-top: 14px;
        color: #3B82F6;
        text-decoration: none;
        font-weight: 600;
      }
      a:hover { text-decoration: underline; }
    </style>
  </head>
  <body>
    <div class="box">
      <h1>ACCESS RESTRICTED</h1>
      <p>Access restricted to NERO Consulting team.</p>
      ${email ? `<p>Signed in as <span class="email">${email}</span></p>` : ""}
      <p><a href="/auth/logout">Sign out and try a different account</a></p>
    </div>
  </body>
</html>`);
});

// Liveness probe for docker-compose healthchecks, nginx upstreams, etc.
app.get("/auth/health", (req, res) => res.json({ status: "ok" }));

// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`[auth] listening on :${PORT}`);
  console.log(`[auth] tenant=${TENANT_ID} client_id=${CLIENT_ID}`);
  console.log(`[auth] redirect_uri=${REDIRECT_URI}`);
  console.log(`[auth] allowed_domain=@${ALLOWED_DOMAIN} default_role=${DEFAULT_ROLE}`);
});
