/*
 * /auth/* routes — FieldDesk pattern.
 *
 *   GET  /auth/login     PKCE start + redirect to Microsoft
 *   GET  /auth/callback  code exchange, issue JWT, redirect /?token=
 *   GET  /auth/me        verify Bearer JWT, return the user blob
 *   POST /auth/logout    destroy session, return {success:true}
 *   GET  /auth/health    cheap liveness probe
 */

const express = require("express");
const fs = require("fs");
const path = require("path");
const jwt = require("jsonwebtoken");

const {
  generateVerifier,
  generateChallenge,
  generateState,
  buildAuthorizeUrl,
  exchangeCodeForTokens,
  decodeIdToken,
} = require("../services/msalClient");

const router = express.Router();

const JWT_SECRET = process.env.JWT_SECRET;
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || "7d";
const PUBLIC_BASE_URL =
  process.env.PUBLIC_BASE_URL || "https://vector.rvmsol.com";

if (!JWT_SECRET) {
  console.error("[auth] JWT_SECRET is required; refusing to start");
  process.exit(1);
}

// ---------------------------------------------------------------------------
// users.json -- domain allowlist + default role.
// ---------------------------------------------------------------------------

const USERS_FILE =
  process.env.USERS_FILE || path.join(__dirname, "..", "users.json");

let USERS_CONFIG;
try {
  USERS_CONFIG = JSON.parse(fs.readFileSync(USERS_FILE, "utf8"));
} catch (err) {
  console.error(`[auth] failed to load ${USERS_FILE}:`, err.message);
  process.exit(1);
}

const ALLOWED_DOMAIN = String(USERS_CONFIG.domain || "").toLowerCase();
const DEFAULT_ROLE = USERS_CONFIG.defaultRole || "admin";

if (!ALLOWED_DOMAIN) {
  console.error(`[auth] users.json missing "domain"`);
  process.exit(1);
}

console.log(
  `[auth] domain=@${ALLOWED_DOMAIN} default_role=${DEFAULT_ROLE} jwt_expires=${JWT_EXPIRES_IN}`,
);

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function initialsFrom(email, name) {
  if (name) {
    const parts = String(name).trim().split(/\s+/).filter(Boolean);
    if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
    if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  }
  if (!email) return "??";
  const local = String(email).split("@")[0];
  const segs = local.split(/[._\-+]/).filter(Boolean);
  if (segs.length >= 2) return (segs[0][0] + segs[1][0]).toUpperCase();
  return local.slice(0, 2).toUpperCase();
}

function requireJwt(req, res, next) {
  const auth = req.headers.authorization || "";
  if (!auth.toLowerCase().startsWith("bearer ")) {
    return res.status(401).json({ authenticated: false, error: "missing bearer" });
  }
  try {
    req.user = jwt.verify(auth.slice(7).trim(), JWT_SECRET);
    return next();
  } catch {
    return res.status(401).json({ authenticated: false, error: "invalid token" });
  }
}

function deniedHtml(email) {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Access Restricted — NERO Vector</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
      :root { color-scheme: dark; }
      body {
        background: #0A0F1E; color: #fff;
        font-family: Inter, system-ui, sans-serif;
        -webkit-font-smoothing: antialiased;
        display: flex; align-items: center; justify-content: center;
        min-height: 100vh; margin: 0;
      }
      .box {
        background: #1a2235; padding: 36px 40px; border-radius: 16px;
        max-width: 460px; text-align: center;
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
        display: inline-block; margin-top: 14px; color: #3B82F6;
        text-decoration: none; font-weight: 600;
      }
      a:hover { text-decoration: underline; }
    </style>
  </head>
  <body>
    <div class="box">
      <h1>ACCESS RESTRICTED</h1>
      <p>Access restricted to NERO Consulting team.</p>
      ${email ? `<p>Signed in as <span class="email">${email}</span></p>` : ""}
      <p><a href="/auth/login">Sign in with a different account</a></p>
    </div>
  </body>
</html>`;
}

// ---------------------------------------------------------------------------
// routes
// ---------------------------------------------------------------------------

router.get("/login", (req, res) => {
  const verifier = generateVerifier();
  const challenge = generateChallenge(verifier);
  const state = generateState();
  req.session.pkce = { verifier, state };
  req.session.save((err) => {
    if (err) {
      console.error("[auth] session save failed", err);
      return res.status(500).send("session error");
    }
    res.redirect(buildAuthorizeUrl({ state, challenge }));
  });
});

router.get("/callback", async (req, res) => {
  const { code, state, error, error_description } = req.query;
  if (error) {
    console.error("[auth] ms error", error, error_description);
    return res
      .status(400)
      .send(`Microsoft returned: ${error} — ${error_description || ""}`);
  }
  if (!code) return res.status(400).send("missing ?code");
  if (!req.session.pkce) return res.status(400).send("missing pkce state");
  if (state !== req.session.pkce.state) return res.status(400).send("state mismatch");

  const { verifier } = req.session.pkce;
  delete req.session.pkce;

  let tokens;
  try {
    tokens = await exchangeCodeForTokens({ code: String(code), verifier });
  } catch (err) {
    console.error(
      "[auth] token exchange failed",
      err?.response?.data || err.message,
    );
    return res.status(500).send("token exchange failed");
  }

  const claims = decodeIdToken(tokens.id_token);
  const rawEmail =
    (claims &&
      (claims.email || claims.preferred_username || claims.upn || "")) ||
    "";
  const email = String(rawEmail).toLowerCase();
  if (!email) return res.status(400).send("id_token missing email");

  if (!email.endsWith("@" + ALLOWED_DOMAIN)) {
    console.warn("[auth] denied", email);
    return res.status(403).set("Content-Type", "text/html").send(deniedHtml(email));
  }

  const name = (claims && claims.name) || email;
  const initials = initialsFrom(email, name);

  const payload = { email, role: DEFAULT_ROLE, name, initials };
  const token = jwt.sign(payload, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });

  console.log("[auth] login", email, `role=${DEFAULT_ROLE}`);

  // Clear the PKCE session now that we're done.
  req.session.destroy(() => {
    const url = new URL("/", PUBLIC_BASE_URL);
    url.searchParams.set("token", token);
    res.redirect(url.toString());
  });
});

router.get("/me", requireJwt, (req, res) => {
  const { email, role, name, initials } = req.user;
  res.json({ authenticated: true, user: { email, role, name, initials } });
});

router.post("/logout", (req, res) => {
  if (req.session) {
    return req.session.destroy(() => {
      res.clearCookie("vector.sid");
      res.json({ success: true });
    });
  }
  res.json({ success: true });
});

router.get("/health", (req, res) => res.json({ status: "ok" }));

module.exports = router;
