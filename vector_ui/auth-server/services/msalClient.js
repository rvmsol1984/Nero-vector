/*
 * Microsoft Identity PKCE helpers, mirroring the NERO FieldDesk pattern.
 *
 * Single-tenant public client (no secret). All routes live inside
 * routes/auth.js; this module owns the low-level bits:
 *   - generate a verifier + S256 challenge
 *   - build the authorize URL
 *   - exchange code + verifier for tokens
 *   - decode the id_token payload (no signature verification: the token
 *     was just returned to us on a direct TLS POST to login.microsoft).
 */

const crypto = require("crypto");
const axios = require("axios");

const CLIENT_ID =
  process.env.AZURE_CLIENT_ID || "4a38585b-ee28-4290-9dc0-fe70cc457fbb";
const TENANT_ID =
  process.env.AZURE_TENANT_ID || "12077321-d5c3-4d90-ad87-d19b58b2f847";
const REDIRECT_URI =
  process.env.AUTH_REDIRECT_URI || "https://vector.rvmsol.com/auth/callback";

const SCOPE = "openid email profile";

const AUTHORIZE_URL = `https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/authorize`;
const TOKEN_URL     = `https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/token`;

function base64url(buf) {
  return buf
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function generateVerifier() {
  return base64url(crypto.randomBytes(32));
}

function generateChallenge(verifier) {
  return base64url(crypto.createHash("sha256").update(verifier).digest());
}

function generateState() {
  return base64url(crypto.randomBytes(16));
}

function buildAuthorizeUrl({ state, challenge }) {
  const params = new URLSearchParams({
    client_id: CLIENT_ID,
    response_type: "code",
    redirect_uri: REDIRECT_URI,
    response_mode: "query",
    scope: SCOPE,
    code_challenge: challenge,
    code_challenge_method: "S256",
    state,
  });
  return `${AUTHORIZE_URL}?${params.toString()}`;
}

async function exchangeCodeForTokens({ code, verifier }) {
  const body = new URLSearchParams({
    client_id: CLIENT_ID,
    grant_type: "authorization_code",
    code,
    redirect_uri: REDIRECT_URI,
    code_verifier: verifier,
    scope: SCOPE,
  });
  const resp = await axios.post(TOKEN_URL, body.toString(), {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    timeout: 15000,
  });
  return resp.data;
}

function decodeIdToken(idToken) {
  if (!idToken) return null;
  const parts = idToken.split(".");
  if (parts.length < 2) return null;
  try {
    const json = Buffer.from(
      parts[1].replace(/-/g, "+").replace(/_/g, "/"),
      "base64",
    ).toString("utf8");
    return JSON.parse(json);
  } catch {
    return null;
  }
}

module.exports = {
  CLIENT_ID,
  TENANT_ID,
  REDIRECT_URI,
  SCOPE,
  generateVerifier,
  generateChallenge,
  generateState,
  buildAuthorizeUrl,
  exchangeCodeForTokens,
  decodeIdToken,
};
