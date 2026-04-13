/*
 * NERO Vector auth server entry point.
 *
 * Express app on :3006 with:
 *   - trust proxy: 1   (nginx + Cloudflare)
 *   - CORS allowlist for the vector-ui SPA origins
 *   - express-session backed by connect-sqlite3 for PKCE state
 *   - /auth/* router from routes/auth.js
 */

const express = require("express");
const session = require("express-session");
const SQLiteStore = require("connect-sqlite3")(session);
const cookieParser = require("cookie-parser");
const cors = require("cors");

const authRouter = require("./routes/auth");

const PORT = parseInt(process.env.PORT || "3006", 10);
const SESSION_SECRET = process.env.SESSION_SECRET || process.env.JWT_SECRET;
const SESSION_DB_DIR = process.env.SESSION_DB_DIR || "/data";

const CORS_ORIGINS = (
  process.env.CORS_ORIGINS || "http://78.46.92.112:3005,https://vector.rvmsol.com"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

if (!SESSION_SECRET) {
  console.error("[auth] SESSION_SECRET or JWT_SECRET is required");
  process.exit(1);
}

const app = express();
// Trust the front-most proxy hop so secure cookies + X-Forwarded-Proto work
// when Cloudflare/nginx terminate TLS upstream.
app.set("trust proxy", 1);
app.disable("x-powered-by");

// ---- CORS ------------------------------------------------------------------
app.use(
  cors({
    origin(origin, cb) {
      // Server-to-server requests (curl, health probes) have no Origin.
      if (!origin) return cb(null, true);
      if (CORS_ORIGINS.includes(origin)) return cb(null, true);
      return cb(new Error(`CORS: origin not allowed (${origin})`));
    },
    credentials: true,
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  }),
);

// ---- middleware ------------------------------------------------------------
app.use(cookieParser());
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

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
      maxAge: 1000 * 60 * 15, // 15 min — session only needs to survive the MS bounce
    },
  }),
);

// ---- routes ----------------------------------------------------------------
app.use("/auth", authRouter);

// Root sanity probe
app.get("/", (req, res) => res.json({ service: "vector-auth-server", ok: true }));

// ---- boot ------------------------------------------------------------------
app.listen(PORT, () => {
  console.log(`[auth] listening on :${PORT}`);
  console.log(`[auth] cors_origins=${CORS_ORIGINS.join(",")}`);
});
