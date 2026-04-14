import { Fragment, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import Avatar from "../components/Avatar.jsx";
import { api } from "../api.js";
import { fmtNumber, fmtRelative, fmtTime } from "../utils/format.js";

// ---------------------------------------------------------------------------
// country flags
// ---------------------------------------------------------------------------
//
// Convert an ISO-3166 alpha-2 country code into the corresponding flag
// emoji by offsetting each letter into the regional indicator block.
// Falls back to a bare "—" for anything that doesn't look like a 2-letter
// code (most commonly the literal string "Unknown" from Azure AD).

function countryFlag(code) {
  if (!code || typeof code !== "string") return "";
  const trimmed = code.trim().toUpperCase();
  if (trimmed.length !== 2 || !/^[A-Z]{2}$/.test(trimmed)) return "";
  const base = 0x1f1e6 - 65;
  return String.fromCodePoint(base + trimmed.charCodeAt(0))
    + String.fromCodePoint(base + trimmed.charCodeAt(1));
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
//
// The BaselineEngine stores login_countries as a JSONB object
// (``{"US": 42, "IN": 3}``) but we also accept a plain array of codes
// for forward-compat. Normalize both shapes into a sorted array of
// ``{ code, count }`` tuples so the table and expand panel can render
// them uniformly.

function normalizeCountries(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) {
    return raw
      .map((code) => ({ code: String(code || ""), count: null }))
      .filter((c) => c.code);
  }
  if (typeof raw === "object") {
    return Object.entries(raw)
      .map(([code, count]) => ({ code: String(code), count: Number(count) || 0 }))
      .sort((a, b) => (b.count || 0) - (a.count || 0));
  }
  return [];
}

function normalizeList(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw.filter((v) => v != null && v !== "");
  if (typeof raw === "object") return Object.keys(raw);
  return [];
}

// ---------------------------------------------------------------------------
// tiny presentational helpers
// ---------------------------------------------------------------------------

function StatCard({ label, value, color, loading, hint }) {
  return (
    <div className="bg-white/[0.03] border border-white/5 rounded-xl px-4 py-3">
      <div className="text-[10px] uppercase tracking-wider text-white/40">
        {label}
      </div>
      <div
        className="text-2xl font-bold mt-1 tabular-nums leading-none"
        style={{ color }}
      >
        {loading ? "—" : value}
      </div>
      {hint && (
        <div className="text-[10px] text-white/40 mt-1">{hint}</div>
      )}
    </div>
  );
}

function Chevron({ open }) {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      className="text-white/40 transition-transform duration-200"
      style={{ transform: open ? "rotate(180deg)" : "rotate(0deg)" }}
    >
      <polyline points="6 9 12 15 18 9" />
    </svg>
  );
}

function CountryChip({ code, count }) {
  const flag = countryFlag(code);
  return (
    <span
      className="inline-flex items-center gap-1 px-2 py-[3px] text-[11px] rounded-md bg-white/5 border border-white/10 text-white/80 whitespace-nowrap"
      title={count != null ? `${code} — ${fmtNumber(count)} logins` : code}
    >
      {flag && <span className="text-sm leading-none">{flag}</span>}
      <span className="uppercase tracking-wider text-[10px]">{code}</span>
      {count != null && count > 0 && (
        <span className="text-white/40 tabular-nums">({fmtNumber(count)})</span>
      )}
    </span>
  );
}

function ValueChip({ value, mono }) {
  return (
    <span
      className={`inline-flex items-center px-2 py-[3px] text-[11px] rounded-md bg-white/5 border border-white/10 text-white/80 whitespace-nowrap ${
        mono ? "font-mono tabular-nums" : ""
      }`}
      title={value}
    >
      {value}
    </span>
  );
}

function Th({ children, align = "left" }) {
  return (
    <th
      className={`px-4 py-2.5 text-[10px] uppercase tracking-[0.15em] text-white/40 font-semibold ${
        align === "right" ? "text-right" : "text-left"
      }`}
    >
      {children}
    </th>
  );
}

// ---------------------------------------------------------------------------
// main page
// ---------------------------------------------------------------------------

export default function Baseline() {
  const [stats, setStats] = useState(null);
  const [rows, setRows] = useState(null);
  const [err, setErr] = useState(null);
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [openId, setOpenId] = useState(null);

  // Debounce the search box so typing doesn't fire a request per
  // keystroke -- the backend does an ILIKE scan so hammering it is
  // unfriendly on larger tables.
  useEffect(() => {
    const t = setTimeout(() => setSearch(searchInput.trim()), 300);
    return () => clearTimeout(t);
  }, [searchInput]);

  // Stats load once on mount; re-fetched alongside the list when
  // the search term is cleared so freshness stays honest.
  useEffect(() => {
    let cancel = false;
    api
      .baselineStats()
      .then((s) => !cancel && setStats(s))
      .catch((e) => !cancel && setErr(String(e.message || e)));
    return () => {
      cancel = true;
    };
  }, []);

  useEffect(() => {
    let cancel = false;
    setRows(null);
    api
      .baselineList({ limit: 200, search: search || undefined })
      .then((r) => !cancel && setRows(r || []))
      .catch((e) => !cancel && (setRows([]), setErr(String(e.message || e))));
    return () => {
      cancel = true;
    };
  }, [search]);

  const avgIpsLabel = useMemo(() => {
    if (stats == null) return "—";
    const n = Number(stats.avg_known_ips || 0);
    return n >= 10 ? n.toFixed(0) : n.toFixed(1);
  }, [stats]);

  return (
    <div className="space-y-5 animate-fade-in">
      {/* ----- header ----- */}
      <div>
        <h1 className="text-2xl font-bold">Baseline</h1>
        <p className="text-white/50 text-sm mt-1">
          Behavioral profiles for anomaly detection.
        </p>
      </div>

      {err && (
        <div className="card border-critical/30 text-critical text-sm px-4 py-3">
          load error: {err}
        </div>
      )}

      {/* ----- stat cards ----- */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        <StatCard
          label="Profiles Built"
          value={stats != null ? fmtNumber(stats.total_baselines) : "—"}
          color="#3B82F6"
          loading={stats == null}
          hint={
            stats?.last_computed
              ? `last run ${fmtRelative(stats.last_computed)}`
              : undefined
          }
        />
        <StatCard
          label="Fresh (< 1 hour)"
          value={stats != null ? fmtNumber(stats.fresh) : "—"}
          color="#10B981"
          loading={stats == null}
        />
        <StatCard
          label="Avg Known IPs"
          value={avgIpsLabel}
          color="rgba(255,255,255,0.75)"
          loading={stats == null}
          hint={
            stats?.avg_countries != null
              ? `${Number(stats.avg_countries || 0).toFixed(1)} avg countries`
              : undefined
          }
        />
      </div>

      {/* ----- search ----- */}
      <div className="flex flex-wrap items-center gap-3">
        <input
          type="search"
          placeholder="search by email…"
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          className="bg-white/5 border border-white/10 rounded-xl px-3 py-2 text-xs text-white placeholder:text-white/40 focus:outline-none focus:border-primary-light w-72"
        />
        {rows != null && (
          <span className="text-[11px] text-white/40 tabular-nums">
            {fmtNumber(rows.length)} profile{rows.length === 1 ? "" : "s"}
          </span>
        )}
      </div>

      {/* ----- table ----- */}
      {rows == null ? (
        <div className="card py-12 text-center text-white/40 text-sm">
          loading…
        </div>
      ) : rows.length === 0 ? (
        <EmptyState search={search} />
      ) : (
        <BaselineTable
          rows={rows}
          openId={openId}
          setOpenId={setOpenId}
        />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// empty state
// ---------------------------------------------------------------------------

function EmptyState({ search }) {
  return (
    <div className="card py-14 flex flex-col items-center text-white/60 text-sm gap-3">
      <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#64748B" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
      </svg>
      <div className="font-semibold">
        {search
          ? `No baselines match "${search}"`
          : "No baseline profiles computed yet"}
      </div>
      <div className="text-white/40 text-[11px] text-center max-w-sm">
        The BaselineEngine builds these hourly from at least 7 days of
        audit history. Check back once the engine has had a full cycle.
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// baseline table with in-place row expand
// ---------------------------------------------------------------------------

function BaselineTable({ rows, openId, setOpenId }) {
  return (
    <div className="card overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full text-[11px]">
          <thead>
            <tr>
              <Th>User</Th>
              <Th align="right">Known IPs</Th>
              <Th>Countries</Th>
              <Th align="right">Devices</Th>
              <Th>Last Computed</Th>
              <Th>{""}</Th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {rows.map((row) => {
              const rowId = `${row.tenant_id}::${row.user_id}`;
              const isOpen = openId === rowId;
              const countries = normalizeCountries(row.login_countries);
              return (
                <Fragment key={rowId}>
                  <tr
                    onClick={() => setOpenId(isOpen ? null : rowId)}
                    className={`cursor-pointer ${isOpen ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"}`}
                  >
                    <td className="px-4 py-2.5">
                      <Link
                        to={`/users/${encodeURIComponent(rowId)}`}
                        onClick={(e) => e.stopPropagation()}
                        className="flex items-center gap-2 hover:text-primary-light"
                        title={row.user_id}
                      >
                        <Avatar email={row.user_id} size={26} />
                        <span className="truncate max-w-[240px]">
                          {row.user_id}
                        </span>
                      </Link>
                    </td>
                    <td className="px-4 py-2.5 text-right tabular-nums">
                      {fmtNumber(row.ip_count)}
                    </td>
                    <td className="px-4 py-2.5">
                      <div className="flex items-center gap-1 flex-wrap max-w-[260px]">
                        {countries.slice(0, 4).map((c) => (
                          <CountryChip
                            key={c.code}
                            code={c.code}
                            count={c.count}
                          />
                        ))}
                        {countries.length > 4 && (
                          <span className="text-[10px] text-white/40 tabular-nums">
                            +{countries.length - 4}
                          </span>
                        )}
                        {countries.length === 0 && (
                          <span className="text-white/30">—</span>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-2.5 text-right tabular-nums">
                      {fmtNumber(row.device_count)}
                    </td>
                    <td className="px-4 py-2.5 text-white/50 whitespace-nowrap">
                      {fmtRelative(row.computed_at)}
                    </td>
                    <td className="px-3 py-2.5 w-8 text-white/40">
                      <Chevron open={isOpen} />
                    </td>
                  </tr>
                  {isOpen && (
                    <tr>
                      <td
                        colSpan={6}
                        className="p-0 border-t border-white/5"
                        style={{ backgroundColor: "#0D1428" }}
                      >
                        <BaselineExpand row={row} countries={countries} />
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// expanded row: known IPs / countries / devices lists
// ---------------------------------------------------------------------------

const IP_VISIBLE = 5;
const DEVICE_VISIBLE = 5;

function BaselineExpand({ row, countries }) {
  const [showAllIps, setShowAllIps] = useState(false);
  const [showAllDevices, setShowAllDevices] = useState(false);

  const ips = useMemo(() => normalizeList(row.known_ips), [row.known_ips]);
  const devices = useMemo(
    () => normalizeList(row.known_devices),
    [row.known_devices],
  );

  const visibleIps = showAllIps ? ips : ips.slice(0, IP_VISIBLE);
  const visibleDevices = showAllDevices ? devices : devices.slice(0, DEVICE_VISIBLE);

  const entityKey = `${row.tenant_id}::${row.user_id}`;

  return (
    <div className="px-5 py-5 space-y-5 animate-fade-in">
      {/* ----- meta header ----- */}
      <div className="flex items-center gap-3 flex-wrap text-[11px]">
        <div className="text-white/40 uppercase tracking-wider text-[9px]">
          Computed
        </div>
        <div className="text-white/80 tabular-nums">
          {fmtTime(row.computed_at)}
        </div>
        {row.baseline_days != null && (
          <>
            <span className="text-white/20">·</span>
            <div className="text-white/40 uppercase tracking-wider text-[9px]">
              History
            </div>
            <div className="text-white/80 tabular-nums">
              {fmtNumber(row.baseline_days)} days
            </div>
          </>
        )}
        {row.avg_daily_events != null && (
          <>
            <span className="text-white/20">·</span>
            <div className="text-white/40 uppercase tracking-wider text-[9px]">
              Avg events / day
            </div>
            <div className="text-white/80 tabular-nums">
              {Number(row.avg_daily_events || 0).toFixed(1)}
            </div>
          </>
        )}
        {row.avg_daily_logins != null && (
          <>
            <span className="text-white/20">·</span>
            <div className="text-white/40 uppercase tracking-wider text-[9px]">
              Avg logins / day
            </div>
            <div className="text-white/80 tabular-nums">
              {Number(row.avg_daily_logins || 0).toFixed(1)}
            </div>
          </>
        )}
        <Link
          to={`/users/${encodeURIComponent(entityKey)}`}
          onClick={(e) => e.stopPropagation()}
          className="ml-auto text-primary-light hover:underline text-[11px]"
        >
          View User →
        </Link>
      </div>

      {/* ----- three panels: IPs / countries / devices ----- */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {/* known IPs */}
        <div>
          <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
            Known IPs ({fmtNumber(ips.length)})
          </div>
          {ips.length === 0 ? (
            <div className="text-white/30 text-[11px]">no IPs recorded</div>
          ) : (
            <div className="flex flex-wrap gap-1.5">
              {visibleIps.map((ip) => (
                <ValueChip key={ip} value={ip} mono />
              ))}
              {!showAllIps && ips.length > IP_VISIBLE && (
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    setShowAllIps(true);
                  }}
                  className="text-[10px] text-primary-light hover:underline px-1 py-[3px]"
                >
                  +{ips.length - IP_VISIBLE} more
                </button>
              )}
            </div>
          )}
        </div>

        {/* login countries */}
        <div>
          <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
            Login Countries ({fmtNumber(countries.length)})
          </div>
          {countries.length === 0 ? (
            <div className="text-white/30 text-[11px]">no countries recorded</div>
          ) : (
            <div className="flex flex-wrap gap-1.5">
              {countries.map((c) => (
                <CountryChip key={c.code} code={c.code} count={c.count} />
              ))}
            </div>
          )}
        </div>

        {/* known devices */}
        <div>
          <div className="text-[10px] uppercase tracking-wider text-white/40 mb-2">
            Known Devices ({fmtNumber(devices.length)})
          </div>
          {devices.length === 0 ? (
            <div className="text-white/30 text-[11px]">no devices recorded</div>
          ) : (
            <div className="flex flex-wrap gap-1.5">
              {visibleDevices.map((d) => (
                <ValueChip key={d} value={d} />
              ))}
              {!showAllDevices && devices.length > DEVICE_VISIBLE && (
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    setShowAllDevices(true);
                  }}
                  className="text-[10px] text-primary-light hover:underline px-1 py-[3px]"
                >
                  +{devices.length - DEVICE_VISIBLE} more
                </button>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
