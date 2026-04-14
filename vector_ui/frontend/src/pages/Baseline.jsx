import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import Avatar from "../components/Avatar.jsx";
import api from "../api.js";

function timeAgo(ts) {
  if (!ts) return "never";
  const diff = Math.floor((Date.now() - new Date(ts)) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff/60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff/3600)}h ago`;
  return `${Math.floor(diff/86400)}d ago`;
}

export default function Baseline() {
  const [stats, setStats] = useState(null);
  const [rows, setRows] = useState([]);
  const [search, setSearch] = useState("");
  const [expanded, setExpanded] = useState(null);

  useEffect(() => {
    api.baselineStats().then(setStats).catch(() => {});
    api.baselineList({ limit: 100 }).then(setRows).catch(() => {});
  }, []);

  useEffect(() => {
    const t = setTimeout(() => {
      api.baselineList({ limit: 100, search }).then(setRows).catch(() => {});
    }, 300);
    return () => clearTimeout(t);
  }, [search]);

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Baseline</h1>
        <p className="text-white/50 text-sm mt-1">Behavioral profiles for anomaly detection</p>
      </div>

      {stats && (
        <div className="grid grid-cols-3 gap-4">
          <div className="bg-white/5 rounded-xl p-4 border border-white/10">
            <div className="text-xs text-white/40 uppercase tracking-wider">Profiles Built</div>
            <div className="text-3xl font-bold text-blue-400 mt-2">{stats.total_baselines}</div>
            <div className="text-xs text-white/30 mt-1">last run {timeAgo(stats.last_computed)}</div>
          </div>
          <div className="bg-white/5 rounded-xl p-4 border border-white/10">
            <div className="text-xs text-white/40 uppercase tracking-wider">Fresh (&lt;1hr)</div>
            <div className="text-3xl font-bold text-green-400 mt-2">{stats.fresh}</div>
          </div>
          <div className="bg-white/5 rounded-xl p-4 border border-white/10">
            <div className="text-xs text-white/40 uppercase tracking-wider">Avg Known IPs</div>
            <div className="text-3xl font-bold text-white/60 mt-2">{Math.round(stats.avg_known_ips || 0)}</div>
          </div>
        </div>
      )}

      <input
        className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2 text-sm text-white placeholder-white/30 focus:outline-none focus:border-blue-500"
        placeholder="Search by email..."
        value={search}
        onChange={e => setSearch(e.target.value)}
      />

      {rows.length === 0 ? (
        <div className="text-center py-16 text-white/30">No baseline profiles yet</div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="text-white/30 text-xs uppercase tracking-wider border-b border-white/5">
              <th className="text-left py-3 px-4">User</th>
              <th className="text-left py-3 px-4">Known IPs</th>
              <th className="text-left py-3 px-4">Countries</th>
              <th className="text-left py-3 px-4">Devices</th>
              <th className="text-left py-3 px-4">Last Computed</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => (
              <>
                <tr
                  key={r.user_id}
                  className="border-b border-white/5 hover:bg-white/5 cursor-pointer"
                  onClick={() => setExpanded(expanded === r.user_id ? null : r.user_id)}
                >
                  <td className="py-3 px-4">
                    <div className="flex items-center gap-2">
                      <Avatar name={r.user_id} size={28} />
                      <span className="text-white/80">{r.user_id}</span>
                    </div>
                  </td>
                  <td className="py-3 px-4 text-white/60">{r.ip_count}</td>
                  <td className="py-3 px-4 text-white/60">{r.country_count}</td>
                  <td className="py-3 px-4 text-white/60">{r.device_count}</td>
                  <td className="py-3 px-4 text-white/40">{timeAgo(r.computed_at)}</td>
                </tr>
                {expanded === r.user_id && (
                  <tr key={r.user_id + "-exp"} className="bg-white/3">
                    <td colSpan={5} className="px-6 py-4">
                      <div className="grid grid-cols-3 gap-6 text-xs">
                        <div>
                          <div className="text-white/40 mb-2 font-semibold uppercase">Known IPs</div>
                          {(r.known_ips || []).slice(0, 5).map(ip => (
                            <div key={ip} className="font-mono text-white/60 py-0.5">{ip}</div>
                          ))}
                          {(r.known_ips || []).length > 5 && (
                            <div className="text-white/30">+{r.known_ips.length - 5} more</div>
                          )}
                        </div>
                        <div>
                          <div className="text-white/40 mb-2 font-semibold uppercase">Countries</div>
                          {(r.login_countries || []).map(c => (
                            <div key={c} className="text-white/60 py-0.5">{c}</div>
                          ))}
                        </div>
                        <div>
                          <div className="text-white/40 mb-2 font-semibold uppercase">Devices</div>
                          {(r.known_devices || []).slice(0, 5).map(d => (
                            <div key={d} className="text-white/60 py-0.5">{d}</div>
                          ))}
                        </div>
                      </div>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
