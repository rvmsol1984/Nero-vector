// Syntax-highlighted JSON block. Matches the FieldDesk surface (background
// #0D1428, border rgba(255,255,255,0.08)) and ships with a copy button.
//
// Tokens are wrapped in spans via a token regex; the input is HTML-escaped
// first so user-controlled strings can't inject markup.

import { useState } from "react";

function escapeHtml(s) {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

const TOKEN_RE =
  /("(?:\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(?:true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g;

function highlight(obj) {
  const pretty = JSON.stringify(obj, null, 2);
  if (pretty === undefined) return "";
  const escaped = escapeHtml(pretty);
  return escaped.replace(TOKEN_RE, (match) => {
    let color = "#F97316"; // number
    if (/^"/.test(match)) {
      color = /:$/.test(match) ? "#3B82F6" : "#22C55E";
    } else if (/true|false/.test(match)) {
      color = "#EF4444";
    } else if (/null/.test(match)) {
      color = "rgba(255,255,255,0.4)";
    }
    return `<span style="color:${color}">${match}</span>`;
  });
}

export default function JsonBlock({ data, loading = false, copyable = true }) {
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(JSON.stringify(data, null, 2));
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      /* clipboard may be unavailable */
    }
  }

  if (loading) {
    return (
      <div
        className="text-xs text-white/40 p-4 border rounded-xl font-mono"
        style={{ background: "#0D1428", borderColor: "rgba(255,255,255,0.08)" }}
      >
        loading raw event…
      </div>
    );
  }
  if (data === null || data === undefined) return null;

  return (
    <div
      className="relative border rounded-xl overflow-hidden animate-slide-up"
      style={{ background: "#0D1428", borderColor: "rgba(255,255,255,0.08)" }}
    >
      {copyable && (
        <button
          type="button"
          onClick={copy}
          className="absolute top-2 right-2 text-[10px] uppercase tracking-wide px-2 py-1 border border-white/10 bg-white/5 text-white/70 hover:text-white hover:border-white/30 rounded-md transition-colors active:scale-95"
        >
          {copied ? "copied" : "copy"}
        </button>
      )}
      <pre
        className="text-[11px] leading-[1.55] overflow-x-auto p-4 pr-16 whitespace-pre"
        style={{
          fontFamily:
            'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace',
        }}
        dangerouslySetInnerHTML={{ __html: highlight(data) }}
      />
    </div>
  );
}
