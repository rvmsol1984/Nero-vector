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

// Writes `text` to the clipboard, using the async Clipboard API when it's
// available (HTTPS / secure contexts) and falling back to the hidden-
// textarea + execCommand trick everywhere else. Returns true on success.
async function copyToClipboard(text) {
  if (navigator.clipboard && window.isSecureContext !== false) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      /* fall through to the textarea fallback */
    }
  }
  try {
    const el = document.createElement("textarea");
    el.value = text;
    el.setAttribute("readonly", "");
    el.style.position = "absolute";
    el.style.left = "-9999px";
    el.style.top = "0";
    document.body.appendChild(el);
    el.select();
    // execCommand is deprecated but still the only universal fallback
    // for non-HTTPS origins and older browsers.
    const ok = document.execCommand("copy");
    document.body.removeChild(el);
    return ok;
  } catch {
    return false;
  }
}

export default function JsonBlock({ data, loading = false, copyable = true }) {
  const [copied, setCopied] = useState(false);

  async function handleCopy(event) {
    // Stop the click from bubbling up into any parent "row toggle" button
    // that may have spawned this JsonBlock.
    event.stopPropagation();
    event.preventDefault();
    const ok = await copyToClipboard(JSON.stringify(data, null, 2));
    if (!ok) return;
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
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
          onClick={handleCopy}
          className={`absolute top-2 right-2 text-[10px] font-semibold uppercase tracking-wide px-2.5 py-1 border rounded-md transition-colors active:scale-95 ${
            copied
              ? "border-status-resolved/50 bg-status-resolved/15 text-status-resolved"
              : "border-white/10 bg-white/5 text-white/70 hover:text-white hover:border-white/30"
          }`}
        >
          {copied ? "Copied!" : "Copy"}
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
