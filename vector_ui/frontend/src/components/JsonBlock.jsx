// Minimal JSON syntax highlighter. Escapes HTML first and then wraps
// tokens in <span class="text-*"> so we can use Tailwind's palette.
//
// Keys   -> accent
// Strings-> success
// Numbers-> warning
// Bool   -> critical
// null   -> muted

function escapeHtml(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

const TOKEN_RE =
  /("(\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g;

function highlight(obj) {
  const pretty = JSON.stringify(obj, null, 2);
  if (pretty === undefined) return "";
  const escaped = escapeHtml(pretty);
  return escaped.replace(TOKEN_RE, (match) => {
    let cls = "text-warning"; // default: number
    if (/^"/.test(match)) {
      cls = /:$/.test(match) ? "text-accent" : "text-success";
    } else if (/true|false/.test(match)) {
      cls = "text-critical";
    } else if (/null/.test(match)) {
      cls = "text-muted";
    }
    return `<span class="${cls}">${match}</span>`;
  });
}

export default function JsonBlock({ data, loading = false }) {
  if (loading) {
    return (
      <div
        className="text-[11px] text-muted p-3 border"
        style={{ background: "#080b0f", borderColor: "#21262d" }}
      >
        loading raw event…
      </div>
    );
  }
  if (data === null || data === undefined) return null;
  return (
    <pre
      className="text-[11px] leading-[1.55] overflow-x-auto p-3 border whitespace-pre"
      style={{
        background: "#080b0f",
        borderColor: "#21262d",
        fontFamily: '"JetBrains Mono", ui-monospace, monospace',
      }}
      dangerouslySetInnerHTML={{ __html: highlight(data) }}
    />
  );
}
