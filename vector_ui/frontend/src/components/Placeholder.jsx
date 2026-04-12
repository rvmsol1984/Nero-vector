// Single-line empty state used by the four Phase-2 nav items. Matches
// the spec: centered icon + one muted line of text, nothing else.

export default function Placeholder({ message }) {
  return (
    <div className="flex flex-col items-center justify-center text-center py-28 text-muted">
      <svg
        width="84"
        height="84"
        viewBox="0 0 84 84"
        fill="none"
        className="opacity-30 mb-5"
      >
        <rect
          x="12"
          y="12"
          width="60"
          height="60"
          stroke="currentColor"
          strokeWidth="1.5"
          strokeDasharray="4 5"
          rx="3"
        />
        <circle cx="42" cy="42" r="18" stroke="currentColor" strokeWidth="1.5" />
        <path
          d="M42 26 V42 L54 42"
          stroke="currentColor"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      </svg>
      <div className="text-xs text-muted">{message}</div>
    </div>
  );
}
