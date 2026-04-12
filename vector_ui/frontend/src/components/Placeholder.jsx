export default function Placeholder({ title, blurb }) {
  return (
    <div className="flex flex-col items-center justify-center text-center py-24 text-muted">
      <svg
        width="84"
        height="84"
        viewBox="0 0 84 84"
        fill="none"
        className="opacity-40 mb-5"
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
      <div className="font-display text-xl tracking-[0.25em] text-slate-200">
        {title.toUpperCase()}
      </div>
      <div className="text-[11px] uppercase tracking-[0.3em] mt-2 text-muted">
        Coming in Phase 2
      </div>
      {blurb && (
        <div className="text-xs mt-6 max-w-md leading-relaxed opacity-70">
          {blurb}
        </div>
      )}
    </div>
  );
}
