import { fmtCompact } from "../utils/format.js";

// SVG stat ring used on the Dashboard. Renders a full circle in the
// requested accent color with a large bold number in the center.
//
// The outer track is drawn first (subtle white), the accent circle
// rotates -90deg so its stroke start sits at 12 o'clock.

export default function StatRing({ value, label, color, size = 128 }) {
  const stroke = 6;
  const r = (size - stroke) / 2;
  const cx = size / 2;
  const cy = size / 2;
  const circumference = 2 * Math.PI * r;

  return (
    <div className="card p-5 flex flex-col items-center animate-fade-in">
      <div className="relative" style={{ width: size, height: size }}>
        <svg width={size} height={size}>
          <circle
            cx={cx}
            cy={cy}
            r={r}
            fill="none"
            stroke="rgba(255,255,255,0.08)"
            strokeWidth={stroke}
          />
          <circle
            cx={cx}
            cy={cy}
            r={r}
            fill="none"
            stroke={color}
            strokeWidth={stroke}
            strokeLinecap="round"
            strokeDasharray={`${circumference} ${circumference}`}
            transform={`rotate(-90 ${cx} ${cy})`}
          />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-2xl font-bold text-white tabular-nums">
            {fmtCompact(value)}
          </span>
        </div>
      </div>
      <div
        className="mt-3 text-[10px] uppercase tracking-[0.18em] font-medium"
        style={{ color }}
      >
        {label}
      </div>
    </div>
  );
}
