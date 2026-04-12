export default function StatCard({ label, value, hint }) {
  return (
    <div className="bg-surface border border-border p-4">
      <div className="text-[10px] uppercase tracking-[0.25em] text-muted">{label}</div>
      <div className="mt-2 font-display text-3xl text-accent leading-none">{value}</div>
      {hint && <div className="mt-2 text-[11px] text-muted">{hint}</div>}
    </div>
  );
}
