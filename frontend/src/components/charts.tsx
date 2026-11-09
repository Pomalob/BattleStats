import { useId, useMemo, useState } from 'react';
import { shortDay } from '../format';

/** A tiny trend line for KPI cards. */
export function Sparkline({ values, color = 'var(--color-accent)' }: { values: (number | null)[]; color?: string }) {
  const id = useId();
  const pts = values.map((v, i) => [i, v] as const).filter((p): p is readonly [number, number] => p[1] != null);
  if (pts.length < 2) return <div className="h-10" />;
  const w = 160, h = 40;
  const ys = pts.map(p => p[1]);
  const min = Math.min(...ys), max = Math.max(...ys), span = max - min || 1;
  const x = (i: number) => (i / (values.length - 1)) * w;
  const y = (v: number) => h - 3 - ((v - min) / span) * (h - 6);
  const line = pts.map(([i, v], k) => `${k ? 'L' : 'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join('');
  const area = `${line}L${x(pts[pts.length - 1][0])},${h}L${x(pts[0][0])},${h}Z`;
  return (
    <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" className="h-10 w-full" aria-hidden="true">
      <defs>
        <linearGradient id={id} x1="0" x2="0" y1="0" y2="1">
          <stop offset="0" stopColor={color} stopOpacity="0.28" />
          <stop offset="1" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={area} fill={`url(#${id})`} />
      <path d={line} fill="none" stroke={color} strokeWidth="1.6" vectorEffect="non-scaling-stroke"
        strokeDasharray="2000" className="animate-draw" />
    </svg>
  );
}

export interface Point { day: string; value: number | null; note?: string }

/** Day-by-day line with the period average dashed across it and a hover readout. */
export function LineChart({ points, format, domain }:
  { points: Point[]; format: (v: number) => string; domain?: [number, number] }) {
  const [hover, setHover] = useState<number | null>(null);
  const w = 720, h = 240, padL = 44, padR = 12, padT = 16, padB = 28;
  const valid = points.filter(p => p.value != null) as (Point & { value: number })[];

  const { lo, hi, ticks, avg } = useMemo(() => {
    const vs = valid.map(p => p.value);
    let lo = domain ? domain[0] : Math.min(...vs), hi = domain ? domain[1] : Math.max(...vs);
    if (!domain) { const pad = (hi - lo) * 0.15 || hi * 0.1 || 1; lo = Math.max(0, lo - pad); hi += pad; }
    const ticks = [0, 0.25, 0.5, 0.75, 1].map(t => lo + (hi - lo) * t);
    const avg = vs.length ? vs.reduce((a, b) => a + b, 0) / vs.length : null;
    return { lo, hi, ticks, avg };
  }, [valid, domain]);

  if (valid.length < 2) {
    return <div className="grid h-[240px] place-items-center text-[13px] text-dim">Мало дней с боями для графика</div>;
  }
  const x = (i: number) => padL + (i / Math.max(points.length - 1, 1)) * (w - padL - padR);
  const y = (v: number) => padT + (1 - (v - lo) / (hi - lo || 1)) * (h - padT - padB);
  const idx = points.map((p, i) => ({ ...p, i })).filter(p => p.value != null) as (Point & { value: number; i: number })[];
  const line = idx.map((p, k) => `${k ? 'L' : 'M'}${x(p.i).toFixed(1)},${y(p.value).toFixed(1)}`).join('');
  const area = `${line}L${x(idx[idx.length - 1].i)},${h - padB}L${x(idx[0].i)},${h - padB}Z`;
  const labelEvery = Math.max(1, Math.ceil(points.length / 6));
  const hp = hover != null ? idx.find(p => p.i === hover) : undefined;

  return (
    <div className="relative">
      <svg viewBox={`0 0 ${w} ${h}`} className="h-[240px] w-full" role="img" aria-label="График по дням"
        onMouseLeave={() => setHover(null)}
        onMouseMove={e => {
          const r = e.currentTarget.getBoundingClientRect();
          const px = ((e.clientX - r.left) / r.width) * w;
          let best = idx[0];
          for (const p of idx) if (Math.abs(x(p.i) - px) < Math.abs(x(best.i) - px)) best = p;
          setHover(best.i);
        }}>
        <defs>
          <linearGradient id="dyn-fill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0" stopColor="var(--color-accent)" stopOpacity="0.22" />
            <stop offset="1" stopColor="var(--color-accent)" stopOpacity="0" />
          </linearGradient>
        </defs>
        {ticks.map(t => (
          <g key={t}>
            <line x1={padL} x2={w - padR} y1={y(t)} y2={y(t)} stroke="var(--color-line-soft)" />
            <text x={padL - 8} y={y(t) + 4} textAnchor="end" className="num fill-dim text-[11px]">{format(t)}</text>
          </g>
        ))}
        {points.map((p, i) => i % labelEvery === 0 && (
          <text key={p.day} x={x(i)} y={h - 8} textAnchor="middle" className="fill-dim text-[11px]">{shortDay(p.day)}</text>
        ))}
        {avg != null && (
          <line x1={padL} x2={w - padR} y1={y(avg)} y2={y(avg)} stroke="var(--color-muted)" strokeDasharray="4 4" opacity="0.7" />
        )}
        <path d={area} fill="url(#dyn-fill)" className="animate-fade" />
        <path d={line} fill="none" stroke="var(--color-accent)" strokeWidth="2" strokeLinejoin="round"
          strokeDasharray="3000" style={{ ['--len' as string]: 3000 }} className="animate-draw" />
        {idx.map(p => (
          <circle key={p.day} cx={x(p.i)} cy={y(p.value)} r={p.i === hover ? 5 : 2.5}
            fill={p.i === hover ? 'var(--color-accent)' : 'var(--color-bg)'} stroke="var(--color-accent)" strokeWidth="1.5" />
        ))}
        {hp && <line x1={x(hp.i)} x2={x(hp.i)} y1={padT} y2={h - padB} stroke="var(--color-line)" />}
      </svg>
      {hp && (
        <div className="pointer-events-none absolute top-1 rounded-md border border-line bg-surface-3 px-2.5 py-1.5 text-[12px] shadow-lg"
          style={{ left: `clamp(0px, calc(${(x(hp.i) / w) * 100}% - 50px), calc(100% - 110px))` }}>
          <div className="num text-[14px] font-semibold">{format(hp.value)}</div>
          <div className="text-muted">{shortDay(hp.day)}{hp.note ? ` · ${hp.note}` : ''}</div>
        </div>
      )}
      <div className="mt-1 flex gap-4 px-1 text-[11.5px] text-muted">
        <span className="flex items-center gap-1.5"><i className="h-0.5 w-4 bg-accent" />По дням</span>
        <span className="flex items-center gap-1.5"><i className="w-4 border-t border-dashed border-muted" />Среднее за период</span>
      </div>
    </div>
  );
}
