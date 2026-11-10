import { useEffect, useMemo, useState } from 'react';
import { api, AuthError, filterQuery, type Daily, type Filters, type Summary } from '../../api';
import { ExplainButton, ExplainView, useExplain, useExplainStatus } from '../../components/AiExplain';
import { LineChart, Sparkline } from '../../components/charts';
import { PlayersTable } from '../../components/PlayersTable';
import { Panel, Segmented, Skeleton } from '../../components/ui';
import { fmt, fmt1, fmtPct, pct, plural } from '../../format';
import { useAnalytics } from './Layout';

interface Data { daily: Daily; prev: Daily | null; summary: Summary }

export function Overview() {
  const { filters, summary, minBattles, setMinBattles, version } = useAnalytics();
  const [daily, setDaily] = useState<{ daily: Daily; prev: Daily | null } | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setError(null);
    setDaily(null);
    Promise.all([
      api<Daily>(`/api/analytics/daily?${filterQuery(filters)}`),
      filters.days > 0 ? api<Daily>(`/api/analytics/daily?${filterQuery(filters, 1)}`) : Promise.resolve(null),
    ])
      .then(([d, prev]) => { if (alive) setDaily({ daily: d, prev }); })
      .catch(err => { if (alive && !(err instanceof AuthError)) setError(err.message); });
    return () => { alive = false; };
  }, [filters, version]);

  if (error) return <div className="rounded-lg border border-loss/40 bg-loss/10 px-4 py-3 text-[13px]">{error}</div>;
  if (!daily || !summary) return <Loading />;
  if (daily.daily.totals.battles === 0) return <Empty days={filters.days} />;
  return <Content key={JSON.stringify(filters)} data={{ ...daily, summary }} filters={filters}
    minBattles={minBattles} onMinBattles={setMinBattles} />;
}

export function Loading() {
  return (
    <div className="space-y-5">
      <div className="grid grid-cols-2 gap-4 xl:grid-cols-4">{[0, 1, 2, 3].map(i => <Skeleton key={i} className="h-[126px]" />)}</div>
      <div className="grid gap-4 xl:grid-cols-3"><Skeleton className="h-[330px] xl:col-span-2" /><Skeleton className="h-[330px]" /></div>
      <Skeleton className="h-[360px]" />
    </div>
  );
}

export function Empty({ days }: { days: number }) {
  return (
    <Panel className="px-6 py-14 text-center">
      <div className="mx-auto mb-3 grid size-12 place-items-center rounded-full bg-accent-soft text-accent">
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M17 8l-5-5-5 5M12 3v12" /></svg>
      </div>
      <h2 className="text-[16px] font-bold">{days ? 'За этот период боёв нет' : 'Пока нет боёв'}</h2>
      <p className="mx-auto mt-1 max-w-md text-[13px] text-muted">
        {days ? 'Попробуй период подлиннее или другой режим.' : 'Загрузи реплеи на «Быстром чеке» и отметь бои, которые нужно сохранить.'}
      </p>
      {!days && <a href="/" className="mt-5 inline-block rounded-md bg-accent px-4 py-2 text-[13px] font-bold text-bg hover:bg-accent-hover">Загрузить реплеи</a>}
    </Panel>
  );
}

type Tone = 'up' | 'down' | 'flat';

function Delta({ value, unit, tone }: { value: number; unit: string; tone: Tone }) {
  const color = tone === 'up' ? 'text-win' : tone === 'down' ? 'text-loss' : 'text-muted';
  const arrow = tone === 'up' ? '↑' : tone === 'down' ? '↓' : '→';
  return <span className={`num text-[12px] font-medium ${color}`}>{arrow} {fmt1(Math.abs(value))}{unit}</span>;
}

function Kpi({ label, value, delta, series, delay }:
  { label: string; value: string; delta: { value: number; unit: string } | null; series: (number | null)[]; delay: number }) {
  const tone: Tone = !delta || Math.abs(delta.value) < 0.05 ? 'flat' : delta.value > 0 ? 'up' : 'down';
  return (
    <div className="animate-rise rounded-lg border border-line-soft bg-surface px-5 pt-4" style={{ animationDelay: `${delay}ms` }}>
      <div className="text-[12.5px] font-semibold text-muted">{label}</div>
      <div className="mt-1 flex items-baseline gap-2.5">
        <span className="text-[30px] font-extrabold leading-none tracking-tight tabular-nums">{value}</span>
        {delta && <Delta {...delta} tone={tone} />}
      </div>
      <div className="-mx-1 mt-2"><Sparkline values={series} /></div>
    </div>
  );
}

const change = (cur: number | null | undefined, prev: number | null | undefined) =>
  cur != null && prev ? ((cur - prev) / prev) * 100 : null;

function Content({ data, filters, minBattles, onMinBattles }:
  { data: Data; filters: Filters; minBattles: number; onMinBattles: (v: number) => void }) {
  const days = filters.days;
  const { daily, prev, summary } = data;
  const t = daily.totals;
  const p = prev && prev.totals.battles > 0 ? prev.totals : null;
  const winrate = pct(t.wins, t.battles);
  const prevWinrate = p ? pct(p.wins, p.battles) : null;
  const series = (f: (d: (typeof daily.days)[number]) => number | null) => daily.days.map(f);
  const [metric, setMetric] = useState<'wins' | 'damage' | 'battles'>('wins');

  const delta = (v: number | null, unit = '%') => (p && v != null ? { value: v, unit } : null);
  // A long period day by day is a saw of 2-battle days: past three weeks of points, weeks read better.
  const buckets = daily.days.length > 21 ? byWeek(daily.days) : daily.days;
  const weekly = daily.days.length > 21;
  const points = buckets.map(d => ({
    day: d.day,
    value: metric === 'wins' ? pct(d.wins, d.battles) : metric === 'damage' ? d.avg_damage : d.battles,
    note: `${weekly ? 'неделя, ' : ''}${d.battles} ${plural(d.battles, 'бой', 'боя', 'боёв')}`,
  }));

  return (
    <>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi label="Бои" value={fmt(t.battles)} delay={0} delta={delta(change(t.battles, p?.battles))}
          series={series(d => d.battles)} />
        <Kpi label="Победы" value={fmtPct(winrate)} delay={40}
          delta={delta(winrate != null && prevWinrate != null ? winrate - prevWinrate : null, ' п.п.')}
          series={series(d => pct(d.wins, d.battles))} />
        <Kpi label="Урон, в среднем" value={fmt(t.avg_damage)} delay={80}
          delta={delta(change(t.avg_damage, p?.avg_damage))} series={series(d => d.avg_damage)} />
        <Kpi label="Ассист, в среднем" value={fmt(t.avg_assists)} delay={120}
          delta={delta(change(t.avg_assists, p?.avg_assists))} series={series(d => d.avg_assists)} />
      </div>
      {!days && <p className="-mt-2 text-[12px] text-dim">Изменение к прошлому периоду видно, когда выбран период.</p>}

      <div className="grid gap-4 xl:grid-cols-3">
        <Panel className="xl:col-span-2" title={weekly ? 'Динамика по неделям' : 'Динамика по дням'}
          aside={<Segmented label="Показатель" value={metric} onChange={setMetric}
            options={[['wins', 'Победы'], ['damage', 'Урон'], ['battles', 'Бои']]} />}>
          <div className="px-3 pb-4 pt-2">
            <LineChart key={metric} points={points}
              domain={metric === 'wins' ? [0, 100] : undefined}
              format={v => (metric === 'wins' ? `${Math.round(v)}%` : fmt(v))} />
          </div>
        </Panel>
        <Insights summary={summary} daily={daily} winrate={winrate} filters={filters} />
      </div>

      <PlayersTable rows={summary.players} total={summary.players_total} minBattles={minBattles} onMinBattles={onMinBattles} />
    </>
  );
}

type DayData = Daily['days'][number];

/** Days folded into weeks starting on Monday; averages weighted by battles. */
function byWeek(days: DayData[]): DayData[] {
  const weeks = new Map<string, DayData[]>();
  for (const d of days) {
    const date = new Date(`${d.day}T00:00:00`);
    date.setDate(date.getDate() - ((date.getDay() + 6) % 7));
    const key = date.toLocaleDateString('sv-SE');
    weeks.set(key, [...(weeks.get(key) ?? []), d]);
  }
  return [...weeks.entries()].map(([day, ds]) => {
    const battles = ds.reduce((s, d) => s + d.battles, 0);
    const avg = (f: (d: DayData) => number | null) => {
      const xs = ds.filter(d => f(d) != null);
      const w = xs.reduce((s, d) => s + d.battles, 0);
      return w ? Math.round(xs.reduce((s, d) => s + (f(d) as number) * d.battles, 0) / w) : null;
    };
    return {
      day, battles,
      wins: ds.reduce((s, d) => s + d.wins, 0), losses: ds.reduce((s, d) => s + d.losses, 0),
      draws: ds.reduce((s, d) => s + d.draws, 0),
      avg_damage: avg(d => d.avg_damage), avg_assists: avg(d => d.avg_assists),
      avg_blocked: avg(d => d.avg_blocked), avg_spotted: avg(d => d.avg_spotted), accuracy: avg(d => d.accuracy),
    };
  });
}

/* --- rule-based summary: what the numbers say, until the language model arrives --- */

interface Note { tone: 'good' | 'bad' | 'info'; text: string }

function notesFor(summary: Summary, daily: Daily, winrate: number | null): Note[] {
  const notes: Note[] = [];
  const t = daily.totals;
  if (winrate != null) {
    notes.push({ tone: winrate >= 50 ? 'good' : 'bad',
      text: `${fmtPct(winrate)} побед в ${fmt(t.battles)} ${plural(t.battles, 'бою', 'боях', 'боях')}.` });
  }
  // Trend: the later half of the period against the earlier one, weighted by battles.
  if (daily.days.length >= 4) {
    const half = Math.floor(daily.days.length / 2);
    const rate = (ds: typeof daily.days) => {
      const b = ds.reduce((s, d) => s + d.battles, 0);
      return b ? (ds.reduce((s, d) => s + d.wins, 0) / b) * 100 : null;
    };
    const early = rate(daily.days.slice(0, half)), late = rate(daily.days.slice(half));
    if (early != null && late != null && Math.abs(late - early) >= 3) {
      notes.push({ tone: late > early ? 'good' : 'bad',
        text: late > early
          ? `Вторая половина периода лучше первой: ${fmtPct(late)} побед против ${fmtPct(early)}.`
          : `Вторая половина периода хуже первой: ${fmtPct(late)} побед против ${fmtPct(early)}.` });
    }
  }
  const maps = summary.maps.filter(m => m.battles >= 3).map(m => ({ ...m, rate: (m.wins / m.battles) * 100 }));
  if (maps.length >= 2) {
    const sorted = [...maps].sort((a, b) => b.rate - a.rate);
    const best = sorted[0], worst = sorted[sorted.length - 1];
    notes.push({ tone: 'good', text: `Сильнее всего карта «${best.map_name}»: ${fmtPct(best.rate)} побед за ${best.battles} ${plural(best.battles, 'бой', 'боя', 'боёв')}.` });
    if (worst.rate < best.rate - 5) {
      notes.push({ tone: 'bad', text: `Просадка на карте «${worst.map_name}»: ${fmtPct(worst.rate)} побед за ${worst.battles} ${plural(worst.battles, 'бой', 'боя', 'боёв')}.` });
    }
  }
  const top = [...summary.players].sort((a, b) => b.avg_damage - a.avg_damage)[0];
  if (top) notes.push({ tone: 'info', text: `Больше всех урона у ${top.name}: ${fmt(top.avg_damage)} за бой в среднем.` });
  return notes.slice(0, 5);
}

function Insights({ summary, daily, winrate, filters }:
  { summary: Summary; daily: Daily; winrate: number | null; filters: Filters }) {
  const notes = useMemo(() => notesFor(summary, daily, winrate), [summary, daily, winrate]);
  const { status, reload } = useExplainStatus();
  const ai = useExplain(filters, reload);
  const dot = { good: 'bg-win', bad: 'bg-loss', info: 'bg-accent' };
  return (
    <Panel title={ai.answer ? 'Разбор от ИИ' : 'Коротко о периоде'}
      aside={!ai.answer && <ExplainButton status={status} filters={filters} busy={ai.busy} onRun={ai.run} />}>
      {ai.error && <div className="mx-5 mt-3 rounded-md border border-loss/40 bg-loss/10 px-3 py-2 text-[12.5px]">{ai.error}</div>}
      {ai.answer ? <ExplainView answer={ai.answer} cached={ai.cached} onBack={ai.reset} /> : (
      <ul className="space-y-3 px-5 pb-5 pt-3">
        {notes.map((n, i) => (
          <li key={i} className="flex animate-fade gap-3 text-[13.5px] leading-snug" style={{ animationDelay: `${i * 60}ms` }}>
            <span className={`mt-1.5 size-2 shrink-0 rounded-full ${dot[n.tone]}`} />
            <span>{n.text}</span>
          </li>
        ))}
      </ul>)}
    </Panel>
  );
}

