import { useEffect, useMemo, useState } from 'react';
import { api, AuthError, filterQuery, send, type BattleListRow } from '../../api';
import { compare, PlayersTable, SortHead, useSort } from '../../components/PlayersTable';
import { Panel, Skeleton } from '../../components/ui';
import { fmt, fmt1, fmtPct, pct, plural, winColor, WN8_HINT, wn8Color } from '../../format';
import { useAnalytics } from './Layout';
import { Empty } from './Overview';

const MODE_LABEL: Record<string, string> = { clan: 'Клановый', onslaught: 'Натиск', standard: 'Стандартный', other: 'Другое' };
const RESULT: Record<string, [string, string]> = {
  win: ['Победа', 'text-win bg-win/12'], loss: ['Поражение', 'text-loss bg-loss/12'], draw: ['Ничья', 'text-draw bg-draw/12'],
};

function WinBar({ rate }: { rate: number | null }) {
  return (
    <div className="flex items-center justify-end gap-2.5">
      <span className="num" style={{ color: winColor(rate) }}>{fmtPct(rate)}</span>
      <span className="h-1.5 w-20 overflow-hidden rounded-full bg-surface-3">
        <span className="block h-full rounded-full" style={{ width: `${rate ?? 0}%`, background: winColor(rate) }} />
      </span>
    </div>
  );
}

function Search({ value, onChange, placeholder }: { value: string; onChange: (v: string) => void; placeholder: string }) {
  return (
    <input type="search" value={value} onChange={e => onChange(e.target.value)} placeholder={placeholder}
      className="h-9 w-56 max-w-full rounded-md border border-line bg-surface-2 px-3 text-[13px] outline-none placeholder:text-dim focus:border-accent" />
  );
}

const thead = 'border-y border-line-soft text-[12px] text-muted';
const row = 'border-b border-line-soft transition-colors hover:bg-surface-2';

/* --- players: everything per player, hide strangers from your own analytics --- */

export function PlayersTab() {
  const { summary, minBattles, setMinBattles, refresh, filters } = useAnalytics();
  const [busy, setBusy] = useState(false);
  if (!summary) return <Skeleton className="h-[420px]" />;
  if (!summary.totals.total_battles) return <Empty days={filters.days} />;

  const act = async (fn: () => Promise<unknown>) => {
    setBusy(true);
    try { await fn(); refresh(); } catch (e) { alert((e as Error).message); } finally { setBusy(false); }
  };
  const hide = (name: string) => {
    if (confirm(`Скрыть «${name}» из своей аналитики? Данные боёв останутся.`)) {
      act(() => send('/api/players/hidden', 'POST', { name }));
    }
  };
  return (
    <div className={`space-y-4 ${busy ? 'pointer-events-none opacity-60' : ''}`}>
      {summary.hidden.length > 0 && (
        <div className="flex flex-wrap items-center gap-2 rounded-lg border border-line-soft bg-surface px-4 py-3 text-[13px]">
          <span className="font-semibold text-muted">Скрыты:</span>
          {summary.hidden.map(name => (
            <button key={name} type="button" title="Вернуть в аналитику"
              onClick={() => act(() => send(`/api/players/hidden/${encodeURIComponent(name)}`, 'DELETE'))}
              className="rounded-full border border-line px-2.5 py-0.5 transition-colors hover:border-accent hover:text-accent">
              {name} ↺
            </button>
          ))}
        </div>
      )}
      <PlayersTable rows={summary.players} total={summary.players_total} minBattles={minBattles}
        onMinBattles={setMinBattles} detailed onHide={hide} limit={50} />
    </div>
  );
}

/* --- tanks: a player on each tank, the same players as the table above --- */

type TankKey = 'name' | 'vehicle' | 'battles' | 'winrate' | 'wn8' | 'survival' | 'avg_damage' | 'avg_assists' | 'avg_blocked'
  | 'avg_frags' | 'avg_shots' | 'accuracy' | 'penetration';

export function TanksTab() {
  const { summary, filters } = useAnalytics();
  const [q, setQ] = useState('');
  const { sort, onSort } = useSort<TankKey>('battles');
  const rows = useMemo(() => (summary?.vehicles ?? [])
    .map(v => ({ ...v, winrate: pct(v.wins, v.battles) ?? 0 }))
    .filter(v => !q || `${v.name} ${v.vehicle}`.toLowerCase().includes(q.toLowerCase()))
    .sort((a, b) => compare(a[sort.key], b[sort.key], sort.desc)), [summary, q, sort]);
  if (!summary) return <Skeleton className="h-[420px]" />;
  if (!summary.totals.total_battles) return <Empty days={filters.days} />;
  const heads: [TankKey, string, string?][] = [
    ['name', 'Игрок', 'text-left'], ['vehicle', 'Танк', 'text-left'], ['battles', 'Бои'], ['winrate', 'Победы'],
    ['wn8', 'WN8'], ['survival', 'Выжил'], ['avg_damage', 'Урон'], ['avg_assists', 'Ассист'], ['avg_blocked', 'Блок'], ['avg_frags', 'Фраги'],
    ['avg_shots', 'Выстрелы'], ['accuracy', 'Точность'], ['penetration', 'Пробитие'],
  ];
  return (
    <Panel title={<>Игрок × танк <span className="ml-1 text-[13px] font-medium text-dim">{rows.length}</span></>}
      aside={<Search value={q} onChange={setQ} placeholder="Игрок или танк" />}>
      <p className="px-5 pt-1 text-[12px] text-dim">Те же игроки, что и в таблице игроков, с тем же порогом боёв.</p>
      <div className="mt-3 overflow-x-auto">
        <table className="w-full min-w-[1100px] border-collapse text-[13.5px]">
          <thead><tr className={thead}>
            {heads.map(([k, l, a]) => <SortHead key={k} k={k} label={l} align={a} title={k === 'wn8' ? WN8_HINT : undefined} sort={sort} onSort={onSort} />)}
          </tr></thead>
          <tbody>
            {rows.map(v => (
              <tr key={`${v.name}|${v.vehicle}`} className={row}>
                <td className="max-w-[200px] truncate px-3 py-2.5 font-semibold">{v.name}</td>
                <td className="px-3 py-2.5 text-muted">{v.vehicle}</td>
                <td className="num px-3 py-2.5 text-right">{fmt(v.battles)}</td>
                <td className="px-3 py-2.5"><WinBar rate={v.winrate} /></td>
                <td className="num px-3 py-2.5 text-right font-semibold" style={{ color: wn8Color(v.wn8) }}>{fmt(v.wn8)}</td>
                <td className="num px-3 py-2.5 text-right">{fmtPct(v.survival)}</td>
                <td className="num px-3 py-2.5 text-right">{fmt(v.avg_damage)}</td>
                <td className="num px-3 py-2.5 text-right">{fmt(v.avg_assists)}</td>
                <td className="num px-3 py-2.5 text-right">{fmt(v.avg_blocked)}</td>
                <td className="num px-3 py-2.5 text-right">{fmt1(v.avg_frags)}</td>
                <td className="num px-3 py-2.5 text-right">{fmt1(v.avg_shots)}</td>
                <td className="num px-3 py-2.5 text-right">{fmtPct(v.accuracy)}</td>
                <td className="num px-3 py-2.5 text-right">{fmtPct(v.penetration)}</td>
              </tr>
            ))}
            {!rows.length && <tr><td colSpan={heads.length} className="py-10 text-center text-muted">Ничего не нашлось</td></tr>}
          </tbody>
        </table>
      </div>
      <div className="h-3" />
    </Panel>
  );
}

/* --- maps --- */

type MapKey = 'map_name' | 'battles' | 'wins' | 'losses' | 'rate';

export function MapsTab() {
  const { summary, filters } = useAnalytics();
  const { sort, onSort } = useSort<MapKey>('battles');
  const rows = useMemo(() => (summary?.maps ?? [])
    .map(m => ({ ...m, rate: pct(m.wins, m.battles) ?? 0 }))
    .sort((a, b) => compare(a[sort.key], b[sort.key], sort.desc)), [summary, sort]);
  if (!summary) return <Skeleton className="h-[420px]" />;
  if (!summary.totals.total_battles) return <Empty days={filters.days} />;
  const heads: [MapKey, string, string?][] = [['map_name', 'Карта', 'text-left'], ['battles', 'Бои'], ['wins', 'Победы'], ['losses', 'Поражения'], ['rate', '% побед']];
  return (
    <Panel title={<>Карты <span className="ml-1 text-[13px] font-medium text-dim">{rows.length}</span></>}>
      <p className="px-5 pt-1 text-[12px] text-dim">Карты, где боёв меньше трёх, — скорее случайность, чем закономерность.</p>
      <div className="mt-3 overflow-x-auto">
        <table className="w-full min-w-[620px] border-collapse text-[13.5px]">
          <thead><tr className={thead}>
            {heads.map(([k, l, a]) => <SortHead key={k} k={k} label={l} align={a} sort={sort} onSort={onSort} />)}
          </tr></thead>
          <tbody>
            {rows.map(m => (
              <tr key={m.map_name} className={`${row} ${m.battles < 3 ? 'text-muted' : ''}`}>
                <td className="px-3 py-2.5 font-semibold">{m.map_name}</td>
                <td className="num px-3 py-2.5 text-right">{fmt(m.battles)}</td>
                <td className="num px-3 py-2.5 text-right text-win">{fmt(m.wins)}</td>
                <td className="num px-3 py-2.5 text-right text-loss">{fmt(m.losses)}</td>
                <td className="px-3 py-2.5"><WinBar rate={m.rate} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="h-3" />
    </Panel>
  );
}

/* --- battles: the list itself; your own uploads can be removed from your history --- */

export function BattlesTab() {
  const { filters, refresh, version } = useAnalytics();
  const [battles, setBattles] = useState<BattleListRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [q, setQ] = useState('');
  const [limit, setLimit] = useState(100);

  useEffect(() => {
    let alive = true;
    setBattles(null);
    api<{ battles: BattleListRow[] }>(`/api/battles?${filterQuery(filters)}`)
      .then(r => { if (alive) setBattles(r.battles); })
      .catch(err => { if (alive && !(err instanceof AuthError)) setError(err.message); });
    return () => { alive = false; };
  }, [filters, version]);

  if (error) return <div className="rounded-lg border border-loss/40 bg-loss/10 px-4 py-3 text-[13px]">{error}</div>;
  if (!battles) return <Skeleton className="h-[420px]" />;
  if (!battles.length) return <Empty days={filters.days} />;

  const shown = battles.filter(b => !q || `${b.map_name} ${b.filename}`.toLowerCase().includes(q.toLowerCase()));
  const remove = async (b: BattleListRow) => {
    if (!confirm(`Убрать бой на карте «${b.map_name}» из своей истории?`)) return;
    try { await send(`/api/battles/${b.id}`, 'DELETE'); refresh(); } catch (e) { alert((e as Error).message); }
  };
  return (
    <Panel title={<>Бои <span className="ml-1 text-[13px] font-medium text-dim">{battles.length} {plural(battles.length, 'бой', 'боя', 'боёв')}</span></>}
      aside={<Search value={q} onChange={setQ} placeholder="Карта или файл" />}>
      <div className="mt-3 overflow-x-auto">
        <table className="w-full min-w-[760px] border-collapse text-[13.5px]">
          <thead><tr className={thead}>
            {['Дата', 'Карта', 'Режим', 'Результат', 'Игроков', 'Файл', ''].map((h, i) => (
              <th key={i} scope="col" className={`px-3 py-2.5 font-semibold ${i === 4 ? 'text-right' : 'text-left'}`}>{h}</th>
            ))}
          </tr></thead>
          <tbody>
            {shown.slice(0, limit).map(b => {
              const [label, cls] = RESULT[b.result] ?? RESULT.draw;
              return (
                <tr key={b.id} className={`group ${row}`}>
                  <td className="num px-3 py-2.5 text-muted">{b.date_time.slice(0, 16).replace('T', ' ')}</td>
                  <td className="px-3 py-2.5 font-semibold">{b.map_name}</td>
                  <td className="px-3 py-2.5 text-muted">{MODE_LABEL[b.mode ?? 'other'] ?? MODE_LABEL.other}</td>
                  <td className="px-3 py-2.5"><span className={`rounded px-2 py-0.5 text-[12px] font-semibold ${cls}`}>{label}</span></td>
                  <td className="num px-3 py-2.5 text-right">{b.player_count}</td>
                  <td className="max-w-[220px] truncate px-3 py-2.5 text-[12px] text-dim" title={b.filename}>
                    {b.mine && <span className="mr-2 rounded bg-accent-soft px-1.5 py-0.5 text-[11px] font-bold text-accent">мой</span>}
                    {b.filename}
                  </td>
                  <td className="px-2 text-right">
                    {b.mine && (
                      <button type="button" onClick={() => remove(b)} title="Убрать из своей истории" aria-label="Удалить бой"
                        className="rounded px-2 py-1 text-dim opacity-0 transition-opacity hover:bg-surface-3 hover:text-loss focus:opacity-100 group-hover:opacity-100">✕</button>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {shown.length > limit ? (
        <button type="button" onClick={() => setLimit(l => l + 200)}
          className="w-full py-3 text-[12.5px] font-semibold text-muted hover:text-accent">Показать ещё</button>
      ) : <div className="h-3" />}
    </Panel>
  );
}
