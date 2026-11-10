import { useMemo, useState, type ReactNode } from 'react';
import type { PlayerRow } from '../api';
import { fmt, fmt1, fmtPct, fmtTime, pct, winColor } from '../format';
import { Panel, Segmented } from './ui';

type SortKey = 'name' | 'battles' | 'winrate' | 'survival' | 'avg_damage' | 'avg_assists' | 'avg_blocked'
  | 'avg_received' | 'avg_frags' | 'avg_spotted' | 'avg_shots' | 'avg_hits' | 'avg_piercings' | 'accuracy'
  | 'penetration' | 'avg_xp' | 'avg_lifetime';

/** Column: key, header, tooltip. Every number is an average per battle unless the tooltip says otherwise. */
type Column = [SortKey, string, string?];

const SURVIVAL: Column = ['survival', 'Выжил', 'Доля боёв, которые игрок пережил'];
const ACCURACY: Column = ['accuracy', 'Точность', 'Попадания от всех выстрелов'];

const BASE: Column[] = [
  ['battles', 'Бои'], ['winrate', 'Победы'], ['avg_damage', 'Урон'], ['avg_assists', 'Ассист'],
  ['avg_blocked', 'Блок'], ['avg_frags', 'Фраги'], ACCURACY,
  ['penetration', 'Пробитие', 'Пробития от всех попаданий'], SURVIVAL,
];
const FULL: Column[] = [
  ['battles', 'Бои'], ['winrate', 'Победы'], SURVIVAL,
  ['avg_lifetime', 'Жизнь', 'Среднее время жизни в бою, мин:сек'],
  ['avg_damage', 'Урон'], ['avg_assists', 'Ассист'], ['avg_blocked', 'Блок'],
  ['avg_received', 'Получено', 'Средний полученный урон'],
  ['avg_frags', 'Фраги'],
  ['avg_spotted', 'Засвет', 'Сколько противников обнаружил за бой'],
  ['avg_shots', 'Выстрелы', 'Среднее число выстрелов за бой'],
  ['avg_hits', 'Попадания', 'Среднее число попаданий за бой'],
  ['avg_piercings', 'Пробития', 'Среднее число пробитий за бой'],
  ACCURACY,
  ['penetration', '% пробитий', 'Пробития от всех попаданий'],
  ['avg_xp', 'Опыт', 'Средний опыт за бой'],
];

export function SortHead<K extends string>({ k, label, sort, onSort, align = 'text-right', title, className = '' }:
  { k: K; label: string; sort: { key: K; desc: boolean }; onSort: (k: K) => void; align?: string; title?: string; className?: string }) {
  return (
    <th scope="col" className={`px-3 py-2.5 font-semibold ${align} ${className}`} title={title}
      aria-sort={sort.key === k ? (sort.desc ? 'descending' : 'ascending') : 'none'}>
      <button type="button" onClick={() => onSort(k)}
        className={`inline-flex items-center gap-1 whitespace-nowrap hover:text-ink ${sort.key === k ? 'text-ink' : ''}`}>
        {label}{sort.key === k && <span className="text-accent">{sort.desc ? '↓' : '↑'}</span>}
      </button>
    </th>
  );
}

export function useSort<K extends string>(initial: K) {
  const [sort, setSort] = useState<{ key: K; desc: boolean }>({ key: initial, desc: true });
  const onSort = (key: K) => setSort(s => ({ key, desc: s.key === key ? !s.desc : key !== 'name' }));
  return { sort, onSort };
}

export function compare(av: unknown, bv: unknown, desc: boolean) {
  const c = typeof av === 'string' ? av.localeCompare(bv as string, 'ru') : (Number(av) || 0) - (Number(bv) || 0);
  return desc ? -c : c;
}

/** Player averages. `detailed` shows every column (shooting, survival, spotting, XP); `onHide` adds a hide button. */
export function PlayersTable({ rows, total, minBattles, onMinBattles, detailed = false, onHide, limit = 12, title }:
  { rows: PlayerRow[]; total: number; minBattles: number; onMinBattles: (v: number) => void;
    detailed?: boolean; onHide?: (name: string) => void; limit?: number; title?: ReactNode }) {
  const { sort, onSort } = useSort<SortKey>('avg_damage');
  const [showAll, setShowAll] = useState(false);
  const columns = detailed ? FULL : BASE;
  const enriched = useMemo(() => rows.map(r => ({ ...r, winrate: pct(r.wins, r.battles) ?? 0 })), [rows]);
  const sorted = useMemo(() => [...enriched].sort((a, b) => compare(a[sort.key], b[sort.key], sort.desc)), [enriched, sort]);
  const maxDamage = Math.max(1, ...enriched.map(r => r.avg_damage));
  const shown = showAll ? sorted : sorted.slice(0, limit);

  return (
    <Panel title={title ?? <>Игроки <span className="ml-1 text-[13px] font-medium text-dim">{rows.length} из {total}</span></>}
      aside={
        <div className="flex items-center gap-2 text-[12px] text-muted">
          От боёв
          <Segmented label="Минимум боёв" value={String(minBattles)} onChange={v => onMinBattles(Number(v))}
            options={[['1', '1'], ['3', '3'], ['5', '5'], ['10', '10']]} />
        </div>}>
      <div className="mt-3 overflow-x-auto">
        <table className={`w-full border-collapse text-[13.5px] ${detailed ? '' : 'min-w-[900px]'}`}>
          <thead>
            <tr className="border-y border-line-soft text-[12px] text-muted">
              <th scope="col" className="w-10 px-3 py-2.5 text-left font-semibold">#</th>
              <SortHead k="name" label="Игрок" align="text-left" sort={sort} onSort={onSort}
                className="sticky left-0 z-10 bg-surface" />
              {columns.map(([k, l, t]) => <SortHead key={k} k={k} label={l} title={t} sort={sort} onSort={onSort} />)}
              {onHide && <th scope="col" className="w-10" />}
            </tr>
          </thead>
          <tbody>
            {shown.map((r, i) => (
              <tr key={r.name} className="group animate-fade border-b border-line-soft transition-colors hover:bg-surface-2"
                style={{ animationDelay: `${Math.min(i, 12) * 25}ms` }}>
                <td className="num px-3 py-2.5 text-dim">{i + 1}</td>
                <td className="sticky left-0 z-10 max-w-[220px] truncate bg-surface px-3 py-2.5 font-semibold transition-colors group-hover:bg-surface-2" title={r.name}>{r.name}</td>
                {columns.map(([k]) => <Cell key={k} k={k} r={r} maxDamage={maxDamage} />)}
                {onHide && (
                  <td className="px-2 text-right">
                    <button type="button" onClick={() => onHide(r.name)} title="Скрыть из своей аналитики"
                      aria-label={`Скрыть ${r.name}`}
                      className="rounded px-2 py-1 text-dim opacity-0 transition-opacity hover:bg-surface-3 hover:text-loss focus:opacity-100 group-hover:opacity-100">✕</button>
                  </td>
                )}
              </tr>
            ))}
            {!shown.length && (
              <tr><td colSpan={columns.length + 3} className="px-3 py-10 text-center text-muted">Нет игроков с таким числом боёв</td></tr>
            )}
          </tbody>
        </table>
      </div>
      {sorted.length > limit ? (
        <button type="button" onClick={() => setShowAll(v => !v)}
          className="w-full py-3 text-[12.5px] font-semibold text-muted transition-colors hover:text-accent">
          {showAll ? 'Свернуть' : `Показать всех (${sorted.length})`}
        </button>
      ) : <div className="h-3" />}
    </Panel>
  );
}

type Enriched = PlayerRow & { winrate: number };

function Cell({ k, r, maxDamage }: { k: SortKey; r: Enriched; maxDamage: number }) {
  const td = 'num px-3 py-2.5 text-right';
  switch (k) {
    case 'winrate':
      return (
        <td className="px-3 py-2.5">
          <div className="flex items-center justify-end gap-2.5">
            <span className="num" style={{ color: winColor(r.winrate) }}>{fmt1(r.winrate)}%</span>
            <span className="h-1.5 w-16 overflow-hidden rounded-full bg-surface-3">
              <span className="block h-full rounded-full" style={{ width: `${r.winrate}%`, background: winColor(r.winrate) }} />
            </span>
          </div>
        </td>
      );
    case 'avg_damage':
      return (
        <td className="px-3 py-2.5 text-right">
          <span className="num rounded px-1.5 py-0.5" title={r.avg_dmg_win != null
            ? `В победах ${fmt(r.avg_dmg_win)}, в поражениях ${fmt(r.avg_dmg_loss)}` : undefined}
            style={{ background: `rgb(232 145 58 / ${(0.25 * r.avg_damage) / maxDamage})` }}>{fmt(r.avg_damage)}</span>
        </td>
      );
    case 'avg_frags': case 'avg_spotted': case 'avg_shots': case 'avg_hits': case 'avg_piercings':
      return <td className={td}>{fmt1(r[k])}</td>;
    case 'accuracy': case 'penetration': case 'survival':
      return <td className={td}>{fmtPct(r[k])}</td>;
    case 'avg_lifetime': return <td className={td}>{fmtTime(r.avg_lifetime)}</td>;
    case 'battles': return <td className={td}>{fmt(r.battles)}</td>;
    default: return <td className={td}>{fmt(r[k as 'avg_assists'])}</td>;
  }
}
