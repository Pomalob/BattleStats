import { useTitle } from '../title';
import { useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, send, upload } from '../api';
import { Panel, Segmented } from '../components/ui';
import { fmt, fmt1, fmtPct, pct, plural } from '../format';
import { load, save } from '../storage';

/* What /api/upload returns for each replay. */
interface ParsedPlayer {
  name: string; vehicle: string; team: number; damage_dealt: number; damage_received: number;
  blocked: number; assists: number; frags: number; shots: number; hits: number; piercings: number;
  xp: number; survived: boolean;
  details?: { prebattleID?: number } | null;
}
interface ParsedBattle {
  filename: string; map_name: string; date_time: string; battle_hash: string; result: 'win' | 'loss' | 'draw';
  player_team: number; player_vehicle: string; complete: boolean; ticket: string;
  details?: { bonusType?: number; battleType?: number } | null;
  recorder?: { playerName?: string } | null;
  players: ParsedPlayer[];
}
interface UploadError { file: string; error: string }

// A replay is around a megabyte, so a whole replay folder is far past one request:
// send it in batches the server limits comfortably allow.
const BATCH_FILES = 20;
const BATCH_BYTES = 25 * 1024 * 1024;
const SAVE_BATCH = 100;

function intoBatches(files: File[]): File[][] {
  const batches: File[][] = [];
  let current: File[] = [], bytes = 0;
  for (const f of files) {
    if (current.length && (current.length >= BATCH_FILES || bytes + f.size > BATCH_BYTES)) {
      batches.push(current); current = []; bytes = 0;
    }
    current.push(f); bytes += f.size;
  }
  if (current.length) batches.push(current);
  return batches;
}

// Same rule as the server (database._SQUAD_FOCUS): in matchmade modes the rest of the team
// are strangers, so only the recorder and their platoon count.
const SQUAD_BONUS_TYPES = new Set([1, 43]);
function ourPlayers(b: ParsedBattle): ParsedPlayer[] {
  const side = b.players.filter(p => p.team === b.player_team);
  const bonus = Number(b.details?.bonusType ?? b.details?.battleType);
  if (!SQUAD_BONUS_TYPES.has(bonus)) return side;
  const me = side.find(p => p.name === b.recorder?.playerName);
  if (!me) return side;
  const platoon = me.details?.prebattleID;
  return side.filter(p => p === me || (platoon && p.details?.prebattleID === platoon));
}

type Metric = 'damage' | 'assists' | 'blocked' | 'received' | 'frags' | 'shots' | 'accuracy' | 'penetration' | 'survived' | 'xp';
const METRICS: [Metric, string][] = [
  ['damage', 'Урон'], ['assists', 'Ассист'], ['blocked', 'Блок'], ['received', 'Получено'], ['frags', 'Фраги'],
  ['shots', 'Выстрелы'], ['accuracy', 'Точность'], ['penetration', 'Пробитие'], ['survived', 'Выжил'], ['xp', 'Опыт'],
];
const value = (p: ParsedPlayer, m: Metric): number => ({
  damage: p.damage_dealt, assists: p.assists, blocked: p.blocked, received: p.damage_received, frags: p.frags,
  shots: p.shots, xp: p.xp, survived: p.survived ? 100 : 0,
  accuracy: p.shots ? (p.hits / p.shots) * 100 : 0,
  penetration: p.hits ? (p.piercings / p.hits) * 100 : 0,
})[m];
const PERCENT: Metric[] = ['accuracy', 'penetration', 'survived'];
/** One battle's cell. */
const show = (v: number | null, m: Metric) => v == null ? '·'
  : m === 'survived' ? (v ? 'да' : 'нет') : PERCENT.includes(m) ? `${Math.round(v)}%` : fmt(v);
/** The player's average over the battles they played. */
const showAvg = (v: number, m: Metric) =>
  PERCENT.includes(m) ? `${Math.round(v)}%` : m === 'frags' || m === 'shots' ? fmt1(v) : fmt(v);

export function QuickCheck() {
  useTitle('Быстрый чек');
  const [battles, setBattles] = useState<ParsedBattle[]>([]);
  const [errors, setErrors] = useState<UploadError[]>([]);
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [saveState, setSaveState] = useState<{ busy: boolean; text: string; ok?: boolean }>({ busy: false, text: '' });
  const [saved, setSaved] = useState<Set<string>>(new Set());
  const [drag, setDrag] = useState(false);
  const input = useRef<HTMLInputElement>(null);

  async function handle(list: FileList | null) {
    const files = [...(list ?? [])];
    if (!files.length) return;
    const replays = files.filter(f => f.name.toLowerCase().endsWith('.mtreplay'));
    const skipped: UploadError[] = files.filter(f => !replays.includes(f)).map(f => ({ file: f.name, error: 'Это не файл .mtreplay' }));
    setBattles([]); setErrors(skipped); setSaved(new Set()); setSaveState({ busy: false, text: '' });
    if (!replays.length) return;
    setProgress({ done: 0, total: replays.length });
    const got: ParsedBattle[] = [], errs: UploadError[] = [...skipped];
    let done = 0;
    for (const batch of intoBatches(replays)) {
      const form = new FormData();
      batch.forEach(f => form.append('files', f));
      try {
        const res = await upload<{ battles: ParsedBattle[]; errors: UploadError[] }>('/api/upload', form);
        got.push(...res.battles); errs.push(...res.errors);
      } catch (e) {
        errs.push(...batch.map(f => ({ file: f.name, error: (e as Error).message })));
        if (/лимит/i.test((e as Error).message)) break;
      }
      done += batch.length;
      setProgress({ done, total: replays.length });
    }
    got.sort((a, b) => a.date_time.localeCompare(b.date_time));
    setBattles(got); setErrors(errs); setProgress(null);
    setSelected(new Set(got.filter(b => b.complete).map(b => b.battle_hash)));
  }

  async function saveSelected() {
    const chosen = battles.filter(b => selected.has(b.battle_hash) && !saved.has(b.battle_hash));
    if (!chosen.length) return;
    setSaveState({ busy: true, text: 'Сохраняем…' });
    const total = { created: 0, linked: 0, skipped: 0 };
    try {
      for (let i = 0; i < chosen.length; i += SAVE_BATCH) {
        const part = chosen.slice(i, i + SAVE_BATCH);
        const r = await send<typeof total>('/api/battles/save', 'POST', { tickets: part.map(b => b.ticket) });
        total.created += r.created; total.linked += r.linked; total.skipped += r.skipped;
        setSaved(s => new Set([...s, ...part.map(b => b.battle_hash)]));
      }
      const parts = [total.created && `новых: ${total.created}`, total.linked && `добавлено к тебе: ${total.linked}`,
        total.skipped && `уже были: ${total.skipped}`].filter(Boolean);
      setSaveState({ busy: false, ok: true, text: `Сохранено — ${parts.join(', ') || 'ничего нового'}.` });
    } catch (e) {
      setSaveState({ busy: false, text: (e as Error).message });
    }
  }

  const complete = battles.filter(b => b.complete);
  const incomplete = battles.filter(b => !b.complete);

  return (
    <div className="mx-auto max-w-[1440px] space-y-5">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-[26px] font-extrabold tracking-tight">Быстрый чек</h1>
          <p className="mt-0.5 text-[13px] text-muted">Перетащи реплеи — разбор сразу, в базу попадёт только то, что отметишь.</p>
        </div>
        <DbStrip />
      </header>

      <div role="button" tabIndex={0} aria-label="Загрузить реплеи"
        onClick={() => input.current?.click()}
        onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); input.current?.click(); } }}
        onDragOver={e => { e.preventDefault(); setDrag(true); }}
        onDragLeave={() => setDrag(false)}
        onDrop={e => { e.preventDefault(); setDrag(false); handle(e.dataTransfer.files); }}
        className={`grid cursor-pointer place-items-center rounded-lg border-2 border-dashed px-6 py-10 text-center transition-colors duration-150 ${
          drag ? 'border-accent bg-accent-soft' : 'border-line hover:border-dim hover:bg-surface/60'}`}>
        <input ref={input} type="file" accept=".mtreplay" multiple hidden onChange={e => { handle(e.target.files); e.target.value = ''; }} />
        <div className="mb-3 grid size-12 place-items-center rounded-full bg-accent-soft text-accent">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M17 8l-5-5-5 5M12 3v12" /></svg>
        </div>
        {progress ? (
          <div className="w-full max-w-sm">
            <div className="text-[14px] font-semibold">Разбираем реплеи… {progress.done} из {progress.total}</div>
            <div className="mt-3 h-1.5 overflow-hidden rounded-full bg-surface-3">
              <div className="h-full rounded-full bg-accent transition-[width] duration-300" style={{ width: `${(progress.done / progress.total) * 100}%` }} />
            </div>
          </div>
        ) : (
          <>
            <div className="text-[15px] font-bold">Перетащи файлы <span className="num text-accent">.mtreplay</span> сюда</div>
            <div className="mt-1 text-[13px] text-muted">или нажми, чтобы выбрать. Можно всю папку реплеев «Мира танков» — отправим частями.</div>
          </>
        )}
      </div>

      {errors.length > 0 && (
        <Notice tone="bad" title={`${errors.length} ${plural(errors.length, 'файл', 'файла', 'файлов')} не удалось разобрать`}>
          {errors.slice(0, 8).map((e, i) => <div key={i}><span className="num text-ink">{e.file}</span> — {e.error}</div>)}
          {errors.length > 8 && <div>и ещё {errors.length - 8}</div>}
        </Notice>
      )}
      {incomplete.length > 0 && (
        <Notice tone="info" title={`${incomplete.length} ${plural(incomplete.length, 'реплей', 'реплея', 'реплеев')} без результатов боя`}>
          Запись оборвалась до конца боя, статистики в ней нет — такие бои не сохраняются.
        </Notice>
      )}

      {complete.length > 0 && (
        <>
          <Totals battles={complete} />
          <Matrix battles={complete} />
          <SaveBar battles={complete} selected={selected} setSelected={setSelected} saved={saved}
            state={saveState} onSave={saveSelected} />
        </>
      )}
    </div>
  );
}

function DbStrip() {
  const [t, setT] = useState<{ total_battles: number; wins: number } | null>(null);
  useEffect(() => { api<{ total_battles: number; wins: number }>('/api/analytics/totals').then(setT).catch(() => {}); }, []);
  if (!t || !t.total_battles) return null;
  return (
    <Link to="/analytics" className="flex items-center gap-4 rounded-lg border border-line-soft bg-surface px-4 py-2.5 text-[13px] transition-colors hover:border-line">
      <span><span className="text-muted">В базе </span><span className="num font-semibold">{fmt(t.total_battles)}</span>{' '}
        {plural(t.total_battles, 'бой', 'боя', 'боёв')}</span>
      <span><span className="text-muted">побед </span><span className="num font-semibold text-win">{fmtPct(pct(t.wins, t.total_battles))}</span></span>
      <span className="font-semibold text-accent">Аналитика →</span>
    </Link>
  );
}

function Notice({ tone, title, children }: { tone: 'bad' | 'info'; title: string; children: React.ReactNode }) {
  return (
    <div className={`animate-rise rounded-lg border px-4 py-3 text-[13px] ${tone === 'bad' ? 'border-loss/40 bg-loss/8' : 'border-line bg-surface'}`}>
      <div className="mb-1 font-bold">{title}</div>
      <div className="space-y-0.5 text-muted">{children}</div>
    </div>
  );
}

function Totals({ battles }: { battles: ParsedBattle[] }) {
  const n = battles.length;
  const count = (r: string) => battles.filter(b => b.result === r).length;
  const cards: [string, number, string][] = [['Победы', count('win'), 'var(--color-win)'], ['Поражения', count('loss'), 'var(--color-loss)'], ['Ничьи', count('draw'), 'var(--color-draw)']];
  return (
    <div className="grid grid-cols-2 gap-4 xl:grid-cols-4">
      <div className="animate-rise rounded-lg border border-line-soft bg-surface px-5 py-4">
        <div className="text-[12.5px] font-semibold text-muted">Боёв в разборе</div>
        <div className="mt-1 text-[30px] font-extrabold leading-none tabular-nums">{n}</div>
      </div>
      {cards.map(([label, v, color], i) => (
        <div key={label} className="animate-rise rounded-lg border border-line-soft bg-surface px-5 py-4" style={{ animationDelay: `${(i + 1) * 40}ms` }}>
          <div className="text-[12.5px] font-semibold text-muted">{label}</div>
          <div className="mt-1 flex items-baseline gap-2">
            <span className="text-[30px] font-extrabold leading-none tabular-nums" style={{ color }}>{v}</span>
            <span className="num text-[12px] text-muted">{fmtPct(pct(v, n))}</span>
          </div>
          <div className="mt-3 h-1 overflow-hidden rounded-full bg-surface-3">
            <div className="h-full rounded-full" style={{ width: `${pct(v, n) ?? 0}%`, background: color }} />
          </div>
        </div>
      ))}
    </div>
  );
}

/** Players × battles for one metric, with the average per player. */
function Matrix({ battles }: { battles: ParsedBattle[] }) {
  const [metric, setMetric] = useState<Metric>(() => load('sb2_metric', 'damage'));
  const rows = useMemo(() => {
    const map = new Map<string, (ParsedPlayer | null)[]>();
    battles.forEach((b, bi) => ourPlayers(b).forEach(p => {
      if (!map.has(p.name)) map.set(p.name, new Array(battles.length).fill(null));
      map.get(p.name)![bi] = p;
    }));
    return [...map.entries()].map(([name, cells]) => {
      const played = cells.filter(Boolean) as ParsedPlayer[];
      const sum = (f: (p: ParsedPlayer) => number) => played.reduce((s, p) => s + f(p), 0);
      // Shooting ratios over the totals, so a one-shot battle does not weigh like a twenty-shot one.
      const avg = metric === 'accuracy' ? pct(sum(p => p.hits), sum(p => p.shots))
        : metric === 'penetration' ? pct(sum(p => p.piercings), sum(p => p.hits))
        : sum(p => value(p, metric)) / played.length;
      return { name, cells, played: played.length, avg: avg ?? 0 };
    }).sort((a, b) => b.avg - a.avg);
  }, [battles, metric]);
  // One scale for the whole table: a lone 34 damage must not glow like the best game of the night.
  const max = Math.max(1, ...rows.flatMap(r => r.cells.map(p => (p ? value(p, metric) : 0))));

  return (
    <Panel title={<>Игроки по боям <span className="ml-1 text-[13px] font-medium text-dim">{rows.length}</span></>}
      aside={<Segmented label="Показатель" value={metric} options={METRICS} onChange={m => { setMetric(m); save('sb2_metric', m); }} />}>
      <p className="px-5 pt-1 text-[12px] text-dim">В стандартных боях и «Натиске» — только ты и твой взвод, в клановых — вся команда.</p>
      <div className="mt-3 overflow-x-auto">
        <table className="w-full border-collapse text-[13px]">
          <thead>
            <tr className="border-y border-line-soft text-[11.5px] text-muted">
              <th scope="col" className="sticky left-0 z-10 bg-surface px-3 py-2 text-left font-semibold">Игрок</th>
              {battles.map((b, i) => (
                <th key={b.battle_hash} scope="col" className="min-w-[76px] px-2 py-2 text-center font-semibold" title={`${b.map_name} · ${b.date_time}`}>
                  <div className={b.result === 'win' ? 'text-win' : b.result === 'loss' ? 'text-loss' : 'text-draw'}>#{i + 1}</div>
                  <div className="max-w-[90px] truncate font-medium text-dim">{b.map_name}</div>
                </th>
              ))}
              <th scope="col" className="sticky right-0 bg-surface px-3 py-2 text-right font-semibold text-ink">Среднее</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => (
              <tr key={r.name} className="border-b border-line-soft hover:bg-surface-2">
                <td className="sticky left-0 z-10 max-w-[200px] bg-surface px-3 py-2">
                  <div className="truncate font-semibold" title={r.name}>{r.name}</div>
                  <div className="text-[11px] text-dim">{r.played} из {battles.length}</div>
                </td>
                {r.cells.map((p, bi) => {
                  const v = p ? value(p, metric) : null;
                  return (
                    <td key={bi} className="num px-2 py-2 text-center"
                      style={v != null ? { background: `rgb(232 145 58 / ${(0.22 * v) / max})` } : undefined}>
                      <span className={v == null ? 'text-dim' : ''}>{show(v, metric)}</span>
                    </td>
                  );
                })}
                <td className="num sticky right-0 bg-surface px-3 py-2 text-right font-semibold">{showAvg(r.avg, metric)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="h-3" />
    </Panel>
  );
}

function SaveBar({ battles, selected, setSelected, saved, state, onSave }: {
  battles: ParsedBattle[]; selected: Set<string>; setSelected: (s: Set<string>) => void; saved: Set<string>;
  state: { busy: boolean; text: string; ok?: boolean }; onSave: () => void;
}) {
  const pending = battles.filter(b => selected.has(b.battle_hash) && !saved.has(b.battle_hash)).length;
  const toggle = (h: string) => {
    const next = new Set(selected);
    if (next.has(h)) next.delete(h); else next.add(h);
    setSelected(next);
  };
  const LABEL = { win: 'Победа', loss: 'Поражение', draw: 'Ничья' };
  return (
    <Panel title="Сохранить в базу"
      aside={<button type="button" className="text-[12.5px] font-semibold text-muted hover:text-accent"
        onClick={() => setSelected(selected.size ? new Set() : new Set(battles.map(b => b.battle_hash)))}>
        {selected.size ? 'Снять все' : 'Выбрать все'}</button>}>
      <div className="grid gap-1 px-5 pt-3 sm:grid-cols-2 xl:grid-cols-3">
        {battles.map(b => (
          <label key={b.battle_hash} className={`flex cursor-pointer items-center gap-2.5 rounded-md px-2 py-1.5 text-[13px] hover:bg-surface-2 ${saved.has(b.battle_hash) ? 'opacity-50' : ''}`}>
            <input type="checkbox" className="accent-[var(--color-accent)]" checked={selected.has(b.battle_hash)}
              disabled={saved.has(b.battle_hash)} onChange={() => toggle(b.battle_hash)} />
            <span className="truncate">{b.map_name}</span>
            <span className={`text-[12px] ${b.result === 'win' ? 'text-win' : b.result === 'loss' ? 'text-loss' : 'text-draw'}`}>{LABEL[b.result]}</span>
            <span className="num ml-auto text-[11.5px] text-dim">{b.date_time.slice(5, 16)}</span>
          </label>
        ))}
      </div>
      <div className="flex flex-wrap items-center gap-3 px-5 py-4">
        <button type="button" disabled={!pending || state.busy} onClick={onSave}
          className="rounded-md bg-accent px-4 py-2 text-[13px] font-bold text-bg transition-colors hover:bg-accent-hover disabled:cursor-not-allowed disabled:opacity-40">
          {state.busy ? 'Сохраняем…' : `Сохранить ${pending || ''}`}
        </button>
        {state.text && <span className={`text-[13px] ${state.ok ? 'text-win' : state.busy ? 'text-muted' : 'text-loss'}`}>{state.text}</span>}
        {state.ok && <Link to="/analytics" className="text-[13px] font-semibold text-accent">Открыть аналитику →</Link>}
      </div>
    </Panel>
  );
}
