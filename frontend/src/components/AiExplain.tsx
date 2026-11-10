import { useEffect, useState } from 'react';
import { api, AuthError, filterQuery, send, type Filters } from '../api';
import { plural } from '../format';

/* The AI analysis of the selected period: the server builds anonymised facts, the model
   puts them into words, the answer is checked against the facts and cached. */

export interface ExplainStatus {
  enabled: boolean; limit: number; used: number; unlimited: boolean; min_battles: number; modes: string[];
}
interface Point { kind: 'сильная сторона' | 'подтянуть' | 'наблюдение'; about: string; text: string; facts: string[] }
export interface ExplainAnswer { summary: string; points: Point[]; limits: string }

const KIND: Record<Point['kind'], string> = {
  'сильная сторона': 'text-win bg-win/12',
  'подтянуть': 'text-loss bg-loss/12',
  'наблюдение': 'text-muted bg-surface-3',
};

export function useExplainStatus() {
  const [status, setStatus] = useState<ExplainStatus | null>(null);
  const reload = () => api<ExplainStatus>('/api/analytics/explain/status').then(setStatus).catch(() => {});
  useEffect(() => { reload(); }, []);
  return { status, reload };
}

/** The request body for the filters: the same period the rest of the page shows. */
function bodyFor(f: Filters) {
  const q = new URLSearchParams(filterQuery(f));
  return { scope: q.get('scope'), mode: q.get('mode'), date_from: q.get('date_from'), date_to: q.get('date_to') };
}

export function useExplain(filters: Filters, onDone: () => void) {
  const [answer, setAnswer] = useState<ExplainAnswer | null>(null);
  const [cached, setCached] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => { setAnswer(null); setError(null); }, [filters]);

  const run = async () => {
    setBusy(true); setError(null);
    try {
      const r = await send<{ answer: ExplainAnswer; cached: boolean }>('/api/analytics/explain', 'POST', bodyFor(filters));
      setAnswer(r.answer); setCached(r.cached);
      onDone();
    } catch (e) {
      if (!(e instanceof AuthError)) setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return { answer, cached, busy, error, run, reset: () => setAnswer(null) };
}

export function ExplainButton({ status, filters, busy, onRun }:
  { status: ExplainStatus | null; filters: Filters; busy: boolean; onRun: () => void }) {
  if (!status?.enabled) {
    return <span className="rounded-full border border-line px-2 py-0.5 text-[11px] font-semibold text-dim"
      title="Разбор от ИИ появится позже">ИИ — скоро</span>;
  }
  const noMode = !filters.mode;
  const left = status.unlimited ? null : Math.max(0, status.limit - status.used);
  const title = noMode ? 'Выбери режим: разбор строится отдельно для стандартных, клановых боёв и «Натиска»'
    : left === 0 ? 'На сегодня разборы закончились; уже готовые разборы открываются без лимита'
    : left == null ? 'Разобрать период с ИИ' : `Осталось ${left} ${plural(left, 'разбор', 'разбора', 'разборов')} на сегодня`;
  return (
    <button type="button" onClick={onRun} disabled={noMode || busy} title={title}
      className="rounded-md bg-accent px-3 py-1.5 text-[12.5px] font-bold text-bg transition-colors hover:bg-accent-hover disabled:cursor-not-allowed disabled:opacity-40">
      {busy ? 'Разбираем…' : 'Разбор от ИИ'}
      {left != null && !noMode && !busy && <span className="ml-1.5 font-semibold opacity-75">{left}/{status.limit}</span>}
    </button>
  );
}

export function ExplainView({ answer, cached, onBack }: { answer: ExplainAnswer; cached: boolean; onBack: () => void }) {
  return (
    <div className="space-y-3 px-5 pb-5 pt-3">
      <p className="animate-fade text-[13.5px] font-semibold leading-snug">{answer.summary}</p>
      <ul className="space-y-2.5">
        {answer.points.map((p, i) => (
          <li key={i} className="animate-fade text-[13px] leading-snug" style={{ animationDelay: `${i * 60}ms` }}>
            <span className={`mr-2 inline-block rounded px-1.5 py-0.5 text-[11px] font-bold ${KIND[p.kind] ?? KIND['наблюдение']}`}>
              {p.kind}{p.about !== 'ты' ? ` · ${p.about}` : ''}
            </span>
            {p.text}
          </li>
        ))}
      </ul>
      <p className="text-[12px] text-dim">{answer.limits}</p>
      <div className="flex items-center justify-between text-[11.5px] text-dim">
        <span>ИИ пересказывает посчитанные цифры, каждая проверена{cached ? ' · уже готовый разбор' : ''}</span>
        <button type="button" onClick={onBack} className="font-semibold hover:text-accent">К короткой сводке</button>
      </div>
    </div>
  );
}
