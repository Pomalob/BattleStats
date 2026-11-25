import { useTitle } from '../../title';
import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import { api, AuthError, filterQuery, type Filters, type Mode, type Summary } from '../../api';
import { Segmented, Select } from '../../components/ui';
import { load, save } from '../../storage';

export const MODES: [Mode, string][] = [['', 'Все режимы'], ['clan', 'Клановые'], ['onslaught', 'Натиск'], ['standard', 'Стандартные']];
const PERIODS: [string, string][] = [['0', 'Всё время'], ['7', '7 дней'], ['30', '30 дней'], ['90', '90 дней']];
const TABS: [string, string][] = [['', 'Обзор'], ['players', 'Игроки'], ['tanks', 'Танки'], ['maps', 'Карты'], ['battles', 'Бои']];

export interface AnalyticsContext {
  filters: Filters;
  summary: Summary | null;
  error: string | null;
  minBattles: number;
  setMinBattles: (v: number) => void;
  /** Reload after something changed the data (a hidden player, a deleted battle). */
  refresh: () => void;
  version: number;
}

const Ctx = createContext<AnalyticsContext | null>(null);

export function useAnalytics() {
  const c = useContext(Ctx);
  if (!c) throw new Error('useAnalytics used outside AnalyticsProvider');
  return c;
}

export function AnalyticsProvider({ filters, children }: { filters: Filters; children: ReactNode }) {
  const [minBattles, setMinBattlesState] = useState<number>(() => load('sb2_min', 3));
  const [summary, setSummary] = useState<Summary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [version, setVersion] = useState(0);

  const setMinBattles = (v: number) => { setMinBattlesState(v); save('sb2_min', v); };
  const refresh = useCallback(() => setVersion(v => v + 1), []);

  useEffect(() => {
    let alive = true;
    setError(null);
    setSummary(null);
    api<Summary>(`/api/analytics/summary?${filterQuery(filters)}&min_battles=${minBattles}`)
      .then(s => { if (alive) setSummary(s); })
      .catch(err => { if (alive && !(err instanceof AuthError)) setError(err.message); });
    return () => { alive = false; };
  }, [filters, minBattles, version]);

  return (
    <Ctx.Provider value={{ filters, summary, error, minBattles, setMinBattles, refresh, version }}>
      {error && <div className="rounded-lg border border-loss/40 bg-loss/10 px-4 py-3 text-[13px]">{error}</div>}
      {children}
    </Ctx.Provider>
  );
}

export function AnalyticsLayout() {
  useTitle('Аналитика');
  const [filters, setFilters] = useState<Filters>(() => {
    const saved = load<Partial<Filters>>('sb2_filters', {});
    return { scope: 'mine', mode: saved.mode ?? '', days: saved.days ?? 0 };
  });

  const update = (patch: Partial<Pick<Filters, 'mode' | 'days'>>) => {
    const next = { ...filters, ...patch };
    setFilters(next);
    save('sb2_filters', next);
  };

  return (
    <div className="mx-auto max-w-[1440px] space-y-5">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-[26px] font-extrabold tracking-tight">Аналитика</h1>
          <p className="mt-0.5 text-[13px] text-muted">
            Только мои · {MODES.find(([m]) => m === filters.mode)?.[1].toLowerCase()} ·{' '}
            {PERIODS.find(([d]) => Number(d) === filters.days)?.[1].toLowerCase()}
          </p>
        </div>
        <div className="flex w-full min-w-0 flex-wrap items-center gap-2 sm:w-auto">
          <Segmented label="Режим боёв" value={filters.mode} options={MODES} onChange={mode => update({ mode })} />
          <Select label="Период" value={String(filters.days)} options={PERIODS} onChange={d => update({ days: Number(d) })} />
        </div>
      </header>

      <nav aria-label="Разделы аналитики" className="flex gap-1 overflow-x-auto border-b border-line-soft [scrollbar-width:none]">
        {TABS.map(([path, label]) => (
          <NavLink key={path} to={`/analytics${path ? `/${path}` : ''}`} end
            className={({ isActive }) => `relative shrink-0 px-3.5 pb-2.5 pt-1 text-[13.5px] font-semibold transition-colors duration-150 ${
              isActive ? 'text-ink after:absolute after:inset-x-2 after:-bottom-px after:h-0.5 after:rounded-full after:bg-accent'
                : 'text-muted hover:text-ink'}`}>
            {label}
          </NavLink>
        ))}
      </nav>

      <AnalyticsProvider filters={filters}>
        <Outlet />
      </AnalyticsProvider>
    </div>
  );
}