import { useTitle } from '../../title';
import { useCallback, useEffect, useState } from 'react';
import { NavLink, Outlet, useOutletContext } from 'react-router-dom';
import { api, AuthError, filterQuery, type Filters, type Mode, type Summary } from '../../api';
import { useShell } from '../../components/Shell';
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
export const useAnalytics = () => useOutletContext<AnalyticsContext>();

export function AnalyticsLayout() {
  useTitle('Аналитика');
  const { me, teams } = useShell();
  const [filters, setFilters] = useState<Filters>(() => load('sb2_filters', { scope: 'visible', mode: '', days: 0 }));
  const [minBattles, setMinBattlesState] = useState<number>(() => load('sb2_min', 3));
  const [summary, setSummary] = useState<Summary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [version, setVersion] = useState(0);

  const update = (patch: Partial<Filters>) => {
    const next = { ...filters, ...patch };
    setFilters(next);
    save('sb2_filters', next);
  };
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

  const scopeOptions: [string, string][] = [
    ['visible', 'Мои и команды'], ['mine', 'Только мои'],
    ...teams.map(t => [`team:${t.id}`, `Команда: ${t.name}`] as [string, string]),
    ...(me?.is_admin ? [['all', 'Вся база']] as [string, string][] : []),
  ];
  const scopeName = scopeOptions.find(([v]) => v === filters.scope)?.[1] ?? 'Мои и команды';

  return (
    <div className="mx-auto max-w-[1440px] space-y-5">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-[26px] font-extrabold tracking-tight">Аналитика</h1>
          <p className="mt-0.5 text-[13px] text-muted">
            {scopeName} · {MODES.find(([m]) => m === filters.mode)?.[1].toLowerCase()} ·{' '}
            {PERIODS.find(([d]) => Number(d) === filters.days)?.[1].toLowerCase()}
          </p>
        </div>
        <div className="flex w-full min-w-0 flex-wrap items-center gap-2 sm:w-auto">
          <Segmented label="Режим боёв" value={filters.mode} options={MODES} onChange={mode => update({ mode })} />
          <Select label="Чьи бои" value={filters.scope} options={scopeOptions} onChange={scope => update({ scope })} />
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

      {error && <div className="rounded-lg border border-loss/40 bg-loss/10 px-4 py-3 text-[13px]">{error}</div>}
      <Outlet context={{ filters, summary, error, minBattles, setMinBattles, refresh, version } satisfies AnalyticsContext} />
    </div>
  );
}
