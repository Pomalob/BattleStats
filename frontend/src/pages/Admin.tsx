import { useTitle } from '../title';
import { useEffect, useState } from 'react';
import { Navigate } from 'react-router-dom';
import { api, AuthError } from '../api';
import { LineChart } from '../components/charts';
import { useShell } from '../components/Shell';
import { Panel, Skeleton } from '../components/ui';
import { fmt, fmtPct, pct } from '../format';

interface Overview {
  battles: number; player_rows: number; users: number; teams: number; accounts: number;
  by_mode: { mode: string; battles: number }[]; by_day: { day: string; battles: number }[];
}
const MODE: Record<string, string> = { clan: 'Клановые', onslaught: 'Натиск', standard: 'Стандартные', other: 'Другое' };

export function AdminPage() {
  useTitle('Админка');
  const { me } = useShell();
  const [data, setData] = useState<Overview | null>(null);
  const [error, setError] = useState('');
  useEffect(() => {
    if (!me?.is_admin) return;
    api<Overview>('/api/admin/overview').then(setData).catch(e => { if (!(e instanceof AuthError)) setError(e.message); });
  }, [me]);

  if (me && !me.is_admin) return <Navigate to="/" replace />;
  return (
    <div className="mx-auto max-w-[1200px] space-y-5">
      <header>
        <h1 className="text-[26px] font-extrabold tracking-tight">Админка</h1>
        <p className="mt-0.5 text-[13px] text-muted">Вся база: сколько собрано и чем. Бои по каждому пользователю — в аналитике с охватом «Вся база».</p>
      </header>
      {error && <div className="rounded-lg border border-loss/40 bg-loss/10 px-4 py-3 text-[13px]">{error}</div>}
      {!data ? <Skeleton className="h-[400px]" /> : (
        <>
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
            {([['Боёв', data.battles], ['Строк игроков', data.player_rows], ['Аккаунтов игры', data.accounts],
              ['Пользователей', data.users], ['Команд', data.teams]] as [string, number][]).map(([label, v], i) => (
              <div key={label} className="animate-rise rounded-lg border border-line-soft bg-surface px-5 py-4" style={{ animationDelay: `${i * 40}ms` }}>
                <div className="text-[12.5px] font-semibold text-muted">{label}</div>
                <div className="mt-1 text-[28px] font-extrabold leading-none tabular-nums">{fmt(v)}</div>
              </div>
            ))}
          </div>
          <div className="grid gap-4 lg:grid-cols-3">
            <Panel title="Бои по дням (последние 30 дней с боями)" className="lg:col-span-2">
              <div className="px-3 pb-4 pt-2">
                <LineChart points={[...data.by_day].reverse().map(d => ({ day: d.day, value: d.battles }))} format={v => fmt(v)} />
              </div>
            </Panel>
            <Panel title="По режимам">
              <ul className="space-y-3 px-5 pb-5 pt-3">
                {data.by_mode.map(m => {
                  const share = pct(m.battles, data.battles) ?? 0;
                  return (
                    <li key={m.mode}>
                      <div className="flex justify-between text-[13px]"><span className="font-semibold">{MODE[m.mode] ?? m.mode}</span>
                        <span className="num text-muted">{fmt(m.battles)} · {fmtPct(share)}</span></div>
                      <div className="mt-1.5 h-1.5 overflow-hidden rounded-full bg-surface-3">
                        <div className="h-full rounded-full bg-accent" style={{ width: `${share}%` }} />
                      </div>
                    </li>
                  );
                })}
              </ul>
            </Panel>
          </div>
        </>
      )}
    </div>
  );
}
