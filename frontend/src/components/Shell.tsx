import { useCallback, useEffect, useState } from 'react';
import { Link, Navigate, NavLink, Outlet, useLocation, useOutletContext } from 'react-router-dom';
import { api, AuthError, send, token, type Me, type Team } from '../api';
import { Button } from './ui';

export interface ShellContext { me: Me | null; teams: Team[]; reloadTeams: () => Promise<void> }
export const useShell = () => useOutletContext<ShellContext>();

export function Logo() {
  return (
    <Link to="/" className="flex items-center gap-3 px-2 py-1">
      <span className="grid size-9 place-items-center rounded-md bg-accent text-bg">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" aria-hidden="true">
          <circle cx="12" cy="12" r="7.5" stroke="currentColor" strokeWidth="2" />
          <circle cx="12" cy="12" r="2" fill="currentColor" />
          <path d="M12 1.5v5M12 17.5v5M1.5 12h5M17.5 12h5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
        </svg>
      </span>
      <span className="leading-tight">
        <span className="block text-[17px] font-extrabold tracking-tight">Stats<span className="text-accent">Battle</span></span>
        <span className="block text-[11px] font-medium text-dim">Аналитика реплеев</span>
      </span>
    </Link>
  );
}

const icons = {
  upload: 'M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M17 8l-5-5-5 5M12 3v12',
  chart: 'M3 3v18h18M7 15l4-4 3 3 5-6',
  team: 'M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75',
  admin: 'M12 2 3 6v6c0 5 3.8 9.4 9 10 5.2-.6 9-5 9-10V6l-9-4z',
};

export function Icon({ d, size = 18 }: { d: string; size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"
      strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d={d} /></svg>
  );
}

const itemClass = 'flex shrink-0 items-center gap-3 whitespace-nowrap rounded-md px-3 py-2.5 text-[13.5px] font-semibold transition-colors duration-150';
const navClass = ({ isActive }: { isActive: boolean }) =>
  `${itemClass} ${isActive ? 'bg-accent-soft text-accent' : 'text-muted hover:bg-surface-2 hover:text-ink'}`;

export function Shell() {
  const [me, setMe] = useState<Me | null>(null);
  const [teams, setTeams] = useState<Team[]>([]);
  const location = useLocation();
  const signedIn = Boolean(token());

  const reloadTeams = useCallback(async () => {
    const t = await api<{ teams: Team[] }>('/api/teams');
    setTeams(t.teams);
  }, []);
  useEffect(() => {
    if (!signedIn) return;
    api<Me>('/api/auth/me').then(setMe).catch(err => { if (!(err instanceof AuthError)) console.error(err); });
    reloadTeams().catch(() => {});
  }, [reloadTeams, signedIn]);

  if (!signedIn) {
    return <Navigate to={`/login?next=${encodeURIComponent(location.pathname + location.search)}`} replace />;
  }

  return (
    <div className="min-h-screen lg:grid lg:grid-cols-[232px_1fr]">
      <aside className="flex flex-col gap-6 border-b border-line-soft bg-surface/60 p-4 lg:sticky lg:top-0 lg:h-screen lg:border-r lg:border-b-0">
        <Logo />
        <nav className="flex gap-1 overflow-x-auto [scrollbar-width:none] lg:flex-col" aria-label="Разделы">
          <NavLink to="/" end className={navClass}><Icon d={icons.upload} /> Быстрый чек</NavLink>
          <NavLink to="/analytics" className={navClass}><Icon d={icons.chart} /> Аналитика</NavLink>
          <NavLink to="/team" className={navClass}><Icon d={icons.team} /> Команда</NavLink>
          {me?.is_admin && <NavLink to="/admin" className={navClass}><Icon d={icons.admin} /> Админка</NavLink>}
        </nav>
        <NavLink to="/account" className={({ isActive }) => `mt-auto hidden items-center gap-3 rounded-md border px-3 py-2.5 transition-colors lg:flex ${
          isActive ? 'border-accent/40 bg-accent-soft' : 'border-line-soft hover:bg-surface-2'}`}>
          <span className="grid size-8 place-items-center rounded-full bg-surface-3 text-[13px] font-bold text-accent">
            {(me?.username ?? '?').slice(0, 1).toUpperCase()}
          </span>
          <span className="min-w-0 flex-1">
            <span className="block truncate text-[13px] font-semibold">{me?.username ?? '…'}</span>
            <span className="block text-[11px] text-dim">Аккаунт</span>
          </span>
        </NavLink>
      </aside>
      <div className="flex min-w-0 flex-col">
        <main className="min-w-0 flex-1 px-4 py-5 sm:px-6 lg:px-8">
          <Outlet context={{ me, teams, reloadTeams } satisfies ShellContext} />
        </main>
        <Footer />
      </div>
      {me?.consent_required && <ConsentPrompt onDone={() => setMe({ ...me, consent_required: false })} />}
    </div>
  );
}

export function Footer() {
  return (
    <footer className="flex flex-wrap items-center justify-between gap-x-6 gap-y-2 border-t border-line-soft px-4 py-4 text-[11.5px] text-dim sm:px-6 lg:px-8">
      <span>StatsBattle — неофициальный проект игроков. Не связан с ООО «Леста Игры» и не одобрен им;
        «Мир танков» — товарный знак его правообладателя.</span>
      <nav className="flex gap-4">
        <Link to="/privacy" className="hover:text-muted">Политика обработки данных</Link>
        <Link to="/consent" className="hover:text-muted">Согласие</Link>
        <Link to="/account" className="hover:text-muted lg:hidden">Аккаунт</Link>
      </nav>
    </footer>
  );
}

/** Accounts made before the consent text existed, or after it changed, accept it here. */
function ConsentPrompt({ onDone }: { onDone: () => void }) {
  const [checked, setChecked] = useState(false);
  const [busy, setBusy] = useState(false);
  return (
    <div role="dialog" aria-label="Согласие на обработку персональных данных"
      className="fixed inset-x-4 bottom-4 z-50 mx-auto max-w-[640px] animate-rise rounded-lg border border-accent/40 bg-surface-2 p-5 text-[13.5px] shadow-2xl">
      <p className="text-muted">Чтобы пользоваться сайтом дальше, нужно согласие на обработку персональных данных:
        логина и данных из загружаемых реплеев. Подробно — в <Link to="/consent" className="text-accent">тексте согласия</Link> и{' '}
        <Link to="/privacy" className="text-accent">политике</Link>.</p>
      <label className="mt-3 flex items-center gap-2.5">
        <input type="checkbox" className="accent-[var(--color-accent)]" checked={checked} onChange={e => setChecked(e.target.checked)} />
        Даю согласие на обработку персональных данных
      </label>
      <div className="mt-4 flex gap-2">
        <Button disabled={!checked || busy} onClick={async () => {
          setBusy(true);
          try { await send('/api/auth/consent', 'POST', { consent: true }); onDone(); } catch { setBusy(false); }
        }}>Принять</Button>
        <Link to="/account" className="rounded-md border border-line px-4 py-2 text-[13px] font-bold text-muted hover:text-ink">Удалить аккаунт</Link>
      </div>
    </div>
  );
}
