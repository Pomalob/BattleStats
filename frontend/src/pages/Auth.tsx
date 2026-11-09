import { useTitle } from '../title';
import { useState, type FormEvent } from 'react';
import { Link, useNavigate, useSearchParams } from 'react-router-dom';
import { authPost, clearSession, safeNext, send, setSession, token } from '../api';
import { Logo } from '../components/Shell';
import { Button, Panel } from '../components/ui';

export function LoginPage() {
  useTitle('Вход');
  const [params] = useSearchParams();
  const next = safeNext(params.get('next'));
  const [mode, setMode] = useState<'login' | 'register'>('login');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [consent, setConsent] = useState(false);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  if (token()) { window.location.replace(next); return null; }

  async function submit(e: FormEvent) {
    e.preventDefault();
    if (mode === 'register' && !consent) { setError('Отметь согласие на обработку персональных данных'); return; }
    setBusy(true); setError('');
    try {
      const r = await authPost(mode, mode === 'register' ? { username, password, consent } : { username, password });
      setSession(r.token, r.username);
      window.location.replace(next);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  const input = 'h-10 w-full rounded-md border border-line bg-surface-2 px-3 text-[14px] outline-none placeholder:text-dim focus:border-accent';
  return (
    <div className="grid min-h-screen place-items-center px-4 py-10">
      <div className="grid w-full max-w-[880px] overflow-hidden rounded-xl border border-line-soft bg-surface md:grid-cols-[1fr_380px]">
        <section className="flex flex-col justify-between gap-8 border-b border-line-soft bg-surface-2/40 p-8 md:border-b-0 md:border-r">
          <Logo />
          <div>
            <h1 className="text-[28px] font-extrabold leading-tight tracking-tight">Разбор реплеев<br />для взвода и клана</h1>
            <p className="mt-3 max-w-sm text-[14px] text-muted">
              Урон, ассист, блок, точность и проценты побед — по каждому бою, по режимам и за любой период.
              Команды видят бои друг друга.
            </p>
          </div>
          <dl className="grid grid-cols-3 gap-4 text-[12px]">
            {[['Формат', '.mtreplay'], ['Режимы', 'клан · натиск · стандарт'], ['Файлы', 'на диск не пишутся']].map(([k, v]) => (
              <div key={k}><dt className="text-dim">{k}</dt><dd className="mt-0.5 font-semibold">{v}</dd></div>
            ))}
          </dl>
        </section>
        <section className="p-8">
          <div role="tablist" className="mb-6 flex gap-1 border-b border-line-soft">
            {([['login', 'Вход'], ['register', 'Регистрация']] as const).map(([m, label]) => (
              <button key={m} type="button" role="tab" aria-selected={mode === m} onClick={() => { setMode(m); setError(''); }}
                className={`relative px-3 pb-2.5 text-[14px] font-bold transition-colors ${mode === m
                  ? 'text-ink after:absolute after:inset-x-2 after:-bottom-px after:h-0.5 after:rounded-full after:bg-accent' : 'text-muted hover:text-ink'}`}>
                {label}
              </button>
            ))}
          </div>
          <form onSubmit={submit} className="space-y-4" noValidate>
            <label className="block">
              <span className="mb-1.5 block text-[12.5px] font-semibold text-muted">Имя пользователя</span>
              <input className={input} value={username} onChange={e => setUsername(e.target.value)} autoComplete="username" required />
            </label>
            <label className="block">
              <span className="mb-1.5 block text-[12.5px] font-semibold text-muted">Пароль</span>
              <input className={input} type="password" value={password} onChange={e => setPassword(e.target.value)}
                autoComplete={mode === 'login' ? 'current-password' : 'new-password'} required />
            </label>
            {mode === 'register' && (
              <>
                <p className="text-[12px] text-dim">Имя — от 3 символов, пароль — от 6.</p>
                <label className="flex items-start gap-2.5 text-[12.5px] text-muted">
                  <input type="checkbox" className="mt-0.5 accent-[var(--color-accent)]" checked={consent} onChange={e => setConsent(e.target.checked)} />
                  <span>Даю <Link to="/consent" target="_blank" className="text-accent">согласие на обработку персональных данных</Link> и
                    принимаю <Link to="/privacy" target="_blank" className="text-accent">политику</Link></span>
                </label>
              </>
            )}
            {error && <p role="alert" className="rounded-md bg-loss/10 px-3 py-2 text-[13px] text-loss">{error}</p>}
            <Button type="submit" disabled={busy} className="w-full py-2.5">
              {busy ? '…' : mode === 'login' ? 'Войти' : 'Зарегистрироваться'}
            </Button>
          </form>
        </section>
      </div>
    </div>
  );
}

export function AccountPage() {
  useTitle('Аккаунт');
  const navigate = useNavigate();
  const [oldPass, setOldPass] = useState('');
  const [newPass, setNewPass] = useState('');
  const [delPass, setDelPass] = useState('');
  const [msg, setMsg] = useState<{ where: string; text: string; ok?: boolean } | null>(null);
  const input = 'h-9 min-w-0 flex-1 rounded-md border border-line bg-surface-2 px-3 text-[13.5px] outline-none placeholder:text-dim focus:border-accent';

  const signOut = () => { clearSession(); navigate('/login'); };
  return (
    <div className="mx-auto max-w-[720px] space-y-4">
      <h1 className="text-[26px] font-extrabold tracking-tight">Аккаунт</h1>
      <Panel title="Сменить пароль">
        <form className="space-y-2 p-5 pt-3" onSubmit={async e => {
          e.preventDefault();
          try {
            const r = await send<{ token: string; username: string }>('/api/auth/password', 'POST', { old_password: oldPass, new_password: newPass });
            setSession(r.token, r.username);
            setOldPass(''); setNewPass('');
            setMsg({ where: 'pass', text: 'Пароль сменён. На других устройствах нужно войти заново.', ok: true });
          } catch (err) { setMsg({ where: 'pass', text: (err as Error).message }); }
        }}>
          <div className="flex gap-2"><input className={input} type="password" placeholder="Текущий пароль" autoComplete="current-password" value={oldPass} onChange={e => setOldPass(e.target.value)} /></div>
          <div className="flex gap-2">
            <input className={input} type="password" placeholder="Новый пароль, от 6 символов" autoComplete="new-password" value={newPass} onChange={e => setNewPass(e.target.value)} />
            <Button type="submit">Сменить</Button>
          </div>
          {msg?.where === 'pass' && <p className={`text-[13px] ${msg.ok ? 'text-win' : 'text-loss'}`}>{msg.text}</p>}
        </form>
      </Panel>
      <Panel title="Сессии">
        <div className="flex flex-wrap items-center gap-3 p-5 pt-3">
          <p className="min-w-0 flex-1 text-[13px] text-muted">Если вход был на чужом компьютере или токен мог утечь — выйди везде сразу.</p>
          <Button variant="ghost" onClick={async () => {
            if (!confirm('Выйти на всех устройствах, включая это?')) return;
            try { await send('/api/auth/logout-all', 'POST'); } finally { signOut(); }
          }}>Выйти на всех устройствах</Button>
          <Button variant="ghost" onClick={signOut}>Выйти</Button>
        </div>
      </Panel>
      <Panel title={<span className="text-loss">Удалить аккаунт</span>}>
        <form className="space-y-3 p-5 pt-3" onSubmit={async e => {
          e.preventDefault();
          if (!confirm('Удалить аккаунт навсегда? Это нельзя отменить.')) return;
          try {
            await send('/api/auth/me', 'DELETE', { password: delPass });
            clearSession();
            alert('Аккаунт удалён.');
            navigate('/login');
          } catch (err) { setMsg({ where: 'del', text: (err as Error).message }); }
        }}>
          <p className="text-[13px] text-muted">Удаляются аккаунт, участие в командах, скрытые игроки и бои, которые загрузил только ты.
            Бои, которые загрузили и другие, остаются у них. Команды, где ты владелец, переходят к участнику, вступившему раньше всех;
            пустые удаляются. Это нельзя отменить.</p>
          <div className="flex gap-2">
            <input className={input} type="password" placeholder="Пароль для подтверждения" autoComplete="current-password" value={delPass} onChange={e => setDelPass(e.target.value)} />
            <Button type="submit" variant="danger">Удалить навсегда</Button>
          </div>
          {msg?.where === 'del' && <p className="text-[13px] text-loss">{msg.text}</p>}
        </form>
      </Panel>
    </div>
  );
}
