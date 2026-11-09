import { useTitle } from '../title';
import { useCallback, useEffect, useState, type FormEvent } from 'react';
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { api, AuthError, send, type Team } from '../api';
import { useShell } from '../components/Shell';
import { Button, Field, Panel, Select } from '../components/ui';
import { fmt, plural } from '../format';
import { save } from '../storage';

interface Member { user_id: number; username: string; role: string; battles: number }
interface Invite { code: string; expires_at: string; max_uses: number | null; uses: number }
interface TeamDetails { id: number; name: string; role: string; members: Member[]; invites: Invite[] }

const ROLES: Record<string, string> = { owner: 'владелец', member: 'участник', admin: 'админ' };

export function TeamPage() {
  useTitle('Команда');
  const { me, teams, reloadTeams } = useShell();
  const [params, setParams] = useSearchParams();
  const current = Number(params.get('id')) || teams[0]?.id || null;
  const [name, setName] = useState('');
  const [createError, setCreateError] = useState('');

  async function create(e: FormEvent) {
    e.preventDefault();
    try {
      const t = await send<Team>('/api/teams', 'POST', { name });
      setName(''); setCreateError('');
      await reloadTeams();
      setParams({ id: String(t.id) });
    } catch (err) { setCreateError((err as Error).message); }
  }

  return (
    <div className="mx-auto max-w-[1200px] space-y-5">
      <header>
        <h1 className="text-[26px] font-extrabold tracking-tight">Команда</h1>
        <p className="mt-0.5 text-[13px] text-muted">Участники видят бои друг друга в аналитике. Вступить можно только по ссылке от владельца.</p>
      </header>
      <div className="grid items-start gap-5 lg:grid-cols-[280px_1fr]">
        <aside className="space-y-4">
          <Panel title="Мои команды">
            <div className="space-y-1 p-3">
              {teams.length ? teams.map(t => (
                <button key={t.id} type="button" onClick={() => setParams({ id: String(t.id) })} aria-current={t.id === current}
                  className={`flex w-full items-center justify-between gap-2 rounded-md border px-3 py-2.5 text-left text-[13.5px] transition-colors ${
                    t.id === current ? 'border-accent/40 bg-accent-soft' : 'border-transparent hover:bg-surface-2'}`}>
                  <span className="truncate font-semibold">{t.name}</span>
                  <span className="shrink-0 text-[12px] text-muted">{t.members} · {ROLES[t.role] ?? t.role}</span>
                </button>
              )) : <p className="px-1 py-2 text-[13px] text-muted">Ты пока не в команде.</p>}
            </div>
          </Panel>
          <Panel title="Новая команда">
            <form onSubmit={create} className="flex gap-2 p-4 pt-3">
              <Field value={name} onChange={setName} placeholder="Название" maxLength={40} required />
              <Button type="submit">Создать</Button>
            </form>
            {createError && <p className="px-4 pb-3 text-[12.5px] text-loss">{createError}</p>}
          </Panel>
        </aside>
        {current ? <TeamView key={current} id={current} meId={me?.id} onChanged={reloadTeams}
          onGone={async () => { await reloadTeams(); setParams({}); }} />
          : <Panel className="p-8 text-center text-[13.5px] text-muted">Создай команду и пригласи друзей по ссылке.</Panel>}
      </div>
    </div>
  );
}

function TeamView({ id, meId, onChanged, onGone }: { id: number; meId?: number; onChanged: () => Promise<void>; onGone: () => Promise<void> }) {
  const [team, setTeam] = useState<TeamDetails | null>(null);
  const [error, setError] = useState('');
  const [days, setDays] = useState('7');
  const [uses, setUses] = useState('');
  const [rename, setRename] = useState('');
  const [copied, setCopied] = useState('');
  const navigate = useNavigate();

  const reload = useCallback(() => {
    api<TeamDetails>(`/api/teams/${id}`).then(t => { setTeam(t); setRename(t.name); })
      .catch(e => { if (!(e instanceof AuthError)) setError(e.message); });
  }, [id]);
  useEffect(reload, [reload]);

  const act = async (fn: () => Promise<unknown>, after: () => unknown = reload) => {
    try { setError(''); await fn(); await after(); } catch (e) { setError((e as Error).message); }
  };
  if (error && !team) return <Panel className="p-6 text-[13.5px]">{error}</Panel>;
  if (!team) return <Panel className="h-64 animate-pulse" ><span /></Panel>;
  const owner = team.role === 'owner';

  return (
    <div className="space-y-4">
      <Panel>
        <div className="flex flex-wrap items-center justify-between gap-3 p-5">
          <div>
            <div className="text-[12px] font-semibold text-muted">Команда · {ROLES[team.role] ?? team.role}</div>
            <div className="text-[22px] font-extrabold tracking-tight">{team.name}</div>
          </div>
          <Button variant="ghost" onClick={() => { save('sb2_filters', { scope: `team:${team.id}`, mode: '', days: 0 }); navigate('/analytics'); }}>
            Аналитика команды →
          </Button>
        </div>
      </Panel>
      {error && <div className="rounded-lg border border-loss/40 bg-loss/10 px-4 py-3 text-[13px]">{error}</div>}

      <Panel title={<>Участники <span className="ml-1 text-[13px] font-medium text-dim">{team.members.length}</span></>}>
        <div className="mt-3 overflow-x-auto">
          <table className="w-full min-w-[520px] border-collapse text-[13.5px]">
            <thead><tr className="border-y border-line-soft text-[12px] text-muted">
              <th className="px-4 py-2.5 text-left font-semibold">Игрок</th>
              <th className="px-4 py-2.5 text-left font-semibold">Роль</th>
              <th className="px-4 py-2.5 text-right font-semibold">Загрузил боёв</th>
              <th />
            </tr></thead>
            <tbody>
              {team.members.map(m => {
                const self = m.user_id === meId;
                return (
                  <tr key={m.user_id} className="border-b border-line-soft">
                    <td className="px-4 py-2.5 font-semibold">{m.username}
                      {self && <span className="ml-2 rounded bg-accent-soft px-1.5 py-0.5 text-[11px] font-bold text-accent">ты</span>}</td>
                    <td className="px-4 py-2.5 text-muted">{ROLES[m.role] ?? m.role}</td>
                    <td className="num px-4 py-2.5 text-right">{fmt(m.battles)}</td>
                    <td className="px-4 py-2 text-right">
                      {owner && !self && m.role !== 'owner' && (
                        <Button variant="ghost" size="sm" onClick={() => confirm(`Исключить ${m.username}? Его бои перестанут быть видны команде.`)
                          && act(() => send(`/api/teams/${team.id}/members/${m.user_id}`, 'DELETE'), async () => { reload(); await onChanged(); })}>Исключить</Button>
                      )}
                      {self && m.role !== 'owner' && (
                        <Button variant="ghost" size="sm" onClick={() => confirm('Выйти из команды? Вернуться можно только по новому приглашению.')
                          && act(() => send(`/api/teams/${team.id}/members/${m.user_id}`, 'DELETE'), onGone)}>Выйти</Button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div className="h-3" />
      </Panel>

      {owner && (
        <Panel title="Приглашения">
          <div className="space-y-2 px-5 pt-3">
            {team.invites.length ? team.invites.map(i => {
              const url = `${location.origin}/join/${i.code}`;
              return (
                <div key={i.code} className="flex flex-wrap items-center gap-3 rounded-md border border-line-soft px-3 py-2">
                  <span className="num min-w-0 flex-1 truncate text-[12.5px] text-muted" title={url}>{url}</span>
                  <span className="text-[12px] text-dim">до {new Date(i.expires_at).toLocaleDateString('ru-RU')} ·{' '}
                    {i.max_uses ? `${i.uses} из ${i.max_uses}` : `${i.uses} ${plural(i.uses, 'вход', 'входа', 'входов')}, без лимита`}</span>
                  <Button variant="ghost" size="sm" onClick={async () => {
                    try { await navigator.clipboard.writeText(url); setCopied(i.code); } catch { prompt('Скопируй ссылку:', url); }
                  }}>{copied === i.code ? 'Скопировано' : 'Копировать'}</Button>
                  <Button variant="ghost" size="sm" onClick={() => confirm('Отозвать ссылку?')
                    && act(() => send(`/api/teams/${team.id}/invites/${encodeURIComponent(i.code)}`, 'DELETE'))}>Отозвать</Button>
                </div>
              );
            }) : <p className="text-[13px] text-muted">Активных приглашений нет.</p>}
          </div>
          <div className="flex flex-wrap items-center gap-2 p-5">
            <span className="text-[12.5px] text-muted">Срок</span>
            <Select label="Срок ссылки" value={days} onChange={setDays} options={[['1', '1 день'], ['7', '7 дней'], ['30', '30 дней']]} />
            <span className="text-[12.5px] text-muted">Сколько человек</span>
            <Select label="Сколько человек" value={uses} onChange={setUses} options={[['1', '1'], ['5', '5'], ['', 'без лимита']]} />
            <Button onClick={() => act(() => send(`/api/teams/${team.id}/invites`, 'POST', { days: Number(days), max_uses: uses ? Number(uses) : null }))}>
              Создать ссылку
            </Button>
          </div>
        </Panel>
      )}

      {owner && (
        <Panel title="Настройки">
          <form className="flex flex-wrap gap-2 p-5 pb-2" onSubmit={e => {
            e.preventDefault();
            act(() => send(`/api/teams/${team.id}`, 'PATCH', { name: rename }), async () => { reload(); await onChanged(); });
          }}>
            <Field value={rename} onChange={setRename} maxLength={40} />
            <Button variant="ghost" type="submit">Переименовать</Button>
            <Button variant="danger" onClick={() => confirm('Удалить команду? Участники перестанут видеть бои друг друга. Сами бои останутся.')
              && act(() => send(`/api/teams/${team.id}`, 'DELETE'), onGone)}>Удалить команду</Button>
          </form>
          <p className="px-5 pb-4 text-[12px] text-dim">Удаление команды не трогает бои: они остаются у тех, кто их загрузил.</p>
        </Panel>
      )}
    </div>
  );
}

export function JoinPage() {
  useTitle('Приглашение');
  const { code = '' } = useParams();
  const { reloadTeams } = useShell();
  const navigate = useNavigate();
  const [invite, setInvite] = useState<{ team_id: number; team_name: string; already_member: boolean } | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    api<typeof invite>(`/api/invites/${encodeURIComponent(code)}`).then(setInvite)
      .catch(e => { if (!(e instanceof AuthError)) setError(e.message); });
  }, [code]);

  const join = async () => {
    try {
      const r = await send<{ team_id: number }>(`/api/invites/${encodeURIComponent(code)}/accept`, 'POST');
      await reloadTeams();
      navigate(`/team?id=${r.team_id}`);
    } catch (e) { setError((e as Error).message); }
  };

  return (
    <div className="mx-auto max-w-lg pt-[8vh]">
      <Panel className="p-6">
        <div className="text-[12px] font-semibold uppercase tracking-wide text-muted">Приглашение в команду</div>
        {error ? (
          <>
            <h1 className="mt-2 text-[22px] font-extrabold">Ссылка не работает</h1>
            <p className="mt-1 text-[13.5px] text-muted">{error}. Попроси у владельца команды новую ссылку.</p>
            <Link to="/" className="mt-5 inline-block text-[13px] font-semibold text-accent">На главную →</Link>
          </>
        ) : !invite ? <p className="mt-3 text-[13.5px] text-muted">Проверяем ссылку…</p> : (
          <>
            <h1 className="mt-2 text-[22px] font-extrabold">{invite.team_name}</h1>
            {invite.already_member ? (
              <>
                <p className="mt-1 text-[13.5px] text-muted">Ты уже в этой команде.</p>
                <Button className="mt-5" onClick={() => navigate(`/team?id=${invite.team_id}`)}>Открыть команду</Button>
              </>
            ) : (
              <>
                <p className="mt-1 text-[13.5px] text-muted">Участники команды видят бои друг друга: твои загруженные бои станут видны им, а их — тебе. Выйти можно в любой момент.</p>
                <div className="mt-5 flex gap-2">
                  <Button onClick={join}>Вступить</Button>
                  <Button variant="ghost" onClick={() => navigate('/')}>Не сейчас</Button>
                </div>
              </>
            )}
          </>
        )}
      </Panel>
    </div>
  );
}
