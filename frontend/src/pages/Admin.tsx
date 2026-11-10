import { useTitle } from '../title';
import { useEffect, useState } from 'react';
import { Navigate } from 'react-router-dom';
import { api, AuthError, send } from '../api';
import { LineChart } from '../components/charts';
import { useShell } from '../components/Shell';
import { Button, Panel, Skeleton } from '../components/ui';
import { fmt, fmt1, fmtPct, pct } from '../format';

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
      <AiLog />
    </div>
  );
}

/* --- AI analysis: settings, connection check, causes of failure, the latest attempts --- */

interface LogRow {
  id: number; created_at: string; username: string | null; mode: string | null; status: string;
  error_kind: string | null; error: Record<string, unknown> | null; problems: string[] | null;
  model: string | null; usage: Record<string, number> | null; duration_ms: number | null;
}
interface AiLogData {
  settings: { enabled: boolean; model: string; base_url: string; daily_limit: number; global_daily_limit: number; timeout_seconds: number };
  messages: Record<string, string>;
  recent: LogRow[];
  by_day: { day: string; ok: number; failed: number; pending: number; input_tokens: number; output_tokens: number; cache_read_tokens: number }[];
  errors_7d: { error_kind: string | null; n: number; last_at: string }[];
}
interface CheckResult { ok: boolean; kind?: string; message?: string; detail?: Record<string, unknown>; reply?: string; model?: string; duration_ms?: number }

const KIND_LABEL: Record<string, string> = {
  payment: 'не оплачен баланс', auth: 'ключ', model: 'модель', rate_limit: 'перегрузка', timeout: 'таймаут',
  connection: 'нет связи', provider: 'сбой сервиса', bad_request: 'запрос отклонён', api: 'ошибка API',
  refusal: 'отказ модели', too_long: 'слишком длинно', check_failed: 'не прошёл проверку', internal: 'ошибка сервера',
  disabled: 'выключен',
};
const STATUS: Record<string, string> = { ok: 'text-win', failed: 'text-loss', pending: 'text-draw' };
const when = (iso: string) => new Date(iso).toLocaleString('ru-RU', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });

function errorText(r: LogRow) {
  if (r.status === 'ok') return '';
  const e = r.error ?? {};
  const parts = [e.status && `HTTP ${e.status}`, e.type, e.error, e.message, e.request_id && `запрос ${e.request_id}`].filter(Boolean);
  return [...parts.map(String), ...(r.problems ?? []).slice(0, 3)].join(' · ');
}

function AiLog() {
  const [data, setData] = useState<AiLogData | null>(null);
  const [error, setError] = useState('');
  const [check, setCheck] = useState<CheckResult | null>(null);
  const [checking, setChecking] = useState(false);
  const reload = () => api<AiLogData>('/api/admin/explanations').then(setData)
    .catch(e => { if (!(e instanceof AuthError)) setError(e.message); });
  useEffect(() => { reload(); }, []);

  const runCheck = async () => {
    setChecking(true);
    try { setCheck(await send<CheckResult>('/api/admin/explanations/check', 'POST')); }
    catch (e) { setCheck({ ok: false, message: (e as Error).message }); }
    finally { setChecking(false); }
  };

  if (error) return <div className="rounded-lg border border-loss/40 bg-loss/10 px-4 py-3 text-[13px]">{error}</div>;
  if (!data) return <Skeleton className="h-[300px]" />;
  const s = data.settings;
  const today = data.by_day[0];
  return (
    <Panel title="ИИ-разбор"
      aside={<div className="flex gap-2">
        <Button size="sm" variant="ghost" onClick={reload}>Обновить</Button>
        <Button size="sm" onClick={runCheck} disabled={checking || !s.enabled}>{checking ? 'Проверяем…' : 'Проверить подключение'}</Button>
      </div>}>
      <div className="space-y-4 px-5 pb-5 pt-3 text-[13px]">
        <div className="flex flex-wrap gap-x-5 gap-y-1 text-muted">
          <span>Состояние: <b className={s.enabled ? 'text-win' : 'text-loss'}>{s.enabled ? 'включён' : 'выключен — нет GPTUNNEL_API_KEY'}</b></span>
          <span>Модель: <b className="num text-ink">{s.model}</b></span>
          <span>Адрес: <b className="num text-ink">{s.base_url}</b></span>
          <span>Лимит: <b className="text-ink">{s.daily_limit}</b> на человека и <b className="text-ink">{s.global_daily_limit}</b> на сайт в сутки</span>
        </div>

        {check && (
          <div className={`rounded-md border px-3 py-2 ${check.ok ? 'border-win/40 bg-win/10' : 'border-loss/40 bg-loss/10'}`}>
            {check.ok
              ? <>Подключение работает: модель <b className="num">{check.model}</b> ответила «{check.reply}» за {fmt(check.duration_ms)} мс.</>
              : <><b>Не работает:</b> {check.message ?? (check.kind ? KIND_LABEL[check.kind] : 'ошибка')}
                  {check.detail && <div className="num mt-1 break-all text-[12px] text-muted">{JSON.stringify(check.detail)}</div>}</>}
          </div>
        )}

        <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
          {([['Сегодня готово', today?.ok], ['Сегодня с ошибкой', today?.failed],
            ['Токенов на входе сегодня', today?.input_tokens], ['Токенов на выходе сегодня', today?.output_tokens]] as [string, number | undefined][]).map(([label, v]) => (
            <div key={label} className="rounded-md border border-line-soft bg-surface-2 px-4 py-3">
              <div className="text-[12px] font-semibold text-muted">{label}</div>
              <div className="mt-1 text-[22px] font-extrabold tabular-nums">{fmt(Number(v ?? 0))}</div>
            </div>
          ))}
        </div>

        {data.errors_7d.length > 0 && (
          <div>
            <div className="mb-1.5 font-semibold">Ошибки за 7 дней</div>
            <ul className="space-y-1">
              {data.errors_7d.map(e => (
                <li key={e.error_kind ?? '—'} className="flex flex-wrap gap-x-3 text-muted">
                  <b className="text-loss">{KIND_LABEL[e.error_kind ?? ''] ?? e.error_kind ?? 'без причины'}</b>
                  <span className="num">{fmt(e.n)} раз, последняя {when(e.last_at)}</span>
                  {e.error_kind && data.messages[e.error_kind] && <span>· пользователь видит: «{data.messages[e.error_kind]}»</span>}
                </li>
              ))}
            </ul>
          </div>
        )}

        <div className="overflow-x-auto">
          <table className="w-full min-w-[860px] border-collapse text-[12.5px]">
            <thead><tr className="border-y border-line-soft text-[11.5px] text-muted">
              {['Когда', 'Кто', 'Режим', 'Итог', 'Причина', 'Токены, вход / выход', 'Время'].map((h, i) => (
                <th key={h} scope="col" className={`px-2 py-2 font-semibold ${i >= 5 ? 'text-right' : 'text-left'}`}>{h}</th>
              ))}
            </tr></thead>
            <tbody>
              {data.recent.map(r => (
                <tr key={r.id} className="border-b border-line-soft align-top">
                  <td className="num whitespace-nowrap px-2 py-2 text-muted">{when(r.created_at)}</td>
                  <td className="px-2 py-2">{r.username ?? '—'}</td>
                  <td className="px-2 py-2 text-muted">{MODE[r.mode ?? ''] ?? r.mode}</td>
                  <td className={`whitespace-nowrap px-2 py-2 font-semibold ${STATUS[r.status] ?? ''}`}>
                    {r.status === 'ok' ? 'готово' : r.status === 'pending' ? 'в работе' : KIND_LABEL[r.error_kind ?? ''] ?? 'ошибка'}
                  </td>
                  <td className="max-w-[420px] break-words px-2 py-2 text-muted">{errorText(r)}</td>
                  <td className="num whitespace-nowrap px-2 py-2 text-right">
                    {r.usage?.input_tokens != null ? `${fmt(r.usage.input_tokens)} / ${fmt(r.usage.output_tokens)}` : '—'}
                  </td>
                  <td className="num whitespace-nowrap px-2 py-2 text-right">{r.duration_ms != null ? `${fmt1(r.duration_ms / 1000)} с` : '—'}</td>
                </tr>
              ))}
              {!data.recent.length && <tr><td colSpan={7} className="py-8 text-center text-muted">Разборов ещё не было</td></tr>}
            </tbody>
          </table>
        </div>
      </div>
    </Panel>
  );
}
