// Thin client for the FastAPI backend. The session token is kept in localStorage under the
// same key the old pages used, so nobody is signed out by the move to the new interface.

export class AuthError extends Error {}

export function token(): string | null {
  try {
    return localStorage.getItem('sb_token');
  } catch {
    return null;
  }
}

export function setSession(t: string, username: string) {
  try {
    localStorage.setItem('sb_token', t);
    localStorage.setItem('sb_username', username);
  } catch { /* private mode: the session lasts for this page only */ }
}

export function clearSession() {
  try {
    localStorage.removeItem('sb_token');
    localStorage.removeItem('sb_username');
  } catch { /* nothing to clear */ }
}

export function toLogin(): never {
  const here = location.pathname + location.search;
  window.location.href = `/login?next=${encodeURIComponent(here)}`;
  throw new AuthError('Нужно войти');
}

/** Only a path on this site: "//evil.example" or "https://..." would make login a redirector. */
export function safeNext(raw: string | null): string {
  return raw && /^\/(?!\/)[\w\-/?=&.%]*$/.test(raw) ? raw : '/';
}

async function request<T>(path: string, init: RequestInit = {}, body?: unknown): Promise<T> {
  const t = token();
  if (!t) toLogin();
  const headers: Record<string, string> = { Authorization: `Bearer ${t}` };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const res = await fetch(path, {
    ...init,
    headers: { ...headers, ...(init.headers as Record<string, string> | undefined) },
    body: body !== undefined ? JSON.stringify(body) : init.body,
  });
  if (res.status === 401) { clearSession(); toLogin(); }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail ?? data.error ?? `Ошибка ${res.status}`);
  return data as T;
}

export const api = <T>(path: string) => request<T>(path);
export const send = <T>(path: string, method: string, body?: unknown) => request<T>(path, { method }, body);
export const upload = <T>(path: string, form: FormData) => request<T>(path, { method: 'POST', body: form });

/** Sign-in and registration need no token yet. */
export async function authPost(mode: 'login' | 'register', body: Record<string, unknown>) {
  const res = await fetch(`/api/auth/${mode}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail ?? `Ошибка ${res.status}`);
  return data as { token: string; username: string };
}

export type Mode = '' | 'clan' | 'onslaught' | 'standard';

export interface Me { id: number; username: string; is_admin: boolean; consent_required: boolean }
export interface Team { id: number; name: string; role: string; members: number }

export interface DayRow {
  day: string; battles: number; wins: number; losses: number; draws: number;
  avg_damage: number | null; avg_assists: number | null; avg_blocked: number | null;
  avg_spotted: number | null; accuracy: number | null;
}
export interface Daily { days: DayRow[]; totals: Omit<DayRow, 'day'> }

export interface PlayerRow {
  name: string; battles: number; wins: number; losses: number; draws: number;
  avg_damage: number; avg_assists: number; avg_blocked: number; avg_received: number;
  avg_frags: number; avg_shots: number; avg_hits: number; avg_piercings: number;
  avg_dmg_win: number | null; avg_dmg_loss: number | null;
  /** Percentages over the totals; null when there was nothing to divide by. */
  accuracy: number | null; penetration: number | null; survival: number;
  avg_spotted: number | null; avg_lifetime: number | null;
  /** null when none of the player's tanks is in the expected-values table. */
  wn8: number | null;
}
export interface MapRow { map_name: string; battles: number; wins: number; losses: number; draws: number; winrate: number }
export interface VehicleRow {
  name: string; vehicle: string; battles: number; avg_damage: number; avg_assists: number;
  avg_frags: number; wins: number; losses: number;
  avg_blocked: number; avg_shots: number; accuracy: number | null; penetration: number | null; survival: number;
  wn8: number | null;
}
export interface Summary {
  players: PlayerRow[]; players_total: number; maps: MapRow[]; vehicles: VehicleRow[]; hidden: string[];
  totals: { total_battles: number; wins: number; losses: number; draws: number; first_battle: string | null; last_battle: string | null };
}
export interface BattleListRow {
  id: number; date_time: string; map_name: string; result: 'win' | 'loss' | 'draw'; filename: string;
  mode: string | null; player_count: number; mine: boolean;
}

export interface Filters { scope: string; mode: Mode; days: number }

/** Query string for the filters; `shift` moves the period back by whole periods (deltas). */
export function filterQuery(f: Filters, shift = 0): string {
  const q = new URLSearchParams({ scope: f.scope });
  if (f.mode) q.set('mode', f.mode);
  if (f.days > 0) {
    const day = (offset: number) => {
      const d = new Date();
      d.setDate(d.getDate() - offset);
      return d.toLocaleDateString('sv-SE'); // YYYY-MM-DD in local time
    };
    q.set('date_from', day(f.days * (shift + 1) - 1));
    q.set('date_to', day(f.days * shift));
  }
  return q.toString();
}
