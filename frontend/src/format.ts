const int = new Intl.NumberFormat('ru-RU', { maximumFractionDigits: 0 });
const one = new Intl.NumberFormat('ru-RU', { minimumFractionDigits: 1, maximumFractionDigits: 1 });

export const fmt = (v: number | null | undefined) => (v == null || Number.isNaN(v) ? '—' : int.format(v));
export const fmt1 = (v: number | null | undefined) => (v == null || Number.isNaN(v) ? '—' : one.format(v));
export const pct = (part: number, whole: number) => (whole > 0 ? (part / whole) * 100 : null);
export const fmtPct = (v: number | null) => (v == null ? '—' : `${one.format(v)}%`);

export function plural(n: number, one: string, few: string, many: string) {
  const m10 = n % 10, m100 = n % 100;
  if (m10 === 1 && m100 !== 11) return one;
  if (m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14)) return few;
  return many;
}

export const shortDay = (iso: string) =>
  new Date(`${iso}T00:00:00`).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' }).replace('.', '');

export function winColor(rate: number | null) {
  if (rate == null) return 'var(--color-dim)';
  if (rate >= 55) return 'var(--color-win)';
  if (rate < 47) return 'var(--color-loss)';
  return 'var(--color-draw)';
}

/** Seconds as m:ss — how long a player stayed alive. */
export const fmtTime = (sec: number | null | undefined) =>
  sec == null || Number.isNaN(sec) ? '—' : `${Math.floor(sec / 60)}:${String(Math.round(sec % 60)).padStart(2, '0')}`;
