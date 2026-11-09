// localStorage can be missing or throw (private mode, blocked site data): it only ever
// remembers conveniences such as the last filters, so failing quietly is fine.

export function load<T>(key: string, fallback: T): T {
  try {
    const v = localStorage.getItem(key);
    return v == null ? fallback : (JSON.parse(v) as T);
  } catch {
    return fallback;
  }
}

export function save(key: string, value: unknown) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch { /* nothing to do */ }
}
