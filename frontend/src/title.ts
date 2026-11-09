import { useEffect } from 'react';

/** The browser tab says which page this is. */
export function useTitle(title: string) {
  useEffect(() => { document.title = `${title} — StatsBattle`; }, [title]);
}
