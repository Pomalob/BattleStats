import type React from 'react';
import type { ReactNode } from 'react';

export function Panel({ title, aside, children, className = '' }:
  { title?: ReactNode; aside?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`animate-rise rounded-lg border border-line-soft bg-surface ${className}`}>
      {(title || aside) && (
        <header className="flex flex-wrap items-center justify-between gap-3 px-5 pt-4">
          {title && <h2 className="text-[15px] font-bold">{title}</h2>}
          {aside}
        </header>
      )}
      {children}
    </section>
  );
}

export function Segmented<T extends string>({ value, options, onChange, label }:
  { value: T; options: [T, string][]; onChange: (v: T) => void; label: string }) {
  return (
    <div role="tablist" aria-label={label} className="inline-flex max-w-full overflow-x-auto rounded-md border border-line bg-bg p-0.5 [scrollbar-width:none]">
      {options.map(([v, text]) => (
        <button key={v} type="button" role="tab" aria-selected={v === value} onClick={() => onChange(v)}
          className={`shrink-0 whitespace-nowrap rounded-[5px] px-3 py-1.5 text-[12.5px] font-semibold transition-colors duration-150 ${
            v === value ? 'bg-accent text-bg' : 'text-muted hover:bg-surface-3 hover:text-ink'}`}>
          {text}
        </button>
      ))}
    </div>
  );
}

export function Select({ value, options, onChange, label }:
  { value: string; options: [string, string][]; onChange: (v: string) => void; label: string }) {
  return (
    <select aria-label={label} value={value} onChange={e => onChange(e.target.value)}
      className="h-9 min-w-0 flex-1 rounded-md border border-line bg-surface-2 pl-3 pr-8 sm:flex-none text-[13px] font-semibold text-ink outline-none transition-colors hover:border-dim focus:border-accent">
      {options.map(([v, text]) => <option key={v} value={v}>{text}</option>)}
    </select>
  );
}

export function Skeleton({ className = '' }: { className?: string }) {
  return <div className={`animate-pulse rounded-md bg-surface-2 ${className}`} />;
}

const BUTTONS = {
  primary: 'bg-accent text-bg hover:bg-accent-hover',
  ghost: 'border border-line text-muted hover:border-dim hover:text-ink',
  danger: 'border border-loss/40 text-loss hover:bg-loss/10',
};

export function Button({ variant = 'primary', size = 'md', className = '', type = 'button', ...props }:
  React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: keyof typeof BUTTONS; size?: 'sm' | 'md' }) {
  return (
    <button type={type} {...props}
      className={`shrink-0 whitespace-nowrap rounded-md font-bold transition-colors duration-150 disabled:cursor-not-allowed disabled:opacity-40 ${
        size === 'sm' ? 'px-2.5 py-1 text-[12px]' : 'px-4 py-2 text-[13px]'} ${BUTTONS[variant]} ${className}`} />
  );
}

export function Field({ value, onChange, className = '', ...props }:
  Omit<React.InputHTMLAttributes<HTMLInputElement>, 'onChange'> & { value: string; onChange: (v: string) => void }) {
  return (
    <input {...props} value={value} onChange={e => onChange(e.target.value)}
      className={`h-9 min-w-0 flex-1 rounded-md border border-line bg-surface-2 px-3 text-[13.5px] outline-none placeholder:text-dim focus:border-accent ${className}`} />
  );
}
