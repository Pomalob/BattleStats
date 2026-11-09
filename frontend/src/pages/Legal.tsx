import { useTitle } from '../title';
import { Link } from 'react-router-dom';
import { Logo } from '../components/Shell';
import { ConsentText, PrivacyText } from './legalText';

/** Public documents: readable without an account (the sign-up form links to them). */
export function LegalPage({ doc }: { doc: 'privacy' | 'consent' }) {
  useTitle(doc === 'privacy' ? 'Политика обработки данных' : 'Согласие на обработку данных');
  return (
    <div className="mx-auto max-w-[820px] px-5 py-8">
      <div className="mb-8 flex items-center justify-between"><Logo /><Link to="/" className="text-[13px] font-semibold text-accent">На сайт →</Link></div>
      <article className="space-y-3 text-[14.5px] leading-relaxed [&_h1]:text-[28px] [&_h1]:font-extrabold [&_h1]:tracking-tight
        [&_h2]:mt-7 [&_h2]:text-[17px] [&_h2]:font-bold [&_h2]:text-accent [&_ul]:list-disc [&_ol]:list-decimal [&_ul]:space-y-1.5
        [&_ol]:space-y-1.5 [&_ul]:pl-6 [&_ol]:pl-6 [&_a]:text-accent [&_a]:underline">
        {doc === 'privacy' ? <PrivacyText /> : <ConsentText />}
      </article>
    </div>
  );
}
