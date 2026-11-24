import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom';
import './index.css';
import { Shell } from './components/Shell';
import { AdminPage } from './pages/Admin';
import { AnalyticsLayout } from './pages/analytics/Layout';
import { Overview } from './pages/analytics/Overview';
import { BattlesTab, MapsTab, PlayersTab, TanksTab } from './pages/analytics/Tabs';
import { AccountPage, LoginPage } from './pages/Auth';
import { LegalPage } from './pages/Legal';
import { QuickCheck } from './pages/QuickCheck';
import { JoinPage, TeamPage } from './pages/Team';
import { api, token } from './api';
import { useState, useEffect } from 'react';

// The whole site: FastAPI serves this app for every path that is not /api.

/** Only supporter users can enter: others stay on / and see the upsell in the shell. */
function SupporterRoute({ children }: { children: React.ReactNode }) {
  const [ok, setOk] = useState<boolean | null>(null);
  useEffect(() => {
    if (!token()) { setOk(false); return; }
    api<{ is_supporter: boolean }>('/api/supporter/me')
      .then(r => setOk(Boolean(r.is_supporter)))
      .catch(() => setOk(false));
  }, []);
  if (ok === null) return null;
  if (!ok) return <Navigate to="/" replace />;
  return children;
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="login" element={<LoginPage />} />
        <Route path="privacy" element={<LegalPage doc="privacy" />} />
        <Route path="consent" element={<LegalPage doc="consent" />} />
        <Route element={<Shell />}>
          <Route index element={<QuickCheck />} />
          <Route path="analytics" element={<SupporterRoute><AnalyticsLayout /></SupporterRoute>}>
            <Route index element={<Overview />} />
            <Route path="players" element={<PlayersTab />} />
            <Route path="tanks" element={<TanksTab />} />
            <Route path="maps" element={<MapsTab />} />
            <Route path="battles" element={<BattlesTab />} />
          </Route>
          <Route path="team" element={<TeamPage />} />
          <Route path="join/:code" element={<JoinPage />} />
          <Route path="account" element={<AccountPage />} />
          <Route path="admin" element={<AdminPage />} />
          <Route path="app/*" element={<Navigate to="/analytics" replace />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </StrictMode>,
);
