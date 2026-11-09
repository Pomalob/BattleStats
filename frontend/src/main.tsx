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

// The whole site: FastAPI serves this app for every path that is not /api.
createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="login" element={<LoginPage />} />
        <Route path="privacy" element={<LegalPage doc="privacy" />} />
        <Route path="consent" element={<LegalPage doc="consent" />} />
        <Route element={<Shell />}>
          <Route index element={<QuickCheck />} />
          <Route path="analytics" element={<AnalyticsLayout />}>
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
