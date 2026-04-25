import { lazy, Suspense } from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { AppShell } from './components/layout/AppShell';
import { FilterProvider } from './context/FilterContext';

const Dashboard = lazy(() => import('./pages/Dashboard').then((m) => ({ default: m.Dashboard })));
const EnrollmentExplorer = lazy(() => import('./pages/EnrollmentExplorer').then((m) => ({ default: m.EnrollmentExplorer })));
const OrganizationAnalysis = lazy(() => import('./pages/OrganizationAnalysis').then((m) => ({ default: m.OrganizationAnalysis })));
const Geography = lazy(() => import('./pages/Geography').then((m) => ({ default: m.Geography })));
const GrowthAnalytics = lazy(() => import('./pages/GrowthAnalytics').then((m) => ({ default: m.GrowthAnalytics })));
const PlanDetail = lazy(() => import('./pages/PlanDetail').then((m) => ({ default: m.PlanDetail })));
const DataManagement = lazy(() => import('./pages/DataManagement').then((m) => ({ default: m.DataManagement })));

// Placeholder components for other pages
const Placeholder = ({ title }: { title: string }) => (
  <div className="flex items-center justify-center h-full">
    <h1 className="text-2xl font-bold text-slate-500">{title} Placeholder</h1>
  </div>
);

const RouteFallback = () => (
  <div className="h-full min-h-[320px] flex items-center justify-center text-slate-500 text-sm">
    Loading...
  </div>
);

function App() {
  return (
    <FilterProvider>
      <Router>
        <AppShell>
          <Suspense fallback={<RouteFallback />}>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/explorer" element={<EnrollmentExplorer />} />
              <Route path="/organizations" element={<OrganizationAnalysis />} />
              <Route path="/plans" element={<PlanDetail />} />
              <Route path="/geography" element={<Geography />} />
              <Route path="/growth" element={<GrowthAnalytics />} />
              <Route path="/data" element={<DataManagement />} />
              <Route path="/exports" element={<Placeholder title="Exports" />} />
            </Routes>
          </Suspense>
        </AppShell>
      </Router>
    </FilterProvider>
  );
}

export default App;
