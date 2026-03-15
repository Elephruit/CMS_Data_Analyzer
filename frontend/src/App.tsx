import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { AppShell } from './components/layout/AppShell';
import { Dashboard } from './pages/Dashboard';
import { EnrollmentExplorer } from './pages/EnrollmentExplorer';
import { OrganizationAnalysis } from './pages/OrganizationAnalysis';
import { Geography } from './pages/Geography';
import { GrowthAnalytics } from './pages/GrowthAnalytics';
import { PlanDetail } from './pages/PlanDetail';
import { CrosswalkAnalysis } from './pages/CrosswalkAnalysis';
import { DataManagement } from './pages/DataManagement';
import { FilterProvider } from './context/FilterContext';
import { OrgDisplayProvider } from './context/OrgDisplayContext';

// Placeholder components for other pages
const Placeholder = ({ title }: { title: string }) => (
  <div className="flex items-center justify-center h-full">
    <h1 className="text-2xl font-bold text-slate-500">{title} Placeholder</h1>
  </div>
);

function App() {
  return (
    <OrgDisplayProvider>
    <FilterProvider>
      <Router>
        <AppShell>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/explorer" element={<EnrollmentExplorer />} />
            <Route path="/organizations" element={<OrganizationAnalysis />} />
            <Route path="/plans" element={<PlanDetail />} />
            <Route path="/geography" element={<Geography />} />
            <Route path="/growth" element={<GrowthAnalytics />} />
            <Route path="/crosswalk" element={<CrosswalkAnalysis />} />
            <Route path="/data" element={<DataManagement />} />
            <Route path="/exports" element={<Placeholder title="Exports" />} />
          </Routes>
        </AppShell>
      </Router>
    </FilterProvider>
    </OrgDisplayProvider>
  );
}

export default App;
