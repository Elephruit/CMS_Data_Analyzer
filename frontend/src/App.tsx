import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { AppShell } from './components/layout/AppShell';
import { Dashboard } from './pages/Dashboard';
import { EnrollmentExplorer } from './pages/EnrollmentExplorer';
import { OrganizationAnalysis } from './pages/OrganizationAnalysis';
import { Geography } from './pages/Geography';
import { DataManagement } from './pages/DataManagement';
import { FilterProvider } from './context/FilterContext';

// Placeholder components for other pages
const Placeholder = ({ title }: { title: string }) => (
  <div className="flex items-center justify-center h-full">
    <h1 className="text-2xl font-bold text-slate-500">{title} Placeholder</h1>
  </div>
);

function App() {
  return (
    <FilterProvider>
      <Router>
        <AppShell>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/explorer" element={<EnrollmentExplorer />} />
            <Route path="/organizations" element={<OrganizationAnalysis />} />
            <Route path="/plans" element={<Placeholder title="Plans" />} />
            <Route path="/geography" element={<Geography />} />
            <Route path="/growth" element={<Placeholder title="Growth & AEP" />} />
            <Route path="/data" element={<DataManagement />} />
            <Route path="/exports" element={<Placeholder title="Exports" />} />
          </Routes>
        </AppShell>
      </Router>
    </FilterProvider>
  );
}

export default App;
