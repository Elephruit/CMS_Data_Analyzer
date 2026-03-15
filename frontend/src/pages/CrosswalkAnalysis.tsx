import React, { useState, useEffect } from 'react';
import { 
  Shuffle, 
  ArrowRight, 
  Search, 
  Download,
  AlertCircle,
  RefreshCw,
  Plus,
  Trash2,
  History,
  X,
  TrendingUp,
  TrendingDown,
  Minus
} from 'lucide-react';
import { PageHeader, StatCard, ChartCard, Badge } from '../components/ui/Primitives';
import { useFilters } from '../context/FilterContext';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

interface CrosswalkData {
  status: string;
  year: number;
  metrics?: {
    renewals: number;
    consolidations: number;
    newPlans: number;
    terminated: number;
    sae: number;
    sar: number;
  };
  rows?: any[];
}

interface AEPSwitching {
  year: number;
  results: {
    organization: string;
    aepGrowth: number;
    estimatedSwitching: number;
  }[];
}

interface LineageRow {
  crosswalk_year: number;
  previous_plan_key: string;
  previous_plan_name: string;
  current_plan_key: string;
  current_plan_name: string;
  status: string;
}

export const CrosswalkAnalysis: React.FC = () => {
  const { filters } = useFilters();
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState<CrosswalkData | null>(null);
  const [aepData, setAepData] = useState<AEPSwitching | null>(null);
  const [searchTerm, setSearch] = useState('');
  
  // Lineage State
  const [selectedLineage, setSelectedLineage] = useState<LineageRow[] | null>(null);
  const [lineageTarget, setLineageTarget] = useState<string | null>(null);

  const fetchData = async () => {
    setLoading(true);
    try {
      const [res, aepRes] = await Promise.all([
        fetch('http://127.0.0.1:3000/api/crosswalk/analysis', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters),
        }),
        fetch('http://127.0.0.1:3000/api/crosswalk/aep-switching', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters),
        })
      ]);

      if (res.ok) setData(await res.json());
      if (aepRes.ok) setAepData(await aepRes.json());
    } catch (e) {
      console.error('Failed to fetch crosswalk data', e);
    } finally {
      setLoading(false);
    }
  };

  const fetchLineage = async (row: any) => {
    setLineageTarget(row.current_plan_key);
    try {
      const contractId = row.current_contract_id;
      const planId = row.current_plan_id;
      const year = data?.year || 2025;
      
      const res = await fetch(`http://127.0.0.1:3000/api/crosswalk/lineage?contract_id=${contractId}&plan_id=${planId}&year=${year}`);
      if (res.ok) {
        setSelectedLineage(await res.json());
      }
    } catch (e) {
      console.error('Failed to fetch lineage', e);
    }
  };

  useEffect(() => {
    fetchData();
  }, [filters]);

  const filteredRows = data?.rows?.filter(r => 
    r.previous_plan_key.toLowerCase().includes(searchTerm.toLowerCase()) ||
    r.current_plan_key.toLowerCase().includes(searchTerm.toLowerCase()) ||
    (r.previous_plan_name && r.previous_plan_name.toLowerCase().includes(searchTerm.toLowerCase())) ||
    (r.current_plan_name && r.current_plan_name.toLowerCase().includes(searchTerm.toLowerCase()))
  ) || [];

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <RefreshCw className="w-8 h-8 text-sky-500 animate-spin" />
      </div>
    );
  }

  if (data?.status === 'not_loaded') {
    return (
      <div className="flex flex-col items-center justify-center h-[60vh] space-y-6">
        <div className="p-6 bg-slate-800/50 rounded-full border border-slate-700">
          <Shuffle className="w-12 h-12 text-slate-500" />
        </div>
        <div className="text-center space-y-2">
          <h2 className="text-2xl font-black text-white">Crosswalk Data Missing</h2>
          <p className="text-slate-400 max-w-md mx-auto">
            Plan Crosswalk data for {data.year} has not been imported into the analytical store.
          </p>
        </div>
        <a 
          href="/data" 
          className="px-6 py-3 bg-sky-500 hover:bg-sky-400 text-white text-xs font-black uppercase tracking-widest rounded-xl transition-all shadow-lg shadow-sky-500/20"
        >
          GO TO DATA MANAGEMENT
        </a>
      </div>
    );
  }

  return (
    <div className="max-w-[1600px] mx-auto space-y-8 pb-20">
      <PageHeader 
        title={`${data?.year} Plan Crosswalk Analysis`} 
        subtitle="Track plan renewals, consolidations, and geographic expansions. Essential for year-over-year lineage tracking."
        action={
          <button className="flex items-center gap-2 px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded-lg border border-slate-700 text-xs font-bold text-slate-300 transition-all">
            <Download className="w-4 h-4" />
            EXPORT TO CSV
          </button>
        }
      />

      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <StatCard 
          title="Total Renewals" 
          value={data?.metrics?.renewals.toLocaleString() || '0'} 
          icon={RefreshCw}
          trend={0}
        />
        <StatCard 
          title="Consolidations" 
          value={data?.metrics?.consolidations.toLocaleString() || '0'} 
          icon={Minus}
          variant="warning"
        />
        <StatCard 
          title="New Plans" 
          value={data?.metrics?.newPlans.toLocaleString() || '0'} 
          icon={Plus}
          variant="success"
        />
        <StatCard 
          title="Terminated" 
          value={data?.metrics?.terminated.toLocaleString() || '0'} 
          icon={Trash2}
          variant="danger"
        />
        <StatCard 
          title="Expansions (SAE)" 
          value={data?.metrics?.sae.toLocaleString() || '0'} 
          icon={TrendingUp}
          variant="success"
        />
        <StatCard 
          title="Reductions (SAR)" 
          value={data?.metrics?.sar.toLocaleString() || '0'} 
          icon={TrendingDown}
          variant="warning"
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        <div className="lg:col-span-2 space-y-8">
          <ChartCard title="Plan Transition Workspace">
            <div className="space-y-6">
              <div className="flex items-center gap-4">
                <div className="relative flex-1">
                  <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
                  <input 
                    type="text"
                    placeholder="Search by Plan ID or Name..."
                    value={searchTerm}
                    onChange={(e) => setSearch(e.target.value)}
                    className="w-full bg-slate-900/50 border border-slate-800 rounded-xl pl-11 pr-4 py-3 text-sm text-white focus:border-sky-500 focus:ring-1 focus:ring-sky-500 outline-none transition-all"
                  />
                </div>
              </div>

              <div className="overflow-x-auto">
                <table className="w-full text-left border-separate border-spacing-y-2">
                  <thead>
                    <tr className="text-[10px] font-black uppercase text-slate-500 tracking-widest">
                      <th className="px-4 py-2">Prior Year Plan ({data?.year ? data.year - 1 : ''})</th>
                      <th className="px-4 py-2 text-center">Transition</th>
                      <th className="px-4 py-2">Current Year Plan ({data?.year})</th>
                      <th className="px-4 py-2">Status</th>
                      <th className="px-4 py-2 text-right">Lineage</th>
                    </tr>
                  </thead>
                  <tbody className="space-y-2">
                    {filteredRows.map((row, idx) => {
                      const s = row.status.toUpperCase();
                      const isNew = s.includes("NEW");
                      const isTerminated = s.includes("TERMINATED") || s.includes("NON-RENEWED");
                      const isConsolidated = s.includes("CONSOLIDATED");
                      
                      return (
                        <tr key={idx} className="group hover:bg-slate-800/30 transition-colors">
                          <td className="px-4 py-4 bg-slate-900/50 rounded-l-xl border-y border-l border-slate-800 group-hover:border-slate-700">
                            <div className="flex flex-col">
                              <span className={cn("text-xs font-black", isNew ? "text-slate-600 line-through" : "text-sky-400")}>
                                {row.previous_plan_key}
                              </span>
                              <span className="text-[10px] text-slate-500 font-medium truncate max-w-[200px]">
                                {row.previous_plan_name || (isNew ? 'N/A' : 'Unknown')}
                              </span>
                            </div>
                          </td>
                          <td className="px-4 py-4 bg-slate-900/50 border-y border-slate-800 group-hover:border-slate-700 text-center">
                            <ArrowRight className={cn(
                              "w-4 h-4 mx-auto",
                              isNew ? "text-emerald-500" : isTerminated ? "text-rose-500" : "text-sky-500"
                            )} />
                          </td>
                          <td className="px-4 py-4 bg-slate-900/50 border-y border-slate-800 group-hover:border-slate-700">
                            <div className="flex flex-col">
                              <span className={cn("text-xs font-black", isTerminated ? "text-slate-600 line-through" : "text-sky-400")}>
                                {row.current_plan_key}
                              </span>
                              <span className="text-[10px] text-slate-500 font-medium truncate max-w-[200px]">
                                {row.current_plan_name || (isTerminated ? 'N/A' : 'Unknown')}
                              </span>
                            </div>
                          </td>
                          <td className="px-4 py-4 bg-slate-900/50 border-y border-slate-800 group-hover:border-slate-700">
                            <Badge 
                              variant={isNew ? 'success' : isTerminated ? 'danger' : isConsolidated ? 'warning' : 'primary'}
                              label={row.status}
                            />
                          </td>
                          <td className="px-4 py-4 bg-slate-900/50 rounded-r-xl border-y border-r border-slate-800 group-hover:border-slate-700 text-right">
                            <button 
                              onClick={() => fetchLineage(row)}
                              className="p-2 hover:bg-sky-500/10 rounded-lg transition-all text-slate-500 hover:text-sky-400"
                              title="View Plan Lineage"
                            >
                              <History className="w-4 h-4" />
                            </button>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </ChartCard>
        </div>

        <div className="space-y-8">
          <ChartCard title="AEP Switching Estimator">
            <div className="space-y-6">
              <div className="p-4 bg-amber-500/5 border border-amber-500/20 rounded-xl space-y-2">
                <div className="flex items-center gap-2 text-amber-500 text-[10px] font-black uppercase tracking-widest">
                  <AlertCircle className="w-3 h-3" />
                  Analytical Rule
                </div>
                <p className="text-[10px] text-slate-400 leading-relaxed">
                  Estimated Switching calculates the difference between actual Feb enrollment and the enrollment expected based solely on Crosswalk mappings from Dec.
                </p>
              </div>

              <div className="space-y-4">
                {aepData?.results.map((res, idx) => (
                  <div key={idx} className="p-4 bg-slate-900/50 border border-slate-800 rounded-xl space-y-3">
                    <div className="flex items-center justify-between">
                      <span className="text-xs font-black text-white uppercase tracking-tight">{res.organization}</span>
                      <Badge 
                        variant={res.estimatedSwitching > 0 ? 'success' : 'danger'}
                        label={`${res.estimatedSwitching > 0 ? '+' : ''}${res.estimatedSwitching.toLocaleString()}`}
                      />
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-1">
                        <span className="text-[9px] font-bold text-slate-500 uppercase tracking-tighter">Total AEP Growth</span>
                        <div className="text-xs font-bold text-slate-300">{res.aepGrowth.toLocaleString()}</div>
                      </div>
                      <div className="space-y-1">
                        <span className="text-[9px] font-bold text-slate-500 uppercase tracking-tighter">Switching Component</span>
                        <div className="text-xs font-bold text-slate-300">{res.estimatedSwitching.toLocaleString()}</div>
                      </div>
                    </div>
                    <div className="h-1 w-full bg-slate-800 rounded-full overflow-hidden">
                      <div 
                        className={cn(
                          "h-full transition-all duration-1000",
                          res.estimatedSwitching > 0 ? "bg-emerald-500" : "bg-rose-500"
                        )}
                        style={{ width: `${Math.min(Math.abs(res.estimatedSwitching) / 10000 * 100, 100)}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </ChartCard>
        </div>
      </div>

      {/* Lineage Modal */}
      {selectedLineage && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm animate-in fade-in duration-200">
          <div className="w-full max-w-2xl bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl overflow-hidden flex flex-col max-h-[80vh]">
            <div className="p-6 border-b border-slate-800 flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-sky-500/10 rounded-lg">
                  <History className="w-5 h-5 text-sky-500" />
                </div>
                <div>
                  <h3 className="text-lg font-black text-white uppercase tracking-tight">Plan Lineage Trace</h3>
                  <p className="text-xs text-slate-500 font-bold uppercase tracking-widest">{lineageTarget}</p>
                </div>
              </div>
              <button 
                onClick={() => setSelectedLineage(null)}
                className="p-2 hover:bg-slate-800 rounded-lg transition-all text-slate-500 hover:text-white"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            <div className="flex-1 overflow-y-auto p-6 space-y-8">
              <div className="relative">
                <div className="absolute left-[19px] top-4 bottom-4 w-px bg-slate-800" />
                <div className="space-y-10">
                  {selectedLineage.map((item, idx) => (
                    <div key={idx} className="relative pl-12">
                      <div className={cn(
                        "absolute left-0 top-1 w-10 h-10 rounded-full border-2 border-slate-900 flex items-center justify-center z-10",
                        idx === 0 ? "bg-sky-500 shadow-lg shadow-sky-500/20" : "bg-slate-800"
                      )}>
                        <span className="text-[10px] font-black text-white">{item.crosswalk_year}</span>
                      </div>
                      
                      <div className="p-4 bg-slate-800/30 border border-slate-800 rounded-xl space-y-3">
                        <div className="flex items-center justify-between">
                          <span className="text-xs font-black text-sky-400 uppercase">{item.current_plan_key}</span>
                          <Badge 
                            variant={item.status.includes("NEW") || item.status.includes("New") ? 'success' : item.status.includes("Consolidated") ? 'warning' : 'primary'}
                            label={item.status}
                          />
                        </div>
                        <p className="text-xs font-bold text-slate-300">{item.current_plan_name}</p>
                        
                        <div className="pt-3 border-t border-slate-800 flex items-center gap-2">
                          <span className="text-[9px] font-bold text-slate-500 uppercase tracking-tighter">Predecessor:</span>
                          <span className="text-[9px] font-mono text-slate-400 bg-slate-900 px-1.5 py-0.5 rounded">
                            {item.previous_plan_key}
                          </span>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div className="p-6 bg-slate-950/50 border-t border-slate-800">
              <button 
                onClick={() => setSelectedLineage(null)}
                className="w-full py-3 bg-slate-800 hover:bg-slate-700 text-white text-xs font-black uppercase tracking-widest rounded-xl transition-all"
              >
                Close Trace
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
