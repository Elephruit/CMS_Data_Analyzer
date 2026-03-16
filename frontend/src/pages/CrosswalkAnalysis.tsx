import React, { useState, useEffect } from 'react';
import {
  Shuffle,
  ArrowRight,
  Search,
  Download,
  RefreshCw,
  Plus,
  Trash2,
  History,
  X,
  TrendingUp,
  TrendingDown,
  Minus,
  Map,
} from 'lucide-react';
import { PageHeader, StatCard, ChartCard, Badge } from '../components/ui/Primitives';
import { useFilters } from '../context/FilterContext';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

interface CountyEntry {
  state: string;
  county: string;
}

interface CrosswalkRow {
  crosswalk_year: number;
  previous_contract_id: string;
  previous_plan_id: string;
  previous_plan_key: string;
  previous_plan_name?: string;
  current_contract_id: string;
  current_plan_id: string;
  current_plan_key: string;
  current_plan_name?: string;
  status: string;
  display_status: string;
  is_new: boolean;
  is_terminated: boolean;
  is_expansion: boolean;
  is_reduction: boolean;
  total_counties: number;
  filtered_counties: number;
  counties_added: number;
  counties_removed: number;
  group_size: number;
  group_counties_added: number;
  group_counties_removed: number;
  // County sets for map display
  renewed_counties: CountyEntry[];
  added_counties: CountyEntry[];
  removed_counties: CountyEntry[];
  org?: string;
  plan_type?: string;
  is_egwp?: boolean;
}

// Merge-arrow SVG: two lines converging into one (many-to-one visual)
const MergeArrow: React.FC<{ color: string }> = ({ color }) => (
  <svg viewBox="0 0 40 48" className="w-10 h-12" fill="none">
    <path d="M4 6 C4 6 20 6 20 24" stroke={color} strokeWidth="2.5" strokeLinecap="round" fill="none" />
    <path d="M4 42 C4 42 20 42 20 24" stroke={color} strokeWidth="2.5" strokeLinecap="round" fill="none" />
    <path d="M20 24 L34 24" stroke={color} strokeWidth="2.5" strokeLinecap="round" />
    <path d="M30 19 L36 24 L30 29" stroke={color} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" fill="none" />
  </svg>
);

function statusVariant(displayStatus: string): 'primary' | 'success' | 'warning' | 'danger' | 'neutral' | 'pink' {
  switch (displayStatus) {
    case 'Closed':                  return 'danger';
    case 'New Plan':                return 'primary';
    case 'Service Area Expansion':  return 'success';
    case 'Service Area Reduction':  return 'pink';
    case 'Service Area Change':     return 'warning';
    case 'Consolidated':            return 'warning';
    default:                        return 'primary';
  }
}

interface CrosswalkGroup {
  groupKey: string;
  predecessors: CrosswalkRow[];
  successor: CrosswalkRow;
  isMany: boolean;
  resolvedDisplayStatus: string;
  sharedOrg: string | null;
}

function groupRows(rows: CrosswalkRow[]): CrosswalkGroup[] {
  const map = new Map<string, CrosswalkRow[]>();
  for (const row of rows) {
    const key = (row.is_terminated || !row.current_plan_key)
      ? `__term__${row.previous_plan_key}`
      : row.current_plan_key;
    if (!map.has(key)) map.set(key, []);
    map.get(key)!.push(row);
  }
  const groups: CrosswalkGroup[] = [];
  for (const [key, members] of map.entries()) {
    const isMany = members.length > 1;
    const resolvedDisplayStatus = isMany ? 'Consolidated' : members[0].display_status;
    // If all predecessors share the same parent org, hoist it as a group header.
    const orgs = [...new Set(members.map(m => m.org).filter(Boolean))];
    const sharedOrg = isMany && orgs.length === 1 ? orgs[0]! : null;
    groups.push({ groupKey: key, predecessors: members, successor: members[0], isMany, resolvedDisplayStatus, sharedOrg });
  }
  return groups;
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
  rows?: CrosswalkRow[];
}

interface LineageRow {
  crosswalk_year: number;
  previous_plan_key: string;
  previous_plan_name: string;
  current_plan_key: string;
  current_plan_name: string;
  status: string;
}

interface LineageYearGroup {
  year: number;
  rows: LineageRow[];
}

function groupLineageByYear(rows: LineageRow[]): LineageYearGroup[] {
  const map = new Map<number, LineageRow[]>();
  for (const row of rows) {
    if (!map.has(row.crosswalk_year)) map.set(row.crosswalk_year, []);
    map.get(row.crosswalk_year)!.push(row);
  }
  return [...map.entries()].map(([year, rows]) => ({ year, rows }));
}

// ──────────────────────────────────────────────────────────────
// County Map Modal
// ──────────────────────────────────────────────────────────────
interface MapModalProps {
  group: CrosswalkGroup;
  onClose: () => void;
}

const MapModal: React.FC<MapModalProps> = ({ group, onClose }) => {
  const rep = group.successor;

  // Merge county sets from all predecessors in the group
  const renewed = rep.renewed_counties ?? [];
  const added   = rep.added_counties   ?? [];
  const removed = rep.removed_counties ?? [];

  // Build a map: state → [{county, category}] sorted by county name
  type Category = 'renewed' | 'added' | 'removed';
  const byState = new Map<string, Array<{ county: string; category: Category }>>();
  const addEntries = (list: CountyEntry[], cat: Category) => {
    for (const { state, county } of list) {
      if (!byState.has(state)) byState.set(state, []);
      byState.get(state)!.push({ county, category: cat });
    }
  };
  addEntries(renewed, 'renewed');
  addEntries(added, 'added');
  addEntries(removed, 'removed');
  // Sort within each state
  for (const entries of byState.values()) {
    entries.sort((a, b) => a.county.localeCompare(b.county));
  }
  const sortedStates = [...byState.keys()].sort();

  const catStyle: Record<Category, string> = {
    renewed: 'bg-sky-500/10 text-sky-400 border border-sky-500/20',
    added:   'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20',
    removed: 'bg-rose-500/10 text-rose-400 border border-rose-500/20',
  };

  const planLabel = rep.is_terminated
    ? rep.previous_plan_key
    : rep.current_plan_key;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm">
      <div className="w-full max-w-3xl bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl overflow-hidden flex flex-col max-h-[85vh]">
        {/* Header */}
        <div className="p-6 border-b border-slate-800 flex items-center justify-between shrink-0">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-sky-500/10 rounded-lg">
              <Map className="w-5 h-5 text-sky-500" />
            </div>
            <div>
              <h3 className="text-lg font-black text-white uppercase tracking-tight">County Service Area</h3>
              <p className="text-xs text-slate-500 font-bold uppercase tracking-widest">{planLabel}</p>
            </div>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-slate-800 rounded-lg transition-all text-slate-500 hover:text-white">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Summary bar */}
        <div className="flex items-center gap-6 px-6 py-3 bg-slate-950/30 border-b border-slate-800 shrink-0">
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-sky-400" />
            <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{renewed.length} Renewed</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-emerald-400" />
            <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{added.length} New</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-rose-400" />
            <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{removed.length} Removed</span>
          </div>
          <span className="text-[10px] font-bold text-slate-600 uppercase tracking-widest ml-auto">
            Total {renewed.length + added.length + removed.length} counties
          </span>
        </div>

        {/* County list */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {sortedStates.length === 0 && (
            <p className="text-sm text-slate-500 text-center py-8">No county data available for this plan.</p>
          )}
          {sortedStates.map(state => (
            <div key={state}>
              <h4 className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2">{state}</h4>
              <div className="flex flex-wrap gap-1.5">
                {byState.get(state)!.map(({ county, category }, i) => (
                  <span key={i} className={cn('px-2 py-0.5 rounded text-[10px] font-bold', catStyle[category])}>
                    {county}
                  </span>
                ))}
              </div>
            </div>
          ))}
        </div>

        <div className="p-4 bg-slate-950/50 border-t border-slate-800 shrink-0">
          <button onClick={onClose} className="w-full py-2.5 bg-slate-800 hover:bg-slate-700 text-white text-xs font-black uppercase tracking-widest rounded-xl transition-all">
            Close
          </button>
        </div>
      </div>
    </div>
  );
};

// ──────────────────────────────────────────────────────────────
// Main Component
// ──────────────────────────────────────────────────────────────
export const CrosswalkAnalysis: React.FC = () => {
  const { filters } = useFilters();
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState<CrosswalkData | null>(null);
  const [searchTerm, setSearch] = useState('');
  const [selectedLineage, setSelectedLineage] = useState<LineageRow[] | null>(null);
  const [lineageTarget, setLineageTarget] = useState<string | null>(null);
  const [selectedMapGroup, setSelectedMapGroup] = useState<CrosswalkGroup | null>(null);

  const fetchData = async () => {
    setLoading(true);
    try {
      const res = await fetch('http://127.0.0.1:3000/api/crosswalk/analysis', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(filters),
      });
      if (res.ok) setData(await res.json());
    } catch (e) {
      console.error('Failed to fetch crosswalk data', e);
    } finally {
      setLoading(false);
    }
  };

  const fetchLineage = async (group: CrosswalkGroup) => {
    const rep = group.successor;
    setLineageTarget(rep.current_plan_key || rep.previous_plan_key);
    try {
      const contractId = rep.is_terminated ? rep.previous_contract_id : rep.current_contract_id;
      const planId     = rep.is_terminated ? rep.previous_plan_id     : rep.current_plan_id;
      const year = data?.year || 2026;
      const res = await fetch(`http://127.0.0.1:3000/api/crosswalk/lineage?contract_id=${contractId}&plan_id=${planId}&year=${year}`);
      if (res.ok) setSelectedLineage(await res.json());
    } catch (e) {
      console.error('Failed to fetch lineage', e);
    }
  };

  useEffect(() => { fetchData(); }, [filters]);

  const filteredRows = data?.rows?.filter(r =>
    r.previous_plan_key.toLowerCase().includes(searchTerm.toLowerCase()) ||
    r.current_plan_key.toLowerCase().includes(searchTerm.toLowerCase()) ||
    (r.previous_plan_name && r.previous_plan_name.toLowerCase().includes(searchTerm.toLowerCase())) ||
    (r.current_plan_name && r.current_plan_name.toLowerCase().includes(searchTerm.toLowerCase())) ||
    (r.org && r.org.toLowerCase().includes(searchTerm.toLowerCase()))
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
        <a href="/data" className="px-6 py-3 bg-sky-500 hover:bg-sky-400 text-white text-xs font-black uppercase tracking-widest rounded-xl transition-all shadow-lg shadow-sky-500/20">
          GO TO DATA MANAGEMENT
        </a>
      </div>
    );
  }

  const groups = groupRows(filteredRows);

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
        <StatCard title="Total Renewals"    value={data?.metrics?.renewals.toLocaleString() || '0'}       icon={RefreshCw}  trend={0} />
        <StatCard title="Consolidations"    value={data?.metrics?.consolidations.toLocaleString() || '0'} icon={Minus}      variant="warning" />
        <StatCard title="New Plans"         value={data?.metrics?.newPlans.toLocaleString() || '0'}       icon={Plus}       variant="success" />
        <StatCard title="Terminated"        value={data?.metrics?.terminated.toLocaleString() || '0'}     icon={Trash2}     variant="danger" />
        <StatCard title="Expansions (SAE)"  value={data?.metrics?.sae.toLocaleString() || '0'}            icon={TrendingUp} variant="success" />
        <StatCard title="Reductions (SAR)"  value={data?.metrics?.sar.toLocaleString() || '0'}            icon={TrendingDown} variant="warning" />
      </div>

      <div className="space-y-8">
        <ChartCard title="Plan Transition Workspace">
          <div className="space-y-6">
            {/* Search */}
            <div className="relative">
              <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
              <input
                type="text"
                placeholder="Search by Plan ID or Name..."
                value={searchTerm}
                onChange={(e) => setSearch(e.target.value)}
                className="w-full bg-slate-900/50 border border-slate-800 rounded-xl pl-11 pr-4 py-3 text-sm text-white focus:border-sky-500 focus:ring-1 focus:ring-sky-500 outline-none transition-all"
              />
            </div>

            {/* Rows */}
            <div className="space-y-2">
              {groups.map((group, gIdx) => {
                const rep = group.successor;
                const { isMany, resolvedDisplayStatus, sharedOrg } = group;

                const displayAdded   = isMany ? rep.group_counties_added   : rep.counties_added;
                const displayRemoved = isMany ? rep.group_counties_removed : rep.counties_removed;

                const svgColor = rep.is_terminated ? '#f43f5e'
                  : resolvedDisplayStatus === 'Consolidated' ? '#f59e0b'
                  : rep.is_new ? '#10b981'
                  : rep.is_expansion && !rep.is_reduction ? '#34d399'
                  : rep.is_reduction && !rep.is_expansion ? '#f472b6'
                  : '#38bdf8';

                return (
                  <div
                    key={gIdx}
                    className="grid items-center gap-x-3 p-3 bg-slate-900/50 border border-slate-800 rounded-xl hover:border-slate-700 transition-colors"
                    style={{ gridTemplateColumns: '1fr 52px 1fr auto auto auto' }}
                  >
                    {/* ── Col 1: Predecessor(s) ── */}
                    <div className="min-w-0">
                      {/* Org header for grouped plans */}
                      {sharedOrg && (
                        <p className="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5 truncate">
                          {sharedOrg}
                        </p>
                      )}
                      <div className="flex flex-col gap-1.5">
                        {group.predecessors.map((pred, pIdx) => (
                          <div key={pIdx} className="flex flex-col min-w-0">
                            <span className={cn('text-xs font-black truncate',
                              rep.is_new ? 'text-slate-500 line-through' : 'text-sky-400'
                            )}>
                              {isMany ? `• ${pred.previous_plan_key}` : pred.previous_plan_key || '—'}
                            </span>
                            {pred.previous_plan_name && (
                              <span className="text-[10px] text-slate-500 font-medium truncate">
                                {isMany ? pred.previous_plan_name : pred.previous_plan_name}
                              </span>
                            )}
                            {/* For single rows, show org under the plan name */}
                            {!isMany && pred.org && (
                              <span className="text-[9px] text-slate-600 font-bold uppercase tracking-tighter truncate">
                                {pred.org}
                              </span>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>

                    {/* ── Col 2: Connector (fixed 52px) ── */}
                    <div className="flex items-center justify-center">
                      {isMany
                        ? <MergeArrow color={svgColor} />
                        : <ArrowRight className="w-4 h-4" style={{ color: svgColor }} />
                      }
                    </div>

                    {/* ── Col 3: Successor ── */}
                    <div className="min-w-0 flex flex-col justify-center">
                      {rep.is_terminated ? (
                        <span className="text-xs font-bold text-slate-600 italic">No successor</span>
                      ) : (
                        <>
                          <span className="text-xs font-black text-sky-400 truncate">
                            {rep.current_plan_key || '—'}
                          </span>
                          {rep.current_plan_name && (
                            <span className="text-[10px] text-slate-500 font-medium truncate">
                              {rep.current_plan_name}
                            </span>
                          )}
                          {rep.plan_type && (
                            <span className="text-[9px] text-slate-600 font-bold uppercase tracking-tighter">
                              {rep.plan_type}{rep.is_egwp ? ' · EGWP' : ''}
                            </span>
                          )}
                        </>
                      )}
                    </div>

                    {/* ── Col 4: Status badge ── */}
                    <div className="flex items-center">
                      <Badge variant={statusVariant(resolvedDisplayStatus)} label={resolvedDisplayStatus} />
                    </div>

                    {/* ── Col 5: County metrics ── */}
                    <div className="flex flex-col items-end justify-center min-w-[52px]">
                      <span className="text-xs font-bold text-slate-300">
                        {rep.is_terminated
                          ? '0'
                          : rep.filtered_counties > 0 && rep.filtered_counties !== rep.total_counties
                            ? `${rep.filtered_counties}/${rep.total_counties}`
                            : (rep.total_counties || '—')}
                      </span>
                      {displayAdded > 0 && (
                        <span className="text-[9px] font-bold text-emerald-500">+{displayAdded}</span>
                      )}
                      {displayRemoved > 0 && (
                        <span className="text-[9px] font-bold text-rose-500">-{displayRemoved}</span>
                      )}
                    </div>

                    {/* ── Col 6: Action buttons ── */}
                    <div className="flex items-center gap-1">
                      <button
                        onClick={() => setSelectedMapGroup(group)}
                        className="p-2 hover:bg-emerald-500/10 rounded-lg transition-all text-slate-500 hover:text-emerald-400"
                        title="View County Map"
                      >
                        <Map className="w-4 h-4" />
                      </button>
                      <button
                        onClick={() => fetchLineage(group)}
                        className="p-2 hover:bg-sky-500/10 rounded-lg transition-all text-slate-500 hover:text-sky-400"
                        title="View Plan Lineage"
                      >
                        <History className="w-4 h-4" />
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </ChartCard>
      </div>

      {/* County Map Modal */}
      {selectedMapGroup && (
        <MapModal group={selectedMapGroup} onClose={() => setSelectedMapGroup(null)} />
      )}

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
              <button onClick={() => setSelectedLineage(null)} className="p-2 hover:bg-slate-800 rounded-lg transition-all text-slate-500 hover:text-white">
                <X className="w-5 h-5" />
              </button>
            </div>

            <div className="flex-1 overflow-y-auto p-6 space-y-8">
              <div className="relative">
                <div className="absolute left-[19px] top-4 bottom-4 w-px bg-slate-800" />
                <div className="space-y-10">
                  {groupLineageByYear(selectedLineage).map((yearGroup, idx) => {
                    const isConsolidation = yearGroup.rows.length > 1;
                    const rep = yearGroup.rows[0];
                    return (
                      <div key={idx} className="relative pl-12">
                        <div className={cn(
                          'absolute left-0 top-1 w-10 h-10 rounded-full border-2 border-slate-900 flex items-center justify-center z-10',
                          idx === 0 ? 'bg-sky-500 shadow-lg shadow-sky-500/20' : 'bg-slate-800'
                        )}>
                          <span className="text-[10px] font-black text-white">{yearGroup.year}</span>
                        </div>

                        <div className="p-4 bg-slate-800/30 border border-slate-800 rounded-xl space-y-3">
                          <div className="flex items-center justify-between">
                            <span className="text-xs font-black text-sky-400 uppercase">{rep.current_plan_key}</span>
                            <Badge
                              variant={rep.status.includes('NEW') || rep.status.includes('New') ? 'success' : rep.status.includes('Consolidated') ? 'warning' : 'primary'}
                              label={rep.status}
                            />
                          </div>
                          <p className="text-xs font-bold text-slate-300">{rep.current_plan_name}</p>

                          <div className="pt-3 border-t border-slate-800">
                            {isConsolidation ? (
                              <>
                                <span className="text-[9px] font-bold text-amber-500 uppercase tracking-tighter">Consolidated from:</span>
                                <div className="flex flex-col gap-1 mt-1.5">
                                  {yearGroup.rows.map((r, rIdx) => (
                                    <span key={rIdx} className="text-[9px] font-mono text-slate-400 bg-slate-900 px-1.5 py-0.5 rounded w-fit">
                                      {r.previous_plan_key}
                                      {r.previous_plan_name ? ` — ${r.previous_plan_name}` : ''}
                                    </span>
                                  ))}
                                </div>
                              </>
                            ) : (
                              <div className="flex items-center gap-2">
                                <span className="text-[9px] font-bold text-slate-500 uppercase tracking-tighter">Predecessor:</span>
                                <span className="text-[9px] font-mono text-slate-400 bg-slate-900 px-1.5 py-0.5 rounded">
                                  {rep.previous_plan_key}
                                </span>
                              </div>
                            )}
                          </div>
                        </div>
                      </div>
                    );
                  })}
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
