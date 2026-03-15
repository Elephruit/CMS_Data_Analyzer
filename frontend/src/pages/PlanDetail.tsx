import React, { useEffect, useState, useMemo } from 'react';
import { useFilters } from '../context/FilterContext';
import { useOrgDisplay } from '../context/OrgDisplayContext';
import { ArrowUpRight, ArrowDownRight, ChevronDown, ChevronRight } from 'lucide-react';

const DEFAULT_COLORS = [
  '#0ea5e9', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899',
  '#14b8a6', '#f97316', '#6366f1', '#22c55e', '#e11d48',
];

interface PlanRow {
  contractId: string;
  planId: string;
  planName: string;
  parentOrg: string;
  planType: string;
  enrollment: number;
  priorEnrollment: number;
  momChange: number;
}

interface PlanListData {
  rows: PlanRow[];
  currentMonth: number;
  priorMonth: number;
}

interface OrgGroup {
  orgName: string;
  totalEnrollment: number;
  plans: PlanRow[];
}

export const PlanDetail: React.FC = () => {
  const { filters } = useFilters();
  const { getDisplayName, getColor } = useOrgDisplay();
  const [data, setData] = useState<PlanListData | null>(null);
  const [loading, setLoading] = useState(true);
  const [collapsedOrgs, setCollapsedOrgs] = useState<Set<string>>(new Set());

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const res = await fetch('http://127.0.0.1:3000/api/query/plan-list', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters),
        });
        const result = await res.json();
        setData(result);
      } catch (err) {
        console.error('Failed to fetch plan list:', err);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, [filters]);

  // Group by parent org, sorted by total enrollment desc
  const groups = useMemo((): OrgGroup[] => {
    if (!data) return [];
    const map = new Map<string, PlanRow[]>();
    for (const row of data.rows) {
      const list = map.get(row.parentOrg) ?? [];
      list.push(row);
      map.set(row.parentOrg, list);
    }
    return Array.from(map.entries())
      .map(([orgName, plans]) => ({
        orgName,
        totalEnrollment: plans.reduce((s, p) => s + p.enrollment, 0),
        plans: [...plans].sort((a, b) => b.enrollment - a.enrollment),
      }))
      .sort((a, b) => b.totalEnrollment - a.totalEnrollment);
  }, [data]);

  const toggleOrg = (orgName: string) => {
    setCollapsedOrgs(prev => {
      const next = new Set(prev);
      if (next.has(orgName)) next.delete(orgName);
      else next.add(orgName);
      return next;
    });
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="flex flex-col items-center gap-4">
          <div className="w-10 h-10 border-4 border-sky-500 border-t-transparent rounded-full animate-spin" />
          <span className="text-slate-400 font-bold uppercase tracking-widest text-xs">Loading Plans...</span>
        </div>
      </div>
    );
  }

  if (groups.length === 0) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-slate-500 text-sm italic">No plans match the current filters.</p>
      </div>
    );
  }

  return (
    <div className="space-y-3 max-w-[1600px] mx-auto pb-8">

      {/* Summary row */}
      <div className="flex items-center justify-between text-xs text-slate-500 px-1">
        <span>
          <span className="text-white font-bold">{data?.rows.length.toLocaleString()}</span> plans across{' '}
          <span className="text-white font-bold">{groups.length.toLocaleString()}</span> organizations
        </span>
        <span className="font-mono">
          {data?.rows.reduce((s, r) => s + r.enrollment, 0).toLocaleString()} total members
        </span>
      </div>

      {/* Grouped plan list */}
      {groups.map((group, gi) => {
        const color = getColor(group.orgName, DEFAULT_COLORS[gi % DEFAULT_COLORS.length]);
        const displayOrg = getDisplayName(group.orgName);
        const isCollapsed = collapsedOrgs.has(group.orgName);

        return (
          <div key={group.orgName} className="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden">
            {/* Org header row */}
            <button
              onClick={() => toggleOrg(group.orgName)}
              className="w-full flex items-center gap-3 px-5 py-3.5 hover:bg-slate-800/50 transition-colors text-left"
            >
              <div className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: color }} />
              <span className="font-bold text-sm text-white flex-1 truncate">{displayOrg}</span>
              {displayOrg !== group.orgName && (
                <span className="text-[10px] text-slate-600 font-mono truncate max-w-[200px] hidden md:block">
                  {group.orgName}
                </span>
              )}
              <span className="text-xs font-mono text-slate-400 shrink-0 ml-2">
                {group.totalEnrollment.toLocaleString()} members
              </span>
              <span className="text-[10px] text-slate-600 shrink-0">
                {group.plans.length} plan{group.plans.length !== 1 ? 's' : ''}
              </span>
              {isCollapsed
                ? <ChevronRight className="w-4 h-4 text-slate-500 shrink-0" />
                : <ChevronDown className="w-4 h-4 text-slate-500 shrink-0" />
              }
            </button>

            {/* Plans under this org */}
            {!isCollapsed && (
              <div className="border-t border-slate-800/60">
                <table className="w-full text-left">
                  <thead>
                    <tr className="border-b border-slate-800/40">
                      <th className="pl-10 pr-4 py-2.5 text-[10px] font-bold text-slate-600 uppercase tracking-widest">Plan</th>
                      <th className="px-4 py-2.5 text-[10px] font-bold text-slate-600 uppercase tracking-widest">ID</th>
                      <th className="px-4 py-2.5 text-[10px] font-bold text-slate-600 uppercase tracking-widest">Type</th>
                      <th className="px-4 py-2.5 text-[10px] font-bold text-slate-600 uppercase tracking-widest text-right">Enrollment</th>
                      <th className="px-4 py-2.5 text-[10px] font-bold text-slate-600 uppercase tracking-widest text-right">MoM</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/30">
                    {group.plans.map((plan) => {
                      const isPos = plan.momChange > 0;
                      const isNeg = plan.momChange < 0;
                      return (
                        <tr key={`${plan.contractId}|${plan.planId}`} className="hover:bg-slate-800/20 transition-colors group">
                          <td className="pl-10 pr-4 py-3">
                            <span className="text-xs font-semibold text-slate-200 group-hover:text-white transition-colors">
                              {plan.planName}
                            </span>
                          </td>
                          <td className="px-4 py-3">
                            <span className="text-[10px] font-mono text-slate-500">
                              {plan.contractId}|{plan.planId}
                            </span>
                          </td>
                          <td className="px-4 py-3">
                            <span className="text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400 font-medium">
                              {plan.planType}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-right">
                            <span className="text-xs font-mono font-bold text-slate-300">
                              {plan.enrollment.toLocaleString()}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-right">
                            <span className={`text-xs font-mono font-bold flex items-center justify-end gap-0.5 ${isPos ? 'text-emerald-400' : isNeg ? 'text-rose-400' : 'text-slate-500'}`}>
                              {isPos && <ArrowUpRight className="w-3 h-3" />}
                              {isNeg && <ArrowDownRight className="w-3 h-3" />}
                              {isPos ? '+' : ''}{plan.momChange.toLocaleString()}
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
};
