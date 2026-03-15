import React, { useEffect, useState, useMemo, useCallback } from 'react';
import { useFilters } from '../context/FilterContext';
import { ArrowUpRight, ArrowDownRight, ChevronDown, ChevronRight } from 'lucide-react';
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid,
  Tooltip as RechartsTooltip, ResponsiveContainer,
} from 'recharts';
import { formatMonthShort, formatEnrollment } from '../utils/formatters';

const TYPE_COLORS = [
  '#0ea5e9', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899',
  '#14b8a6', '#f97316', '#6366f1', '#22c55e', '#e11d48',
];

// ── Types ──────────────────────────────────────────────────────────────────────
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

interface TypeGroup {
  planType: string;
  totalEnrollment: number;
  totalMomChange: number;
  plans: PlanRow[];
}

interface TrendPoint {
  month: string;
  enrollment: number;
}

// ── Inline trend chart domain helper ──────────────────────────────────────────
function niceChartDomain(values: number[]): [number, number] | ['auto', 'auto'] {
  if (values.length === 0) return ['auto', 'auto'];
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || max * 0.1;
  const buffer = range * 0.2;
  const rawStep = (range + 2 * buffer) / 5;
  const exp = Math.pow(10, Math.floor(Math.log10(rawStep)));
  const norm = rawStep / exp;
  const niceStep = norm <= 1 ? exp : norm <= 2 ? 2 * exp : norm <= 2.5 ? 2.5 * exp : norm <= 5 ? 5 * exp : 10 * exp;
  return [
    Math.max(0, Math.floor((min - buffer) / niceStep) * niceStep),
    Math.ceil((max + buffer) / niceStep) * niceStep,
  ];
}

// ── MoM badge ─────────────────────────────────────────────────────────────────
const MomBadge: React.FC<{ change: number; className?: string }> = ({ change, className = '' }) => {
  const isPos = change > 0;
  const isNeg = change < 0;
  return (
    <span className={`font-mono font-bold flex items-center gap-0.5 ${isPos ? 'text-emerald-400' : isNeg ? 'text-rose-400' : 'text-slate-500'} ${className}`}>
      {isPos && <ArrowUpRight className="w-3 h-3 shrink-0" />}
      {isNeg && <ArrowDownRight className="w-3 h-3 shrink-0" />}
      {isPos ? '+' : ''}{change.toLocaleString()}
    </span>
  );
};

// ── Main component ─────────────────────────────────────────────────────────────
export const PlanDetail: React.FC = () => {
  const { filters } = useFilters();
  const [data, setData] = useState<PlanListData | null>(null);
  const [loading, setLoading] = useState(true);
  const [collapsedTypes, setCollapsedTypes] = useState<Set<string>>(new Set());
  const [expandedPlan, setExpandedPlan] = useState<string | null>(null);
  const [planTrends, setPlanTrends] = useState<Record<string, TrendPoint[]>>({});
  const [trendLoading, setTrendLoading] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setExpandedPlan(null);
      setPlanTrends({});
      try {
        const res = await fetch('http://127.0.0.1:3000/api/query/plan-list', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters),
        });
        setData(await res.json());
      } catch (err) {
        console.error('Failed to fetch plan list:', err);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, [filters]);

  // Group by plan type, sorted by total enrollment desc
  const groups = useMemo((): TypeGroup[] => {
    if (!data) return [];
    const map = new Map<string, PlanRow[]>();
    for (const row of data.rows) {
      const list = map.get(row.planType) ?? [];
      list.push(row);
      map.set(row.planType, list);
    }
    return Array.from(map.entries())
      .map(([planType, plans]) => ({
        planType,
        totalEnrollment: plans.reduce((s, p) => s + p.enrollment, 0),
        totalMomChange: plans.reduce((s, p) => s + p.momChange, 0),
        plans: [...plans].sort((a, b) => b.enrollment - a.enrollment),
      }))
      .sort((a, b) => b.totalEnrollment - a.totalEnrollment);
  }, [data]);

  const toggleType = useCallback((planType: string) => {
    setCollapsedTypes(prev => {
      const next = new Set(prev);
      if (next.has(planType)) next.delete(planType); else next.add(planType);
      return next;
    });
  }, []);

  const togglePlan = useCallback(async (plan: PlanRow) => {
    const key = `${plan.contractId}|${plan.planId}`;
    if (expandedPlan === key) { setExpandedPlan(null); return; }
    setExpandedPlan(key);
    if (planTrends[key]) return; // already cached
    setTrendLoading(key);
    try {
      const res = await fetch('http://127.0.0.1:3000/api/query/plan-details', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contract_id: plan.contractId, plan_id: plan.planId }),
      });
      const result = await res.json();
      const chartData: TrendPoint[] = (result.trend ?? []).map(
        ({ month, value }: { month: number; value: number }) => ({
          month: month.toString().replace(/(\d{4})(\d{2})/, '$1-$2'),
          enrollment: value,
        })
      );
      setPlanTrends(prev => ({ ...prev, [key]: chartData }));
    } catch (e) {
      console.error('Failed to fetch plan trend:', e);
    } finally {
      setTrendLoading(null);
    }
  }, [expandedPlan, planTrends]);

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

      {/* Summary bar */}
      <div className="flex items-center justify-between text-xs text-slate-500 px-1">
        <span>
          <span className="text-white font-bold">{data?.rows.length.toLocaleString()}</span> plans across{' '}
          <span className="text-white font-bold">{groups.length}</span> plan types
        </span>
        <span className="font-mono">
          {data?.rows.reduce((s, r) => s + r.enrollment, 0).toLocaleString()} total members
        </span>
      </div>

      {/* Type groups */}
      {groups.map((group, gi) => {
        const color = TYPE_COLORS[gi % TYPE_COLORS.length];
        const isCollapsed = collapsedTypes.has(group.planType);
        return (
          <div key={group.planType} className="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden">

            {/* Type header */}
            <button
              onClick={() => toggleType(group.planType)}
              className="w-full flex items-center gap-3 px-5 py-3.5 hover:bg-slate-800/50 transition-colors text-left"
            >
              <div className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: color }} />
              <span className="font-bold text-sm text-white flex-1">{group.planType}</span>
              <span className="text-[10px] text-slate-600 shrink-0">
                {group.plans.length} plan{group.plans.length !== 1 ? 's' : ''}
              </span>
              <span className="text-xs font-mono text-slate-300 shrink-0 ml-2 min-w-[90px] text-right">
                {group.totalEnrollment.toLocaleString()}
              </span>
              <MomBadge change={group.totalMomChange} className="text-xs shrink-0 min-w-[80px] justify-end" />
              {isCollapsed
                ? <ChevronRight className="w-4 h-4 text-slate-500 shrink-0" />
                : <ChevronDown className="w-4 h-4 text-slate-500 shrink-0" />
              }
            </button>

            {/* Plans table */}
            {!isCollapsed && (
              <div className="border-t border-slate-800/60">
                <table className="w-full text-left">
                  <thead>
                    <tr className="border-b border-slate-800/40">
                      <th className="pl-10 pr-4 py-2.5 text-[10px] font-bold text-slate-600 uppercase tracking-widest">Plan</th>
                      <th className="px-4 py-2.5 text-[10px] font-bold text-slate-600 uppercase tracking-widest">ID</th>
                      <th className="px-4 py-2.5 text-[10px] font-bold text-slate-600 uppercase tracking-widest">Parent Org</th>
                      <th className="px-4 py-2.5 text-[10px] font-bold text-slate-600 uppercase tracking-widest text-right">Enrollment</th>
                      <th className="px-4 py-2.5 text-[10px] font-bold text-slate-600 uppercase tracking-widest text-right">MoM</th>
                    </tr>
                  </thead>
                  <tbody>
                    {group.plans.map((plan) => {
                      const planKey = `${plan.contractId}|${plan.planId}`;
                      const isExpanded = expandedPlan === planKey;
                      const trend = planTrends[planKey];
                      const isLoadingTrend = trendLoading === planKey;

                      return (
                        <React.Fragment key={planKey}>
                          {/* Plan row — clickable */}
                          <tr
                            onClick={() => togglePlan(plan)}
                            className={`border-b border-slate-800/30 cursor-pointer transition-colors group ${isExpanded ? 'bg-slate-800/30' : 'hover:bg-slate-800/20'}`}
                          >
                            <td className="pl-10 pr-4 py-3">
                              <div className="flex items-center gap-2">
                                {isExpanded
                                  ? <ChevronDown className="w-3 h-3 text-sky-500 shrink-0" />
                                  : <ChevronRight className="w-3 h-3 text-slate-600 shrink-0 group-hover:text-slate-400" />
                                }
                                <span className={`text-xs font-semibold transition-colors ${isExpanded ? 'text-sky-400' : 'text-slate-200 group-hover:text-white'}`}>
                                  {plan.planName}
                                </span>
                              </div>
                            </td>
                            <td className="px-4 py-3">
                              <span className="text-[10px] font-mono text-slate-500">{planKey}</span>
                            </td>
                            <td className="px-4 py-3">
                              <span className="text-[10px] text-slate-500 truncate max-w-[160px] block">{plan.parentOrg}</span>
                            </td>
                            <td className="px-4 py-3 text-right">
                              <span className="text-xs font-mono font-bold text-slate-300">
                                {plan.enrollment.toLocaleString()}
                              </span>
                            </td>
                            <td className="px-4 py-3 text-right">
                              <MomBadge change={plan.momChange} className="text-xs justify-end" />
                            </td>
                          </tr>

                          {/* Expanded trend chart row */}
                          {isExpanded && (
                            <tr className="bg-slate-950/60 border-b border-slate-800/30">
                              <td colSpan={5} className="px-10 py-4">
                                {isLoadingTrend ? (
                                  <div className="flex items-center justify-center h-[180px] gap-3">
                                    <div className="w-5 h-5 border-2 border-sky-500 border-t-transparent rounded-full animate-spin" />
                                    <span className="text-slate-500 text-xs">Loading trend...</span>
                                  </div>
                                ) : trend && trend.length > 0 ? (
                                  <div>
                                    <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-3">
                                      Enrollment History — {plan.planName}
                                    </div>
                                    <div style={{ height: 180 }}>
                                      <ResponsiveContainer width="100%" height="100%">
                                        <AreaChart data={trend} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
                                          <defs>
                                            <linearGradient id={`grad-${planKey.replace(/[|]/g, '-')}`} x1="0" y1="0" x2="0" y2="1">
                                              <stop offset="5%" stopColor={color} stopOpacity={0.2} />
                                              <stop offset="95%" stopColor={color} stopOpacity={0} />
                                            </linearGradient>
                                          </defs>
                                          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                                          <XAxis
                                            dataKey="month"
                                            axisLine={false}
                                            tickLine={false}
                                            tick={{ fill: '#64748b', fontSize: 9 }}
                                            dy={8}
                                            tickFormatter={formatMonthShort}
                                          />
                                          <YAxis
                                            axisLine={false}
                                            tickLine={false}
                                            tick={{ fill: '#64748b', fontSize: 9 }}
                                            tickFormatter={formatEnrollment}
                                            domain={niceChartDomain(trend.map(t => t.enrollment))}
                                            width={48}
                                          />
                                          <RechartsTooltip
                                            contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '10px', fontSize: 11 }}
                                            itemStyle={{ color: '#f1f5f9', fontWeight: 700 }}
                                            labelStyle={{ color: '#94a3b8', fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.05em' }}
                                            labelFormatter={(val: any) => formatMonthShort(val)}
                                            formatter={(v: any) => [Number(v).toLocaleString(), 'Enrollment']}
                                          />
                                          <Area
                                            type="monotone"
                                            dataKey="enrollment"
                                            stroke={color}
                                            strokeWidth={2}
                                            fill={`url(#grad-${planKey.replace(/[|]/g, '-')})`}
                                            fillOpacity={1}
                                            dot={{ fill: color, stroke: '#0f172a', strokeWidth: 2, r: 3 }}
                                            activeDot={{ r: 5, fill: color, stroke: '#fff', strokeWidth: 2 }}
                                          />
                                        </AreaChart>
                                      </ResponsiveContainer>
                                    </div>
                                  </div>
                                ) : (
                                  <div className="flex items-center justify-center h-[100px] text-slate-600 text-xs italic">
                                    No trend data available for this plan.
                                  </div>
                                )}
                              </td>
                            </tr>
                          )}
                        </React.Fragment>
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
