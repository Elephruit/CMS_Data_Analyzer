import React, { useEffect, useState, useMemo, useCallback } from 'react';
import { useFilters } from '../context/FilterContext';
import { useOrgDisplay } from '../context/OrgDisplayContext';
import { ArrowUpRight, ArrowDownRight, ChevronDown, ChevronRight } from 'lucide-react';
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid,
  Tooltip as RechartsTooltip, ResponsiveContainer,
} from 'recharts';
import { formatMonthShort, formatEnrollment } from '../utils/formatters';

const DEFAULT_COLORS = [
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
  aepGrowth: number;
  aepGrowthPct: number;
  aepDecEnrollment: number;
  isNew: boolean;
}

interface PlanListData {
  rows: PlanRow[];
  currentMonth: number;
  priorMonth: number;
  aepFebMonth: number;
  aepDecMonth: number;
}

interface TypeSubGroup {
  planType: string;
  totalEnrollment: number;
  totalMomChange: number;
  totalAepGrowth: number;
  totalAepDecEnrollment: number;
  plans: PlanRow[];
}

interface OrgGroup {
  orgName: string;
  totalEnrollment: number;
  totalMomChange: number;
  totalAepGrowth: number;
  totalAepDecEnrollment: number;
  typeGroups: TypeSubGroup[];
}

interface TrendPoint {
  month: string;
  enrollment: number;
}

// ── Helpers ────────────────────────────────────────────────────────────────────
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

// ── Change badges ──────────────────────────────────────────────────────────────
const ChangeBadge: React.FC<{ change: number; pct?: number; className?: string }> = ({ change, pct, className = '' }) => {
  const isPos = change > 0;
  const isNeg = change < 0;
  if (change === 0 && (pct === undefined || pct === 0)) {
    return <span className={`font-mono text-slate-600 tabular-nums ${className}`}>—</span>;
  }
  return (
    <span className={`font-mono font-bold flex items-center gap-0.5 tabular-nums ${isPos ? 'text-emerald-400' : isNeg ? 'text-rose-400' : 'text-slate-500'} ${className}`}>
      {isPos && <ArrowUpRight className="w-3 h-3 shrink-0" />}
      {isNeg && <ArrowDownRight className="w-3 h-3 shrink-0" />}
      {isPos ? '+' : ''}{change.toLocaleString()}
      {pct !== undefined && (
        <span className="text-[10px] font-normal opacity-75 ml-0.5">
          ({isPos ? '+' : ''}{pct.toFixed(1)}%)
        </span>
      )}
    </span>
  );
};

// Compact column header cell
const ColHeader: React.FC<{ children: React.ReactNode; className?: string }> = ({ children, className = '' }) => (
  <th className={`px-3 py-2 text-[10px] font-bold text-slate-700 uppercase tracking-widest whitespace-nowrap ${className}`}>
    {children}
  </th>
);

// ── Main component ─────────────────────────────────────────────────────────────
export const PlanDetail: React.FC = () => {
  const { filters } = useFilters();
  const { getDisplayName, getColor } = useOrgDisplay();
  const [data, setData] = useState<PlanListData | null>(null);
  const [loading, setLoading] = useState(true);
  const [collapsedOrgs, setCollapsedOrgs] = useState<Set<string>>(new Set());
  const [collapsedTypeKeys, setCollapsedTypeKeys] = useState<Set<string>>(new Set());
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

  // Two-level grouping: parent org → plan type → plans
  const groups = useMemo((): OrgGroup[] => {
    if (!data) return [];

    const orgMap = new Map<string, Map<string, PlanRow[]>>();
    for (const row of data.rows) {
      if (!orgMap.has(row.parentOrg)) orgMap.set(row.parentOrg, new Map());
      const typeMap = orgMap.get(row.parentOrg)!;
      if (!typeMap.has(row.planType)) typeMap.set(row.planType, []);
      typeMap.get(row.planType)!.push(row);
    }

    return Array.from(orgMap.entries())
      .map(([orgName, typeMap]) => {
        const typeGroups: TypeSubGroup[] = Array.from(typeMap.entries())
          .map(([planType, plans]) => ({
            planType,
            totalEnrollment: plans.reduce((s, p) => s + p.enrollment, 0),
            totalMomChange: plans.reduce((s, p) => s + p.momChange, 0),
            totalAepGrowth: plans.reduce((s, p) => s + p.aepGrowth, 0),
            totalAepDecEnrollment: plans.reduce((s, p) => s + p.aepDecEnrollment, 0),
            plans: [...plans].sort((a, b) => b.enrollment - a.enrollment),
          }))
          .sort((a, b) => b.totalEnrollment - a.totalEnrollment);

        return {
          orgName,
          totalEnrollment: typeGroups.reduce((s, g) => s + g.totalEnrollment, 0),
          totalMomChange: typeGroups.reduce((s, g) => s + g.totalMomChange, 0),
          totalAepGrowth: typeGroups.reduce((s, g) => s + g.totalAepGrowth, 0),
          totalAepDecEnrollment: typeGroups.reduce((s, g) => s + g.totalAepDecEnrollment, 0),
          typeGroups,
        };
      })
      .sort((a, b) => b.totalEnrollment - a.totalEnrollment);
  }, [data]);

  // AEP column header label: "AEP (Feb YY vs Dec YY)"
  const aepLabel = useMemo(() => {
    if (!data) return 'AEP Growth';
    const feb = data.aepFebMonth;
    const dec = data.aepDecMonth;
    if (!feb || !dec) return 'AEP Growth';
    const febStr = formatMonthShort(feb.toString().replace(/(\d{4})(\d{2})/, '$1-$2'));
    const decStr = formatMonthShort(dec.toString().replace(/(\d{4})(\d{2})/, '$1-$2'));
    return `AEP  ${febStr} vs ${decStr}`;
  }, [data]);

  const toggleOrg = useCallback((orgName: string) => {
    setCollapsedOrgs(prev => {
      const next = new Set(prev);
      if (next.has(orgName)) next.delete(orgName); else next.add(orgName);
      return next;
    });
  }, []);

  const toggleType = useCallback((orgName: string, planType: string) => {
    const key = `${orgName}||${planType}`;
    setCollapsedTypeKeys(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key); else next.add(key);
      return next;
    });
  }, []);

  const togglePlan = useCallback(async (plan: PlanRow) => {
    const key = `${plan.contractId}|${plan.planId}`;
    if (expandedPlan === key) { setExpandedPlan(null); return; }
    setExpandedPlan(key);
    if (planTrends[key]) return;
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
          <span className="text-white font-bold">{data?.rows.length.toLocaleString()}</span> plans ·{' '}
          <span className="text-white font-bold">{groups.length}</span> organizations
        </span>
        <span className="font-mono">
          {data?.rows.reduce((s, r) => s + r.enrollment, 0).toLocaleString()} total members
        </span>
      </div>

      {/* Org groups */}
      {groups.map((org, oi) => {
        const orgColor = getColor(org.orgName, DEFAULT_COLORS[oi % DEFAULT_COLORS.length]);
        const displayOrg = getDisplayName(org.orgName);
        const isOrgCollapsed = collapsedOrgs.has(org.orgName);
        const orgAepPct = org.totalAepDecEnrollment > 0
          ? (org.totalAepGrowth / org.totalAepDecEnrollment) * 100 : 0;

        return (
          <div key={org.orgName} className="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden">

            {/* ── Org header ── */}
            <button
              onClick={() => toggleOrg(org.orgName)}
              className="w-full flex items-center gap-3 px-5 py-4 hover:bg-slate-800/50 transition-colors text-left"
            >
              <div className="w-3 h-3 rounded-full shrink-0" style={{ backgroundColor: orgColor }} />
              <span className="font-bold text-sm text-white flex-1 truncate min-w-0">{displayOrg}</span>
              {displayOrg !== org.orgName && (
                <span className="text-[10px] text-slate-600 font-mono truncate max-w-[160px] hidden lg:block shrink-0">
                  {org.orgName}
                </span>
              )}
              <span className="text-[10px] text-slate-600 shrink-0">
                {org.typeGroups.length} type{org.typeGroups.length !== 1 ? 's' : ''} · {data?.rows.filter(r => r.parentOrg === org.orgName).length} plans
              </span>
              {/* Enrollment */}
              <span className="text-xs font-mono text-slate-300 shrink-0 w-[90px] text-right">
                {org.totalEnrollment.toLocaleString()}
              </span>
              {/* MoM */}
              <span className="shrink-0 w-[80px] flex justify-end">
                <ChangeBadge change={org.totalMomChange} className="text-xs" />
              </span>
              {/* AEP */}
              <span className="shrink-0 w-[110px] flex justify-end">
                <ChangeBadge change={org.totalAepGrowth} pct={orgAepPct} className="text-xs" />
              </span>
              {isOrgCollapsed
                ? <ChevronRight className="w-4 h-4 text-slate-500 shrink-0" />
                : <ChevronDown className="w-4 h-4 text-slate-500 shrink-0" />
              }
            </button>

            {/* ── Plan type sub-groups ── */}
            {!isOrgCollapsed && (
              <div className="border-t border-slate-800/60 divide-y divide-slate-800/40">
                {org.typeGroups.map((tg) => {
                  const typeKey = `${org.orgName}||${tg.planType}`;
                  const isTypeCollapsed = collapsedTypeKeys.has(typeKey);
                  const typeAepPct = tg.totalAepDecEnrollment > 0
                    ? (tg.totalAepGrowth / tg.totalAepDecEnrollment) * 100 : 0;

                  return (
                    <div key={typeKey}>

                      {/* Type header */}
                      <button
                        onClick={() => toggleType(org.orgName, tg.planType)}
                        className="w-full flex items-center gap-3 pl-10 pr-5 py-2.5 hover:bg-slate-800/40 transition-colors text-left"
                      >
                        <div className="w-1.5 h-1.5 rounded-full shrink-0 bg-slate-600" />
                        <span className="text-xs font-bold text-slate-300 flex-1">{tg.planType}</span>
                        <span className="text-[10px] text-slate-600 shrink-0">
                          {tg.plans.length} plan{tg.plans.length !== 1 ? 's' : ''}
                        </span>
                        {/* Enrollment */}
                        <span className="text-xs font-mono text-slate-400 shrink-0 w-[90px] text-right">
                          {tg.totalEnrollment.toLocaleString()}
                        </span>
                        {/* MoM */}
                        <span className="shrink-0 w-[80px] flex justify-end">
                          <ChangeBadge change={tg.totalMomChange} className="text-xs" />
                        </span>
                        {/* AEP */}
                        <span className="shrink-0 w-[110px] flex justify-end">
                          <ChangeBadge change={tg.totalAepGrowth} pct={typeAepPct} className="text-xs" />
                        </span>
                        {isTypeCollapsed
                          ? <ChevronRight className="w-3.5 h-3.5 text-slate-600 shrink-0" />
                          : <ChevronDown className="w-3.5 h-3.5 text-slate-600 shrink-0" />
                        }
                      </button>

                      {/* Plans table */}
                      {!isTypeCollapsed && (
                        <div className="border-t border-slate-800/30">
                          <table className="w-full text-left">
                            <thead>
                              <tr className="border-b border-slate-800/30">
                                <ColHeader className="pl-16 pr-3">Plan</ColHeader>
                                <ColHeader className="px-3">ID</ColHeader>
                                <ColHeader className="px-3 text-right">Enrollment</ColHeader>
                                <ColHeader className="px-3 text-right">MoM</ColHeader>
                                <ColHeader className="px-3 text-right">{aepLabel}</ColHeader>
                              </tr>
                            </thead>
                            <tbody>
                              {tg.plans.map((plan) => {
                                const planKey = `${plan.contractId}|${plan.planId}`;
                                const isExpanded = expandedPlan === planKey;
                                const trend = planTrends[planKey];
                                const isLoadingTrend = trendLoading === planKey;

                                return (
                                  <React.Fragment key={planKey}>
                                    <tr
                                      onClick={() => togglePlan(plan)}
                                      className={`border-b border-slate-800/20 cursor-pointer transition-colors group ${isExpanded ? 'bg-slate-800/25' : 'hover:bg-slate-800/15'}`}
                                    >
                                      {/* Plan name + NEW badge */}
                                      <td className="pl-16 pr-3 py-2.5">
                                        <div className="flex items-center gap-2">
                                          {isExpanded
                                            ? <ChevronDown className="w-3 h-3 text-sky-500 shrink-0" />
                                            : <ChevronRight className="w-3 h-3 text-slate-700 shrink-0 group-hover:text-slate-500" />
                                          }
                                          <span className={`text-xs font-medium transition-colors ${isExpanded ? 'text-sky-400' : 'text-slate-300 group-hover:text-white'}`}>
                                            {plan.planName}
                                          </span>
                                          {plan.isNew && (
                                            <span className="text-[9px] font-bold px-1 py-px rounded bg-sky-500/15 text-sky-400 tracking-wide shrink-0">NEW</span>
                                          )}
                                        </div>
                                      </td>
                                      {/* Contract·Plan ID */}
                                      <td className="px-3 py-2.5">
                                        <span className="text-[10px] font-mono text-slate-600">{planKey}</span>
                                      </td>
                                      {/* Enrollment */}
                                      <td className="px-3 py-2.5 text-right">
                                        <span className="text-xs font-mono font-bold text-slate-400">
                                          {plan.enrollment.toLocaleString()}
                                        </span>
                                      </td>
                                      {/* MoM */}
                                      <td className="px-3 py-2.5 text-right">
                                        <ChangeBadge change={plan.momChange} className="text-xs justify-end" />
                                      </td>
                                      {/* AEP Growth */}
                                      <td className="px-3 py-2.5 text-right">
                                        <ChangeBadge
                                          change={plan.aepGrowth}
                                          pct={plan.aepDecEnrollment > 0 ? plan.aepGrowthPct : undefined}
                                          className="text-xs justify-end"
                                        />
                                      </td>
                                    </tr>

                                    {/* Inline trend chart */}
                                    {isExpanded && (
                                      <tr className="bg-slate-950/50 border-b border-slate-800/20">
                                        <td colSpan={5} className="pl-16 pr-6 py-4">
                                          {isLoadingTrend ? (
                                            <div className="flex items-center gap-3 h-[160px] justify-center">
                                              <div className="w-4 h-4 border-2 border-sky-500 border-t-transparent rounded-full animate-spin" />
                                              <span className="text-slate-500 text-xs">Loading trend...</span>
                                            </div>
                                          ) : trend && trend.length > 0 ? (
                                            <div>
                                              <div className="text-[10px] font-bold text-slate-600 uppercase tracking-widest mb-3">
                                                {plan.planName} · Enrollment History
                                              </div>
                                              <div style={{ height: 160 }}>
                                                <ResponsiveContainer width="100%" height="100%">
                                                  <AreaChart data={trend} margin={{ top: 4, right: 12, bottom: 0, left: 0 }}>
                                                    <defs>
                                                      <linearGradient id={`g-${planKey.replace('|', '-')}`} x1="0" y1="0" x2="0" y2="1">
                                                        <stop offset="5%" stopColor={orgColor} stopOpacity={0.2} />
                                                        <stop offset="95%" stopColor={orgColor} stopOpacity={0} />
                                                      </linearGradient>
                                                    </defs>
                                                    <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                                                    <XAxis
                                                      dataKey="month"
                                                      axisLine={false}
                                                      tickLine={false}
                                                      tick={{ fill: '#475569', fontSize: 9 }}
                                                      dy={8}
                                                      tickFormatter={formatMonthShort}
                                                    />
                                                    <YAxis
                                                      axisLine={false}
                                                      tickLine={false}
                                                      tick={{ fill: '#475569', fontSize: 9 }}
                                                      tickFormatter={formatEnrollment}
                                                      domain={niceChartDomain(trend.map(t => t.enrollment))}
                                                      width={46}
                                                    />
                                                    <RechartsTooltip
                                                      contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '10px' }}
                                                      itemStyle={{ color: '#f1f5f9', fontSize: 11, fontWeight: 700 }}
                                                      labelStyle={{ color: '#64748b', fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.05em' }}
                                                      labelFormatter={(val: any) => formatMonthShort(val)}
                                                      formatter={(v: any) => [Number(v).toLocaleString(), 'Enrollment']}
                                                    />
                                                    <Area
                                                      type="monotone"
                                                      dataKey="enrollment"
                                                      stroke={orgColor}
                                                      strokeWidth={2}
                                                      fill={`url(#g-${planKey.replace('|', '-')})`}
                                                      fillOpacity={1}
                                                      dot={{ fill: orgColor, stroke: '#0f172a', strokeWidth: 2, r: 3 }}
                                                      activeDot={{ r: 5, fill: orgColor, stroke: '#fff', strokeWidth: 2 }}
                                                    />
                                                  </AreaChart>
                                                </ResponsiveContainer>
                                              </div>
                                            </div>
                                          ) : (
                                            <div className="flex items-center justify-center h-[80px] text-slate-700 text-xs italic">
                                              No trend data available.
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
            )}
          </div>
        );
      })}
    </div>
  );
};
