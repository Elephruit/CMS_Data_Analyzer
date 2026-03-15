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

// ── Change badge: shows absolute change + optional % ──────────────────────────
const ChangeBadge: React.FC<{ change: number; pct?: number; className?: string }> = ({ change, pct, className = '' }) => {
  const isPos = change > 0;
  const isNeg = change < 0;
  if (change === 0 && (pct === undefined || pct === 0)) {
    return <span className={`font-mono text-slate-700 tabular-nums text-xs ${className}`}>—</span>;
  }
  return (
    <span className={`font-mono font-bold inline-flex items-center gap-0.5 tabular-nums ${isPos ? 'text-emerald-400' : isNeg ? 'text-rose-400' : 'text-slate-500'} ${className}`}>
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

  // AEP column header: "AEP  Feb 26 vs Dec 25"
  const aepLabel = useMemo(() => {
    if (!data?.aepFebMonth || !data?.aepDecMonth) return 'AEP Growth';
    const febStr = formatMonthShort(data.aepFebMonth.toString().replace(/(\d{4})(\d{2})/, '$1-$2'));
    const decStr = formatMonthShort(data.aepDecMonth.toString().replace(/(\d{4})(\d{2})/, '$1-$2'));
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
      setPlanTrends(prev => ({
        ...prev,
        [key]: (result.trend ?? []).map(({ month, value }: { month: number; value: number }) => ({
          month: month.toString().replace(/(\d{4})(\d{2})/, '$1-$2'),
          enrollment: value,
        })),
      }));
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

      {/* Org groups — each is a single table so all rows share column widths */}
      {groups.map((org, oi) => {
        const orgColor = getColor(org.orgName, DEFAULT_COLORS[oi % DEFAULT_COLORS.length]);
        const displayOrg = getDisplayName(org.orgName);
        const isOrgCollapsed = collapsedOrgs.has(org.orgName);
        const orgAepPct = org.totalAepDecEnrollment > 0
          ? (org.totalAepGrowth / org.totalAepDecEnrollment) * 100 : 0;

        return (
          <div key={org.orgName} className="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden">
            <table className="w-full text-left border-collapse">
              {/*
                Fixed column widths keep every row — org, type, plan — pixel-aligned.
                Col 1 (name) grows; cols 2-6 are fixed.
              */}
              <colgroup>
                <col />                {/* name — grows */}
                <col style={{ width: 110 }} />  {/* ID */}
                <col style={{ width: 120 }} />  {/* Enrollment */}
                <col style={{ width: 110 }} />  {/* MoM */}
                <col style={{ width: 180 }} />  {/* AEP */}
                <col style={{ width: 36 }} />   {/* chevron */}
              </colgroup>

              {/* Column header — shown once per org card */}
              <thead>
                <tr className="border-b border-slate-800/60">
                  <th className="pl-5 pr-3 py-2 text-[10px] font-bold text-slate-600 uppercase tracking-widest text-left">
                    Plan
                  </th>
                  <th className="px-3 py-2 text-[10px] font-bold text-slate-600 uppercase tracking-widest text-left">
                    ID
                  </th>
                  <th className="px-3 py-2 text-[10px] font-bold text-slate-600 uppercase tracking-widest text-right">
                    Enrollment
                  </th>
                  <th className="px-3 py-2 text-[10px] font-bold text-slate-600 uppercase tracking-widest text-right">
                    MoM
                  </th>
                  <th className="px-3 py-2 text-[10px] font-bold text-slate-600 uppercase tracking-widest text-right">
                    {aepLabel}
                  </th>
                  <th />
                </tr>
              </thead>

              <tbody>
                {/* ── Org summary row ── */}
                <tr
                  onClick={() => toggleOrg(org.orgName)}
                  className="cursor-pointer hover:bg-slate-800/50 transition-colors border-b border-slate-800/60 group"
                >
                  <td className="pl-5 pr-3 py-3.5">
                    <div className="flex items-center gap-2.5 min-w-0">
                      <div className="w-3 h-3 rounded-full shrink-0" style={{ backgroundColor: orgColor }} />
                      <span className="font-bold text-sm text-white truncate">{displayOrg}</span>
                      {displayOrg !== org.orgName && (
                        <span className="text-[10px] text-slate-600 font-mono truncate hidden lg:block">
                          {org.orgName}
                        </span>
                      )}
                      <span className="text-[10px] text-slate-600 shrink-0 ml-1">
                        {org.typeGroups.length} type{org.typeGroups.length !== 1 ? 's' : ''} · {data?.rows.filter(r => r.parentOrg === org.orgName).length} plans
                      </span>
                    </div>
                  </td>
                  <td />
                  <td className="px-3 py-3.5 text-right">
                    <span className="text-sm font-mono font-bold text-slate-200">
                      {org.totalEnrollment.toLocaleString()}
                    </span>
                  </td>
                  <td className="px-3 py-3.5 text-right">
                    <ChangeBadge change={org.totalMomChange} className="text-sm justify-end" />
                  </td>
                  <td className="px-3 py-3.5 text-right">
                    <ChangeBadge change={org.totalAepGrowth} pct={orgAepPct} className="text-sm justify-end" />
                  </td>
                  <td className="pr-4 text-right">
                    {isOrgCollapsed
                      ? <ChevronRight className="w-4 h-4 text-slate-500 inline" />
                      : <ChevronDown className="w-4 h-4 text-slate-500 inline" />}
                  </td>
                </tr>

                {/* ── Type sub-groups ── */}
                {!isOrgCollapsed && org.typeGroups.map((tg) => {
                  const typeKey = `${org.orgName}||${tg.planType}`;
                  const isTypeCollapsed = collapsedTypeKeys.has(typeKey);
                  const typeAepPct = tg.totalAepDecEnrollment > 0
                    ? (tg.totalAepGrowth / tg.totalAepDecEnrollment) * 100 : 0;

                  return (
                    <React.Fragment key={typeKey}>

                      {/* Type header row */}
                      <tr
                        onClick={() => toggleType(org.orgName, tg.planType)}
                        className="cursor-pointer hover:bg-slate-800/40 transition-colors border-b border-slate-800/40 group"
                      >
                        <td className="pl-11 pr-3 py-2.5">
                          <div className="flex items-center gap-2">
                            <div className="w-1.5 h-1.5 rounded-full bg-slate-600 shrink-0" />
                            <span className="text-xs font-bold text-slate-300">{tg.planType}</span>
                            <span className="text-[10px] text-slate-600 ml-1">
                              {tg.plans.length} plan{tg.plans.length !== 1 ? 's' : ''}
                            </span>
                          </div>
                        </td>
                        <td />
                        <td className="px-3 py-2.5 text-right">
                          <span className="text-xs font-mono font-bold text-slate-400">
                            {tg.totalEnrollment.toLocaleString()}
                          </span>
                        </td>
                        <td className="px-3 py-2.5 text-right">
                          <ChangeBadge change={tg.totalMomChange} className="text-xs justify-end" />
                        </td>
                        <td className="px-3 py-2.5 text-right">
                          <ChangeBadge change={tg.totalAepGrowth} pct={typeAepPct} className="text-xs justify-end" />
                        </td>
                        <td className="pr-4 text-right">
                          {isTypeCollapsed
                            ? <ChevronRight className="w-3.5 h-3.5 text-slate-600 inline" />
                            : <ChevronDown className="w-3.5 h-3.5 text-slate-600 inline" />}
                        </td>
                      </tr>

                      {/* Plan rows */}
                      {!isTypeCollapsed && tg.plans.map((plan) => {
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
                              {/* Plan name */}
                              <td className="pl-16 pr-3 py-2.5">
                                <div className="flex items-center gap-1.5">
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
                              {/* ID */}
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
                              {/* AEP */}
                              <td className="px-3 py-2.5 text-right">
                                <ChangeBadge
                                  change={plan.aepGrowth}
                                  pct={plan.aepDecEnrollment > 0 ? plan.aepGrowthPct : undefined}
                                  className="text-xs justify-end"
                                />
                              </td>
                              <td />
                            </tr>

                            {/* Inline trend chart */}
                            {isExpanded && (
                              <tr className="bg-slate-950/50 border-b border-slate-800/20">
                                <td colSpan={6} className="pl-16 pr-6 py-4">
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
                                              axisLine={false} tickLine={false}
                                              tick={{ fill: '#475569', fontSize: 9 }}
                                              dy={8}
                                              tickFormatter={formatMonthShort}
                                            />
                                            <YAxis
                                              axisLine={false} tickLine={false}
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
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        );
      })}
    </div>
  );
};
