import React, { useEffect, useMemo, useState } from 'react';
import { useFilters } from '../context/FilterContext';
import { Card, PageHeader, StatCard } from '../components/ui/Primitives';
import { 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  AreaChart,
  Area
} from 'recharts';
import { ArrowUpRight, ArrowDownRight, LayoutDashboard, Building2, Users, MapPin, Briefcase, UserCheck, ShieldCheck, Pill, Activity } from 'lucide-react';
import { formatEnrollment, formatFullEnrollment, formatMonthYear, formatMonthShort } from '../utils/formatters';

interface DashboardSummary {
  totalEnrollment: number;
  priorEnrollment: number;
  planCount: number;
  countyCount: number;
  orgCount: number;
  orgCountPriorYear: number;
  orgChange: number;
  breakdowns: {
    egwp: number;
    egwp_pdp: number;
    individual_non_snp: number;
    pdp: number;
    snp: {
      total: number;
      dsnp: number;
      csnp: number;
      isnp: number;
    }
  }
}

interface TrendPoint {
  month: string;
  enrollment: number;
}

interface Mover {
  contract_id: string;
  plan_id: string;
  plan_name: string;
  change: number;
  prior: number;
}

interface MoversResponse {
  increases: Mover[];
  decreases: Mover[];
}

export const Dashboard: React.FC = () => {
  const { filters, availableMonths } = useFilters();
  const [summary, setSummary] = useState<DashboardSummary | null>(null);
  const [trend, setTrend] = useState<TrendPoint[]>([]);
  const [movers, setMovers] = useState<MoversResponse>({ increases: [], decreases: [] });
  const [summaryLoading, setSummaryLoading] = useState(true);
  const [moversLoading, setMoversLoading] = useState(true);
  const availableMonthValues = useMemo(
    () => availableMonths.map((m) => `${m.year}-${m.month.toString().padStart(2, '0')}`),
    [availableMonths]
  );

  // Find the most appropriate comparison December
  const comparisonMonth = (() => {
    if (availableMonthValues.length === 0 || !filters.analysisMonth) return null;
    
    const analysisYear = parseInt(filters.analysisMonth.split('-')[0]);
    const analysisMonth = parseInt(filters.analysisMonth.split('-')[1]);
    
    // Ideal comparison is Dec of previous year
    const ideal = `${analysisYear - 1}-12`;
    if (availableMonthValues.includes(ideal)) return ideal;
    
    // Otherwise, find the LATEST December that is EARLIER than analysis month
    const decs = availableMonthValues
      .filter(m => m.endsWith('-12'))
      .filter(m => {
        const [y] = m.split('-').map(Number);
        return y < analysisYear || (y === analysisYear && 12 < analysisMonth);
      })
      .sort()
      .reverse();
      
    return decs[0] || null;
  })();

  useEffect(() => {
    let cancelled = false;

    const mapMovers = (list: [string, string, string, number, number | null | undefined][]) => list.map(([cid, pid, name, change, prior]) => ({
      contract_id: cid,
      plan_id: pid,
      plan_name: name,
      change,
      prior: prior ?? 0,
    }));

    const fetchSecondaryData = async () => {
      if (!filters.analysisMonth || cancelled) return;
      setMoversLoading(true);
      try {
        const moversFrom = comparisonMonth || `${parseInt(filters.analysisMonth.split('-')[0]) - 1}-12`;
        const [trendRes, moversRes] = await Promise.all([
          fetch('http://127.0.0.1:3000/api/query/global-trend', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(filters),
          }),
          fetch('http://127.0.0.1:3000/api/query/top-movers', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              ...filters,
              from: moversFrom,
              to: filters.analysisMonth,
              limit: 5
            }),
          })
        ]);

        const trendDataRaw = await trendRes.json();
        const moversDataRaw = await moversRes.json();
        if (cancelled) return;

        setTrend(trendDataRaw.map(([m, val]: [number, number]) => ({
          month: m.toString().replace(/(\d{4})(\d{2})/, '$1-$2'),
          enrollment: val
        })));

        setMovers({
          increases: mapMovers(moversDataRaw.increases || []),
          decreases: mapMovers(moversDataRaw.decreases || [])
        });
      } catch (error) {
        if (!cancelled) console.error('Failed to fetch dashboard secondary data:', error);
      } finally {
        if (!cancelled) setMoversLoading(false);
      }
    };

    const fetchSummary = async () => {
      if (!filters.analysisMonth) return;
      setSummaryLoading(true);
      setMoversLoading(true);
      try {
        const summaryRes = await fetch('http://127.0.0.1:3000/api/query/dashboard-summary', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters),
        });
        const summaryData = await summaryRes.json();
        if (cancelled) return;
        setSummary(summaryData);
      } catch (error) {
        if (!cancelled) console.error('Failed to fetch dashboard summary:', error);
      } finally {
        if (!cancelled) {
          setSummaryLoading(false);
          void fetchSecondaryData();
        }
      }
    };

    fetchSummary();

    return () => {
      cancelled = true;
    };
  }, [filters, comparisonMonth]);

  const calculateEnrollmentChange = () => {
    if (!summary || summary.priorEnrollment === 0) return null;
    const diff = summary.totalEnrollment - summary.priorEnrollment;
    const pct = (diff / summary.priorEnrollment) * 100;
    const sign = diff >= 0 ? '+' : '';
    return `${sign}${pct.toFixed(1)}% | ${sign}${formatEnrollment(Math.abs(diff))} MoM`;
  };

  const renderMoverList = (list: Mover[], isDecrease: boolean) => (
    <div className="space-y-4">
      <h3 className={`text-[10px] font-bold uppercase tracking-widest mb-4 ${isDecrease ? 'text-rose-500' : 'text-emerald-500'}`}>
        {isDecrease ? 'Top Decreases' : 'Top Increases'}
      </h3>
      {list.length === 0 ? (
        <div className="text-slate-600 text-xs italic py-2">None detected.</div>
      ) : (
        list.map((mover, i) => (
          <div key={i} className="flex items-center justify-between group cursor-pointer">
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <div className="text-xs font-bold text-white truncate group-hover:text-sky-400 transition-colors">{mover.plan_name}</div>
                {mover.prior === 0 && (
                  <span className="shrink-0 text-[9px] font-bold px-1.5 py-0.5 rounded bg-sky-500/20 text-sky-400 tracking-wide">NEW</span>
                )}
              </div>
              <div className="text-[10px] text-slate-500 font-mono mt-0.5">{mover.contract_id}|{mover.plan_id}</div>
            </div>
            <div className="flex items-center gap-2 ml-4">
              <span className={`text-xs font-mono font-bold ${mover.change >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                {mover.change >= 0 ? '+' : ''}{mover.change.toLocaleString()}
              </span>
              {mover.change > 0 ? (
                <ArrowUpRight className="w-3.5 h-3.5 text-emerald-500" />
              ) : (
                <ArrowDownRight className="w-3.5 h-3.5 text-rose-500" />
              )}
            </div>
          </div>
        ))
      )}
    </div>
  );

  return (
    <div className="space-y-8 max-w-[1600px] mx-auto pb-12">
      <PageHeader 
        title="Executive Overview" 
        subtitle="Market-wide enrollment metrics and top-line trends."
      />
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard 
          label="Total Enrollment" 
          value={summary ? formatEnrollment(summary.totalEnrollment) : '0'} 
          change={calculateEnrollmentChange() || undefined}
          changeType={(summary?.totalEnrollment || 0) >= (summary?.priorEnrollment || 0) ? 'positive' : 'negative'}
          icon={LayoutDashboard}
          loading={summaryLoading}
        />
        <StatCard 
          label="Parent Organizations" 
          value={summary ? summary.orgCount.toLocaleString() : '0'} 
          change={summary && summary.orgChange !== 0 ? `${summary.orgChange >= 0 ? '+' : ''}${summary.orgChange} YoY` : undefined}
          changeType={summary && summary.orgChange >= 0 ? 'positive' : 'negative'}
          icon={Building2}
          loading={summaryLoading}
        />
        <StatCard 
          label="Total Plans" 
          value={summary ? summary.planCount.toLocaleString() : '0'} 
          icon={Users}
          loading={summaryLoading}
        />
        <StatCard 
          label="Counties" 
          value={summary ? summary.countyCount.toLocaleString() : '0'} 
          icon={MapPin}
          loading={summaryLoading}
        />
      </div>
      
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-2 flex flex-col min-h-[450px]">
          <div className="flex items-center justify-between mb-8">
            <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest text-center">Market Enrollment Trend</h2>
            <div className="text-[10px] font-mono text-sky-500 font-bold px-2 py-1 bg-sky-500/10 rounded">LIVE DATA</div>
          </div>
          <div className="flex-1 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={trend}>
                <defs>
                  <linearGradient id="colorEnroll" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#0ea5e9" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#0ea5e9" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis
                  dataKey="month"
                  axisLine={false}
                  tickLine={false}
                  tick={{fill: '#64748b', fontSize: 10}}
                  dy={10}
                  tickFormatter={formatMonthShort}
                />
                <YAxis 
                  axisLine={false} 
                  tickLine={false} 
                  tick={{fill: '#64748b', fontSize: 10}}
                  tickFormatter={(val) => formatEnrollment(val)}
                  domain={['auto', 'auto']}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  itemStyle={{ color: '#f1f5f9', fontSize: '12px', fontWeight: 'bold' }}
                  labelStyle={{ color: '#94a3b8', fontSize: '10px', marginBottom: '4px', textTransform: 'uppercase', fontWeight: 'black', letterSpacing: '0.1em' }}
                  labelFormatter={(val) => formatMonthYear(val)}
                  formatter={(val: unknown) => [val !== undefined ? formatFullEnrollment(Number(val)) : '0', 'Enrollment']}
                />
                <Area 
                  type="monotone" 
                  dataKey="enrollment" 
                  stroke="#0ea5e9" 
                  strokeWidth={3}
                  fillOpacity={1} 
                  fill="url(#colorEnroll)" 
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Card>

        <Card className="flex flex-col h-full min-h-[450px]">
          <div className="flex items-center justify-between mb-8">
            <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest">Growth Analytics</h2>
            <div className="text-[10px] text-slate-500 font-mono uppercase tracking-tight">
              vs {comparisonMonth ? formatMonthShort(comparisonMonth) : 'Prior Dec'}
            </div>
          </div>
          <div className="flex-1 space-y-10">
            {moversLoading ? (
              <div className="h-full flex items-center justify-center text-slate-600 text-sm italic">Loading movers...</div>
            ) : movers.increases.length === 0 && movers.decreases.length === 0 ? (
              <div className="h-full flex items-center justify-center text-slate-600 text-sm italic">No movers detected in range.</div>
            ) : (
              <>
                {renderMoverList(movers.increases, false)}
                <div className="border-t border-slate-800/50 pt-6">
                  {renderMoverList(movers.decreases, true)}
                </div>
              </>
            )}
          </div>
        </Card>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
        <StatCard 
          label="EGWP" 
          value={summary ? formatEnrollment(summary.breakdowns?.egwp || 0) : '0'} 
          icon={Briefcase}
          loading={summaryLoading}
        />
        <StatCard 
          label="EGWP PDP" 
          value={summary ? formatEnrollment(summary.breakdowns?.egwp_pdp || 0) : '0'} 
          icon={Pill}
          loading={summaryLoading}
        />
        <StatCard 
          label="Indiv Non-SNP" 
          value={summary ? formatEnrollment(summary.breakdowns?.individual_non_snp || 0) : '0'} 
          icon={UserCheck}
          loading={summaryLoading}
        />
        <StatCard 
          label="Indiv PDP" 
          value={summary ? formatEnrollment(summary.breakdowns?.pdp || 0) : '0'} 
          icon={ShieldCheck}
          loading={summaryLoading}
        />
        <Card className="relative group hover:border-slate-700 transition-all duration-300 min-h-[120px]">
          {summaryLoading && (
            <div className="absolute inset-0 bg-slate-900/40 backdrop-blur-[1px] z-10 flex items-center justify-center rounded-2xl">
              <div className="w-4 h-4 border-2 border-sky-500 border-t-transparent rounded-full animate-spin"></div>
            </div>
          )}
          <div className="flex items-start justify-between mb-4">
            <div>
              <p className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-1">SNP Total</p>
              <h3 className="text-2xl font-bold text-white group-hover:text-sky-400 transition-colors">
                {summary ? formatEnrollment(summary.breakdowns?.snp?.total || 0) : '0'}
              </h3>
            </div>
            <div className="p-2 bg-slate-800 rounded-lg text-slate-400 group-hover:text-sky-400 transition-colors">
              <Activity className="w-5 h-5" />
            </div>
          </div>
          <div className="grid grid-cols-3 gap-2 border-t border-slate-800/50 pt-3">
            <div className="text-center">
              <p className="text-[8px] text-slate-500 uppercase font-bold">D-SNP</p>
              <p className="text-[10px] font-mono font-bold text-slate-300">{summary ? formatEnrollment(summary.breakdowns?.snp?.dsnp || 0) : '0'}</p>
            </div>
            <div className="text-center border-x border-slate-800/50">
              <p className="text-[8px] text-slate-500 uppercase font-bold">C-SNP</p>
              <p className="text-[10px] font-mono font-bold text-slate-300">{summary ? formatEnrollment(summary.breakdowns?.snp?.csnp || 0) : '0'}</p>
            </div>
            <div className="text-center">
              <p className="text-[8px] text-slate-500 uppercase font-bold">I-SNP</p>
              <p className="text-[10px] font-mono font-bold text-slate-300">{summary ? formatEnrollment(summary.breakdowns?.snp?.isnp || 0) : '0'}</p>
            </div>
          </div>
        </Card>
      </div>
    </div>
  );
};
