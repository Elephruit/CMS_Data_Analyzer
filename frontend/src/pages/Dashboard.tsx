import React, { useEffect, useState, useMemo } from 'react';
import { useFilters } from '../context/FilterContext';
import { Card, StatCard } from '../components/ui/Primitives';
import {
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area
} from 'recharts';
import { ArrowUpRight, ArrowDownRight, LayoutDashboard, Building2, Users } from 'lucide-react';
import { formatEnrollment, formatFullEnrollment, formatMonthYear, formatMonthShort } from '../utils/formatters';

interface DashboardSummary {
  totalEnrollment: number;
  planCount: number;
  countyCount: number;
  orgCount: number;
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

interface MoverRowProps {
  mover: Mover;
  rank: number;
  direction: 'up' | 'down';
}

const MoverRow: React.FC<MoverRowProps> = ({ mover, rank, direction }) => {
  const isUp = direction === 'up';
  return (
    <div className="flex items-center gap-3 py-3 border-b border-slate-800/60 last:border-0 group">
      <div className={`w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-black shrink-0 tabular-nums ${
        isUp ? 'bg-emerald-500/10 text-emerald-500' : 'bg-rose-500/10 text-rose-500'
      }`}>
        {rank}
      </div>
      <div className="min-w-0 flex-1">
        <div className="text-xs font-semibold text-slate-200 truncate group-hover:text-white transition-colors leading-tight">
          {mover.plan_name}
        </div>
        <div className="text-[10px] text-slate-600 font-mono mt-0.5">
          {mover.contract_id} · {mover.plan_id}
          {mover.prior === 0 && (
            <span className="ml-1.5 text-[9px] font-bold px-1 py-px rounded bg-sky-500/15 text-sky-400 tracking-wide not-mono">NEW</span>
          )}
        </div>
      </div>
      <div className="shrink-0 flex items-center gap-1">
        <span className={`text-sm font-black font-mono tabular-nums ${isUp ? 'text-emerald-400' : 'text-rose-400'}`}>
          {isUp ? '+' : ''}{mover.change.toLocaleString()}
        </span>
        {isUp
          ? <ArrowUpRight className="w-3.5 h-3.5 text-emerald-500 shrink-0" />
          : <ArrowDownRight className="w-3.5 h-3.5 text-rose-500 shrink-0" />
        }
      </div>
    </div>
  );
};

export const Dashboard: React.FC = () => {
  const { filters } = useFilters();
  const [summary, setSummary] = useState<DashboardSummary | null>(null);
  const [trend, setTrend] = useState<TrendPoint[]>([]);
  const [gainers, setGainers] = useState<Mover[]>([]);
  const [losers, setLosers] = useState<Mover[]>([]);
  const [loading, setLoading] = useState(true);

  const priorDecember = (() => {
    const year = parseInt(filters.analysisMonth.split('-')[0]);
    return `${year - 1}-12`;
  })();

  // Context-aware Y-axis domain: picks a scale-appropriate "nice" step so
  // the chart uses most of its vertical space regardless of data magnitude.
  const trendDomain = useMemo((): [number, number] | ['auto', 'auto'] => {
    if (trend.length === 0) return ['auto', 'auto'];
    const values = trend.map(t => t.enrollment);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = max - min || max * 0.1;
    const buffer = range * 0.2;

    // Pick a "nice" step size scaled to the padded range / ~5 ticks
    const rawStep = (range + 2 * buffer) / 5;
    const exp = Math.pow(10, Math.floor(Math.log10(rawStep)));
    const norm = rawStep / exp;
    const niceStep = norm <= 1 ? exp
      : norm <= 2 ? 2 * exp
      : norm <= 2.5 ? 2.5 * exp
      : norm <= 5 ? 5 * exp
      : 10 * exp;

    const lo = Math.max(0, Math.floor((min - buffer) / niceStep) * niceStep);
    const hi = Math.ceil((max + buffer) / niceStep) * niceStep;
    return [lo, hi];
  }, [trend]);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const [summaryRes, trendRes, moversRes] = await Promise.all([
          fetch('http://127.0.0.1:3000/api/query/dashboard-summary', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(filters),
          }),
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
              from: priorDecember,
              to: filters.analysisMonth,
              limit: 20,
            }),
          }),
        ]);

        const summaryData = await summaryRes.json();
        const trendDataRaw = await trendRes.json();
        const moversData = await moversRes.json();

        setSummary(summaryData);
        setTrend(trendDataRaw.map(([m, val]: [number, number]) => ({
          month: m.toString().replace(/(\d{4})(\d{2})/, '$1-$2'),
          enrollment: val,
        })));

        const allMovers: Mover[] = moversData.map(([cid, pid, name, change, prior]: any) => ({
          contract_id: cid,
          plan_id: pid,
          plan_name: name,
          change,
          prior: prior ?? 0,
        }));

        setGainers(
          allMovers.filter(m => m.change > 0).sort((a, b) => b.change - a.change).slice(0, 5)
        );
        setLosers(
          allMovers.filter(m => m.change < 0).sort((a, b) => a.change - b.change).slice(0, 5)
        );
      } catch (error) {
        console.error('Failed to fetch dashboard data:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [filters, priorDecember]);

  return (
    <div className="space-y-4 max-w-[1600px] mx-auto pb-8">

      {/* Summary tiles — 3 columns, Counties removed */}
      <div className="grid grid-cols-3 gap-4">
        <StatCard
          label="Total Enrollment"
          value={summary ? formatEnrollment(summary.totalEnrollment) : '—'}
          icon={LayoutDashboard}
          loading={loading}
        />
        <StatCard
          label="Parent Organizations"
          value={summary ? summary.orgCount.toLocaleString() : '—'}
          icon={Building2}
          loading={loading}
        />
        <StatCard
          label="Total Plans"
          value={summary ? summary.planCount.toLocaleString() : '—'}
          icon={Users}
          loading={loading}
        />
      </div>

      {/* Enrollment trend — full width, context-aware axis, dot markers */}
      <Card className="flex flex-col h-[300px]">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xs font-bold text-slate-400 uppercase tracking-widest">Market Enrollment Trend</h2>
          <div className="text-[10px] font-mono text-sky-500 font-bold px-2 py-1 bg-sky-500/10 rounded">LIVE DATA</div>
        </div>
        <div className="flex-1">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={trend} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
              <defs>
                <linearGradient id="colorEnroll" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#0ea5e9" stopOpacity={0.22} />
                  <stop offset="95%" stopColor="#0ea5e9" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
              <XAxis
                dataKey="month"
                axisLine={false}
                tickLine={false}
                tick={{ fill: '#64748b', fontSize: 10 }}
                dy={10}
                tickFormatter={formatMonthShort}
              />
              <YAxis
                axisLine={false}
                tickLine={false}
                tick={{ fill: '#64748b', fontSize: 10 }}
                tickFormatter={(val) => formatEnrollment(val)}
                domain={trendDomain}
                width={52}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                itemStyle={{ color: '#f1f5f9', fontSize: '12px', fontWeight: 'bold' }}
                labelStyle={{ color: '#94a3b8', fontSize: '10px', marginBottom: '4px', textTransform: 'uppercase', fontWeight: 900, letterSpacing: '0.1em' }}
                labelFormatter={(val) => formatMonthYear(val)}
                formatter={(val: any) => [val !== undefined ? formatFullEnrollment(val) : '0', 'Enrollment']}
              />
              <Area
                type="monotone"
                dataKey="enrollment"
                stroke="#0ea5e9"
                strokeWidth={2.5}
                fillOpacity={1}
                fill="url(#colorEnroll)"
                dot={{ fill: '#0ea5e9', stroke: '#0f172a', strokeWidth: 2, r: 4 }}
                activeDot={{ r: 6, fill: '#0ea5e9', stroke: '#ffffff', strokeWidth: 2 }}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </Card>

      {/* Top movers — increasing and decreasing side by side */}
      <div className="grid grid-cols-2 gap-4">
        <Card>
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <div className="w-1 h-4 rounded-full bg-emerald-500 shrink-0" />
              <h2 className="text-xs font-bold text-slate-300 uppercase tracking-widest">Top Increasing Plans</h2>
            </div>
            <span className="text-[10px] text-slate-600 font-mono">vs {formatMonthShort(priorDecember)}</span>
          </div>
          {gainers.length === 0 ? (
            <div className="py-10 text-center text-slate-600 text-sm italic">No gainers detected in range.</div>
          ) : (
            gainers.map((m, i) => <MoverRow key={i} mover={m} rank={i + 1} direction="up" />)
          )}
        </Card>

        <Card>
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <div className="w-1 h-4 rounded-full bg-rose-500 shrink-0" />
              <h2 className="text-xs font-bold text-slate-300 uppercase tracking-widest">Top Decreasing Plans</h2>
            </div>
            <span className="text-[10px] text-slate-600 font-mono">vs {formatMonthShort(priorDecember)}</span>
          </div>
          {losers.length === 0 ? (
            <div className="py-10 text-center text-slate-600 text-sm italic">No decliners detected in range.</div>
          ) : (
            losers.map((m, i) => <MoverRow key={i} mover={m} rank={i + 1} direction="down" />)
          )}
        </Card>
      </div>

    </div>
  );
};
