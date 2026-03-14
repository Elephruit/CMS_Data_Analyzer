import React, { useEffect, useState, useMemo } from 'react';
import { 
  Database, 
  Download, 
  Trash2, 
  RefreshCw, 
  CheckCircle2, 
  AlertCircle, 
  Calendar,
  ChevronDown,
  CloudDownload,
  Trash
} from 'lucide-react';
import { Card, PageHeader } from '../components/ui/Primitives';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

interface IngestedMonth {
  year: number;
  month: number;
}

const MONTH_NAMES = [
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
];

export const DataManagement: React.FC = () => {
  const [ingestedMonths, setIngestedMonths] = useState<IngestedMonth[]>([]);
  const [loading, setLoading] = useState(true);
  const [processing, setProcessing] = useState<Record<string, boolean>>({});
  
  const currentYear = new Date().getFullYear();
  const currentMonth = new Date().getMonth() + 1;

  const years = useMemo(() => {
    const y = [];
    for (let i = currentYear; i >= 2016; i--) {
      y.push(i);
    }
    return y;
  }, [currentYear]);

  const fetchMonths = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://127.0.0.1:3000/api/data/months');
      const data = await response.json();
      setIngestedMonths(data);
    } catch (error) {
      console.error('Failed to fetch months:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMonths();
  }, []);

  const isIngested = (year: number, month: number) => {
    return ingestedMonths.some(m => m.year === year && m.month === month);
  };

  const isFuture = (year: number, month: number) => {
    if (year > currentYear) return true;
    if (year === currentYear && month > currentMonth) return true;
    return false;
  };

  const handleAction = async (action: 'ingest' | 'delete', year: number, month: number) => {
    const monthKey = `${year}-${month.toString().padStart(2, '0')}`;
    setProcessing(prev => ({ ...prev, [monthKey]: true }));
    
    try {
      const endpoint = action === 'ingest' ? 'ingest' : 'delete-month';
      const response = await fetch(`http://127.0.0.1:3000/api/data/${endpoint}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ month: monthKey }),
      });
      
      if (response.ok) {
        await fetchMonths();
      }
    } catch (error) {
      console.error(`Action ${action} failed for ${monthKey}:`, error);
    } finally {
      setProcessing(prev => ({ ...prev, [monthKey]: false }));
    }
  };

  const handleBulkAction = async (action: 'ingest' | 'delete', year: number) => {
    const yearKey = `year-${year}`;
    setProcessing(prev => ({ ...prev, [yearKey]: true }));

    try {
      if (action === 'delete') {
        const response = await fetch(`http://127.0.0.1:3000/api/data/delete-year`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ year }),
        });
        if (response.ok) await fetchMonths();
      } else {
        // Sequentially ingest missing months for the year
        for (let m = 1; m <= 12; m++) {
          if (!isIngested(year, m) && !isFuture(year, m)) {
            await handleAction('ingest', year, m);
          }
        }
      }
    } catch (error) {
      console.error(`Bulk ${action} failed for ${year}:`, error);
    } finally {
      setProcessing(prev => ({ ...prev, [yearKey]: false }));
    }
  };

  return (
    <div className="max-w-[1200px] mx-auto space-y-8 pb-20">
      <PageHeader 
        title="Data Management" 
        subtitle="Control your local analytical store. Ingest and manage CMS datasets by period."
        action={
          <button 
            onClick={fetchMonths}
            className="p-2 bg-slate-800 hover:bg-slate-700 rounded-xl border border-slate-700 transition-all group"
          >
            <RefreshCw className={cn("w-5 h-5 text-slate-400 group-hover:text-sky-400 transition-colors", loading && "animate-spin")} />
          </button>
        }
      />

      <div className="space-y-6">
        {years.map((year) => {
          const yearKey = `year-${year}`;
          const isYearProcessing = processing[yearKey];
          const yearIngestedCount = ingestedMonths.filter(m => m.year === year).length;
          
          return (
            <Card key={year} className="group overflow-visible border-slate-800 hover:border-slate-700 transition-all duration-300" noPadding>
              {/* Year Header */}
              <div className="px-6 py-4 border-b border-slate-800 flex items-center justify-between bg-slate-900/50 rounded-t-2xl">
                <div className="flex items-center gap-4">
                  <div className="w-10 h-10 bg-slate-800 rounded-xl flex items-center justify-center font-black text-white border border-slate-700 shadow-inner group-hover:border-sky-500/50 transition-colors">
                    {year}
                  </div>
                  <div>
                    <h2 className="text-lg font-bold text-white leading-none">{year} Dataset</h2>
                    <p className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mt-1">
                      {yearIngestedCount} / 12 MONTHS LOADED
                    </p>
                  </div>
                </div>

                <div className="flex items-center gap-2">
                  <button 
                    onClick={() => handleBulkAction('ingest', year)}
                    disabled={isYearProcessing || yearIngestedCount === 12}
                    className="flex items-center gap-2 px-3 py-1.5 bg-sky-500/10 hover:bg-sky-500 text-sky-400 hover:text-white text-[10px] font-black uppercase tracking-widest rounded-lg border border-sky-500/20 transition-all disabled:opacity-50 disabled:pointer-events-none"
                  >
                    {isYearProcessing ? <RefreshCw className="w-3 h-3 animate-spin" /> : <CloudDownload className="w-3 h-3" />}
                    Download All
                  </button>
                  <button 
                    onClick={() => handleBulkAction('delete', year)}
                    disabled={isYearProcessing || yearIngestedCount === 0}
                    className="flex items-center gap-2 px-3 py-1.5 bg-rose-500/10 hover:bg-rose-500 text-rose-400 hover:text-white text-[10px] font-black uppercase tracking-widest rounded-lg border border-rose-500/20 transition-all disabled:opacity-50 disabled:pointer-events-none"
                  >
                    <Trash className="w-3 h-3" />
                    Clear Year
                  </button>
                </div>
              </div>

              {/* Month Grid */}
              <div className="p-6 grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-3">
                {MONTH_NAMES.map((name, idx) => {
                  const monthNum = idx + 1;
                  const ingested = isIngested(year, monthNum);
                  const future = isFuture(year, monthNum);
                  const monthKey = `${year}-${monthNum.toString().padStart(2, '0')}`;
                  const isProcessing = processing[monthKey];

                  return (
                    <div 
                      key={monthKey}
                      className={cn(
                        "relative group/month p-3 rounded-xl border transition-all duration-300 flex flex-col gap-2",
                        ingested 
                          ? "bg-sky-500/5 border-sky-500/30 hover:border-sky-500 shadow-lg shadow-sky-500/5" 
                          : future 
                          ? "bg-slate-900/50 border-slate-800 opacity-40 grayscale pointer-events-none" 
                          : "bg-slate-800/30 border-slate-700 hover:border-slate-500"
                      )}
                    >
                      <div className="flex items-center justify-between">
                        <span className={cn(
                          "text-[10px] font-black uppercase tracking-tighter",
                          ingested ? "text-sky-400" : "text-slate-500"
                        )}>
                          {name}
                        </span>
                        {ingested ? (
                          <CheckCircle2 className="w-3 h-3 text-sky-500" />
                        ) : future ? (
                          <Calendar className="w-3 h-3 text-slate-700" />
                        ) : (
                          <div className="w-1.5 h-1.5 rounded-full bg-slate-700" />
                        )}
                      </div>

                      <div className="flex items-center justify-between mt-1">
                        <span className={cn(
                          "text-sm font-bold",
                          ingested ? "text-white" : "text-slate-400"
                        )}>
                          {monthNum.toString().padStart(2, '0')}
                        </span>
                        
                        {!future && (
                          <button 
                            onClick={() => handleAction(ingested ? 'delete' : 'ingest', year, monthNum)}
                            disabled={isProcessing}
                            className={cn(
                              "p-1.5 rounded-lg transition-all",
                              ingested 
                                ? "text-slate-500 hover:text-rose-400 hover:bg-rose-500/10" 
                                : "text-slate-400 hover:text-sky-400 hover:bg-sky-500/10"
                            )}
                          >
                            {isProcessing ? (
                              <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                            ) : ingested ? (
                              <Trash2 className="w-3.5 h-3.5" />
                            ) : (
                              <Download className="w-3.5 h-3.5" />
                            )}
                          </button>
                        )}
                      </div>

                      {/* Tooltip-like status */}
                      <div className="absolute -top-2 left-1/2 -translate-x-1/2 px-2 py-0.5 rounded bg-slate-800 border border-slate-700 text-[8px] font-black uppercase tracking-widest opacity-0 group-hover/month:opacity-100 transition-opacity pointer-events-none z-20">
                        {ingested ? 'Populated' : future ? 'Unavailable' : 'Available'}
                      </div>
                    </div>
                  );
                })}
              </div>
            </Card>
          );
        })}
      </div>

      {/* Footer Info */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 flex items-start gap-4">
        <div className="p-3 bg-sky-500/10 rounded-xl">
          <Info className="w-6 h-6 text-sky-500" />
        </div>
        <div className="space-y-1">
          <h3 className="text-sm font-bold text-white">System Information</h3>
          <p className="text-xs text-slate-400 leading-relaxed max-w-2xl">
            The data management engine discovers monthly zip files directly from CMS.gov. Populated months are stored in an optimized Parquet columnar format, partitioned by year and state for high-performance sub-second querying across the entire application.
          </p>
        </div>
      </div>
    </div>
  );
};

interface InfoProps {
  className?: string;
}

const Info: React.FC<InfoProps> = ({ className }) => (
  <svg 
    xmlns="http://www.w3.org/2000/svg" 
    viewBox="0 0 24 24" 
    fill="none" 
    stroke="currentColor" 
    strokeWidth="2" 
    strokeLinecap="round" 
    strokeLinejoin="round" 
    className={className}
  >
    <circle cx="12" cy="12" r="10" />
    <path d="M12 16v-4" />
    <path d="M12 8h.01" />
  </svg>
);
