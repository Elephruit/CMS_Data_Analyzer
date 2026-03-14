import React, { useState, useMemo } from 'react';
import { 
  Download, 
  Trash2, 
  RefreshCw, 
  CheckCircle2, 
  Calendar,
  CloudDownload,
  Trash
} from 'lucide-react';
import { PageHeader } from '../components/ui/Primitives';
import { useFilters } from '../context/FilterContext';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

const MONTH_NAMES = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December'
];

export const DataManagement: React.FC = () => {
  const { availableMonths: ingestedMonths, refreshAvailableMonths } = useFilters();
  const [loading, setLoading] = useState(false);
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
        await refreshAvailableMonths();
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
        if (response.ok) await refreshAvailableMonths();
      } else {
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
    <div className="max-w-[1400px] mx-auto space-y-10 pb-24 px-4">
      <PageHeader 
        title="Analytical Store Management" 
        subtitle="Provision and manage multi-year CMS datasets. High-performance Parquet storage management."
        action={
          <button 
            onClick={() => {
              setLoading(true);
              refreshAvailableMonths().finally(() => setLoading(false));
            }}
            className="flex items-center gap-2 px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded-lg border border-slate-700 text-xs font-bold text-slate-300 transition-all"
          >
            <RefreshCw className={cn("w-4 h-4", loading && "animate-spin")} />
            REFRESH STATUS
          </button>
        }
      />

      <div className="grid grid-cols-1 gap-8">
        {years.map((year) => {
          const yearKey = `year-${year}`;
          const isYearProcessing = processing[yearKey];
          const yearIngestedCount = ingestedMonths.filter(m => m.year === year).length;
          
          return (
            <div key={year} className="space-y-4">
              <div className="flex items-center justify-between px-2">
                <div className="flex items-baseline gap-3">
                  <h2 className="text-xl font-black text-white tracking-tight">{year} FISCAL YEAR</h2>
                  <span className="text-[10px] font-bold text-slate-500 uppercase tracking-[0.2em]">
                    {yearIngestedCount} / 12 Months Populated
                  </span>
                </div>
                
                <div className="flex items-center gap-3">
                  <button 
                    onClick={() => handleBulkAction('ingest', year)}
                    disabled={isYearProcessing || yearIngestedCount === 12}
                    className="flex items-center gap-2 px-3 py-1.5 text-[10px] font-black uppercase tracking-widest text-sky-400 hover:text-white transition-colors disabled:opacity-30"
                  >
                    {isYearProcessing ? <RefreshCw className="w-3 h-3 animate-spin" /> : <CloudDownload className="w-3 h-3" />}
                    Sync Year
                  </button>
                  <div className="w-px h-4 bg-slate-800" />
                  <button 
                    onClick={() => handleBulkAction('delete', year)}
                    disabled={isYearProcessing || yearIngestedCount === 0}
                    className="flex items-center gap-2 px-3 py-1.5 text-[10px] font-black uppercase tracking-widest text-slate-500 hover:text-rose-500 transition-colors disabled:opacity-30"
                  >
                    <Trash className="w-3 h-3" />
                    Purge
                  </button>
                </div>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-2">
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
                        "group/month p-4 rounded-xl border flex flex-col justify-between h-24 transition-all duration-200",
                        ingested 
                          ? "bg-sky-500/5 border-sky-500/20 hover:border-sky-500/50" 
                          : future 
                          ? "bg-slate-900/20 border-slate-800/50 opacity-30 grayscale cursor-not-allowed" 
                          : "bg-slate-800/20 border-slate-800 hover:border-slate-600"
                      )}
                    >
                      <div className="flex items-start justify-between">
                        <span className={cn(
                          "text-[10px] font-bold uppercase tracking-widest",
                          ingested ? "text-sky-400" : "text-slate-500"
                        )}>
                          {name}
                        </span>
                        {ingested && <CheckCircle2 className="w-3.5 h-3.5 text-sky-500" />}
                        {future && <Calendar className="w-3.5 h-3.5 text-slate-700" />}
                      </div>

                      <div className="flex items-end justify-between">
                        <div className="flex flex-col">
                          <span className={cn(
                            "text-xs font-bold leading-none",
                            ingested ? "text-white" : "text-slate-500"
                          )}>
                            {monthNum.toString().padStart(2, '0')}
                          </span>
                          <span className="text-[8px] font-bold text-slate-600 mt-1 uppercase tracking-tighter">
                            {ingested ? 'Populated' : future ? 'Locked' : 'Available'}
                          </span>
                        </div>

                        {!future && (
                          <button 
                            onClick={() => handleAction(ingested ? 'delete' : 'ingest', year, monthNum)}
                            disabled={isProcessing}
                            className={cn(
                              "p-2 rounded-lg transition-all",
                              ingested 
                                ? "bg-slate-800/50 text-slate-400 hover:text-rose-400" 
                                : "bg-sky-500/10 text-sky-400 hover:bg-sky-500 hover:text-white"
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
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};
