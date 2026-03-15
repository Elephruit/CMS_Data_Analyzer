import React, { useState, useMemo, useEffect } from 'react';
import { 
  Download, 
  Trash2, 
  RefreshCw, 
  CheckCircle2, 
  Calendar,
  CloudDownload,
  Trash,
  Settings2,
  FileText,
  Star
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
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [showSettings, setShowSettings] = useState(false);
  
  // Landscape state
  const [landscapeStatus, setLandscapeStatus] = useState<{
    status: string,
    imported_years: number[],
    available_years: number[]
  } | null>(null);

  // Crosswalk state
  const [crosswalkStatus, setCrosswalkStatus] = useState<{
    status: string,
    imported_years: number[],
    available_years: number[]
  } | null>(null);

  const refreshLandscape = async () => {
    try {
      const res = await fetch('http://127.0.0.1:3000/api/data/landscape/status');
      if (res.ok) {
        const data = await res.json();
        setLandscapeStatus(data);
      }
    } catch (e) {
      console.error('Failed to fetch landscape status', e);
    }
  };

  const refreshCrosswalk = async () => {
    try {
      const res = await fetch('http://127.0.0.1:3000/api/data/crosswalk/status');
      if (res.ok) {
        const data = await res.json();
        setCrosswalkStatus(data);
      }
    } catch (e) {
      console.error('Failed to fetch crosswalk status', e);
    }
  };

  const handleDiscoverLandscape = async () => {
    setLoading(true);
    try {
      const res = await fetch('http://127.0.0.1:3000/api/data/landscape/discover');
      if (res.ok) await refreshLandscape();
    } finally {
      setLoading(false);
    }
  };

  const handleDiscoverCrosswalk = async () => {
    setLoading(true);
    try {
      const res = await fetch('http://127.0.0.1:3000/api/data/crosswalk/discover');
      if (res.ok) await refreshCrosswalk();
    } finally {
      setLoading(false);
    }
  };

  const handleIngestLandscape = async (year: number) => {
    const key = `landscape-${year}`;
    setProcessing(prev => ({ ...prev, [key]: true }));
    try {
      const res = await fetch('http://127.0.0.1:3000/api/data/landscape/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ year }),
      });
      if (res.ok) await refreshLandscape();
    } finally {
      setProcessing(prev => ({ ...prev, [key]: false }));
    }
  };

  const handleIngestCrosswalk = async (year: number) => {
    const key = `crosswalk-${year}`;
    setProcessing(prev => ({ ...prev, [key]: true }));
    try {
      const res = await fetch('http://127.0.0.1:3000/api/data/crosswalk/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ year }),
      });
      if (res.ok) await refreshCrosswalk();
    } finally {
      setProcessing(prev => ({ ...prev, [key]: false }));
    }
  };

  useEffect(() => {
    refreshLandscape();
    refreshCrosswalk();
  }, []);
  
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
    setErrors(prev => { const next = { ...prev }; delete next[monthKey]; return next; });

    try {
      const endpoint = action === 'ingest' ? 'ingest' : 'delete-month';
      const response = await fetch(`http://127.0.0.1:3000/api/data/${endpoint}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ month: monthKey }),
      });

      if (response.ok) {
        await refreshAvailableMonths();
      } else {
        const errorText = await response.text();
        setErrors(prev => ({ ...prev, [monthKey]: errorText || `HTTP ${response.status}` }));
      }
    } catch (error) {
      console.error(`Action ${action} failed for ${monthKey}:`, error);
      setErrors(prev => ({ ...prev, [monthKey]: String(error) }));
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
        subtitle="Manage and provision enrollment, landscape, crosswalk, and star rating data for multi-year analysis."
        action={
          <div className="flex items-center gap-3">
            <button 
              onClick={() => setShowSettings(!showSettings)}
              className={cn(
                "flex items-center gap-2 px-4 py-2 rounded-lg border text-xs font-bold transition-all",
                showSettings 
                  ? "bg-sky-500/10 border-sky-500/50 text-sky-400" 
                  : "bg-slate-800 hover:bg-slate-700 border-slate-700 text-slate-300"
              )}
            >
              <Settings2 className="w-4 h-4" />
              ARCHIVE CONFIG
            </button>
            <button 
              onClick={() => {
                setLoading(true);
                Promise.all([refreshAvailableMonths(), refreshLandscape(), refreshCrosswalk()]).finally(() => setLoading(false));
              }}
              className="flex items-center gap-2 px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded-lg border border-slate-700 text-xs font-bold text-slate-300 transition-all"
            >
              <RefreshCw className={cn("w-4 h-4", loading && "animate-spin")} />
              REFRESH ALL
            </button>
          </div>
        }
      />

      {showSettings && (
        <div className="bg-slate-800/20 border border-slate-800 rounded-2xl p-8 space-y-6 animate-in fade-in slide-in-from-top-4 duration-300">
          <div className="flex flex-col gap-2">
            <h3 className="text-lg font-black text-white tracking-tight uppercase flex items-center gap-2">
              <Settings2 className="w-5 h-5 text-sky-500" />
              Annual Dataset Discovery
            </h3>
            <p className="text-sm text-slate-400 max-w-2xl">
              Discover Landscape, Crosswalk, and Star Rating archives directly from CMS. This will scan official CMS websites, identify available ZIP archives, and prepare them for local ingestion.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="space-y-4 p-6 bg-slate-900/50 rounded-xl border border-slate-800">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2 text-xs font-bold text-white uppercase tracking-widest">
                  <FileText className="w-4 h-4 text-sky-400" />
                  Landscape Dataset
                </div>
                {landscapeStatus?.status === 'active' && (
                  <span className="px-2 py-0.5 bg-emerald-500/10 text-emerald-500 text-[10px] font-black uppercase rounded border border-emerald-500/20">
                    Discovered
                  </span>
                )}
              </div>
              <p className="text-[11px] text-slate-500 leading-relaxed">
                Source: CMS Prescription Drug Coverage (Landscape ZIPs)
              </p>
              <button
                onClick={handleDiscoverLandscape}
                disabled={loading}
                className="w-full py-2.5 bg-sky-500/10 hover:bg-sky-500 text-sky-400 hover:text-white text-[10px] font-black uppercase tracking-widest rounded-lg transition-all border border-sky-500/20 flex items-center justify-center gap-2"
              >
                {loading ? <RefreshCw className="w-3.5 h-3.5 animate-spin" /> : <CloudDownload className="w-3.5 h-3.5" />}
                {loading ? "Discovering..." : "Discover Landscape"}
              </button>
            </div>

            <div className="space-y-4 p-6 bg-slate-900/50 rounded-xl border border-slate-800">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2 text-xs font-bold text-white uppercase tracking-widest">
                  <RefreshCw className="w-4 h-4 text-purple-400" />
                  Plan Crosswalk
                </div>
                {crosswalkStatus?.status === 'active' && (
                  <span className="px-2 py-0.5 bg-emerald-500/10 text-emerald-500 text-[10px] font-black uppercase rounded border border-emerald-500/20">
                    Discovered
                  </span>
                )}
              </div>
              <p className="text-[11px] text-slate-500 leading-relaxed">
                Source: CMS Plan Crosswalks (2006-2025+)
              </p>
              <button
                onClick={handleDiscoverCrosswalk}
                disabled={loading}
                className="w-full py-2.5 bg-purple-500/10 hover:bg-purple-500 text-purple-400 hover:text-white text-[10px] font-black uppercase tracking-widest rounded-lg transition-all border border-purple-500/20 flex items-center justify-center gap-2"
              >
                {loading ? <RefreshCw className="w-3.5 h-3.5 animate-spin" /> : <CloudDownload className="w-3.5 h-3.5" />}
                {loading ? "Discovering..." : "Discover Crosswalk"}
              </button>
            </div>

            <div className="space-y-4 p-6 bg-slate-900/50 rounded-xl border border-slate-800 opacity-50 grayscale cursor-not-allowed">
              <div className="flex items-center gap-2 text-xs font-bold text-white uppercase tracking-widest">
                <Star className="w-4 h-4 text-amber-400" />
                Star Ratings Dataset
              </div>
              <p className="text-[11px] text-slate-500 leading-relaxed">
                Source: CMS Part C and D Performance Data
              </p>
              <button disabled className="w-full py-2.5 bg-slate-800 text-slate-500 text-[10px] font-black uppercase tracking-widest rounded-lg border border-slate-700">
                Discovery Locked
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 gap-12">
        {years.map((year) => {
          const yearKey = `year-${year}`;
          const isYearProcessing = processing[yearKey];
          const yearIngestedCount = ingestedMonths.filter(m => m.year === year).length;
          
          // Landscape status for this year
          const landscapeAvailable = landscapeStatus?.available_years.includes(year);
          const landscapeIngested = landscapeStatus?.imported_years.includes(year);
          const isLandscapeProcessing = processing[`landscape-${year}`];

          // Crosswalk status for this year
          const crosswalkAvailable = crosswalkStatus?.available_years.includes(year);
          const crosswalkIngested = crosswalkStatus?.imported_years.includes(year);
          const isCrosswalkProcessing = processing[`crosswalk-${year}`];

          return (
            <div key={year} className="space-y-6">
              <div className="flex items-center justify-between px-2">
                <div className="flex items-baseline gap-3">
                  <h2 className="text-2xl font-black text-white tracking-tight italic uppercase">{year} Fiscal Year</h2>
                  <div className="h-px w-24 bg-slate-800 mx-2 hidden md:block" />
                  <span className="text-[10px] font-bold text-slate-500 uppercase tracking-[0.2em]">
                    Monthly Enrollment: {yearIngestedCount} / 12 Populated
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

              {/* Annual Files Section */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className={cn(
                  "p-4 rounded-xl border flex items-center justify-between transition-all",
                  landscapeIngested 
                    ? "bg-emerald-500/5 border-emerald-500/20" 
                    : landscapeAvailable 
                    ? "bg-sky-500/5 border-sky-500/20"
                    : "bg-slate-900/40 border-slate-800 opacity-60"
                )}>
                  <div className="flex items-center gap-3">
                    <div className={cn(
                      "p-2 rounded-lg",
                      landscapeIngested ? "bg-emerald-500/10 text-emerald-500" : "bg-sky-500/10 text-sky-400"
                    )}>
                      <FileText className="w-4 h-4" />
                    </div>
                    <div className="flex flex-col">
                      <span className="text-[10px] font-black uppercase tracking-widest text-white">Landscape Dataset</span>
                      <span className="text-[9px] font-bold text-slate-500 uppercase">
                        {landscapeIngested ? "Status: Populated" : landscapeAvailable ? "Status: Ready for Import" : "Status: Not Found"}
                      </span>
                    </div>
                  </div>
                  
                  {landscapeAvailable && !landscapeIngested && (
                    <button 
                      onClick={() => handleIngestLandscape(year)}
                      disabled={isLandscapeProcessing}
                      className="flex items-center gap-2 px-3 py-1.5 bg-sky-500 hover:bg-sky-400 text-white text-[9px] font-black uppercase tracking-widest rounded-lg transition-all shadow-lg shadow-sky-500/20 disabled:opacity-50"
                    >
                      {isLandscapeProcessing ? <RefreshCw className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
                      {isLandscapeProcessing ? "Importing..." : "Download"}
                    </button>
                  )}
                  
                  {landscapeIngested && (
                    <div className="flex items-center gap-2 text-emerald-500">
                      <CheckCircle2 className="w-4 h-4" />
                      <span className="text-[9px] font-black uppercase tracking-widest">Active</span>
                    </div>
                  )}
                </div>

                <div className={cn(
                  "p-4 rounded-xl border flex items-center justify-between transition-all",
                  crosswalkIngested 
                    ? "bg-emerald-500/5 border-emerald-500/20" 
                    : crosswalkAvailable 
                    ? "bg-purple-500/5 border-purple-500/20"
                    : "bg-slate-900/40 border-slate-800 opacity-60"
                )}>
                  <div className="flex items-center gap-3">
                    <div className={cn(
                      "p-2 rounded-lg",
                      crosswalkIngested ? "bg-emerald-500/10 text-emerald-500" : "bg-purple-500/10 text-purple-400"
                    )}>
                      <RefreshCw className="w-4 h-4" />
                    </div>
                    <div className="flex flex-col">
                      <span className="text-[10px] font-black uppercase tracking-widest text-white">Plan Crosswalk</span>
                      <span className="text-[9px] font-bold text-slate-500 uppercase">
                        {crosswalkIngested ? "Status: Populated" : crosswalkAvailable ? "Status: Ready for Import" : "Status: Not Found"}
                      </span>
                    </div>
                  </div>
                  
                  {crosswalkAvailable && !crosswalkIngested && (
                    <button 
                      onClick={() => handleIngestCrosswalk(year)}
                      disabled={isCrosswalkProcessing}
                      className="flex items-center gap-2 px-3 py-1.5 bg-purple-500 hover:bg-purple-400 text-white text-[9px] font-black uppercase tracking-widest rounded-lg transition-all shadow-lg shadow-purple-500/20 disabled:opacity-50"
                    >
                      {isCrosswalkProcessing ? <RefreshCw className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
                      {isCrosswalkProcessing ? "Importing..." : "Download"}
                    </button>
                  )}
                  
                  {crosswalkIngested && (
                    <div className="flex items-center gap-2 text-emerald-500">
                      <CheckCircle2 className="w-4 h-4" />
                      <span className="text-[9px] font-black uppercase tracking-widest">Active</span>
                    </div>
                  )}
                </div>

                <div className="p-4 rounded-xl border border-slate-800 bg-slate-900/40 opacity-30 grayscale cursor-not-allowed flex items-center gap-3">
                  <div className="p-2 bg-amber-500/10 text-amber-500 rounded-lg">
                    <Star className="w-4 h-4" />
                  </div>
                  <div className="flex flex-col">
                    <span className="text-[10px] font-black uppercase tracking-widest text-white">Star Ratings</span>
                    <span className="text-[9px] font-bold text-slate-500 uppercase">Status: Locked</span>
                  </div>
                </div>
              </div>

              {/* Monthly Enrollment Grid */}
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
                        "group/month p-4 rounded-xl border flex flex-col gap-2 transition-all duration-200",
                        ingested
                          ? "bg-sky-500/5 border-sky-500/20 hover:border-sky-500/50"
                          : future
                          ? "bg-slate-900/20 border-slate-800/50 opacity-30 grayscale cursor-not-allowed"
                          : errors[monthKey]
                          ? "bg-rose-500/5 border-rose-500/30"
                          : "bg-slate-800/20 border-slate-800 hover:border-slate-600"
                      )}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex flex-col gap-1">
                          <span className={cn(
                            "text-xs font-black uppercase tracking-widest",
                            ingested ? "text-sky-400" : "text-slate-500"
                          )}>
                            {name}
                          </span>
                          <div className="flex items-center gap-1.5">
                            {ingested ? (
                              <span className="text-[9px] font-bold text-sky-500/80 uppercase tracking-tighter flex items-center gap-1">
                                <CheckCircle2 className="w-2.5 h-2.5" />
                                Populated
                              </span>
                            ) : future ? (
                              <span className="text-[9px] font-bold text-slate-600 uppercase tracking-tighter flex items-center gap-1">
                                <Calendar className="w-2.5 h-2.5" />
                                Locked
                              </span>
                            ) : (
                              <span className="text-[9px] font-bold text-slate-500 uppercase tracking-tighter">
                                Available
                              </span>
                            )}
                          </div>
                        </div>

                        {!future && (
                          <div className="flex items-center gap-2">
                            <button
                              onClick={() => handleAction(ingested ? 'delete' : 'ingest', year, monthNum)}
                              disabled={isProcessing}
                              className={cn(
                                "p-2 rounded-lg transition-all",
                                ingested
                                  ? "bg-slate-800/50 text-slate-400 hover:text-rose-400 hover:bg-rose-500/10"
                                  : "bg-sky-500/10 text-sky-400 hover:bg-sky-500 hover:text-white shadow-lg shadow-sky-500/0 hover:shadow-sky-500/20"
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
                          </div>
                        )}
                      </div>

                      {errors[monthKey] && (
                        <p className="text-[9px] font-mono text-rose-400 leading-tight break-all">
                          {errors[monthKey]}
                        </p>
                      )}
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
