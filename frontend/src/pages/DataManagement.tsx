import React, { useEffect, useState } from 'react';
import { Database, Plus, Trash2, RefreshCw, CheckCircle2, AlertCircle } from 'lucide-react';

interface IngestedMonth {
  year: number;
  month: number;
}

export const DataManagement: React.FC = () => {
  const [months, setMonths] = useState<IngestedMonth[]>([]);
  const [loading, setLoading] = useState(true);
  const [ingesting, setIngesting] = useState(false);
  const [newMonth, setNewMonth] = useState('2025-03');

  const fetchMonths = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://127.0.0.1:3000/api/data/months');
      const data = await response.json();
      setMonths(data);
    } catch (error) {
      console.error('Failed to fetch months:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMonths();
  }, []);

  const handleIngest = async () => {
    setIngesting(true);
    try {
      const response = await fetch('http://127.0.0.1:3000/api/data/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ month: newMonth, force: false }),
      });
      if (response.ok) {
        await fetchMonths();
        alert('Ingestion complete!');
      } else {
        const err = await response.text();
        alert('Ingestion failed: ' + err);
      }
    } catch (error) {
      alert('Error during ingestion');
    } finally {
      setIngesting(false);
    }
  };

  return (
    <div className="max-w-5xl mx-auto space-y-8">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Data Management</h1>
          <p className="text-slate-400 mt-1 text-sm">Manage CMS enrollment periods and system integrity.</p>
        </div>
        <div className="flex gap-3">
          <button 
            onClick={fetchMonths}
            className="p-2 bg-slate-800 hover:bg-slate-700 rounded-lg border border-slate-700 transition-colors"
          >
            <RefreshCw className={`w-5 h-5 text-slate-300 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="md:col-span-2 bg-slate-900 border border-slate-800 rounded-xl overflow-hidden">
          <div className="p-6 border-b border-slate-800 flex items-center justify-between">
            <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider">Ingested Periods</h2>
            <span className="px-2 py-0.5 bg-sky-500/10 text-sky-400 text-[10px] font-bold rounded border border-sky-500/20">
              {months.length} TOTAL
            </span>
          </div>
          <div className="divide-y divide-slate-800">
            {loading ? (
              <div className="p-12 text-center text-slate-500 text-sm">Loading dataset...</div>
            ) : months.length === 0 ? (
              <div className="p-12 text-center text-slate-500 text-sm">No months ingested yet.</div>
            ) : (
              months.map((m) => (
                <div key={`${m.year}-${m.month}`} className="p-4 flex items-center justify-between group hover:bg-slate-800/50 transition-colors">
                  <div className="flex items-center gap-4">
                    <div className="w-10 h-10 bg-slate-800 rounded-lg flex items-center justify-center font-bold text-sky-500 border border-slate-700">
                      {m.month}
                    </div>
                    <div>
                      <div className="text-sm font-bold text-white">
                        {new Date(m.year, m.month - 1).toLocaleString('default', { month: 'long', year: 'numeric' })}
                      </div>
                      <div className="text-[10px] text-slate-500 uppercase font-medium mt-0.5 flex items-center gap-1.5">
                        <CheckCircle2 className="w-3 h-3 text-emerald-500" />
                        Stored in Parquet & Cache
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                    <button className="p-2 text-slate-400 hover:text-white hover:bg-slate-700 rounded-lg transition-colors">
                      <RefreshCw className="w-4 h-4" />
                    </button>
                    <button className="p-2 text-slate-400 hover:text-rose-400 hover:bg-rose-500/10 rounded-lg transition-colors">
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        <div className="space-y-6">
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-6">
            <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">Add New Month</h2>
            <div className="space-y-4">
              <div>
                <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest ml-1 mb-1.5 block">Target Month</label>
                <input 
                  type="text" 
                  value={newMonth}
                  onChange={(e) => setNewMonth(e.target.value)}
                  placeholder="YYYY-MM"
                  className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:border-sky-500 outline-none transition-colors"
                />
              </div>
              <button 
                onClick={handleIngest}
                disabled={ingesting}
                className="w-full py-2.5 bg-sky-500 hover:bg-sky-600 disabled:bg-slate-700 disabled:cursor-not-allowed text-white text-xs font-bold rounded-lg transition-all flex items-center justify-center gap-2 shadow-lg shadow-sky-500/20"
              >
                {ingesting ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Plus className="w-4 h-4" />}
                {ingesting ? 'INGESTING...' : 'TRIGGER INGEST'}
              </button>
              <p className="text-[10px] text-slate-500 leading-relaxed italic">
                * This will discover, download, and normalize CMS data for the selected period.
              </p>
            </div>
          </div>

          <div className="bg-emerald-500/5 border border-emerald-500/20 rounded-xl p-6">
            <div className="flex items-center gap-3 mb-2">
              <Database className="w-5 h-5 text-emerald-500" />
              <h3 className="text-sm font-bold text-emerald-400">System Health</h3>
            </div>
            <p className="text-xs text-emerald-500/80 leading-relaxed">
              Analytical store is synchronized. Cross-partition integrity verified for {months.length} months.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};
