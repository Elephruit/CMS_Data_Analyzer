import React from 'react';

export const Dashboard: React.FC = () => {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold tracking-tight text-white">Dashboard</h1>
        <div className="text-sm text-slate-400">Last updated: Mar 14, 2026</div>
      </div>
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {[
          { label: 'Total Enrollment', value: '2.2M', change: '+1.2%' },
          { label: 'Avg. Plan Growth', value: '0.8%', change: '+0.1%' },
          { label: 'Parent Orgs', value: '482', change: '0' },
          { label: 'Total Plans', value: '6,173', change: '+36' },
        ].map((kpi) => (
          <div key={kpi.label} className="p-6 bg-slate-900 border border-slate-800 rounded-xl">
            <div className="text-sm font-medium text-slate-400 mb-1">{kpi.label}</div>
            <div className="flex items-baseline gap-2">
              <div className="text-2xl font-bold text-white">{kpi.value}</div>
              <div className="text-xs font-medium text-emerald-400">{kpi.change}</div>
            </div>
          </div>
        ))}
      </div>
      
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 p-6 bg-slate-900 border border-slate-800 rounded-xl h-80 flex items-center justify-center">
          <span className="text-slate-500 font-medium text-sm">Enrollment Trend Chart Placeholder</span>
        </div>
        <div className="p-6 bg-slate-900 border border-slate-800 rounded-xl h-80 flex items-center justify-center">
          <span className="text-slate-500 font-medium text-sm">Top Movers Table Placeholder</span>
        </div>
      </div>
    </div>
  );
};
