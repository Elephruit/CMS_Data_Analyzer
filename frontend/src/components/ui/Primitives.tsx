import React from 'react';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

interface CardProps {
  children: React.ReactNode;
  className?: string;
  noPadding?: boolean;
}

export const Card: React.FC<CardProps> = ({ children, className, noPadding = false }) => {
  return (
    <div className={cn(
      "bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden shadow-xl",
      !noPadding && "p-6",
      className
    )}>
      {children}
    </div>
  );
};

interface StatCardProps {
  label: string;
  value: string | number;
  change?: string | number;
  changeType?: 'positive' | 'negative' | 'neutral';
  icon?: React.ElementType;
  loading?: boolean;
}

export const StatCard: React.FC<StatCardProps> = ({ 
  label, 
  value, 
  change, 
  changeType = 'positive', 
  icon: Icon,
  loading = false
}) => {
  return (
    <Card className="relative group hover:border-slate-700 transition-all duration-300">
      {loading && (
        <div className="absolute inset-0 bg-slate-900/40 backdrop-blur-[1px] z-10 flex items-center justify-center">
          <div className="w-4 h-4 border-2 border-sky-500 border-t-transparent rounded-full animate-spin"></div>
        </div>
      )}
      <div className="flex items-start justify-between">
        <div>
          <p className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-1">{label}</p>
          <h3 className="text-2xl font-bold text-white group-hover:text-sky-400 transition-colors">{value}</h3>
          {change !== undefined && (
            <div className={cn(
              "flex items-center gap-1 mt-1 text-xs font-bold",
              changeType === 'positive' ? "text-emerald-400" : 
              changeType === 'negative' ? "text-rose-400" : "text-slate-500"
            )}>
              {change}
            </div>
          )}
        </div>
        {Icon && (
          <div className="p-2 bg-slate-800 rounded-lg text-slate-400 group-hover:text-sky-400 transition-colors">
            <Icon className="w-5 h-5" />
          </div>
        )}
      </div>
    </Card>
  );
};

interface PageHeaderProps {
  title: string;
  subtitle?: string;
  action?: React.ReactNode;
}

export const PageHeader: React.FC<PageHeaderProps> = ({ title, subtitle, action }) => {
  return (
    <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-8">
      <div>
        <h1 className="text-2xl font-bold text-white tracking-tight">{title}</h1>
        {subtitle && <p className="text-slate-400 text-sm mt-1">{subtitle}</p>}
      </div>
      {action && <div className="flex items-center gap-3">{action}</div>}
    </div>
  );
};
