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
  label?: string;
  title?: string;
  value: string | number;
  change?: string | number;
  changeType?: 'positive' | 'negative' | 'neutral';
  trend?: number;
  icon?: React.ReactNode | React.ElementType;
  loading?: boolean;
  variant?: 'primary' | 'success' | 'warning' | 'danger';
}

export const StatCard: React.FC<StatCardProps> = ({ 
  label, 
  title,
  value, 
  change, 
  changeType = 'positive', 
  trend,
  icon,
  loading = false,
  variant = 'primary'
}) => {
  const displayLabel = title || label;
  
  const variantClasses = {
    primary: "text-sky-400 bg-sky-500/10",
    success: "text-emerald-400 bg-emerald-500/10",
    warning: "text-amber-400 bg-amber-500/10",
    danger: "text-rose-400 bg-rose-500/10",
  };

  const renderIcon = () => {
    if (!icon) return null;
    if (React.isValidElement(icon)) return icon;
    const IconComponent = icon as React.ElementType;
    return <IconComponent className="w-5 h-5" />;
  };

  return (
    <Card className="relative group hover:border-slate-700 transition-all duration-300">
      {loading && (
        <div className="absolute inset-0 bg-slate-900/40 backdrop-blur-[1px] z-10 flex items-center justify-center">
          <div className="w-4 h-4 border-2 border-sky-500 border-t-transparent rounded-full animate-spin"></div>
        </div>
      )}
      <div className="flex items-start justify-between">
        <div>
          <p className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-1">{displayLabel}</p>
          <h3 className="text-2xl font-bold text-white group-hover:text-sky-400 transition-colors">{value}</h3>
          {(change !== undefined || trend !== undefined) && (
            <div className={cn(
              "flex items-center gap-1 mt-1 text-xs font-bold",
              changeType === 'positive' ? "text-emerald-400" : 
              changeType === 'negative' ? "text-rose-400" : "text-slate-500"
            )}>
              {change || (trend !== undefined && `${trend > 0 ? '+' : ''}${trend}%`)}
            </div>
          )}
        </div>
        {icon && (
          <div className={cn("p-2 rounded-lg transition-colors", variantClasses[variant])}>
            {renderIcon()}
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

interface BadgeProps {
  label: string;
  variant?: 'primary' | 'success' | 'warning' | 'danger' | 'neutral';
  className?: string;
}

export const Badge: React.FC<BadgeProps> = ({ label, variant = 'primary', className }) => {
  const variants = {
    primary: "bg-sky-500/10 text-sky-400 border-sky-500/20",
    success: "bg-emerald-500/10 text-emerald-400 border-emerald-500/20",
    warning: "bg-amber-500/10 text-amber-400 border-amber-500/20",
    danger: "bg-rose-500/10 text-rose-400 border-rose-500/20",
    neutral: "bg-slate-500/10 text-slate-400 border-slate-500/20",
  };

  return (
    <span className={cn(
      "px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider border",
      variants[variant],
      className
    )}>
      {label}
    </span>
  );
};

interface ChartCardProps {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
  className?: string;
  action?: React.ReactNode;
}

export const ChartCard: React.FC<ChartCardProps> = ({ title, subtitle, children, className, action }) => {
  return (
    <Card className={cn("flex flex-col", className)}>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest">{title}</h2>
          {subtitle && <p className="text-[10px] text-slate-500 font-medium uppercase tracking-tight mt-0.5">{subtitle}</p>}
        </div>
        {action && <div>{action}</div>}
      </div>
      <div className="flex-1 min-h-0">
        {children}
      </div>
    </Card>
  );
};
