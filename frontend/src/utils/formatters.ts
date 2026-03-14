export const formatEnrollment = (value: number): string => {
  if (value >= 1_000_000) {
    return (value / 1_000_000).toFixed(2).replace(/\.00$/, '') + 'M';
  }
  if (value >= 1_000) {
    return (value / 1_000).toFixed(1).replace(/\.0$/, '') + 'K';
  }
  return value.toLocaleString();
};

export const formatFullEnrollment = (value: number): string => {
  return value.toLocaleString();
};

export const formatMonthYear = (monthStr: string): string => {
  // input: YYYY-MM
  const [year, month] = monthStr.split('-');
  const date = new Date(parseInt(year), parseInt(month) - 1);
  return date.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
};

export const formatMonthShort = (monthStr: string): string => {
  // input: YYYY-MM → "Feb 25"
  const [year, month] = monthStr.split('-');
  const date = new Date(parseInt(year), parseInt(month) - 1);
  return date.toLocaleDateString('en-US', { month: 'short', year: '2-digit' });
};
