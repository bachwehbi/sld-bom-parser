import { Diagram } from '../types';

interface StatusBarProps {
  diagrams: Diagram[];
  isLoading: boolean;
}

function formatRelative(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60)  return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60)  return `${m}m ago`;
  const h = Math.floor(m / 60);
  return `${h}h ago`;
}

export default function StatusBar({ diagrams, isLoading }: StatusBarProps) {
  const succeeded  = diagrams.filter((d) => d.status === 'SUCCESS');
  const inProgress = diagrams.filter((d) => d.status === 'IN_PROGRESS');
  const errors     = diagrams.filter((d) => d.status === 'ERROR');

  const latest = succeeded
    .filter((d) => d.processed_at)
    .sort((a, b) => new Date(b.processed_at!).getTime() - new Date(a.processed_at!).getTime())[0];

  return (
    <div className="shrink-0 flex items-center justify-between px-4 py-1.5 glass border-t border-gray-800/50 text-[11px] text-gray-500">
      <div className="flex items-center gap-4">
        <span className="flex items-center gap-1.5">
          <span className="w-1.5 h-1.5 rounded-full bg-green-500" />
          {succeeded.length} processed
        </span>
        {inProgress.length > 0 && (
          <span className="flex items-center gap-1.5 text-blue-400">
            <span className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse" />
            {inProgress.length} extracting
          </span>
        )}
        {errors.length > 0 && (
          <span className="flex items-center gap-1.5 text-red-400">
            <span className="w-1.5 h-1.5 rounded-full bg-red-400" />
            {errors.length} failed
          </span>
        )}
      </div>
      <div className="flex items-center gap-4">
        {latest && (
          <span>last: {latest.file_name} · {formatRelative(latest.processed_at!)}</span>
        )}
        {isLoading && (
          <span className="flex items-center gap-1 text-gray-600">
            <svg className="w-3 h-3 animate-spin" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            syncing
          </span>
        )}
      </div>
    </div>
  );
}
