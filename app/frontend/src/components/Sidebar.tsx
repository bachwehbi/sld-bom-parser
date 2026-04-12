import { useState } from 'react';
import { Diagram } from '../types';

interface SidebarProps {
  diagrams: Diagram[];
  unprocessed: string[];
  activeFileName: string | null;
  isCollapsed: boolean;
  onToggleCollapse: () => void;
  onSelectDiagram: (d: Diagram) => void;
  onExtractUnprocessed: (fileName: string) => void;
  onUpload: () => void;
}

function StatusDot({ status }: { status: Diagram['status'] }) {
  if (status === 'SUCCESS')     return <span className="w-2 h-2 rounded-full bg-green-500 shrink-0" />;
  if (status === 'IN_PROGRESS') return <span className="w-2 h-2 rounded-full bg-blue-400 animate-pulse shrink-0" />;
  if (status === 'ERROR')       return <span className="w-2 h-2 rounded-full bg-red-500 shrink-0" />;
  return null;
}

export default function Sidebar({
  diagrams, unprocessed, activeFileName, isCollapsed,
  onToggleCollapse, onSelectDiagram, onExtractUnprocessed, onUpload,
}: SidebarProps) {
  const [showUnprocessed, setShowUnprocessed] = useState(false);

  const processed   = diagrams.filter((d) => d.status !== 'UNPROCESSED');

  return (
    <div className={`flex flex-col h-full bg-gray-950 border-r border-gray-800/50 sidebar-transition ${isCollapsed ? 'w-14' : 'w-72'}`}>
      {/* Header */}
      <div className={`shrink-0 flex items-center ${isCollapsed ? 'justify-center py-3 px-1' : 'justify-between px-3 py-3'} border-b border-gray-800/50`}>
        {!isCollapsed && <span className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Diagrams</span>}
        <button
          onClick={onToggleCollapse}
          className="p-1.5 text-gray-500 hover:text-gray-300 hover:bg-gray-800 rounded-lg transition-colors"
          title={isCollapsed ? 'Expand' : 'Collapse'}
        >
          <svg className={`w-4 h-4 transition-transform duration-200 ${isCollapsed ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 19l-7-7 7-7m8 14l-7-7 7-7" />
          </svg>
        </button>
      </div>

      {/* Diagram list */}
      <div className="flex-1 overflow-y-auto py-2 min-h-0">
        {processed.length === 0 && !isCollapsed && (
          <div className="px-4 py-10 text-center">
            <svg className="w-8 h-8 mx-auto mb-2 text-gray-800" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
            <p className="text-xs text-gray-700">No diagrams yet</p>
          </div>
        )}

        {/* Processed diagrams */}
        {!isCollapsed && processed.length > 0 && (
          <div className="mb-2">
            <h3 className="px-3 mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-gray-600">
              Processed ({processed.length})
            </h3>
            <div className="space-y-0.5">
              {processed.map((d) => {
                const isActive = d.file_name === activeFileName;
                return (
                  <button
                    key={d.file_name}
                    onClick={() => onSelectDiagram(d)}
                    className={`w-full flex items-start gap-2.5 px-3 py-2 mx-1.5 rounded-lg text-left transition-all duration-150 group ${
                      isActive
                        ? 'bg-gray-800 text-white'
                        : 'text-gray-400 hover:bg-gray-800/50 hover:text-gray-200'
                    }`}
                    style={{ width: 'calc(100% - 12px)' }}
                  >
                    <StatusDot status={d.status} />
                    <div className="flex-1 min-w-0">
                      <p className="text-xs font-medium truncate leading-tight">{d.file_name}</p>
                      {d.status === 'SUCCESS' && (
                        <p className="text-[10px] text-gray-600 mt-0.5">
                          {d.component_count} comp · {d.match_pct}%
                        </p>
                      )}
                      {d.status === 'IN_PROGRESS' && d.progress_msg && (
                        <p className="text-[10px] text-blue-500/80 mt-0.5 truncate">{d.progress_msg}</p>
                      )}
                      {d.status === 'ERROR' && (
                        <p className="text-[10px] text-red-500/80 mt-0.5 truncate">{d.error_message || 'Failed'}</p>
                      )}
                    </div>
                  </button>
                );
              })}
            </div>
          </div>
        )}

        {/* Collapsed icons */}
        {isCollapsed && processed.map((d) => (
          <button
            key={d.file_name}
            onClick={() => onSelectDiagram(d)}
            className={`w-full p-2.5 flex items-center justify-center transition-colors ${d.file_name === activeFileName ? 'text-white' : 'text-gray-500 hover:text-gray-300'}`}
            title={d.file_name}
          >
            <StatusDot status={d.status} />
          </button>
        ))}

        {/* Unprocessed section */}
        {!isCollapsed && unprocessed.length > 0 && (
          <div className="mt-2 px-3">
            <button
              onClick={() => setShowUnprocessed((v) => !v)}
              className="w-full flex items-center justify-between py-1.5 text-[10px] font-semibold uppercase tracking-wider text-gray-600 hover:text-gray-400 transition-colors"
            >
              <span>Unprocessed ({unprocessed.length})</span>
              <svg className={`w-3 h-3 transition-transform ${showUnprocessed ? 'rotate-90' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>
            </button>
            {showUnprocessed && (
              <div className="mt-1 space-y-0.5 animate-fade-in">
                {unprocessed.map((fn) => (
                  <div key={fn} className="flex items-center justify-between gap-2 px-2 py-1.5 rounded-lg hover:bg-gray-800/30 group">
                    <span className="text-xs text-gray-600 truncate flex-1">{fn}</span>
                    <button
                      onClick={() => onExtractUnprocessed(fn)}
                      className="shrink-0 text-[10px] px-2 py-0.5 rounded-md bg-green-500/10 text-green-500 hover:bg-green-500/20 border border-green-500/20 transition-colors opacity-0 group-hover:opacity-100"
                    >
                      Extract
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Footer: upload button */}
      <div className={`shrink-0 border-t border-gray-800/50 ${isCollapsed ? 'px-1.5 py-2' : 'px-3 py-2'}`}>
        <button
          onClick={onUpload}
          className={`flex items-center gap-2 w-full rounded-lg border border-gray-700/50 text-gray-300 hover:text-white hover:bg-gray-800 hover:border-gray-600 transition-all duration-150 ${isCollapsed ? 'justify-center p-2.5' : 'px-3 py-2.5'}`}
        >
          <svg className="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
          {!isCollapsed && <span className="text-sm font-medium">Upload PDF</span>}
        </button>
      </div>
    </div>
  );
}
