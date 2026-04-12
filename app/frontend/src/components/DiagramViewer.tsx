import { useState, useCallback } from 'react';
import { Diagram } from '../types';
import BOMTable from './BOMTable';
import PDFViewer from './PDFViewer';
import ReferencesTab from './ReferencesTab';

type Tab = 'bom' | 'diagram' | 'references';

interface DiagramViewerProps {
  diagram: Diagram;
  onClose: () => void;
  onReExtract: (fileName: string) => void;
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleString('en-GB', {
    day: '2-digit', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit',
  });
}

export default function DiagramViewer({ diagram, onClose, onReExtract }: DiagramViewerProps) {
  const [tab, setTab] = useState<Tab>('bom');
  const [diagramMaximized, setDiagramMaximized] = useState(false);
  const [pdfError, setPdfError] = useState<string | null>(null);
  const [pdfLoading, setPdfLoading] = useState(false);

  const isInProgress = diagram.status === 'IN_PROGRESS';
  const isError      = diagram.status === 'ERROR';

  const handleDownloadPdf = useCallback(async () => {
    setPdfError(null);
    setPdfLoading(true);
    try {
      const resp = await fetch(`/api/annotated/${encodeURIComponent(diagram.file_name)}`);
      if (resp.status === 404) {
        setPdfError('Annotated PDF not yet generated — re-run extraction to create it');
        return;
      }
      if (!resp.ok) {
        setPdfError(`Download failed (${resp.status})`);
        return;
      }
      const blob = await resp.blob();
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement('a');
      a.href     = url;
      a.download = `${diagram.file_name.replace(/\.pdf$/i, '')}_annotated.pdf`;
      a.click();
      URL.revokeObjectURL(url);
    } catch {
      setPdfError('Download failed — check network connection');
    } finally {
      setPdfLoading(false);
    }
  }, [diagram.file_name]);

  return (
    <div className="flex flex-col h-full">
      {/* Viewer header */}
      <div className="shrink-0 flex items-center justify-between px-4 py-3 glass border-b border-gray-800/50">
        <div className="flex items-center gap-3 min-w-0">
          <div className="flex items-center gap-2 min-w-0">
            <span className="text-sm font-semibold text-gray-200 truncate max-w-[200px]" title={diagram.file_name}>
              {diagram.file_name}
            </span>
            {diagram.status === 'SUCCESS' && (
              <span className="shrink-0 text-[10px] px-1.5 py-0.5 rounded-full bg-green-500/20 text-green-400 border border-green-500/20">
                {diagram.match_pct}% matched
              </span>
            )}
            {isInProgress && (
              <span className="shrink-0 flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded-full bg-blue-500/20 text-blue-400 border border-blue-500/20">
                <span className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse" />
                extracting
              </span>
            )}
            {isError && (
              <span className="shrink-0 text-[10px] px-1.5 py-0.5 rounded-full bg-red-500/20 text-red-400 border border-red-500/20">
                error
              </span>
            )}
          </div>
          {diagram.processed_at && (
            <span className="text-[11px] text-gray-600 shrink-0">{formatDate(diagram.processed_at)}</span>
          )}
        </div>

        <div className="flex items-center gap-2 shrink-0">
          {/* Download annotated PDF */}
          {diagram.status === 'SUCCESS' && (
            <button
              onClick={handleDownloadPdf}
              disabled={pdfLoading}
              className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium text-gray-400 hover:text-gray-200 hover:bg-gray-800 rounded-lg border border-gray-700/50 hover:border-gray-600 transition-all disabled:opacity-40 disabled:cursor-not-allowed"
              title="Download annotated PDF"
            >
              {pdfLoading
                ? <svg className="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg>
                : <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3M3 17v3a2 2 0 002 2h14a2 2 0 002-2v-3" /></svg>
              }
              PDF
            </button>
          )}
          {/* Re-extract */}
          <button
            onClick={() => onReExtract(diagram.file_name)}
            disabled={isInProgress}
            className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium text-gray-400 hover:text-gray-200 hover:bg-gray-800 rounded-lg border border-gray-700/50 hover:border-gray-600 transition-all disabled:opacity-40 disabled:cursor-not-allowed"
            title="Re-run extraction"
          >
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Re-extract
          </button>
          {/* Close */}
          <button
            onClick={onClose}
            className="p-1.5 text-gray-500 hover:text-gray-300 hover:bg-gray-800 rounded-lg transition-colors"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      </div>

      {/* PDF download error */}
      {pdfError && (
        <div className="shrink-0 flex items-center justify-between gap-2 px-4 py-2 bg-yellow-500/5 border-b border-yellow-500/20">
          <p className="text-xs text-yellow-300">{pdfError}</p>
          <button onClick={() => setPdfError(null)} className="text-yellow-500 hover:text-yellow-300 transition-colors">
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      )}

      {/* Extraction in progress */}
      {isInProgress && (
        <div className="shrink-0 px-4 py-3 bg-blue-500/5 border-b border-blue-500/20">
          <div className="flex items-center gap-2 text-sm text-blue-300 mb-2">
            <svg className="w-4 h-4 animate-spin shrink-0" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            Extraction in progress
          </div>
          {diagram.progress_msg && (
            <p className="text-xs text-blue-400/70">{diagram.progress_msg}</p>
          )}
        </div>
      )}

      {/* Error state */}
      {isError && (
        <div className="shrink-0 px-4 py-3 bg-red-500/5 border-b border-red-500/20">
          <p className="text-xs text-red-300 font-medium mb-1">Extraction failed</p>
          <p className="text-xs text-red-400/70">{diagram.error_message || 'Unknown error'}</p>
        </div>
      )}

      {/* Tabs — only shown for SUCCESS */}
      {diagram.status === 'SUCCESS' && (
        <div className="shrink-0 flex items-center border-b border-gray-800/50 px-4">
          {(['bom', 'diagram', 'references'] as Tab[]).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`px-3 py-2.5 text-xs font-medium border-b-2 transition-all -mb-px ${
                tab === t
                  ? 'border-green-500 text-green-400'
                  : 'border-transparent text-gray-500 hover:text-gray-300'
              }`}
            >
              {t === 'bom' ? `BOM (${diagram.component_count})` : t === 'diagram' ? 'Diagram' : 'References'}
            </button>
          ))}
          {/* Maximize button — only on Diagram tab */}
          {tab === 'diagram' && (
            <button
              onClick={() => setDiagramMaximized(true)}
              className="ml-auto p-1.5 text-gray-500 hover:text-gray-300 hover:bg-gray-800 rounded-lg transition-colors"
              title="Maximize diagram"
            >
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4" />
              </svg>
            </button>
          )}
        </div>
      )}

      {/* Content */}
      <div className="flex-1 overflow-auto min-h-0">
        {diagram.status === 'SUCCESS' && tab === 'bom' && (
          <BOMTable
            components={diagram.components}
            threshold_met={diagram.threshold_met}
            attempts_made={diagram.attempts_made}
          />
        )}
        {diagram.status === 'SUCCESS' && tab === 'diagram' && (
          <PDFViewer
            fileName={diagram.file_name}
            componentCount={diagram.component_count}
            matchedCount={diagram.matched_count}
            pdfType={diagram.pdf_type}
          />
        )}
        {diagram.status === 'SUCCESS' && tab === 'references' && (
          <ReferencesTab fileName={diagram.file_name} />
        )}
        {(isInProgress || isError) && !diagram.components?.length && (
          <div className="flex items-center justify-center h-full text-sm text-gray-700">
            {isInProgress ? 'Waiting for results…' : 'No results available'}
          </div>
        )}
      </div>

      {/* Fullscreen diagram overlay */}
      {diagramMaximized && (
        <div className="fixed inset-0 z-50 bg-gray-950 flex flex-col">
          <div className="shrink-0 flex items-center justify-between px-4 py-2 border-b border-gray-800/50 bg-gray-900/80">
            <span className="text-sm font-medium text-gray-300 truncate">{diagram.file_name}</span>
            <button
              onClick={() => setDiagramMaximized(false)}
              className="p-1.5 text-gray-500 hover:text-gray-200 hover:bg-gray-800 rounded-lg transition-colors"
              title="Exit fullscreen"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 9L4 4m0 0v4m0-4h4m11 0l-5 5m5-5v4m0-4h-4M9 15l-5 5m0 0v-4m0 4h4m11 0l-5-5m5 5v-4m0 4h-4" />
              </svg>
            </button>
          </div>
          <div className="flex-1 min-h-0">
            <PDFViewer
              fileName={diagram.file_name}
              componentCount={diagram.component_count}
              matchedCount={diagram.matched_count}
              pdfType={diagram.pdf_type}
            />
          </div>
        </div>
      )}
    </div>
  );
}
