import { useState, useCallback, useEffect } from 'react';
import { Diagram } from './types';
import { useChat } from './hooks/useChat';
import { useDiagrams } from './hooks/useDiagrams';
import { ToastProvider, useToast } from './components/Toast';
import Header from './components/Header';
import Sidebar from './components/Sidebar';
import ChatPanel from './components/ChatPanel';
import DiagramViewer from './components/DiagramViewer';
import StatusBar from './components/StatusBar';
import UploadModal from './components/UploadModal';
import { ExtractionParams } from './components/UploadModal';

function AppContent() {
  const { messages, isLoading, sendMessage } = useChat();
  const { diagrams, unprocessed, loading: diagramsLoading, fetchDiagrams, addInProgress } = useDiagrams();
  const { addToast } = useToast();

  const [selectedDiagram, setSelectedDiagram] = useState<Diagram | null>(null);
  const [isViewerVisible, setIsViewerVisible]  = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [chatCollapsed, setChatCollapsed]       = useState(false);
  const [showUpload, setShowUpload]             = useState(false);

  // Sync viewer with latest diagram data from polls
  useEffect(() => {
    if (selectedDiagram) {
      const updated = diagrams.find((d) => d.file_name === selectedDiagram.file_name);
      if (updated) setSelectedDiagram(updated);
    }
  }, [diagrams]); // eslint-disable-line react-hooks/exhaustive-deps

  // Animate viewer in
  useEffect(() => {
    if (selectedDiagram) {
      requestAnimationFrame(() => setIsViewerVisible(true));
    }
  }, [selectedDiagram?.file_name]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleSelectDiagram = useCallback((d: Diagram) => {
    if (selectedDiagram?.file_name === d.file_name) {
      // Close if clicking same diagram
      setIsViewerVisible(false);
      setTimeout(() => setSelectedDiagram(null), 300);
    } else {
      setSelectedDiagram(d);
    }
  }, [selectedDiagram]);

  const handleCloseViewer = useCallback(() => {
    setIsViewerVisible(false);
    setTimeout(() => setSelectedDiagram(null), 300);
  }, []);

  const handleExtract = useCallback(async (fileName: string, params?: ExtractionParams) => {
    try {
      const resp = await fetch('/api/extract', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          file_name:    fileName,
          model:        params?.model        ?? 'databricks-claude-sonnet-4-6',
          enable_retry: params?.enable_retry ?? true,
          max_retries:  params?.max_retries  ?? 2,
          threshold:    params?.threshold    ?? 0.75,
        }),
      });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(err.detail ?? `HTTP ${resp.status}`);
      }
      addInProgress(fileName);
      addToast('success', `Extraction started for ${fileName}`);
      // Auto-open viewer in IN_PROGRESS state
      const placeholder: Diagram = {
        file_name: fileName, file_path: null, status: 'IN_PROGRESS',
        progress_msg: 'Job submitted…', processed_at: null, attempts_made: null,
        threshold_met: null, error_message: null, pdf_type: null,
        component_count: 0, matched_count: 0, match_pct: 0, components: [],
      };
      setSelectedDiagram(placeholder);
    } catch (e) {
      addToast('error', `Failed to start extraction: ${e instanceof Error ? e.message : e}`);
    }
  }, [addInProgress, addToast]);

  const handleUploadSubmit = useCallback(async (file: File, params: ExtractionParams) => {
    // 1. Upload PDF
    const form = new FormData();
    form.append('file', file);
    const uploadResp = await fetch('/api/upload', { method: 'POST', body: form });
    if (!uploadResp.ok) {
      const err = await uploadResp.json().catch(() => ({}));
      throw new Error(err.detail ?? `Upload failed: HTTP ${uploadResp.status}`);
    }
    addToast('info', `${file.name} uploaded successfully`);

    // 2. Trigger extraction
    await handleExtract(file.name, params);
    await fetchDiagrams();
  }, [handleExtract, addToast, fetchDiagrams]);

  // Keyboard: Ctrl+B sidebar, Escape viewer
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'b' && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        setSidebarCollapsed((v) => !v);
      }
      if (e.key === '\\' && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        setChatCollapsed((v) => !v);
      }
      if (e.key === 'Escape' && selectedDiagram) {
        handleCloseViewer();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [selectedDiagram, handleCloseViewer]);

  return (
    <div className="h-screen flex flex-col bg-gray-950 text-gray-100">
      <Header
        isLoading={isLoading}
        onUpload={() => setShowUpload(true)}
      />

      <div className="flex flex-1 min-h-0 overflow-hidden">
        <Sidebar
          diagrams={diagrams}
          unprocessed={unprocessed}
          activeFileName={selectedDiagram?.file_name ?? null}
          isCollapsed={sidebarCollapsed}
          onToggleCollapse={() => setSidebarCollapsed((v) => !v)}
          onSelectDiagram={handleSelectDiagram}
          onExtractUnprocessed={(fn) => handleExtract(fn)}
          onUpload={() => setShowUpload(true)}
        />

        <div className="flex flex-1 min-w-0 overflow-hidden">
          {/* Diagram viewer — takes all available space */}
          <div className="flex flex-col flex-1 min-w-0 overflow-hidden">
            {selectedDiagram ? (
              <div
                className={`flex flex-col h-full overflow-hidden transition-opacity duration-300 ease-out ${
                  isViewerVisible ? 'opacity-100' : 'opacity-0'
                }`}
              >
                <DiagramViewer
                  diagram={selectedDiagram}
                  onClose={handleCloseViewer}
                  onReExtract={(fn) => handleExtract(fn)}
                />
              </div>
            ) : (
              /* Empty state — prompt to select a diagram */
              <div className="flex flex-col items-center justify-center h-full gap-4 text-center px-8">
                <div className="w-14 h-14 rounded-2xl bg-gray-900 flex items-center justify-center border border-gray-800">
                  <svg className="w-7 h-7 text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                  </svg>
                </div>
                <div>
                  <p className="text-sm font-medium text-gray-500">Select a diagram to view</p>
                  <p className="text-xs text-gray-700 mt-1">BOM components, annotated PDF, and reference matches</p>
                </div>
              </div>
            )}
          </div>

          {/* Chat panel — fixed width on right, collapsible */}
          <div
            className={`flex shrink-0 overflow-hidden transition-all duration-300 ease-out border-l border-gray-800/50 ${
              chatCollapsed ? 'w-0' : 'w-[360px]'
            }`}
          >
            <ChatPanel
              messages={messages}
              isLoading={isLoading}
              diagrams={diagrams}
              onSendMessage={sendMessage}
              onSelectDiagram={handleSelectDiagram}
              selectedFileName={selectedDiagram?.file_name ?? null}
            />
          </div>

          {/* Chat toggle button */}
          <button
            onClick={() => setChatCollapsed((v) => !v)}
            className="shrink-0 flex items-center justify-center w-5 bg-gray-900 hover:bg-gray-800 border-l border-gray-800/50 transition-colors group"
            title={chatCollapsed ? 'Show chat' : 'Hide chat'}
          >
            <svg
              className={`w-3 h-3 text-gray-600 group-hover:text-gray-400 transition-transform duration-300 ${chatCollapsed ? 'rotate-180' : ''}`}
              fill="none" stroke="currentColor" viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
          </button>
        </div>
      </div>

      <StatusBar diagrams={diagrams} isLoading={diagramsLoading} />

      {showUpload && (
        <UploadModal
          onClose={() => setShowUpload(false)}
          onSubmit={handleUploadSubmit}
        />
      )}
    </div>
  );
}

export default function App() {
  return (
    <ToastProvider>
      <AppContent />
    </ToastProvider>
  );
}
