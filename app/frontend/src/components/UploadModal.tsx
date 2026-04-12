import { useState, useRef, DragEvent, ChangeEvent } from 'react';

interface UploadModalProps {
  onClose: () => void;
  onSubmit: (file: File, params: ExtractionParams) => Promise<void>;
}

export interface ExtractionParams {
  model: string;
  enable_retry: boolean;
  max_retries: number;
  threshold: number;
}

export default function UploadModal({ onClose, onSubmit }: UploadModalProps) {
  const [file, setFile]         = useState<File | null>(null);
  const [dragging, setDragging] = useState(false);
  const [loading, setLoading]   = useState(false);
  const [params, setParams]     = useState<ExtractionParams>({
    model:        'databricks-claude-sonnet-4-6',
    enable_retry: true,
    max_retries:  2,
    threshold:    0.75,
  });
  const inputRef = useRef<HTMLInputElement>(null);

  const handleDrop = (e: DragEvent) => {
    e.preventDefault();
    setDragging(false);
    const dropped = e.dataTransfer.files[0];
    if (dropped?.name.toLowerCase().endsWith('.pdf')) setFile(dropped);
  };

  const handleFileChange = (e: ChangeEvent<HTMLInputElement>) => {
    const chosen = e.target.files?.[0];
    if (chosen?.name.toLowerCase().endsWith('.pdf')) setFile(chosen);
  };

  const handleSubmit = async () => {
    if (!file) return;
    setLoading(true);
    try {
      await onSubmit(file, params);
      onClose();
    } finally {
      setLoading(false);
    }
  };

  const fileSizeMB = file ? (file.size / 1024 / 1024).toFixed(1) : null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm animate-fade-in">
      <div className="w-full max-w-md bg-gray-900 border border-gray-700/50 rounded-2xl shadow-2xl animate-scale-in overflow-hidden">
        {/* Modal header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-800/50">
          <h2 className="text-sm font-semibold text-gray-200">Upload & Extract</h2>
          <button onClick={onClose} className="p-1.5 text-gray-500 hover:text-gray-300 hover:bg-gray-800 rounded-lg transition-colors">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div className="px-5 py-4 flex flex-col gap-4">
          {/* Drop zone */}
          <div
            onClick={() => inputRef.current?.click()}
            onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
            onDragLeave={() => setDragging(false)}
            onDrop={handleDrop}
            className={`relative flex flex-col items-center justify-center gap-2 h-32 rounded-xl border-2 border-dashed cursor-pointer transition-all duration-150 ${
              dragging
                ? 'border-green-500/60 bg-green-500/5'
                : file
                ? 'border-green-500/40 bg-green-500/5'
                : 'border-gray-700/50 hover:border-gray-600 hover:bg-gray-800/30'
            }`}
          >
            <input ref={inputRef} type="file" accept=".pdf" className="hidden" onChange={handleFileChange} />
            {file ? (
              <>
                <svg className="w-8 h-8 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                <p className="text-sm font-medium text-gray-200">{file.name}</p>
                <p className="text-xs text-gray-500">{fileSizeMB} MB — click to change</p>
                {Number(fileSizeMB) > 50 && (
                  <p className="text-xs text-yellow-400">Large file — upload may take a moment</p>
                )}
              </>
            ) : (
              <>
                <svg className="w-8 h-8 text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                </svg>
                <p className="text-sm text-gray-400">Drop a PDF here or <span className="text-green-400">browse</span></p>
                <p className="text-xs text-gray-600">Single Line Diagrams only</p>
              </>
            )}
          </div>

          {/* Extraction params */}
          <div className="flex flex-col gap-3">
            <p className="text-[10px] font-semibold uppercase tracking-wider text-gray-600">Extraction parameters</p>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs text-gray-500 mb-1">Model</label>
                <select
                  value={params.model}
                  onChange={(e) => setParams({ ...params, model: e.target.value })}
                  className="w-full bg-gray-800 border border-gray-700/50 rounded-lg px-2.5 py-1.5 text-xs text-gray-200 focus:outline-none focus:ring-1 focus:ring-green-500/40"
                >
                  <option value="databricks-claude-sonnet-4-6">Claude Sonnet 4.6</option>
                  <option value="databricks-claude-opus-4-6">Claude Opus 4.6</option>
                  <option value="databricks-claude-haiku-4-5-20251001">Claude Haiku 4.5</option>
                </select>
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Match threshold</label>
                <input
                  type="number" min="0" max="1" step="0.05"
                  value={params.threshold}
                  onChange={(e) => setParams({ ...params, threshold: parseFloat(e.target.value) })}
                  className="w-full bg-gray-800 border border-gray-700/50 rounded-lg px-2.5 py-1.5 text-xs text-gray-200 focus:outline-none focus:ring-1 focus:ring-green-500/40"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Max retries</label>
                <input
                  type="number" min="0" max="5"
                  value={params.max_retries}
                  onChange={(e) => setParams({ ...params, max_retries: parseInt(e.target.value) })}
                  className="w-full bg-gray-800 border border-gray-700/50 rounded-lg px-2.5 py-1.5 text-xs text-gray-200 focus:outline-none focus:ring-1 focus:ring-green-500/40"
                />
              </div>
              <div className="flex items-end">
                <label className="flex items-center gap-2 cursor-pointer">
                  <div
                    onClick={() => setParams({ ...params, enable_retry: !params.enable_retry })}
                    className={`relative w-9 h-5 rounded-full transition-colors cursor-pointer ${params.enable_retry ? 'bg-green-600' : 'bg-gray-700'}`}
                  >
                    <span className={`absolute top-0.5 left-0.5 w-4 h-4 rounded-full bg-white shadow transition-transform ${params.enable_retry ? 'translate-x-4' : ''}`} />
                  </div>
                  <span className="text-xs text-gray-400">Enable retry</span>
                </label>
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 px-5 py-4 border-t border-gray-800/50">
          <button
            onClick={onClose}
            className="px-4 py-2 text-xs font-medium text-gray-400 hover:text-gray-200 hover:bg-gray-800 rounded-lg transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSubmit}
            disabled={!file || loading}
            className="flex items-center gap-2 px-4 py-2 text-xs font-medium text-white bg-green-600 hover:bg-green-500 rounded-lg transition-all disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {loading ? (
              <>
                <svg className="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
                Uploading…
              </>
            ) : (
              <>
                <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                </svg>
                Upload & Extract
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
