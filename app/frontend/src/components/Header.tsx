interface HeaderProps {
  isLoading: boolean;
  onUpload: () => void;
}

export default function Header({ isLoading, onUpload }: HeaderProps) {
  return (
    <header className="flex items-center justify-between px-5 py-2.5 glass border-b border-gray-800/50 shrink-0 z-10">
      {/* Left: branding */}
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2 shrink-0">
          {/* Schneider Electric green square logo mark */}
          <div className="w-7 h-7 rounded-md bg-green-500 flex items-center justify-center shrink-0">
            <svg className="w-4 h-4 text-white" fill="currentColor" viewBox="0 0 24 24">
              <path d="M13 3L4 14h7l-2 7 9-11h-7l2-7z" />
            </svg>
          </div>
          <div className="w-px h-5 bg-gray-700/50" />
        </div>
        <div>
          <h1 className="text-sm font-semibold text-gray-200 leading-tight">SLD BOM Parser</h1>
          <p className="text-[10px] text-gray-500 leading-tight">Schneider Electric · Electrical Diagrams</p>
        </div>
      </div>

      {/* Center: thinking indicator */}
      {isLoading && (
        <div className="flex items-center gap-2 px-3 py-1 rounded-full bg-blue-500/10 border border-blue-500/20 animate-fade-in">
          <div className="flex gap-0.5">
            <span className="w-1 h-1 bg-blue-400 rounded-full animate-bounce [animation-delay:0ms]" />
            <span className="w-1 h-1 bg-blue-400 rounded-full animate-bounce [animation-delay:150ms]" />
            <span className="w-1 h-1 bg-blue-400 rounded-full animate-bounce [animation-delay:300ms]" />
          </div>
          <span className="text-xs text-blue-300">Agent thinking…</span>
        </div>
      )}

      {/* Right: upload button */}
      <button
        onClick={onUpload}
        className="flex items-center gap-2 px-3 py-1.5 text-xs font-medium text-white bg-green-600 hover:bg-green-500 rounded-lg transition-all duration-150"
      >
        <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
        </svg>
        Upload PDF
      </button>
    </header>
  );
}
