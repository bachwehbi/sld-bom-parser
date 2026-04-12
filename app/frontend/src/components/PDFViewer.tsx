import { useState } from 'react';

interface PDFViewerProps {
  fileName: string;
  componentCount: number;
  matchedCount: number;
  pdfType?: string | null;
}

export default function PDFViewer({ fileName, componentCount, matchedCount, pdfType }: PDFViewerProps) {
  const [error, setError] = useState(false);

  if (pdfType === 'scanned') {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-gray-600">
        <svg className="w-10 h-10" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        <p className="text-sm font-medium text-gray-500">Diagram not available</p>
        <p className="text-xs text-gray-600 text-center max-w-xs">
          This document is a scanned image PDF — component annotations cannot be generated without vector text.
        </p>
      </div>
    );
  }

  if (pdfType === 'unrecognized') {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-gray-600">
        <svg className="w-10 h-10" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
        <p className="text-sm font-medium text-gray-500">Diagram not available</p>
        <p className="text-xs text-gray-600 text-center max-w-xs">
          This document uses a non-standard format — electrical spec annotations were not detected in the vector text.
        </p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-gray-600">
        <svg className="w-10 h-10" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        <p className="text-sm text-gray-500">Annotated diagram not available</p>
        <p className="text-xs text-gray-700">Re-run extraction to generate it</p>
      </div>
    );
  }

  const src = `/api/annotated/${encodeURIComponent(fileName)}`;

  return (
    <div className="relative flex flex-col h-full bg-gray-950/50">
      {/* Stats badge */}
      <div className="absolute top-3 right-3 z-10 flex items-center gap-2 px-3 py-1.5 rounded-full bg-gray-900/90 border border-gray-700/50 backdrop-blur text-xs">
        <span className="w-2 h-2 rounded-full bg-green-500" />
        <span className="text-gray-200 font-medium">{matchedCount}/{componentCount}</span>
        <span className="text-gray-500">annotated</span>
      </div>
      <iframe
        src={src}
        title={`Annotated diagram for ${fileName}`}
        className="flex-1 w-full border-0"
        onError={() => setError(true)}
      />
    </div>
  );
}
