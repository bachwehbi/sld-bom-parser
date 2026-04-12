import { useState } from 'react';

interface OverlayImageProps {
  fileName: string;
  componentCount: number;
  matchedCount: number;
  pdfType?: string | null;
}

export default function OverlayImage({ fileName, componentCount, matchedCount, pdfType }: OverlayImageProps) {
  const [error, setError] = useState(false);
  const [loaded, setLoaded] = useState(false);

  const src = `/api/overlay/${encodeURIComponent(fileName)}`;

  // For non-vector PDFs, skip the image request entirely and explain why
  if (pdfType === 'scanned') {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-gray-600">
        <svg className="w-10 h-10" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        <p className="text-sm font-medium text-gray-500">Overlay not available</p>
        <p className="text-xs text-gray-600 text-center max-w-xs">
          This document is a scanned image PDF — component positions cannot be detected without vector text.
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
        <p className="text-sm font-medium text-gray-500">Overlay not available</p>
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
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
        </svg>
        <p className="text-sm">Overlay not available</p>
        <p className="text-xs text-gray-700">{fileName}</p>
      </div>
    );
  }

  return (
    <div className="relative flex items-center justify-center h-full bg-gray-950/50 overflow-auto p-4">
      {!loaded && (
        <div className="absolute inset-0 flex items-center justify-center">
          <svg className="w-8 h-8 animate-spin text-gray-700" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
        </div>
      )}
      {/* Stats badge */}
      {loaded && (
        <div className="absolute top-3 right-3 flex items-center gap-2 px-3 py-1.5 rounded-full bg-gray-900/90 border border-gray-700/50 backdrop-blur text-xs animate-fade-in">
          <span className="w-2 h-2 rounded-full bg-green-500" />
          <span className="text-gray-200 font-medium">{matchedCount}/{componentCount}</span>
          <span className="text-gray-500">matched</span>
        </div>
      )}
      <img
        src={src}
        alt={`Overlay for ${fileName}`}
        className={`max-w-full max-h-full object-contain rounded-lg transition-opacity duration-300 ${loaded ? 'opacity-100' : 'opacity-0'}`}
        onLoad={() => setLoaded(true)}
        onError={() => setError(true)}
      />
    </div>
  );
}
