import { useState, useEffect, useCallback, useRef } from 'react';
import { Diagram } from '../types';

const FAST_INTERVAL = 2000;   // 2s when extractions are in progress
const SLOW_INTERVAL = 30000;  // 30s when all done

export function useDiagrams() {
  const [diagrams, setDiagrams]       = useState<Diagram[]>([]);
  const [unprocessed, setUnprocessed] = useState<string[]>([]);
  const [loading, setLoading]         = useState(false);
  const timerRef                      = useRef<ReturnType<typeof setTimeout> | null>(null);

  const fetchDiagrams = useCallback(async () => {
    try {
      setLoading(true);
      const [dRes, uRes] = await Promise.all([
        fetch('/api/diagrams'),
        fetch('/api/unprocessed'),
      ]);
      if (dRes.ok) {
        const data: Diagram[] = await dRes.json();
        setDiagrams(data);
      }
      if (uRes.ok) {
        const data: { files: string[] } = await uRes.json();
        setUnprocessed(data.files);
      }
    } catch {
      // silent — status bar will show loading state
    } finally {
      setLoading(false);
    }
  }, []);

  // Schedule next poll based on whether anything is in progress
  const scheduleNext = useCallback((currentDiagrams: Diagram[]) => {
    if (timerRef.current) clearTimeout(timerRef.current);
    const hasInProgress = currentDiagrams.some((d) => d.status === 'IN_PROGRESS');
    timerRef.current = setTimeout(async () => {
      await fetchDiagrams();
    }, hasInProgress ? FAST_INTERVAL : SLOW_INTERVAL);
  }, [fetchDiagrams]);

  // Re-schedule whenever diagrams change
  useEffect(() => {
    scheduleNext(diagrams);
    return () => { if (timerRef.current) clearTimeout(timerRef.current); };
  }, [diagrams, scheduleNext]);

  // Initial fetch
  useEffect(() => {
    fetchDiagrams();
  }, [fetchDiagrams]);

  // Optimistically add an IN_PROGRESS diagram (before first poll confirms it)
  const addInProgress = useCallback((fileName: string) => {
    setDiagrams((prev) => {
      const exists = prev.find((d) => d.file_name === fileName);
      if (exists) {
        return prev.map((d) =>
          d.file_name === fileName
            ? { ...d, status: 'IN_PROGRESS' as const, progress_msg: 'Job submitted…' }
            : d
        );
      }
      return [
        {
          file_name: fileName, file_path: null, status: 'IN_PROGRESS' as const,
          progress_msg: 'Job submitted…', processed_at: null, attempts_made: null,
          threshold_met: null, error_message: null, pdf_type: null,
          component_count: 0, matched_count: 0, match_pct: 0, components: [],
        },
        ...prev,
      ];
    });
    setUnprocessed((prev) => prev.filter((f) => f !== fileName));
    // Trigger fast poll immediately
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(fetchDiagrams, FAST_INTERVAL);
  }, [fetchDiagrams]);

  return { diagrams, unprocessed, loading, fetchDiagrams, addInProgress };
}
