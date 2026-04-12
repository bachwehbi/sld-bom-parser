import { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { MatchRow, ReferenceCandidate } from '../types';

const TYPE_LABELS: Record<string, string> = {
  circuit_breaker:   'Circuit Breaker',
  rcd:               'RCD / RCCB',
  contactor:         'Contactor',
  timer:             'Timer',
  surge_protector:   'Surge Protector',
  fuse_holder:       'Fuse Holder',
  energy_meter:      'Energy Meter',
  ats:               'ATS',
  load_break_switch: 'Load Break Switch',
};

// Extract component type from summary string like "circuit breaker 16A — circuit [panel]"
function typeFromSummary(summary: string): string {
  const s = summary.toLowerCase();
  if (s.startsWith('circuit breaker')) return 'circuit_breaker';
  if (s.startsWith('rcd'))             return 'rcd';
  if (s.startsWith('contactor'))       return 'contactor';
  if (s.startsWith('timer'))           return 'timer';
  if (s.startsWith('surge'))           return 'surge_protector';
  if (s.startsWith('fuse'))            return 'fuse_holder';
  if (s.startsWith('energy'))          return 'energy_meter';
  if (s.startsWith('ats'))             return 'ats';
  if (s.startsWith('load break'))      return 'load_break_switch';
  return summary.split(' ')[0] || 'unknown';
}

interface ReferencesTabProps {
  fileName: string;
}

type TabState = 'loading' | 'not_matched' | 'running' | 'matched' | 'error';

function StockBadge({ status }: { status: string | null }) {
  if (!status) return null;
  const styles: Record<string, string> = {
    IN_STOCK:    'bg-green-500/20 text-green-400 border-green-500/20',
    LOW_STOCK:   'bg-yellow-500/20 text-yellow-400 border-yellow-500/20',
    OUT_OF_STOCK: 'bg-red-500/20 text-red-400 border-red-500/20',
  };
  const labels: Record<string, string> = {
    IN_STOCK: 'In Stock', LOW_STOCK: 'Low Stock', OUT_OF_STOCK: 'Out of Stock',
  };
  return (
    <span className={`text-[10px] px-1.5 py-0.5 rounded-full border ${styles[status] ?? 'bg-gray-700/30 text-gray-400 border-gray-600/20'}`}>
      {labels[status] ?? status}
    </span>
  );
}

function TierBadge({ tier }: { tier: string | null }) {
  if (!tier) return null;
  const styles: Record<string, string> = {
    economy:  'bg-gray-500/20 text-gray-400',
    standard: 'bg-blue-500/20 text-blue-400',
    premium:  'bg-purple-500/20 text-purple-400',
  };
  return (
    <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${styles[tier] ?? 'bg-gray-700/30 text-gray-400'}`}>
      {tier}
    </span>
  );
}

export default function ReferencesTab({ fileName }: ReferencesTabProps) {
  const [tabState, setTabState]         = useState<TabState>('loading');
  const [matches, setMatches]           = useState<MatchRow[]>([]);
  const [error, setError]               = useState<string | null>(null);
  const [runId, setRunId]               = useState<number | null>(null);
  const [overrides, setOverrides]       = useState<Record<number, string>>({});
  const [saving, setSaving]             = useState(false);
  const [saveError, setSaveError]       = useState<string | null>(null);
  const [exporting, setExporting]       = useState(false);
  const [exportError, setExportError]   = useState<string | null>(null);
  const [hasChanges, setHasChanges]     = useState(false);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const groups = useMemo(() => {
    const map = new Map<string, MatchRow[]>();
    for (const m of matches) {
      const t = typeFromSummary(m.component_summary);
      if (!map.has(t)) map.set(t, []);
      map.get(t)!.push(m);
    }
    return Array.from(map.entries())
      .map(([type, rows]) => ({ type, label: TYPE_LABELS[type] ?? type.replace(/_/g, ' '), rows }))
      .sort((a, b) => b.rows.length - a.rows.length);
  }, [matches]);

  const toggleGroup = (type: string) => {
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(type)) next.delete(type);
      else next.add(type);
      return next;
    });
  };

  const allCollapsed = groups.length > 0 && groups.every((g) => collapsedGroups.has(g.type));
  const toggleAll = () => {
    if (allCollapsed) setCollapsedGroups(new Set());
    else setCollapsedGroups(new Set(groups.map((g) => g.type)));
  };

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  const loadMatches = useCallback(async () => {
    try {
      const resp = await fetch(`/api/matches/${encodeURIComponent(fileName)}`);
      if (!resp.ok) {
        const text = await resp.text().catch(() => '');
        throw new Error(`HTTP ${resp.status}${text ? ': ' + text.slice(0, 200) : ''}`);
      }
      const data = await resp.json();
      if (data.status === 'MATCHED' && data.matches?.length) {
        setMatches(data.matches);
        setTabState('matched');
        stopPolling();
        return 'found';
      }
      return 'not_found';
    } catch (e) {
      return e instanceof Error ? e.message : 'Request failed';
    }
  }, [fileName]);

  // Initial load
  useEffect(() => {
    loadMatches().then((result) => {
      if (result === 'found') return;
      if (result === 'not_found') { setTabState('not_matched'); return; }
      setError(result);
      setTabState('error');
    });
    return stopPolling;
  }, [loadMatches]);

  // Poll job run status when running
  useEffect(() => {
    if (tabState !== 'running' || !runId) return;

    const poll = async () => {
      try {
        const resp = await fetch(`/api/match/run-status/${runId}`);
        if (!resp.ok) return;
        const data = await resp.json();
        const lc = data.life_cycle;
        const rs = data.result_state;

        if (lc === 'TERMINATED') {
          if (rs === 'SUCCESS') {
            // Load results
            const result = await loadMatches();
            if (result !== 'found') {
              setTabState('error');
              setError(result === 'not_found' ? 'Job succeeded but no matches found' : result);
            }
          } else {
            stopPolling();
            setTabState('error');
            setError(data.state_message || `Job ${rs}`);
          }
        } else if (lc === 'INTERNAL_ERROR' || lc === 'SKIPPED') {
          stopPolling();
          setTabState('error');
          setError(data.state_message || 'Job failed');
        }
        // PENDING/RUNNING → keep polling
      } catch {
        // Network hiccup — keep polling
      }
    };

    pollRef.current = setInterval(poll, 5000);
    poll(); // immediate first check
    return stopPolling;
  }, [tabState, runId, loadMatches]);

  const handleGetReferences = async () => {
    setTabState('running');
    setError(null);
    try {
      const resp = await fetch('/api/match', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ file_name: fileName }),
      });
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || `HTTP ${resp.status}`);
      }
      const data = await resp.json();
      setRunId(data.run_id);
    } catch (e: unknown) {
      setTabState('error');
      setError(e instanceof Error ? e.message : 'Failed to start matching job');
    }
  };

  const handleOverrideChange = (componentIdx: number, ref: string) => {
    setOverrides((prev) => ({ ...prev, [componentIdx]: ref }));
    setHasChanges(true);
    setSaveError(null);
  };

  const handleSave = async () => {
    setSaving(true);
    setSaveError(null);
    const items = Object.entries(overrides).map(([idx, ref]) => {
      const original = matches.find((m) => m.component_idx === Number(idx));
      const isOriginalTop = original?.suggested_references?.[0]?.reference === ref && !original.user_overridden;
      return {
        component_idx: Number(idx),
        selected_reference: ref,
        status: isOriginalTop ? 'ACCEPTED' : 'OVERRIDDEN',
      };
    });

    try {
      const resp = await fetch('/api/matches', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ file_name: fileName, overrides: items }),
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      // Refresh
      await loadMatches();
      setOverrides({});
      setHasChanges(false);
    } catch (e: unknown) {
      setSaveError(e instanceof Error ? e.message : 'Save failed');
    } finally {
      setSaving(false);
    }
  };

  const handleExport = async () => {
    setExporting(true);
    setExportError(null);
    try {
      const resp = await fetch(`/api/export/${encodeURIComponent(fileName)}`, { method: 'POST' });
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || `HTTP ${resp.status}`);
      }
      const blob = await resp.blob();
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement('a');
      a.href     = url;
      const stem = fileName.replace(/\.pdf$/i, '');
      a.download = `${stem}_bom_references.xlsx`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e: unknown) {
      setExportError(e instanceof Error ? e.message : 'Export failed');
    } finally {
      setExporting(false);
    }
  };

  // ── Render states ─────────────────────────────────────────────────────────

  if (tabState === 'loading') {
    return (
      <div className="flex items-center justify-center h-full text-sm text-gray-600">
        <svg className="w-4 h-4 animate-spin mr-2" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
        Checking reference matches…
      </div>
    );
  }

  if (tabState === 'not_matched') {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-4 text-center px-8">
        <div className="w-12 h-12 rounded-full bg-gray-800 flex items-center justify-center">
          <svg className="w-6 h-6 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
              d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
          </svg>
        </div>
        <div>
          <p className="text-sm font-medium text-gray-300">No reference matches yet</p>
          <p className="text-xs text-gray-600 mt-1">Match extracted components against the Schneider Electric catalog</p>
        </div>
        <button
          onClick={handleGetReferences}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-green-600 hover:bg-green-500 rounded-lg transition-colors"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
          Get References
        </button>
      </div>
    );
  }

  if (tabState === 'running') {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-center">
        <svg className="w-6 h-6 animate-spin text-green-400" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
        <div>
          <p className="text-sm font-medium text-gray-300">Matching in progress…</p>
          <p className="text-xs text-gray-600 mt-1">Run ID: {runId}</p>
        </div>
      </div>
    );
  }

  if (tabState === 'error') {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-4 text-center px-8">
        <div className="w-12 h-12 rounded-full bg-red-500/10 flex items-center justify-center">
          <svg className="w-6 h-6 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
        </div>
        <div>
          <p className="text-sm font-medium text-red-300">Matching failed</p>
          <p className="text-xs text-red-400/70 mt-1">{error}</p>
        </div>
        <button
          onClick={() => { setTabState('not_matched'); setError(null); }}
          className="text-xs text-gray-500 hover:text-gray-300 underline"
        >
          Try again
        </button>
      </div>
    );
  }

  // ── MATCHED state — main table ────────────────────────────────────────────

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="shrink-0 flex items-center justify-between px-4 py-2.5 border-b border-gray-800/50 gap-3">
        <div className="flex items-center gap-2 text-xs text-gray-500">
          <span>{matches.length} components</span>
          <span className="text-gray-700">·</span>
          <span className="text-green-400">
            {matches.filter((m) => (overrides[m.component_idx] ?? m.selected_reference)).length} referenced
          </span>
          <span className="text-gray-700">·</span>
          <button onClick={toggleAll} className="text-gray-500 hover:text-gray-300 underline transition-colors">
            {allCollapsed ? 'expand all' : 'collapse all'}
          </button>
        </div>
        <div className="flex items-center gap-2">
          {/* Re-run matching */}
          <button
            onClick={handleGetReferences}
            className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs text-gray-400 hover:text-gray-200 hover:bg-gray-800 rounded-lg border border-gray-700/50 transition-all"
            title="Re-run matching"
          >
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Re-run
          </button>
          {/* Save overrides */}
          {hasChanges && (
            <button
              onClick={handleSave}
              disabled={saving}
              className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium text-white bg-blue-600 hover:bg-blue-500 rounded-lg transition-all disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {saving
                ? <svg className="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg>
                : <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" /></svg>
              }
              Save
            </button>
          )}
          {/* Download Excel */}
          <button
            onClick={handleExport}
            disabled={exporting}
            className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium text-gray-400 hover:text-gray-200 hover:bg-gray-800 rounded-lg border border-gray-700/50 hover:border-gray-600 transition-all disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {exporting
              ? <svg className="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg>
              : <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3M3 17v3a2 2 0 002 2h14a2 2 0 002-2v-3" /></svg>
            }
            Excel
          </button>
        </div>
      </div>

      {/* Inline error banners */}
      {saveError && (
        <div className="shrink-0 flex items-center justify-between gap-2 px-4 py-2 bg-red-500/5 border-b border-red-500/20">
          <p className="text-xs text-red-300">{saveError}</p>
          <button onClick={() => setSaveError(null)} className="text-red-500 hover:text-red-300">
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      )}
      {exportError && (
        <div className="shrink-0 flex items-center justify-between gap-2 px-4 py-2 bg-yellow-500/5 border-b border-yellow-500/20">
          <p className="text-xs text-yellow-300">{exportError}</p>
          <button onClick={() => setExportError(null)} className="text-yellow-500 hover:text-yellow-300">
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      )}

      {/* Matches — grouped */}
      <div className="flex-1 overflow-auto min-h-0 p-3 flex flex-col gap-2">
        {groups.map((group) => {
          const isCollapsed = collapsedGroups.has(group.type);
          const avgScore = group.rows.length > 0
            ? Math.round(group.rows.reduce((s, m) => {
                const refs = m.suggested_references ?? [];
                const currentRef = overrides[m.component_idx] ?? m.selected_reference ?? '';
                const detail = refs.find((r) => r.reference === currentRef) ?? refs[0];
                return s + (detail?.score ?? 0);
              }, 0) / group.rows.length)
            : 0;

          return (
            <div key={group.type} className="rounded-lg border border-gray-800/70 overflow-hidden">
              {/* Group header */}
              <button
                onClick={() => toggleGroup(group.type)}
                className="w-full flex items-center gap-3 px-3 py-2 bg-gray-900/80 hover:bg-gray-900 transition-colors text-left"
              >
                <svg
                  className={`w-3.5 h-3.5 text-gray-500 transition-transform shrink-0 ${isCollapsed ? '' : 'rotate-90'}`}
                  fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
                <span className="text-xs font-semibold text-gray-300">{group.label}</span>
                <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-gray-700/60 text-gray-400">{group.rows.length}</span>
                <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-green-500/20 text-green-400">avg score {avgScore}</span>
              </button>

              {/* Group table */}
              {!isCollapsed && (
                <div className="overflow-x-auto">
                  <table className="w-full text-xs border-collapse">
                    <thead>
                      <tr className="bg-gray-950/60">
                        {['Component', 'Reference', 'Description', 'Tier', 'Price (€)', 'Stock', 'DC / ETA'].map((h) => (
                          <th key={h} className="px-3 py-1.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-600 border-b border-gray-800/50 whitespace-nowrap">
                            {h}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {group.rows.map((m) => {
                        const currentRef = overrides[m.component_idx] ?? m.selected_reference ?? '';
                        const refs       = m.suggested_references ?? [];
                        const detail: ReferenceCandidate | undefined = refs.find((r) => r.reference === currentRef) ?? refs[0];
                        const isChanged    = overrides[m.component_idx] !== undefined && overrides[m.component_idx] !== m.selected_reference;
                        const isOverridden = m.user_overridden && !isChanged;

                        return (
                          <tr key={m.component_idx}
                            className={`border-b border-gray-800/30 hover:bg-gray-800/20 transition-colors ${isChanged ? 'bg-blue-500/5' : ''}`}>
                            <td className="px-3 py-1.5 text-gray-300 max-w-[180px]">
                              <span className="line-clamp-2 text-[11px]">{m.component_summary}</span>
                              {isOverridden && <span className="text-[9px] text-blue-400/70 block mt-0.5">overridden</span>}
                            </td>
                            <td className="px-3 py-1.5">
                              {refs.length > 1 ? (
                                <select value={currentRef}
                                  onChange={(e) => handleOverrideChange(m.component_idx, e.target.value)}
                                  className="bg-gray-800 border border-gray-700 text-gray-200 text-xs rounded px-2 py-0.5 w-full max-w-[150px] focus:outline-none focus:border-blue-500">
                                  {refs.map((r) => (
                                    <option key={r.reference} value={r.reference}>{r.reference} ({r.score})</option>
                                  ))}
                                  <option value="">— None —</option>
                                </select>
                              ) : (
                                <span className="font-mono text-green-400/80 text-[11px]">{currentRef || '—'}</span>
                              )}
                            </td>
                            <td className="px-3 py-1.5 text-gray-400 max-w-[200px]">
                              <span className="line-clamp-2 text-[11px]">{detail?.product_description ?? '—'}</span>
                            </td>
                            <td className="px-3 py-1.5"><TierBadge tier={detail?.tier ?? null} /></td>
                            <td className="px-3 py-1.5 text-gray-300 text-right whitespace-nowrap">
                              {detail?.list_price_eur != null ? detail.list_price_eur.toFixed(2) : '—'}
                            </td>
                            <td className="px-3 py-1.5">
                              <StockBadge status={detail?.stock_status ?? null} />
                              {detail?.qty_available != null && (
                                <span className="text-[9px] text-gray-600 block mt-0.5">{detail.qty_available} units</span>
                              )}
                            </td>
                            <td className="px-3 py-1.5 text-gray-500 whitespace-nowrap">
                              {detail?.distribution_center && <span className="text-gray-400">{detail.distribution_center}</span>}
                              {detail?.expected_date && <span className="text-[9px] text-yellow-400/70 block mt-0.5">ETA {detail.expected_date}</span>}
                              {!detail?.distribution_center && !detail?.expected_date && '—'}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
