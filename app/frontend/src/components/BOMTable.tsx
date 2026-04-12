import { useState, useMemo } from 'react';
import { BOMComponent } from '../types';

interface BOMTableProps {
  components: BOMComponent[];
  threshold_met: boolean | null;
  attempts_made: number | null;
}

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

const DISPLAY_COLUMNS: { key: keyof BOMComponent; label: string }[] = [
  { key: 'amperage_a',     label: 'Rating (A)' },
  { key: 'poles',          label: 'Poles' },
  { key: 'curve',          label: 'Curve' },
  { key: 'breaking_ka',    label: 'Breaking (kA)' },
  { key: 'voltage_v',      label: 'Voltage (V)' },
  { key: 'sensitivity_ma', label: 'Sensitivity (mA)' },
  { key: 'rcd_type',       label: 'RCD Type' },
  { key: 'rcd_block_type', label: 'Block' },
  { key: 'timer_function', label: 'Timer Fn' },
  { key: 'max_current_ka', label: 'I max (kA)' },
  { key: 'panel',          label: 'Panel' },
  { key: 'circuit',        label: 'Circuit' },
];

function getVisibleColumns(components: BOMComponent[]): { key: keyof BOMComponent; label: string }[] {
  return DISPLAY_COLUMNS.filter(({ key }) =>
    components.some((c) => c[key] != null && c[key] !== '')
  );
}

function MatchBadge({ score }: { score: number | null | undefined }) {
  if (score == null) return null;
  const color = score >= 10 ? 'bg-green-500/20 text-green-300' :
                score >= 5  ? 'bg-yellow-500/20 text-yellow-300' :
                              'bg-gray-700 text-gray-400';
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium ${color}`}>
      {score}
    </span>
  );
}

function unique(values: (string | number | null | undefined)[]): string[] {
  return Array.from(new Set(values.map((v) => String(v ?? '')))).filter(Boolean).sort();
}

interface ComponentGroup {
  type: string;
  label: string;
  matched: BOMComponent[];
  unmatched: BOMComponent[];
}

function buildGroups(components: BOMComponent[]): ComponentGroup[] {
  const map = new Map<string, { matched: BOMComponent[]; unmatched: BOMComponent[] }>();
  for (const c of components) {
    const t = c.component_type || 'unknown';
    if (!map.has(t)) map.set(t, { matched: [], unmatched: [] });
    const bucket = map.get(t)!;
    if (c.precise_cx != null) bucket.matched.push(c);
    else bucket.unmatched.push(c);
  }
  return Array.from(map.entries())
    .map(([type, { matched, unmatched }]) => ({
      type,
      label: TYPE_LABELS[type] ?? type.replace(/_/g, ' '),
      matched,
      unmatched,
    }))
    .sort((a, b) => (b.matched.length + b.unmatched.length) - (a.matched.length + a.unmatched.length));
}

export default function BOMTable({ components, threshold_met, attempts_made }: BOMTableProps) {
  const [filterType, setFilterType]       = useState('');
  const [filterCircuit, setFilterCircuit] = useState('');
  const [filterCalib, setFilterCalib]     = useState('');
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());

  const typeOptions    = useMemo(() => unique(components.map((c) => c.component_type)), [components]);
  const circuitOptions = useMemo(() => unique(components.map((c) => c.circuit)),        [components]);
  const calibOptions   = useMemo(() => unique(components.map((c) => c.amperage_a)),     [components]);

  const filtered = useMemo(() => components.filter((c) => {
    if (filterType    && String(c.component_type ?? '') !== filterType)    return false;
    if (filterCircuit && String(c.circuit        ?? '') !== filterCircuit) return false;
    if (filterCalib   && String(c.amperage_a     ?? '') !== filterCalib)   return false;
    return true;
  }), [components, filterType, filterCircuit, filterCalib]);

  const groups = useMemo(() => buildGroups(filtered), [filtered]);
  const hasFilter = filterType || filterCircuit || filterCalib;

  const toggleGroup = (type: string) => {
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(type)) next.delete(type);
      else next.add(type);
      return next;
    });
  };

  const allCollapsed = groups.every((g) => collapsedGroups.has(g.type));
  const toggleAll = () => {
    if (allCollapsed) setCollapsedGroups(new Set());
    else setCollapsedGroups(new Set(groups.map((g) => g.type)));
  };

  return (
    <div className="flex flex-col gap-3 p-4">
      {/* Warnings */}
      {threshold_met === false && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-yellow-500/10 border border-yellow-500/20 text-xs text-yellow-300">
          <svg className="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
          </svg>
          Match rate below threshold — review unmatched components below
        </div>
      )}
      {attempts_made != null && attempts_made > 1 && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-gray-800/60 border border-gray-700/50 text-xs text-gray-400">
          <svg className="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          Matched after {attempts_made} attempt{attempts_made > 1 ? 's' : ''} (retry)
        </div>
      )}

      {/* Filters + collapse-all */}
      <div className="flex flex-wrap items-center gap-2">
        <select value={filterType} onChange={(e) => setFilterType(e.target.value)}
          className="text-xs bg-gray-900 border border-gray-700/50 rounded-lg px-2 py-1.5 text-gray-300 focus:outline-none focus:border-gray-500">
          <option value="">All types</option>
          {typeOptions.map((v) => <option key={v} value={v}>{TYPE_LABELS[v] ?? v}</option>)}
        </select>

        <select value={filterCircuit} onChange={(e) => setFilterCircuit(e.target.value)}
          className="text-xs bg-gray-900 border border-gray-700/50 rounded-lg px-2 py-1.5 text-gray-300 focus:outline-none focus:border-gray-500">
          <option value="">All circuits</option>
          {circuitOptions.map((v) => <option key={v} value={v}>{v}</option>)}
        </select>

        <select value={filterCalib} onChange={(e) => setFilterCalib(e.target.value)}
          className="text-xs bg-gray-900 border border-gray-700/50 rounded-lg px-2 py-1.5 text-gray-300 focus:outline-none focus:border-gray-500">
          <option value="">All ratings</option>
          {calibOptions.map((v) => <option key={v} value={v}>{v} A</option>)}
        </select>

        {hasFilter && (
          <button onClick={() => { setFilterType(''); setFilterCircuit(''); setFilterCalib(''); }}
            className="text-[10px] px-2 py-1 rounded-md text-gray-500 hover:text-gray-300 hover:bg-gray-800 border border-gray-700/50 transition-colors">
            Clear
          </button>
        )}

        <div className="ml-auto flex items-center gap-2">
          <span className="text-[10px] text-gray-600">{filtered.length} / {components.length} shown</span>
          <button onClick={toggleAll}
            className="text-[10px] px-2 py-1 rounded-md text-gray-500 hover:text-gray-300 hover:bg-gray-800 border border-gray-700/50 transition-colors">
            {allCollapsed ? 'Expand all' : 'Collapse all'}
          </button>
        </div>
      </div>

      {/* Groups */}
      {groups.map((group) => {
        const isCollapsed = collapsedGroups.has(group.type);
        const total = group.matched.length + group.unmatched.length;
        const matchPct = total > 0 ? Math.round(group.matched.length / total * 100) : 0;
        const groupCols = getVisibleColumns([...group.matched, ...group.unmatched]);

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
              <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-gray-700/60 text-gray-400">{total}</span>
              <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${
                matchPct === 100 ? 'bg-green-500/20 text-green-400' :
                matchPct >= 50   ? 'bg-yellow-500/20 text-yellow-400' :
                                   'bg-red-500/20 text-red-400'
              }`}>
                {matchPct}% matched
              </span>
              {group.unmatched.length > 0 && (
                <span className="text-[10px] text-yellow-500/70 ml-1">{group.unmatched.length} unmatched</span>
              )}
            </button>

            {/* Group rows */}
            {!isCollapsed && (
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="bg-gray-950/60">
                      {groupCols.map(({ key, label }) => (
                        <th key={key} className="px-3 py-1.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-600 whitespace-nowrap border-b border-gray-800/50">
                          {label}
                        </th>
                      ))}
                      <th className="px-3 py-1.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-600 border-b border-gray-800/50">Score</th>
                    </tr>
                  </thead>
                  <tbody>
                    {group.matched.map((comp, i) => (
                      <tr key={`m-${i}`} className="border-b border-gray-800/30 hover:bg-gray-800/20 transition-colors">
                        {groupCols.map(({ key }) => (
                          <td key={key} className="px-3 py-1.5 text-gray-300 whitespace-nowrap">
                            {comp[key] != null ? String(comp[key]) : <span className="text-gray-700">—</span>}
                          </td>
                        ))}
                        <td className="px-3 py-1.5"><MatchBadge score={comp.match_score} /></td>
                      </tr>
                    ))}
                    {group.unmatched.map((comp, i) => (
                      <tr key={`u-${i}`} className="border-b border-gray-800/30 bg-yellow-500/5">
                        {groupCols.map(({ key }) => (
                          <td key={key} className="px-3 py-1.5 text-gray-500 whitespace-nowrap">
                            {comp[key] != null ? String(comp[key]) : <span className="text-gray-700">—</span>}
                          </td>
                        ))}
                        <td className="px-3 py-1.5 text-[9px] text-yellow-600">no bbox</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        );
      })}

      {filtered.length === 0 && components.length > 0 && (
        <div className="py-6 text-center text-sm text-gray-600">No components match the current filters</div>
      )}
      {components.length === 0 && (
        <div className="py-10 text-center text-sm text-gray-600">No components extracted</div>
      )}
    </div>
  );
}
