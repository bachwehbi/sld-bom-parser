export interface BOMComponent {
  component_type: string;
  amperage_a: number | null;
  poles: number | null;
  curve: string | null;
  breaking_ka: number | null;
  sensitivity_ma: number | null;
  rcd_type: string | null;
  rcd_selectivity: string | null;
  rcd_sensitivity_class: string | null;
  rcd_block_type: string | null;
  timer_function: string | null;
  max_current_ka: number | null;
  voltage_v: number | null;
  panel: string | null;
  circuit: string | null;
  precise_cx?: number | null;
  precise_cy?: number | null;
  precise_x0?: number | null;
  precise_y0?: number | null;
  precise_x1?: number | null;
  precise_y1?: number | null;
  match_score?: number | null;
  bbox_page_w?: number | null;
  bbox_page_h?: number | null;
}

export interface Diagram {
  file_name: string;
  file_path: string | null;
  status: 'SUCCESS' | 'IN_PROGRESS' | 'ERROR' | 'UNPROCESSED';
  progress_msg: string | null;
  processed_at: string | null;
  attempts_made: number | null;
  threshold_met: boolean | null;
  error_message: string | null;
  pdf_type: 'vector' | 'scanned' | 'unrecognized' | null;
  component_count: number;
  matched_count: number;
  match_pct: number;
  components: BOMComponent[];
}

export interface ReferenceCandidate {
  reference: string;
  product_description: string | null;
  product_long_description: string | null;
  range: string | null;
  tier: string | null;
  status: string | null;
  superseded_by: string | null;
  list_price_eur: number | null;
  score: number;
  stock_status: 'IN_STOCK' | 'LOW_STOCK' | 'OUT_OF_STOCK' | null;
  qty_available: number | null;
  distribution_center: string | null;
  expected_date: string | null;
}

export interface MatchRow {
  component_idx: number;
  component_summary: string;
  suggested_references: ReferenceCandidate[];
  selected_reference: string | null;
  user_overridden: boolean;
  status: 'PENDING' | 'ACCEPTED' | 'OVERRIDDEN' | 'SKIPPED';
  updated_at: string | null;
}

export interface DiagramRef {
  file_name: string;
}

export interface Message {
  role: 'user' | 'assistant';
  content: string;
  file_refs?: DiagramRef[];
  isLoading?: boolean;
}

export interface Conversation {
  id: string;
  title: string;
  messages: Message[];
  updatedAt: number;
}
