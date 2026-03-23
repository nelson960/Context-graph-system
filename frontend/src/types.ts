export type NodeDto = {
  id: string;
  type: string;
  business_key: string;
  display_label: string;
  subtitle: string | null;
  status: string | null;
  amount: number | string | null;
  currency: string | null;
  document_date: string | null;
  source_tables: string[];
  metadata: Record<string, unknown>;
  default_visible: boolean;
  inbound_edge_count?: number | null;
  outbound_edge_count?: number | null;
};

export type EdgeDto = {
  id: string;
  type: string;
  source: string;
  target: string;
  link_status: string;
  derivation_rule: string;
  provenance_columns: string[];
  quantity: number | string | null;
  date: string | null;
  metadata: Record<string, unknown>;
};

export type GraphResponse = {
  center_node_id: string;
  depth: number;
  nodes: NodeDto[];
  edges: EdgeDto[];
  cluster_mode?: "type" | null;
};

export type EntitySearchResult = {
  node_id: string;
  node_type: string;
  business_key: string;
  display_label: string;
  subtitle: string | null;
  score: number;
};

export type EntityDetailResponse = {
  node: NodeDto;
};

export type ChatQueryRequest = {
  message: string;
  selectedNodeIds: string[];
  conversationId?: string | null;
  clusterMode?: "type" | null;
};

export type CitationNode = {
  id: string;
  type: string;
  business_key: string;
  display_label: string;
};

export type CitationEdge = {
  id: string;
  type: string;
  source: string;
  target: string;
};

export type ConversationMemoryState = {
  selected_node_ids: string[];
  resolved_entities: Array<Record<string, unknown>>;
  highlighted_node_ids: string[];
  graph_center_node_id: string | null;
  active_filters: string[];
  last_intent: string | null;
  last_route: string | null;
};

export type ChatQueryResponse = {
  conversation_id: string | null;
  answer: string | null;
  assumptions: string[];
  intent: string | null;
  route: string | null;
  query_plan: Record<string, unknown> | null;
  sql: string | null;
  row_count: number;
  rows: Array<Record<string, unknown>>;
  highlighted_node_ids: string[];
  highlighted_edge_ids: string[];
  cited_nodes: CitationNode[];
  cited_edges: CitationEdge[];
  graph_center_node_id: string | null;
  provenance_note: string | null;
  memory_state: ConversationMemoryState | null;
  error: string | null;
};

export type ChatStreamEvent =
  | { type: "conversation"; conversation_id: string }
  | { type: "status"; stage: string; message: string }
  | {
      type: "plan_ready";
      intent: string;
      route: string;
      query_plan: Record<string, unknown>;
    }
  | {
      type: "execution_ready";
      route: string;
      sql: string | null;
      row_count: number;
      graph_center_node_id: string | null;
    }
  | { type: "answer_delta"; delta: string }
  | { type: "final"; data: ChatQueryResponse };
