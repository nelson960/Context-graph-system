from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


IntentType = Literal[
    "aggregate_analytics",
    "document_trace",
    "anomaly_detection",
    "entity_lookup",
    "relationship_exploration",
]
RouteType = Literal["sql", "graph", "hybrid"]
TraceDirection = Literal["upstream", "downstream", "both"]
ClusterMode = Literal["type"]


class NodeDTO(BaseModel):
    id: str
    type: str
    business_key: str
    display_label: str
    subtitle: str | None = None
    status: str | None = None
    amount: float | int | str | None = None
    currency: str | None = None
    document_date: str | None = None
    source_tables: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    default_visible: bool
    inbound_edge_count: int | None = None
    outbound_edge_count: int | None = None


class EdgeDTO(BaseModel):
    id: str
    type: str
    source: str
    target: str
    link_status: str
    derivation_rule: str
    provenance_columns: list[str] = Field(default_factory=list)
    quantity: float | int | str | None = None
    date: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class GraphResponse(BaseModel):
    center_node_id: str
    depth: int
    nodes: list[NodeDTO]
    edges: list[EdgeDTO]
    cluster_mode: ClusterMode | None = None


class EntitySearchResult(BaseModel):
    node_id: str
    node_type: str
    business_key: str
    display_label: str
    subtitle: str | None = None
    score: int


class EntityDetailResponse(BaseModel):
    node: NodeDTO


class CitationNode(BaseModel):
    id: str
    type: str
    business_key: str
    display_label: str


class CitationEdge(BaseModel):
    id: str
    type: str
    source: str
    target: str


class QueryPlanEntity(BaseModel):
    reference: str
    entity_type: str | None = None
    resolved_node_id: str | None = None
    resolved_business_key: str | None = None


class QueryPlan(BaseModel):
    intent: IntentType
    route: RouteType
    entities: list[QueryPlanEntity] = Field(default_factory=list)
    grain: str | None = None
    metrics: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    time_window: str | None = None
    sort: list[str] = Field(default_factory=list)
    limit: int | None = None
    trace_start: str | None = None
    trace_direction: TraceDirection | None = None
    assumptions: list[str] = Field(default_factory=list)
    output_shape: str | None = None


class PlannerEnvelope(BaseModel):
    status: Literal["ok", "out_of_domain"]
    refusal_message: str | None = None
    query_plan: QueryPlan | None = None


class SqlEnvelope(BaseModel):
    sql: str
    provenance_note: str
    assumptions: list[str] = Field(default_factory=list)


class AnswerEnvelope(BaseModel):
    answer: str
    provenance_note: str
    assumptions: list[str] = Field(default_factory=list)


class ChatQueryRequest(BaseModel):
    message: str
    selectedNodeIds: list[str] = Field(default_factory=list)
    conversationId: str | None = None
    clusterMode: ClusterMode | None = None


class ConversationMemoryState(BaseModel):
    selected_node_ids: list[str] = Field(default_factory=list)
    resolved_entities: list[QueryPlanEntity] = Field(default_factory=list)
    highlighted_node_ids: list[str] = Field(default_factory=list)
    graph_center_node_id: str | None = None
    active_filters: list[str] = Field(default_factory=list)
    last_intent: str | None = None
    last_route: str | None = None


class ChatQueryResponse(BaseModel):
    conversation_id: str | None = None
    answer: str | None = None
    assumptions: list[str] = Field(default_factory=list)
    intent: str | None = None
    route: str | None = None
    query_plan: dict[str, Any] | None = None
    sql: str | None = None
    row_count: int = 0
    rows: list[dict[str, Any]] = Field(default_factory=list)
    highlighted_node_ids: list[str] = Field(default_factory=list)
    highlighted_edge_ids: list[str] = Field(default_factory=list)
    cited_nodes: list[CitationNode] = Field(default_factory=list)
    cited_edges: list[CitationEdge] = Field(default_factory=list)
    graph_center_node_id: str | None = None
    provenance_note: str | None = None
    memory_state: ConversationMemoryState | None = None
    error: str | None = None
