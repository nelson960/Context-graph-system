from __future__ import annotations

import shutil
from pathlib import Path

from context_graph.catalog_service import CatalogService
from context_graph.conversation_store import ConversationStore
from context_graph.entity_service import EntityService
from context_graph.evidence_service import EvidenceService
from context_graph.graph_service import GraphService
from context_graph.observability import QueryLogger
from context_graph.plan_validator import QueryPlanValidator
from context_graph.query_service import QueryService
from context_graph.schemas import (
    AnswerEnvelope,
    ChatQueryRequest,
    PlannerEnvelope,
    QueryPlan,
    QueryPlanEntity,
)
from context_graph.sql_guard import SqlExecutor, SqlValidator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "artifacts" / "sqlite" / "context_graph.db"
SEMANTIC_CATALOG_PATH = PROJECT_ROOT / "artifacts" / "reports" / "semantic_catalog.json"


class GraphPlannerStub:
    def plan(self, message, selected_nodes, memory_context=None):
        return PlannerEnvelope(
            status="ok",
            query_plan=QueryPlan(
                intent="entity_lookup",
                route="graph",
                entities=[QueryPlanEntity(reference="AS05", entity_type="Plant")],
                assumptions=[],
                output_shape="summary",
            ),
        )

    def generate_sql(self, user_message, query_plan):  # pragma: no cover - must not be called
        raise AssertionError("generate_sql should not be called for graph routes")

    def compose_answer(self, user_message, query_plan, sql, rows, row_count):  # pragma: no cover
        raise AssertionError("compose_answer should not be called for graph routes")

    def compose_graph_answer(self, user_message, query_plan, center_node, graph_response):
        assert center_node is not None
        return AnswerEnvelope(
            answer=f"{center_node.type} {center_node.business_key}",
            provenance_note="graph summary",
            assumptions=[],
        )

    def stream_sql_answer(self, user_message, query_plan, sql, rows, row_count):  # pragma: no cover
        raise AssertionError("stream_sql_answer should not be called for graph routes")

    def stream_graph_answer(self, user_message, query_plan, center_node, graph_response):
        assert center_node is not None
        yield center_node.type
        yield " "
        yield center_node.business_key


class MemoryPlannerStub(GraphPlannerStub):
    def __init__(self) -> None:
        self._call_count = 0

    def plan(self, message, selected_nodes, memory_context=None):
        self._call_count += 1
        if self._call_count == 1:
            return super().plan(message, selected_nodes, memory_context=memory_context)
        return PlannerEnvelope(
            status="ok",
            query_plan=QueryPlan(
                intent="relationship_exploration",
                route="graph",
                entities=[],
                assumptions=[],
                output_shape="summary",
            ),
        )


class NoPlanLookupStub(GraphPlannerStub):
    def plan(self, message, selected_nodes, memory_context=None):  # pragma: no cover - should not run
        raise AssertionError("plan should not be called for direct entity lookup prompts")


def build_query_service(tmp_path: Path, planner=None) -> QueryService:
    test_db_path = tmp_path / "context_graph.test.db"
    shutil.copyfile(DB_PATH, test_db_path)
    catalog_service = CatalogService(
        db_path=test_db_path,
        semantic_catalog_path=SEMANTIC_CATALOG_PATH,
    )
    entity_service = EntityService(
        db_path=test_db_path,
        glossary=catalog_service.glossary,
    )
    graph_service = GraphService(
        db_path=test_db_path,
        max_nodes=40,
        max_edges=80,
    )
    return QueryService(
        entity_service=entity_service,
        graph_service=graph_service,
        planner=planner or GraphPlannerStub(),
        sql_validator=SqlValidator(catalog_service=catalog_service, max_rows=200),
        sql_executor=SqlExecutor(db_path=test_db_path, timeout_ms=5000),
        query_logger=QueryLogger(tmp_path / "query_events.jsonl"),
        conversation_store=ConversationStore(test_db_path),
        plan_validator=QueryPlanValidator(),
        evidence_service=EvidenceService(graph_service),
    )


def test_entity_search_supports_glossary_augmented_queries() -> None:
    catalog_service = CatalogService(
        db_path=DB_PATH,
        semantic_catalog_path=SEMANTIC_CATALOG_PATH,
    )
    entity_service = EntityService(
        db_path=DB_PATH,
        glossary=catalog_service.glossary,
    )
    results = entity_service.search("invoice 90504219", limit=5)
    assert any(result.node_id == "billing_document:90504219" for result in results)


def test_graph_service_supports_type_clustering() -> None:
    graph_service = GraphService(
        db_path=DB_PATH,
        max_nodes=40,
        max_edges=80,
    )
    graph = graph_service.get_subgraph("plant:AS05", depth=2, cluster_mode="type")
    assert graph.cluster_mode == "type"
    assert any(node.metadata.get("is_cluster") for node in graph.nodes)


def test_query_service_executes_graph_route_without_sql(tmp_path) -> None:
    query_service = build_query_service(tmp_path)
    response = query_service.handle_chat_request(
        ChatQueryRequest(message="explain as05")
    )
    assert response.error is None
    assert response.route == "graph"
    assert response.sql is None
    assert response.graph_center_node_id == "plant:AS05"
    assert any(node.id == "plant:AS05" for node in response.cited_nodes)
    assert response.conversation_id is not None
    assert response.memory_state is not None
    assert response.memory_state.last_route == "graph"


def test_query_service_short_circuits_clear_entity_lookup_before_planner(tmp_path) -> None:
    query_service = build_query_service(tmp_path, planner=NoPlanLookupStub())
    response = query_service.handle_chat_request(ChatQueryRequest(message="explain AS05"))

    assert response.error is None
    assert response.route == "graph"
    assert response.intent == "entity_lookup"
    assert response.graph_center_node_id == "plant:AS05"
    assert response.assumptions
    assert "entity lookup" in response.assumptions[0].lower()


def test_query_service_stream_chat_request_emits_answer_deltas_and_final(tmp_path) -> None:
    query_service = build_query_service(tmp_path)
    events = list(query_service.stream_chat_request(ChatQueryRequest(message="explain as05")))
    event_types = [event["type"] for event in events]
    assert event_types[0] == "conversation"
    assert "answer_delta" in event_types
    assert event_types[-1] == "final"
    final_payload = events[-1]["data"]
    assert final_payload["route"] == "graph"
    assert final_payload["answer"] == "Plant AS05"
    assert final_payload["graph_center_node_id"] == "plant:AS05"


def test_query_service_reuses_conversation_entities_when_follow_up_is_implicit(tmp_path) -> None:
    query_service = build_query_service(tmp_path, planner=MemoryPlannerStub())
    first_response = query_service.handle_chat_request(ChatQueryRequest(message="explain as05"))
    assert first_response.conversation_id is not None

    second_response = query_service.handle_chat_request(
        ChatQueryRequest(
            message="show connected nodes",
            conversationId=first_response.conversation_id,
        )
    )

    assert second_response.error is None
    assert second_response.route == "graph"
    assert second_response.graph_center_node_id == "plant:AS05"
    assert second_response.memory_state is not None
    assert second_response.memory_state.last_route == "graph"
    assert any(
        entity.resolved_node_id == "plant:AS05"
        for entity in second_response.memory_state.resolved_entities
    )
