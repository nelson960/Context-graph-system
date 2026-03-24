from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3

from fastapi.testclient import TestClient

from context_graph.catalog_service import CatalogService
from context_graph.exceptions import ConfigurationError
from context_graph.main import create_app
from context_graph.schemas import ChatQueryResponse
from context_graph.sql_guard import SqlValidator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("CONTEXT_GRAPH_PROJECT_ROOT", str(PROJECT_ROOT))


def test_health_endpoint() -> None:
    client = TestClient(create_app())
    response = client.get("/api/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"


def test_entity_search_finds_billing_document() -> None:
    client = TestClient(create_app())
    response = client.get("/api/entities/search", params={"q": "90504219"})
    assert response.status_code == 200
    payload = response.json()
    assert any(item["node_id"] == "billing_document:90504219" for item in payload)


def test_graph_path_returns_billing_trace() -> None:
    client = TestClient(create_app())
    response = client.get(
        "/api/graph/path",
        params={"node_id": "billing_document:90504219", "direction": "both", "depth": 6},
    )
    assert response.status_code == 200
    payload = response.json()
    node_ids = {node["id"] for node in payload["nodes"]}
    assert "billing_document:90504219" in node_ids
    assert "delivery:80738051" in node_ids
    assert "sales_order:740520" in node_ids


def test_graph_query_supports_combined_subgraphs() -> None:
    client = TestClient(create_app())
    response = client.post(
        "/api/graph/query",
        json={
            "mode": "combined_subgraph",
            "node_ids": ["billing_document:90504219", "sales_order:740520"],
            "depth": 1,
            "include_hidden": False,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    node_ids = {node["id"] for node in payload["nodes"]}
    assert "billing_document:90504219" in node_ids
    assert "sales_order:740520" in node_ids


def test_graph_subgraph_supports_cluster_mode() -> None:
    client = TestClient(create_app())
    response = client.get(
        "/api/graph/subgraph",
        params={"node_id": "plant:AS05", "depth": 2, "cluster_mode": "type"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["cluster_mode"] == "type"
    assert any(node["metadata"].get("is_cluster") for node in payload["nodes"])


def test_chat_stream_endpoint_returns_ndjson_events() -> None:
    app = create_app()

    class QueryServiceStub:
        def stream_chat_request(self, payload):
            assert payload.message == "explain as05"
            final = ChatQueryResponse(
                conversation_id="conv-1",
                answer="Plant AS05",
                intent="entity_lookup",
                route="graph",
                query_plan={"intent": "entity_lookup", "route": "graph"},
                graph_center_node_id="plant:AS05",
            )
            yield {"type": "conversation", "conversation_id": "conv-1"}
            yield {"type": "status", "stage": "planning", "message": "Planning query"}
            yield {"type": "answer_delta", "delta": "Plant AS05"}
            yield {"type": "final", "data": final.model_dump()}

    app.state.runtime.query_service = QueryServiceStub()
    client = TestClient(app)
    response = client.post(
        "/api/chat/query/stream",
        json={"message": "explain as05", "selectedNodeIds": []},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    events = [json.loads(line) for line in response.text.splitlines() if line.strip()]
    assert events[0] == {"type": "conversation", "conversation_id": "conv-1"}
    assert events[1]["type"] == "status"
    assert events[2] == {"type": "answer_delta", "delta": "Plant AS05"}
    assert events[-1]["type"] == "final"
    assert events[-1]["data"]["graph_center_node_id"] == "plant:AS05"


def test_chat_query_endpoint_returns_http_error_on_service_failure() -> None:
    app = create_app()

    class QueryServiceStub:
        def handle_chat_request(self, payload):
            raise ConfigurationError("MODEL_API_KEY or OPENAI_API_KEY is required")

    app.state.runtime.query_service = QueryServiceStub()
    client = TestClient(app)
    response = client.post(
        "/api/chat/query",
        json={"message": "which products have the most billing documents?", "selectedNodeIds": []},
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "MODEL_API_KEY or OPENAI_API_KEY is required"


def test_sql_validator_rejects_non_approved_tables() -> None:
    db_path = PROJECT_ROOT / "artifacts" / "sqlite" / "context_graph.db"
    semantic_catalog = PROJECT_ROOT / "artifacts" / "reports" / "semantic_catalog.json"
    validator = SqlValidator(
        catalog_service=CatalogService(db_path=db_path, semantic_catalog_path=semantic_catalog),
        max_rows=200,
    )
    try:
        validator.validate("SELECT so.salesOrder FROM sales_orders AS so LIMIT 10")
    except Exception as exc:
        assert "not approved" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected raw-table SQL to be rejected")


def test_delivery_flow_view_preserves_one_row_per_delivery_item() -> None:
    db_path = PROJECT_ROOT / "artifacts" / "sqlite" / "context_graph.db"
    with sqlite3.connect(db_path) as connection:
        row_count = connection.execute("SELECT COUNT(*) FROM v_delivery_flow").fetchone()[0]
        distinct_item_count = connection.execute(
            """
            SELECT COUNT(DISTINCT delivery_document || ':' || delivery_document_item)
            FROM v_delivery_flow
            """
        ).fetchone()[0]
    assert row_count == distinct_item_count
