from __future__ import annotations

from pathlib import Path

from context_graph.catalog_service import CatalogService
from context_graph.conversation_store import ConversationStore
from context_graph.evidence_service import EvidenceService
from context_graph.entity_service import EntityService
from context_graph.graph_service import GraphService
from context_graph.observability import QueryLogger
from context_graph.plan_validator import QueryPlanValidator
from context_graph.pipeline import build_context_graph_artifacts
from context_graph.planner import OpenAIPlanner
from context_graph.query_service import QueryService
from context_graph.settings import AppSettings
from context_graph.sql_guard import SqlExecutor, SqlValidator


class AppRuntime:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.catalog_service: CatalogService | None = None
        self.entity_service: EntityService | None = None
        self.graph_service: GraphService | None = None
        self.query_service: QueryService | None = None

    def startup(self) -> None:
        self.settings.ensure_runtime_dirs()
        self._ensure_artifacts()
        self.catalog_service = CatalogService(
            db_path=self.settings.db_path,
            semantic_catalog_path=self.settings.artifacts_root / "reports" / "semantic_catalog.json",
        )
        self.entity_service = EntityService(
            db_path=self.settings.db_path,
            glossary=self.catalog_service.glossary,
        )
        self.graph_service = GraphService(
            db_path=self.settings.db_path,
            max_nodes=self.settings.max_graph_nodes,
            max_edges=self.settings.max_graph_edges,
        )
        planner = OpenAIPlanner(settings=self.settings, catalog_service=self.catalog_service)
        sql_validator = SqlValidator(
            catalog_service=self.catalog_service,
            max_rows=self.settings.max_query_rows,
        )
        sql_executor = SqlExecutor(
            db_path=self.settings.db_path,
            timeout_ms=self.settings.query_timeout_ms,
        )
        query_logger = QueryLogger(self.settings.query_log_path)
        conversation_store = ConversationStore(self.settings.db_path)
        evidence_service = EvidenceService(self.graph_service)
        plan_validator = QueryPlanValidator()
        self.query_service = QueryService(
            entity_service=self.entity_service,
            graph_service=self.graph_service,
            planner=planner,
            sql_validator=sql_validator,
            sql_executor=sql_executor,
            query_logger=query_logger,
            conversation_store=conversation_store,
            plan_validator=plan_validator,
            evidence_service=evidence_service,
        )

    def _ensure_artifacts(self) -> None:
        semantic_catalog_path = self.settings.artifacts_root / "reports" / "semantic_catalog.json"
        required_paths = [
            self.settings.db_path,
            semantic_catalog_path,
        ]
        if all(path.exists() for path in required_paths):
            return
        build_context_graph_artifacts(
            dataset_root=self.settings.dataset_root,
            output_root=self.settings.artifacts_root,
        )


def build_runtime() -> AppRuntime:
    runtime = AppRuntime(AppSettings.from_env())
    runtime.startup()
    return runtime
