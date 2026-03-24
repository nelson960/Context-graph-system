from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from context_graph.config import ALLOWED_METRICS, APPROVED_VIEWS
from context_graph.sqlite_utils import connect_readonly_sqlite


class CatalogService:
    def __init__(self, db_path: Path, semantic_catalog_path: Path) -> None:
        self._db_path = db_path
        self._semantic_catalog_path = semantic_catalog_path
        self._catalog = self._load_catalog()
        self._view_columns = self._discover_view_columns()

    @property
    def glossary(self) -> dict[str, str]:
        return dict(self._catalog["glossary"])

    @property
    def approved_views(self) -> dict[str, str]:
        return dict(self._catalog["approved_views"])

    @property
    def allowed_metrics(self) -> tuple[str, ...]:
        return ALLOWED_METRICS

    @property
    def view_columns(self) -> dict[str, set[str]]:
        return {name: set(columns) for name, columns in self._view_columns.items()}

    def compact_prompt_context(self) -> str:
        lines: list[str] = []
        lines.append("Business glossary:")
        for source_term, normalized_term in sorted(self.glossary.items()):
            lines.append(f"- {source_term} -> {normalized_term}")
        lines.append("")
        lines.append("Approved analytical views:")
        for view_name, description in sorted(self.approved_views.items()):
            columns = ", ".join(sorted(self._view_columns.get(view_name, set())))
            lines.append(f"- {view_name}: {description}")
            lines.append(f"  columns: {columns}")
        lines.append("")
        lines.append("Allowed metrics:")
        lines.append(", ".join(self.allowed_metrics))
        lines.append("")
        lines.append("Query rules:")
        lines.append("- Stay inside the order-to-cash dataset.")
        lines.append("- Use only approved views.")
        lines.append("- Fully qualify all column references.")
        lines.append("- Avoid SELECT * and avoid unknown columns.")
        lines.append("- Respect item-vs-header grain explicitly.")
        return "\n".join(lines)

    def is_allowed_view(self, view_name: str) -> bool:
        return view_name in self._view_columns and view_name in APPROVED_VIEWS

    def allowed_columns_for_view(self, view_name: str) -> set[str]:
        return set(self._view_columns.get(view_name, set()))

    def _load_catalog(self) -> dict[str, Any]:
        if not self._semantic_catalog_path.exists():
            raise FileNotFoundError(
                f"Semantic catalog not found at {self._semantic_catalog_path}"
            )
        return json.loads(self._semantic_catalog_path.read_text(encoding="utf-8"))

    def _discover_view_columns(self) -> dict[str, set[str]]:
        columns: dict[str, set[str]] = {}
        with connect_readonly_sqlite(self._db_path) as connection:
            for view_name in APPROVED_VIEWS:
                cursor = connection.execute(f"SELECT * FROM {view_name} LIMIT 0")
                column_names = {description[0] for description in cursor.description or []}
                columns[view_name] = column_names
        return columns
