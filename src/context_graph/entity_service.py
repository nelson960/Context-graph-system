from __future__ import annotations

import sqlite3
from pathlib import Path

from context_graph.exceptions import AmbiguousEntityError, EntityResolutionError
from context_graph.schemas import EntitySearchResult
from context_graph.sqlite_utils import connect_readonly_sqlite, connect_writable_sqlite


ENTITY_TYPE_TO_PREFIX = {
    "SalesOrder": "sales_order:",
    "SalesOrderItem": "sales_order_item:",
    "ScheduleLine": "schedule_line:",
    "Delivery": "delivery:",
    "DeliveryItem": "delivery_item:",
    "BillingDocument": "billing_document:",
    "BillingItem": "billing_item:",
    "JournalEntry": "journal_entry:",
    "Payment": "payment:",
    "Customer": "customer:",
    "Address": "address:",
    "Product": "product:",
    "Plant": "plant:",
    "StorageLocation": "storage_location:",
    "CompanyCode": "company_code:",
    "SalesArea": "sales_area:",
}


class EntityService:
    def __init__(
        self,
        db_path: Path,
        glossary: dict[str, str] | None = None,
        read_only: bool = True,
    ) -> None:
        self._db_path = db_path
        self._read_only = read_only
        self._glossary = {
            key.strip().lower(): value.strip().lower()
            for key, value in (glossary or {}).items()
        }
        ensure_entity_search_index(self._db_path, read_only=self._read_only)

    def search(self, query: str, limit: int = 10, node_type: str | None = None) -> list[EntitySearchResult]:
        if not query.strip():
            return []
        needle = query.strip().lower()
        pattern = f"%{needle}%"
        prefix = ENTITY_TYPE_TO_PREFIX.get(node_type) if node_type else None
        fts_query = self._fts_query(self._expand_query_terms(needle))
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT *
                FROM (
                    SELECT
                        node_id,
                        node_type,
                        business_key,
                        display_label,
                        subtitle,
                        MAX(score) AS score
                    FROM (
                        SELECT
                            node_id,
                            node_type,
                            business_key,
                            display_label,
                            subtitle,
                            CASE
                                WHEN lower(business_key) = :needle THEN 500
                                WHEN lower(display_label) = :needle THEN 480
                                WHEN lower(COALESCE(subtitle, '')) = :needle THEN 460
                                WHEN lower(business_key) LIKE :pattern THEN 420
                                WHEN lower(display_label) LIKE :pattern THEN 360
                                WHEN lower(COALESCE(subtitle, '')) LIKE :pattern THEN 320
                                ELSE 0
                            END AS score
                        FROM graph_nodes
                        WHERE (
                            lower(business_key) = :needle
                            OR lower(display_label) = :needle
                            OR lower(COALESCE(subtitle, '')) = :needle
                            OR lower(business_key) LIKE :pattern
                            OR lower(display_label) LIKE :pattern
                            OR lower(COALESCE(subtitle, '')) LIKE :pattern
                        )
                        AND (:prefix IS NULL OR node_id LIKE :prefix || '%')
                        UNION ALL
                        SELECT
                            graph_nodes.node_id,
                            graph_nodes.node_type,
                            graph_nodes.business_key,
                            graph_nodes.display_label,
                            graph_nodes.subtitle,
                            CAST(250 - (bm25(entity_search_fts) * 10.0) AS INTEGER) AS score
                        FROM entity_search_fts
                        JOIN graph_nodes
                            ON graph_nodes.node_id = entity_search_fts.node_id
                        WHERE (:fts_query IS NOT NULL AND entity_search_fts MATCH :fts_query)
                        AND (:prefix IS NULL OR graph_nodes.node_id LIKE :prefix || '%')
                    )
                    GROUP BY node_id, node_type, business_key, display_label, subtitle
                )
                WHERE score > 0
                ORDER BY score DESC, node_type ASC, display_label ASC
                LIMIT :limit
                """,
                {
                    "needle": needle,
                    "pattern": pattern,
                    "limit": limit,
                    "prefix": prefix,
                    "fts_query": fts_query,
                },
            ).fetchall()
        return [
            EntitySearchResult(
                node_id=row["node_id"],
                node_type=row["node_type"],
                business_key=row["business_key"],
                display_label=row["display_label"],
                subtitle=row["subtitle"],
                score=int(row["score"]),
            )
            for row in rows
        ]

    def resolve(self, reference: str, node_type: str | None = None) -> EntitySearchResult:
        matches = self.search(reference, limit=5, node_type=node_type)
        if not matches:
            raise EntityResolutionError(
                f"No dataset entity matched '{reference}'"
                + (f" as {node_type}" if node_type else "")
            )
        if len(matches) == 1:
            return matches[0]
        top_score = matches[0].score
        tied = [match for match in matches if match.score == top_score]
        exact_matches = [
            match
            for match in tied
            if match.business_key.lower() == reference.strip().lower()
            or match.display_label.lower() == reference.strip().lower()
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]
        raise AmbiguousEntityError(
            f"Reference '{reference}' matched multiple entities: "
            + ", ".join(match.node_id for match in tied[:5])
        )

    def _expand_query_terms(self, query: str) -> list[str]:
        tokens = [token for token in query.replace(":", " ").split() if token]
        expanded: list[str] = []
        for token in tokens:
            expanded.append(token)
            mapped = self._glossary.get(token)
            if mapped and mapped not in expanded:
                expanded.append(mapped)
        normalized_query = " ".join(tokens)
        mapped_query = self._glossary.get(normalized_query)
        if mapped_query and mapped_query not in expanded:
            expanded.extend(mapped_query.split())
        return expanded or [query]

    def _fts_query(self, terms: list[str]) -> str | None:
        sanitized = []
        for term in terms:
            candidate = "".join(character for character in term if character.isalnum() or character in {"_", "-"})
            if candidate:
                sanitized.append(candidate)
        if not sanitized:
            return None
        return " OR ".join(f'"{term}"' for term in sanitized)

    def _connect(self):
        if self._read_only:
            return connect_readonly_sqlite(self._db_path)
        return connect_writable_sqlite(self._db_path)


def ensure_entity_search_index(db_path: Path, read_only: bool) -> None:
    connector = connect_readonly_sqlite if read_only else connect_writable_sqlite
    with connector(db_path) as connection:
        exists = connection.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE name = 'entity_search_fts'
            """
        ).fetchone()
        if read_only and exists is None:
            raise RuntimeError(
                "Read-only analytics database is missing entity_search_fts. "
                "Rebuild artifacts before deployment."
            )
        if not read_only:
            connection.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS entity_search_fts USING fts5(
                    node_id UNINDEXED,
                    node_type,
                    business_key,
                    display_label,
                    subtitle,
                    metadata_text,
                    tokenize = 'porter unicode61'
                )
                """
            )
        current_count = connection.execute(
            "SELECT COUNT(*) FROM entity_search_fts"
        ).fetchone()[0]
        graph_node_count = connection.execute(
            "SELECT COUNT(*) FROM graph_nodes"
        ).fetchone()[0]
        if read_only and current_count != graph_node_count:
            raise RuntimeError(
                "Read-only analytics database has an out-of-sync entity_search_fts index. "
                "Rebuild artifacts before deployment."
            )
        if not read_only and current_count != graph_node_count:
            connection.execute("DELETE FROM entity_search_fts")
            connection.execute(
                """
                INSERT INTO entity_search_fts (
                    node_id,
                    node_type,
                    business_key,
                    display_label,
                    subtitle,
                    metadata_text
                )
                SELECT
                    node_id,
                    node_type,
                    business_key,
                    display_label,
                    COALESCE(subtitle, ''),
                    COALESCE(metadata_json, '')
                FROM graph_nodes
                """
            )
        if not read_only:
            connection.commit()
