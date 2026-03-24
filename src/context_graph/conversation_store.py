from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from context_graph.schemas import ConversationMemoryState, QueryPlanEntity
from context_graph.sqlite_utils import connect_writable_sqlite


@dataclass(frozen=True)
class ConversationContext:
    conversation_id: str
    turns: list[dict[str, str]]
    state: ConversationMemoryState


class ConversationStore:
    def __init__(self, db_path: Path, recent_turn_limit: int = 6) -> None:
        self._db_path = db_path
        self._recent_turn_limit = recent_turn_limit
        self._ensure_schema()

    def ensure_conversation(self, conversation_id: str | None) -> str:
        resolved_id = conversation_id or str(uuid4())
        timestamp = self._utcnow()
        with connect_writable_sqlite(self._db_path) as connection:
            connection.execute(
                """
                INSERT INTO conversations (conversation_id, created_at, updated_at)
                VALUES (:conversation_id, :created_at, :updated_at)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    updated_at = excluded.updated_at
                """,
                {
                    "conversation_id": resolved_id,
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            )
            connection.commit()
        return resolved_id

    def load_context(self, conversation_id: str | None) -> ConversationContext | None:
        if not conversation_id:
            return None
        with connect_writable_sqlite(self._db_path) as connection:
            connection.row_factory = sqlite3.Row
            exists = connection.execute(
                "SELECT 1 FROM conversations WHERE conversation_id = ?",
                (conversation_id,),
            ).fetchone()
            if exists is None:
                return None
            turns = [
                {"role": row["role"], "content": row["content"]}
                for row in connection.execute(
                    """
                    SELECT role, content
                    FROM conversation_turns
                    WHERE conversation_id = ?
                    ORDER BY created_at DESC, rowid DESC
                    LIMIT ?
                    """,
                    (conversation_id, self._recent_turn_limit),
                ).fetchall()
            ]
            turns.reverse()
            state_row = connection.execute(
                """
                SELECT
                    selected_node_ids_json,
                    resolved_entities_json,
                    highlighted_node_ids_json,
                    graph_center_node_id,
                    active_filters_json,
                    last_intent,
                    last_route
                FROM conversation_state
                WHERE conversation_id = ?
                """,
                (conversation_id,),
            ).fetchone()
        if state_row is None:
            state = ConversationMemoryState()
        else:
            state = ConversationMemoryState(
                selected_node_ids=self._loads_json_list(state_row["selected_node_ids_json"]),
                resolved_entities=[
                    QueryPlanEntity.model_validate(item)
                    for item in self._loads_json_list(state_row["resolved_entities_json"])
                ],
                highlighted_node_ids=self._loads_json_list(state_row["highlighted_node_ids_json"]),
                graph_center_node_id=state_row["graph_center_node_id"],
                active_filters=self._loads_json_list(state_row["active_filters_json"]),
                last_intent=state_row["last_intent"],
                last_route=state_row["last_route"],
            )
        return ConversationContext(
            conversation_id=conversation_id,
            turns=turns,
            state=state,
        )

    def record_interaction(
        self,
        conversation_id: str,
        user_message: str,
        assistant_message: str,
        state: ConversationMemoryState,
        request_payload: dict,
        response_payload: dict,
    ) -> None:
        timestamp = self._utcnow()
        with connect_writable_sqlite(self._db_path) as connection:
            connection.execute(
                "UPDATE conversations SET updated_at = ? WHERE conversation_id = ?",
                (timestamp, conversation_id),
            )
            connection.execute(
                """
                INSERT INTO conversation_turns (
                    conversation_id,
                    role,
                    content,
                    payload_json,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    conversation_id,
                    "user",
                    user_message,
                    json.dumps(request_payload, sort_keys=True),
                    timestamp,
                ),
            )
            connection.execute(
                """
                INSERT INTO conversation_turns (
                    conversation_id,
                    role,
                    content,
                    payload_json,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    conversation_id,
                    "assistant",
                    assistant_message,
                    json.dumps(response_payload, sort_keys=True),
                    timestamp,
                ),
            )
            connection.execute(
                """
                INSERT INTO conversation_state (
                    conversation_id,
                    selected_node_ids_json,
                    resolved_entities_json,
                    highlighted_node_ids_json,
                    graph_center_node_id,
                    active_filters_json,
                    last_intent,
                    last_route,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    selected_node_ids_json = excluded.selected_node_ids_json,
                    resolved_entities_json = excluded.resolved_entities_json,
                    highlighted_node_ids_json = excluded.highlighted_node_ids_json,
                    graph_center_node_id = excluded.graph_center_node_id,
                    active_filters_json = excluded.active_filters_json,
                    last_intent = excluded.last_intent,
                    last_route = excluded.last_route,
                    updated_at = excluded.updated_at
                """,
                (
                    conversation_id,
                    json.dumps(state.selected_node_ids),
                    json.dumps([entity.model_dump() for entity in state.resolved_entities]),
                    json.dumps(state.highlighted_node_ids),
                    state.graph_center_node_id,
                    json.dumps(state.active_filters),
                    state.last_intent,
                    state.last_route,
                    timestamp,
                ),
            )
            connection.commit()

    def _ensure_schema(self) -> None:
        with connect_writable_sqlite(self._db_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    conversation_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_turns (
                    conversation_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_state (
                    conversation_id TEXT PRIMARY KEY,
                    selected_node_ids_json TEXT NOT NULL,
                    resolved_entities_json TEXT NOT NULL,
                    highlighted_node_ids_json TEXT NOT NULL,
                    graph_center_node_id TEXT,
                    active_filters_json TEXT NOT NULL,
                    last_intent TEXT,
                    last_route TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.commit()

    def _loads_json_list(self, payload: str | None) -> list:
        if not payload:
            return []
        loaded = json.loads(payload)
        return loaded if isinstance(loaded, list) else []

    def _utcnow(self) -> str:
        return datetime.now(timezone.utc).isoformat()
