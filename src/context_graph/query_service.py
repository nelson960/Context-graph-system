from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
import re
from typing import Any

from context_graph.conversation_store import ConversationContext, ConversationStore
from context_graph.entity_service import EntityService
from context_graph.evidence_service import EvidenceBundle, EvidenceService
from context_graph.exceptions import EntityResolutionError, QueryExecutionError
from context_graph.graph_service import GraphService
from context_graph.observability import QueryLogger
from context_graph.plan_validator import QueryPlanValidator
from context_graph.planner import OpenAIPlanner
from context_graph.schemas import (
    ChatQueryRequest,
    ChatQueryResponse,
    ConversationMemoryState,
    GraphResponse,
    NodeDTO,
    QueryPlan,
)
from context_graph.sql_guard import QueryExecutionResult, SqlExecutor, SqlValidationResult, SqlValidator


@dataclass(frozen=True)
class ExecutionArtifacts:
    query_plan: QueryPlan
    sql_validation: SqlValidationResult | None
    execution: QueryExecutionResult | None
    graph_response: GraphResponse | None
    center_node: NodeDTO | None
    evidence: EvidenceBundle
    provenance_note: str


class QueryService:
    def __init__(
        self,
        entity_service: EntityService,
        graph_service: GraphService,
        planner: OpenAIPlanner,
        sql_validator: SqlValidator,
        sql_executor: SqlExecutor,
        query_logger: QueryLogger,
        conversation_store: ConversationStore,
        plan_validator: QueryPlanValidator,
        evidence_service: EvidenceService,
    ) -> None:
        self._entity_service = entity_service
        self._graph_service = graph_service
        self._planner = planner
        self._sql_validator = sql_validator
        self._sql_executor = sql_executor
        self._query_logger = query_logger
        self._conversation_store = conversation_store
        self._plan_validator = plan_validator
        self._evidence_service = evidence_service

    def handle_chat_request(self, request: ChatQueryRequest) -> ChatQueryResponse:
        event: dict[str, Any] = {
            "message": request.message,
            "selected_node_ids": request.selectedNodeIds,
            "cluster_mode": request.clusterMode,
        }
        conversation_id = self._conversation_store.ensure_conversation(request.conversationId)
        event["conversation_id"] = conversation_id
        try:
            prepared = self._initialize_request(request, conversation_id)
            if isinstance(prepared, ChatQueryResponse):
                event["result"] = prepared.model_dump()
                self._query_logger.write(event)
                return prepared
            memory_context, selected_node_ids, selected_nodes, query_plan = prepared
            artifacts = self._execute_route(
                request=request,
                query_plan=query_plan,
                selected_node_ids=selected_node_ids,
            )
            answer_envelope = self._compose_non_stream_answer(request, artifacts)
            response = self._build_response(
                request=request,
                conversation_id=conversation_id,
                artifacts=artifacts,
                answer=answer_envelope.answer,
                assumptions=list(
                    dict.fromkeys(query_plan.assumptions + answer_envelope.assumptions)
                ),
                memory_state=self._memory_state(
                    query_plan=query_plan,
                    selected_node_ids=selected_node_ids,
                    evidence=artifacts.evidence,
                ),
                provenance_note=answer_envelope.provenance_note or artifacts.provenance_note,
            )
            self._record_and_log_success(
                conversation_id=conversation_id,
                request=request,
                response=response,
                query_plan=query_plan,
                selected_nodes=selected_nodes,
                memory_context=memory_context,
                artifacts=artifacts,
                event=event,
            )
            return response
        except Exception as exc:
            event["error"] = str(exc)
            self._query_logger.write(event)
            raise

    def stream_chat_request(self, request: ChatQueryRequest) -> Iterator[dict[str, Any]]:
        event: dict[str, Any] = {
            "message": request.message,
            "selected_node_ids": request.selectedNodeIds,
            "cluster_mode": request.clusterMode,
        }
        conversation_id = self._conversation_store.ensure_conversation(request.conversationId)
        event["conversation_id"] = conversation_id
        yield {"type": "conversation", "conversation_id": conversation_id}
        try:
            yield {"type": "status", "stage": "planning", "message": "Planning query"}
            prepared = self._initialize_request(request, conversation_id)
            if isinstance(prepared, ChatQueryResponse):
                event["result"] = prepared.model_dump()
                self._query_logger.write(event)
                yield {"type": "final", "data": prepared.model_dump()}
                return
            memory_context, selected_node_ids, selected_nodes, query_plan = prepared
            yield {
                "type": "plan_ready",
                "intent": query_plan.intent,
                "route": query_plan.route,
                "query_plan": query_plan.model_dump(),
            }
            artifacts = self._execute_route(
                request=request,
                query_plan=query_plan,
                selected_node_ids=selected_node_ids,
            )
            yield {
                "type": "execution_ready",
                "route": query_plan.route,
                "sql": artifacts.execution.executed_sql if artifacts.execution else None,
                "row_count": artifacts.execution.row_count if artifacts.execution else 0,
                "graph_center_node_id": artifacts.evidence.graph_center_node_id,
            }
            yield {"type": "status", "stage": "answering", "message": "Composing answer"}
            answer_parts: list[str] = []
            for delta in self._compose_streaming_answer(request, artifacts):
                answer_parts.append(delta)
                yield {"type": "answer_delta", "delta": delta}
            answer = "".join(answer_parts).strip()
            response = self._build_response(
                request=request,
                conversation_id=conversation_id,
                artifacts=artifacts,
                answer=answer or None,
                assumptions=list(dict.fromkeys(query_plan.assumptions)),
                memory_state=self._memory_state(
                    query_plan=query_plan,
                    selected_node_ids=selected_node_ids,
                    evidence=artifacts.evidence,
                ),
                provenance_note=artifacts.provenance_note,
            )
            self._record_and_log_success(
                conversation_id=conversation_id,
                request=request,
                response=response,
                query_plan=query_plan,
                selected_nodes=selected_nodes,
                memory_context=memory_context,
                artifacts=artifacts,
                event=event,
            )
            yield {"type": "final", "data": response.model_dump()}
        except Exception as exc:
            event["error"] = str(exc)
            self._query_logger.write(event)
            yield {"type": "error", "error": str(exc)}

    def _initialize_request(
        self,
        request: ChatQueryRequest,
        conversation_id: str,
    ) -> tuple[dict[str, Any], list[str], list[dict[str, Any]], QueryPlan] | ChatQueryResponse:
        conversation_context = self._conversation_store.load_context(conversation_id)
        selected_node_ids = request.selectedNodeIds or (
            conversation_context.state.selected_node_ids if conversation_context else []
        )
        selected_nodes = self._selected_node_context(selected_node_ids)
        planning_context = self._memory_context_for_prompt(conversation_context, selected_node_ids)
        planning_context["candidate_entities"] = self._candidate_entities_for_message(request.message)
        query_plan = self._deterministic_entity_lookup_plan(
            request.message,
            planning_context["candidate_entities"],
            selected_node_ids,
        )
        if query_plan is None:
            planner_envelope = self._planner.plan(
                request.message,
                selected_nodes,
                memory_context=planning_context,
            )
        else:
            planner_envelope = None
        if planner_envelope and planner_envelope.status == "out_of_domain":
            memory_state = conversation_context.state if conversation_context else ConversationMemoryState()
            if planning_context["candidate_entities"]:
                memory_state = memory_state.model_copy(
                    update={
                        "resolved_entities": query_plan.entities if query_plan else memory_state.resolved_entities
                    }
                )
            return ChatQueryResponse(
                conversation_id=conversation_id,
                answer=planner_envelope.refusal_message,
                intent=None,
                route=None,
                query_plan=None,
                sql=None,
                row_count=0,
                rows=[],
                highlighted_node_ids=[],
                highlighted_edge_ids=[],
                cited_nodes=[],
                cited_edges=[],
                provenance_note="Out-of-domain request rejected before planning",
                memory_state=memory_state,
                error=None,
            )
        resolved_query_plan = self._resolve_plan_entities(
            query_plan if query_plan is not None else planner_envelope.query_plan,
            conversation_context=conversation_context,
        )
        resolved_query_plan = self._plan_validator.validate(resolved_query_plan, selected_node_ids)
        return planning_context, selected_node_ids, selected_nodes, resolved_query_plan

    def _execute_route(
        self,
        request: ChatQueryRequest,
        query_plan: QueryPlan,
        selected_node_ids: list[str],
    ) -> ExecutionArtifacts:
        if query_plan.route == "graph":
            return self._execute_graph_route(request, query_plan, selected_node_ids)
        return self._execute_sql_route(request, query_plan, selected_node_ids)

    def _execute_graph_route(
        self,
        request: ChatQueryRequest,
        query_plan: QueryPlan,
        selected_node_ids: list[str],
    ) -> ExecutionArtifacts:
        candidate_node_ids = self._graph_candidate_nodes(query_plan, selected_node_ids)
        if not candidate_node_ids:
            raise EntityResolutionError("Graph route did not resolve any graph nodes")
        if query_plan.intent == "document_trace":
            graph_response = self._graph_service.get_path(
                node_id=candidate_node_ids[0],
                direction=query_plan.trace_direction,
                depth=6,
                cluster_mode=request.clusterMode,
            )
        elif len(candidate_node_ids) > 1:
            graph_response = self._graph_service.get_combined_subgraph(
                candidate_node_ids[:6],
                depth=2,
                include_hidden=False,
                cluster_mode=request.clusterMode,
            )
            if graph_response is None:
                raise QueryExecutionError(
                    "Could not derive a combined graph response for the selected entities"
                )
        else:
            graph_response = self._graph_service.get_subgraph(
                node_id=candidate_node_ids[0],
                depth=2 if query_plan.intent == "relationship_exploration" else 1,
                include_hidden=False,
                cluster_mode=request.clusterMode,
            )
        evidence = self._evidence_service.from_graph_response(
            graph_response,
            preferred_node_ids=candidate_node_ids,
        )
        center_node = (
            self._graph_service.get_node(evidence.graph_center_node_id)
            if evidence.graph_center_node_id and self._graph_service.has_node(evidence.graph_center_node_id)
            else None
        )
        return ExecutionArtifacts(
            query_plan=query_plan,
            sql_validation=None,
            execution=None,
            graph_response=graph_response,
            center_node=center_node,
            evidence=evidence,
            provenance_note=(
                f"Graph answer grounded in {len(graph_response.nodes)} nodes and "
                f"{len(graph_response.edges)} edges from the context graph."
            ),
        )

    def _execute_sql_route(
        self,
        request: ChatQueryRequest,
        query_plan: QueryPlan,
        selected_node_ids: list[str],
    ) -> ExecutionArtifacts:
        sql_envelope = self._planner.generate_sql(request.message, query_plan)
        validation = self._sql_validator.validate(sql_envelope.sql)
        execution = self._sql_executor.execute(validation)
        base_evidence = self._evidence_service.from_sql_rows(
            execution.rows,
            additional_node_ids=[
                entity.resolved_node_id for entity in query_plan.entities if entity.resolved_node_id
            ]
            + selected_node_ids,
        )
        graph_response = (
            self._graph_service.get_combined_subgraph(
                base_evidence.highlighted_node_ids[:6],
                depth=1,
                include_hidden=False,
                cluster_mode=request.clusterMode,
            )
            if base_evidence.highlighted_node_ids
            else None
        )
        graph_evidence = (
            self._evidence_service.from_graph_response(
                graph_response,
                preferred_node_ids=base_evidence.highlighted_node_ids,
            )
            if graph_response
            else None
        )
        evidence = self._merge_evidence(base_evidence, graph_evidence)
        center_node = (
            self._graph_service.get_node(evidence.graph_center_node_id)
            if evidence.graph_center_node_id and self._graph_service.has_node(evidence.graph_center_node_id)
            else None
        )
        return ExecutionArtifacts(
            query_plan=query_plan,
            sql_validation=validation,
            execution=execution,
            graph_response=graph_response,
            center_node=center_node,
            evidence=evidence,
            provenance_note=sql_envelope.provenance_note,
        )

    def _compose_non_stream_answer(
        self,
        request: ChatQueryRequest,
        artifacts: ExecutionArtifacts,
    ):
        if artifacts.query_plan.route == "graph":
            return self._planner.compose_graph_answer(
                user_message=request.message,
                query_plan=artifacts.query_plan,
                center_node=artifacts.center_node,
                graph_response=artifacts.graph_response,
            )
        assert artifacts.execution is not None
        return self._planner.compose_answer(
            user_message=request.message,
            query_plan=artifacts.query_plan,
            sql=artifacts.execution.executed_sql,
            rows=artifacts.execution.rows,
            row_count=artifacts.execution.row_count,
        )

    def _compose_streaming_answer(
        self,
        request: ChatQueryRequest,
        artifacts: ExecutionArtifacts,
    ) -> Iterator[str]:
        if artifacts.query_plan.route == "graph":
            assert artifacts.graph_response is not None
            yield from self._planner.stream_graph_answer(
                user_message=request.message,
                query_plan=artifacts.query_plan,
                center_node=artifacts.center_node,
                graph_response=artifacts.graph_response,
            )
            return
        assert artifacts.execution is not None
        yield from self._planner.stream_sql_answer(
            user_message=request.message,
            query_plan=artifacts.query_plan,
            sql=artifacts.execution.executed_sql,
            rows=artifacts.execution.rows,
            row_count=artifacts.execution.row_count,
        )

    def _build_response(
        self,
        request: ChatQueryRequest,
        conversation_id: str,
        artifacts: ExecutionArtifacts,
        answer: str | None,
        assumptions: list[str],
        memory_state: ConversationMemoryState,
        provenance_note: str,
    ) -> ChatQueryResponse:
        execution = artifacts.execution
        return ChatQueryResponse(
            conversation_id=conversation_id,
            answer=answer,
            assumptions=assumptions,
            intent=artifacts.query_plan.intent,
            route=artifacts.query_plan.route,
            query_plan=artifacts.query_plan.model_dump(),
            sql=execution.executed_sql if execution else None,
            row_count=execution.row_count if execution else 0,
            rows=execution.rows if execution else [],
            highlighted_node_ids=artifacts.evidence.highlighted_node_ids,
            highlighted_edge_ids=artifacts.evidence.highlighted_edge_ids,
            cited_nodes=artifacts.evidence.cited_nodes,
            cited_edges=artifacts.evidence.cited_edges,
            graph_center_node_id=artifacts.evidence.graph_center_node_id,
            provenance_note=provenance_note,
            memory_state=memory_state,
            error=None,
        )

    def _record_and_log_success(
        self,
        conversation_id: str,
        request: ChatQueryRequest,
        response: ChatQueryResponse,
        query_plan: QueryPlan,
        selected_nodes: list[dict[str, Any]],
        memory_context: dict[str, Any],
        artifacts: ExecutionArtifacts,
        event: dict[str, Any],
    ) -> None:
        assert response.memory_state is not None
        self._conversation_store.record_interaction(
            conversation_id=conversation_id,
            user_message=request.message,
            assistant_message=response.answer or response.error or "",
            state=response.memory_state,
            request_payload=request.model_dump(),
            response_payload=response.model_dump(),
        )
        execution = artifacts.execution
        event.update(
            {
                "intent": query_plan.intent,
                "route": query_plan.route,
                "selected_nodes": selected_nodes,
                "memory_context": memory_context,
                "resolved_entities": [
                    entity.model_dump() for entity in query_plan.entities
                ],
                "query_plan": query_plan.model_dump(),
                "generated_sql": artifacts.sql_validation.generated_sql if artifacts.sql_validation else None,
                "executed_sql": execution.executed_sql if execution else None,
                "row_count": execution.row_count if execution else 0,
                "duration_ms": execution.duration_ms if execution else None,
                "highlighted_node_ids": response.highlighted_node_ids,
                "highlighted_edge_ids": response.highlighted_edge_ids,
                "cited_node_ids": [node.id for node in response.cited_nodes],
                "cited_edge_ids": [edge.id for edge in response.cited_edges],
                "answer": response.answer,
                "result": response.model_dump(),
            }
        )
        self._query_logger.write(event)

    def _graph_candidate_nodes(
        self,
        query_plan: QueryPlan,
        selected_node_ids: list[str],
    ) -> list[str]:
        candidate_node_ids = [
            entity.resolved_node_id
            for entity in query_plan.entities
            if entity.resolved_node_id
        ] + selected_node_ids
        return self._graph_service.filter_existing_node_ids(list(dict.fromkeys(candidate_node_ids)))

    def _memory_context_for_prompt(
        self,
        conversation_context: ConversationContext | None,
        selected_node_ids: list[str],
    ) -> dict[str, Any]:
        if conversation_context is None:
            return {
                "recent_turns": [],
                "state": ConversationMemoryState(
                    selected_node_ids=selected_node_ids
                ).model_dump(),
            }
        state = conversation_context.state.model_copy(
            update={"selected_node_ids": selected_node_ids or conversation_context.state.selected_node_ids}
        )
        return {
            "recent_turns": conversation_context.turns,
            "state": state.model_dump(),
        }

    def _candidate_entities_for_message(self, message: str) -> list[dict[str, Any]]:
        trimmed_message = message.strip()
        if not trimmed_message:
            return []
        search_queries = [trimmed_message]
        tokens = [
            token
            for token in re.split(r"[^A-Za-z0-9:_-]+", trimmed_message)
            if len(token) >= 2
        ]
        for token in tokens[:8]:
            if token.lower() != trimmed_message.lower():
                search_queries.append(token)
        ranked_results: dict[str, Any] = {}
        for query in search_queries:
            for result in self._entity_service.search(query, limit=5):
                current = ranked_results.get(result.node_id)
                if current is None or result.score > current.score:
                    ranked_results[result.node_id] = result
        return [
            result.model_dump()
            for result in sorted(
                ranked_results.values(),
                key=lambda item: (-item.score, item.node_type, item.display_label),
            )[:6]
        ]

    def _deterministic_entity_lookup_plan(
        self,
        message: str,
        candidate_entities: list[dict[str, Any]],
        selected_node_ids: list[str],
    ) -> QueryPlan | None:
        if selected_node_ids or not candidate_entities:
            return None
        top_candidate = candidate_entities[0]
        top_score = int(top_candidate["score"])
        if top_score < 460:
            return None
        if sum(1 for item in candidate_entities if int(item["score"]) == top_score) != 1:
            return None

        normalized_message = " ".join(message.strip().lower().split())
        message_tokens = [
            token
            for token in re.split(r"[^a-z0-9:_-]+", normalized_message)
            if token
        ]
        if not message_tokens:
            return None

        stopwords = {
            "a",
            "an",
            "about",
            "detail",
            "details",
            "describe",
            "explain",
            "for",
            "inspect",
            "is",
            "lookup",
            "me",
            "open",
            "please",
            "show",
            "the",
            "view",
            "what",
        }
        business_key = str(top_candidate["business_key"]).lower()
        display_label = str(top_candidate["display_label"]).lower()
        subtitle = str(top_candidate.get("subtitle") or "").lower()
        entity_terms = {
            token
            for value in (business_key, display_label, subtitle)
            for token in re.split(r"[^a-z0-9:_-]+", value)
            if token
        }
        meaningful_other_tokens = [
            token
            for token in message_tokens
            if token not in stopwords and token not in entity_terms
        ]
        is_direct_reference = normalized_message in {business_key, display_label, subtitle}
        is_lookup_prompt = message_tokens[0] in {
            "describe",
            "detail",
            "details",
            "explain",
            "inspect",
            "lookup",
            "open",
            "show",
            "view",
            "what",
        }
        if meaningful_other_tokens:
            return None
        if not is_direct_reference and not is_lookup_prompt:
            return None

        return QueryPlan(
            intent="entity_lookup",
            route="graph",
            entities=[
                {
                    "reference": str(top_candidate["business_key"]),
                    "entity_type": str(top_candidate["node_type"]),
                    "resolved_node_id": str(top_candidate["node_id"]),
                    "resolved_business_key": str(top_candidate["business_key"]),
                }
            ],
            assumptions=[
                (
                    "Interpreted the request as a direct dataset entity lookup for "
                    f"{top_candidate['node_type']} {top_candidate['business_key']}."
                )
            ],
            output_shape="summary",
        )

    def _resolve_plan_entities(
        self,
        query_plan: QueryPlan,
        conversation_context: ConversationContext | None,
    ) -> QueryPlan:
        if not query_plan.entities and conversation_context and conversation_context.state.resolved_entities:
            return query_plan.model_copy(
                update={"entities": conversation_context.state.resolved_entities}
            )
        resolved_entities = []
        for entity in query_plan.entities:
            if entity.resolved_node_id and entity.resolved_business_key:
                resolved_entities.append(entity)
                continue
            resolved = self._entity_service.resolve(
                reference=entity.reference,
                node_type=entity.entity_type,
            )
            resolved_entities.append(
                entity.model_copy(
                    update={
                        "resolved_node_id": resolved.node_id,
                        "resolved_business_key": resolved.business_key,
                    }
                )
            )
        return query_plan.model_copy(update={"entities": resolved_entities})

    def _memory_state(
        self,
        query_plan: QueryPlan,
        selected_node_ids: list[str],
        evidence: EvidenceBundle,
    ) -> ConversationMemoryState:
        return ConversationMemoryState(
            selected_node_ids=selected_node_ids,
            resolved_entities=query_plan.entities,
            highlighted_node_ids=evidence.highlighted_node_ids,
            graph_center_node_id=evidence.graph_center_node_id,
            active_filters=query_plan.filters,
            last_intent=query_plan.intent,
            last_route=query_plan.route,
        )

    def _selected_node_context(self, node_ids: list[str]) -> list[dict[str, Any]]:
        context: list[dict[str, Any]] = []
        for node_id in node_ids[:5]:
            if not self._graph_service.has_node(node_id):
                continue
            node = self._graph_service.get_node(node_id)
            context.append(
                {
                    "id": node.id,
                    "type": node.type,
                    "business_key": node.business_key,
                    "display_label": node.display_label,
                    "subtitle": node.subtitle,
                }
            )
        return context

    def _merge_evidence(
        self,
        primary: EvidenceBundle,
        secondary: EvidenceBundle | None,
    ) -> EvidenceBundle:
        if secondary is None:
            return primary
        merged_node_ids = list(dict.fromkeys(primary.highlighted_node_ids + secondary.highlighted_node_ids))
        merged_edge_ids = list(dict.fromkeys(primary.highlighted_edge_ids + secondary.highlighted_edge_ids))
        cited_nodes_by_id = {node.id: node for node in primary.cited_nodes}
        for node in secondary.cited_nodes:
            cited_nodes_by_id.setdefault(node.id, node)
        cited_edges_by_id = {edge.id: edge for edge in primary.cited_edges}
        for edge in secondary.cited_edges:
            cited_edges_by_id.setdefault(edge.id, edge)
        return EvidenceBundle(
            highlighted_node_ids=merged_node_ids,
            highlighted_edge_ids=merged_edge_ids,
            cited_nodes=list(cited_nodes_by_id.values()),
            cited_edges=list(cited_edges_by_id.values()),
            graph_center_node_id=secondary.graph_center_node_id or primary.graph_center_node_id,
        )
