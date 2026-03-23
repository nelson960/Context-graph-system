from __future__ import annotations

import json
import textwrap
from collections.abc import Iterator
from typing import Any

from openai import OpenAI

from context_graph.catalog_service import CatalogService
from context_graph.exceptions import ConfigurationError, PlannerError
from context_graph.schemas import AnswerEnvelope, GraphResponse, NodeDTO, PlannerEnvelope, QueryPlan, SqlEnvelope
from context_graph.settings import AppSettings


DOMAIN_REFUSAL_MESSAGE = (
    "This system is designed to answer questions about the provided order-to-cash dataset only."
)


PLANNER_EXAMPLES = textwrap.dedent(
    """
    Example 1
    user question: Which products are associated with the highest number of billing documents?
    expected intent: aggregate_analytics
    expected route: sql
    expected preferred view: v_product_billing_summary

    Example 2
    user question: Trace billing document 90504219 through delivery, sales order, journal entry, and payment.
    expected intent: document_trace
    expected route: graph
    expected preferred view: v_billing_trace

    Example 3
    user question: Identify sales orders that were delivered but not billed.
    expected intent: anomaly_detection
    expected route: hybrid
    expected preferred view: v_incomplete_order_flows

    Example 4
    user question: Explain AS05.
    expected intent: entity_lookup
    expected route: graph
    expected preferred entity: Plant AS05

    Example 5
    user question: Write me a poem about supply chains.
    expected status: out_of_domain
    """
).strip()


def _extract_json_payload(payload: str) -> str:
    stripped = payload.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3:
            stripped = "\n".join(lines[1:-1]).strip()
    start_index = stripped.find("{")
    end_index = stripped.rfind("}")
    if start_index == -1 or end_index == -1 or end_index <= start_index:
        raise PlannerError(f"Model output did not contain a JSON object: {payload}")
    return stripped[start_index : end_index + 1]


class OpenAIPlanner:
    def __init__(self, settings: AppSettings, catalog_service: CatalogService) -> None:
        self._settings = settings
        self._catalog_service = catalog_service
        self._client: OpenAI | None = None

    def plan(
        self,
        message: str,
        selected_nodes: list[dict[str, Any]],
        memory_context: dict[str, Any] | None = None,
    ) -> PlannerEnvelope:
        response_text = self._json_completion(
            instructions=self._planner_instructions(),
            prompt=self._planner_prompt(
                message=message,
                selected_nodes=selected_nodes,
                memory_context=memory_context or {},
            ),
        )
        envelope = PlannerEnvelope.model_validate_json(_extract_json_payload(response_text))
        if envelope.status == "out_of_domain" and not envelope.refusal_message:
            envelope = PlannerEnvelope(
                status="out_of_domain",
                refusal_message=DOMAIN_REFUSAL_MESSAGE,
                query_plan=None,
            )
        if envelope.status == "ok" and envelope.query_plan is None:
            raise PlannerError("Planner returned status=ok without a query plan")
        return envelope

    def generate_sql(
        self,
        user_message: str,
        query_plan: QueryPlan,
    ) -> SqlEnvelope:
        response_text = self._json_completion(
            instructions=self._sql_instructions(),
            prompt=self._sql_prompt(user_message=user_message, query_plan=query_plan),
        )
        return SqlEnvelope.model_validate_json(_extract_json_payload(response_text))

    def compose_answer(
        self,
        user_message: str,
        query_plan: QueryPlan,
        sql: str,
        rows: list[dict[str, Any]],
        row_count: int,
    ) -> AnswerEnvelope:
        response_text = self._json_completion(
            instructions=self._answer_instructions(),
            prompt=self._answer_prompt(
                user_message=user_message,
                query_plan=query_plan,
                sql=sql,
                rows=rows,
                row_count=row_count,
            ),
        )
        return AnswerEnvelope.model_validate_json(_extract_json_payload(response_text))

    def compose_graph_answer(
        self,
        user_message: str,
        query_plan: QueryPlan,
        center_node: NodeDTO | None,
        graph_response: GraphResponse,
    ) -> AnswerEnvelope:
        response_text = self._json_completion(
            instructions=self._answer_instructions(),
            prompt=self._graph_answer_prompt(
                user_message=user_message,
                query_plan=query_plan,
                center_node=center_node,
                graph_response=graph_response,
            ),
        )
        return AnswerEnvelope.model_validate_json(_extract_json_payload(response_text))

    def stream_sql_answer(
        self,
        user_message: str,
        query_plan: QueryPlan,
        sql: str,
        rows: list[dict[str, Any]],
        row_count: int,
    ) -> Iterator[str]:
        yield from self._stream_text_completion(
            instructions=self._stream_answer_instructions(),
            prompt=self._answer_prompt(
                user_message=user_message,
                query_plan=query_plan,
                sql=sql,
                rows=rows,
                row_count=row_count,
            ),
        )

    def stream_graph_answer(
        self,
        user_message: str,
        query_plan: QueryPlan,
        center_node: NodeDTO | None,
        graph_response: GraphResponse,
    ) -> Iterator[str]:
        yield from self._stream_text_completion(
            instructions=self._stream_answer_instructions(),
            prompt=self._graph_answer_prompt(
                user_message=user_message,
                query_plan=query_plan,
                center_node=center_node,
                graph_response=graph_response,
            ),
        )

    def _json_completion(self, instructions: str, prompt: str) -> str:
        if not self._settings.openai_api_key:
            raise ConfigurationError(
                "MODEL_API_KEY or OPENAI_API_KEY is required for planner-backed chat queries"
            )
        if self._client is None:
            client_kwargs: dict[str, Any] = {"api_key": self._settings.openai_api_key}
            if self._settings.openai_base_url:
                client_kwargs["base_url"] = self._settings.openai_base_url
            self._client = OpenAI(**client_kwargs)
        try:
            request_kwargs = self._chat_completion_kwargs(
                instructions=instructions,
                prompt=prompt,
                stream=False,
            )
            response = self._client.chat.completions.create(**request_kwargs)
        except Exception as exc:  # pragma: no cover - network-dependent path
            raise PlannerError(f"Model request failed: {exc}") from exc
        output_text = self._extract_chat_completion_text(response)
        if not output_text:
            raise PlannerError("Model response did not contain output text")
        return output_text

    def _stream_text_completion(self, instructions: str, prompt: str) -> Iterator[str]:
        if not self._settings.openai_api_key:
            raise ConfigurationError(
                "MODEL_API_KEY or OPENAI_API_KEY is required for planner-backed chat queries"
            )
        if self._client is None:
            client_kwargs: dict[str, Any] = {"api_key": self._settings.openai_api_key}
            if self._settings.openai_base_url:
                client_kwargs["base_url"] = self._settings.openai_base_url
            self._client = OpenAI(**client_kwargs)
        try:
            stream = self._client.chat.completions.create(
                **self._chat_completion_kwargs(
                    instructions=instructions,
                    prompt=prompt,
                    stream=True,
                )
            )
        except Exception as exc:  # pragma: no cover - network-dependent path
            raise PlannerError(f"Model request failed: {exc}") from exc
        for chunk in stream:
            choices = getattr(chunk, "choices", None) or []
            if not choices:
                continue
            delta = getattr(choices[0], "delta", None)
            if delta is None:
                continue
            content = getattr(delta, "content", None)
            if isinstance(content, str):
                if content:
                    yield content
                continue
            if isinstance(content, list):
                for item in content:
                    text = item.get("text") if isinstance(item, dict) else getattr(item, "text", None)
                    if isinstance(text, str) and text:
                        yield text

    def _extract_chat_completion_text(self, response: Any) -> str:
        choices = getattr(response, "choices", None) or []
        if not choices:
            return ""
        message = getattr(choices[0], "message", None)
        if message is None:
            return ""
        content = getattr(message, "content", None)
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            text_parts: list[str] = []
            for item in content:
                if isinstance(item, dict):
                    text = item.get("text")
                    if isinstance(text, str):
                        text_parts.append(text)
                    continue
                text = getattr(item, "text", None)
                if isinstance(text, str):
                    text_parts.append(text)
            return "\n".join(part for part in text_parts if part)
        return ""

    def _planner_instructions(self) -> str:
        schema = json.dumps(PlannerEnvelope.model_json_schema(), indent=2)
        return textwrap.dedent(
            f"""
            You are a planner for a grounded SAP order-to-cash analytics system.
            Stay strictly inside the provided business domain.
            Return JSON only. Do not wrap it in markdown.
            If the question is unrelated to the order-to-cash dataset, return:
            {{
              "status": "out_of_domain",
              "refusal_message": "{DOMAIN_REFUSAL_MESSAGE}"
            }}

            If the question is in domain, return JSON matching this schema:
            {schema}
            """
        ).strip()

    def _planner_prompt(
        self,
        message: str,
        selected_nodes: list[dict[str, Any]],
        memory_context: dict[str, Any],
    ) -> str:
        return textwrap.dedent(
            f"""
            {self._catalog_service.compact_prompt_context()}

            Few-shot guidance:
            {PLANNER_EXAMPLES}

            Selected graph nodes:
            {json.dumps(selected_nodes, indent=2)}

            Conversation and entity grounding context:
            {json.dumps(memory_context, indent=2)}

            User question:
            {message}
            """
        ).strip()

    def _sql_instructions(self) -> str:
        schema = json.dumps(SqlEnvelope.model_json_schema(), indent=2)
        return textwrap.dedent(
            f"""
            You generate read-only SQLite SQL for a grounded order-to-cash analytics system.
            Return JSON only and match this schema exactly:
            {schema}

            SQL rules:
            - Use only approved analytical views.
            - Generate exactly one SELECT or WITH...SELECT query.
            - Fully qualify all column references.
            - Do not use SELECT *.
            - Include an explicit LIMIT.
            - Prefer:
              - v_product_billing_summary for product ranking
              - v_billing_trace for billing document lineage
              - v_incomplete_order_flows for anomalies
              - v_customer_360 for customer summaries
              - v_financial_flow for billing/journal/payment flow
              - v_delivery_flow for delivery-centered questions
              - v_billing_flow for billing-centered header/item questions
            """
        ).strip()

    def _sql_prompt(self, user_message: str, query_plan: QueryPlan) -> str:
        return textwrap.dedent(
            f"""
            {self._catalog_service.compact_prompt_context()}

            User question:
            {user_message}

            Validated query plan:
            {query_plan.model_dump_json(indent=2)}
            """
        ).strip()

    def _answer_instructions(self) -> str:
        schema = json.dumps(AnswerEnvelope.model_json_schema(), indent=2)
        return textwrap.dedent(
            f"""
            You explain results for a grounded order-to-cash analytics system.
            The SQL results are the only source of truth.
            Return JSON only and match this schema exactly:
            {schema}

            Rules:
            - Use only the supplied rows and row count.
            - If no rows matched, say so directly.
            - Mention assumptions only if they exist in the supplied plan.
            - Do not invent entities, counts, or links that are not present in the results.
            """
        ).strip()

    def _stream_answer_instructions(self) -> str:
        return textwrap.dedent(
            """
            You explain results for a grounded order-to-cash analytics system.
            Use only the supplied execution evidence.
            Return plain text only.
            Do not output JSON or markdown code fences.
            If no result exists, say so directly.
            Do not invent entities, counts, or links.
            """
        ).strip()

    def _answer_prompt(
        self,
        user_message: str,
        query_plan: QueryPlan,
        sql: str,
        rows: list[dict[str, Any]],
        row_count: int,
    ) -> str:
        rows_preview = rows[:25]
        return textwrap.dedent(
            f"""
            User question:
            {user_message}

            Query plan:
            {query_plan.model_dump_json(indent=2)}

            Executed SQL:
            {sql}

            Row count:
            {row_count}

            Rows preview:
            {json.dumps(rows_preview, indent=2)}
            """
        ).strip()

    def _graph_answer_prompt(
        self,
        user_message: str,
        query_plan: QueryPlan,
        center_node: NodeDTO | None,
        graph_response: GraphResponse,
    ) -> str:
        node_preview = [node.model_dump() for node in graph_response.nodes[:20]]
        edge_preview = [edge.model_dump() for edge in graph_response.edges[:20]]
        return textwrap.dedent(
            f"""
            User question:
            {user_message}

            Query plan:
            {query_plan.model_dump_json(indent=2)}

            Center node:
            {json.dumps(center_node.model_dump() if center_node else None, indent=2)}

            Graph summary:
            {json.dumps(
                {
                    "center_node_id": graph_response.center_node_id,
                    "depth": graph_response.depth,
                    "cluster_mode": graph_response.cluster_mode,
                    "node_count": len(graph_response.nodes),
                    "edge_count": len(graph_response.edges),
                    "nodes": node_preview,
                    "edges": edge_preview,
                },
                indent=2,
            )}
            """
        ).strip()

    def _chat_completion_kwargs(
        self,
        instructions: str,
        prompt: str,
        stream: bool,
    ) -> dict[str, Any]:
        request_kwargs: dict[str, Any] = {
            "model": self._settings.openai_model,
            "messages": [
                {"role": "system", "content": instructions},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0,
            "stream": stream,
        }
        if self._settings.model_provider == "openai":
            request_kwargs["max_completion_tokens"] = 1800
        else:
            request_kwargs["max_tokens"] = 1800
        return request_kwargs
