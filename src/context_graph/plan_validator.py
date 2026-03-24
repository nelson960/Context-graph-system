from __future__ import annotations

from context_graph.exceptions import PlannerError
from context_graph.schemas import QueryPlan


ALLOWED_ROUTES_BY_INTENT = {
    "aggregate_analytics": {"sql", "hybrid"},
    "document_trace": {"graph", "hybrid"},
    "anomaly_detection": {"sql", "hybrid"},
    "entity_lookup": {"graph", "hybrid"},
    "relationship_exploration": {"graph", "hybrid"},
}


class QueryPlanValidator:
    def validate(self, query_plan: QueryPlan, selected_node_ids: list[str]) -> QueryPlan:
        allowed_routes = ALLOWED_ROUTES_BY_INTENT.get(query_plan.intent, set())
        if query_plan.route not in allowed_routes:
            raise PlannerError(
                f"Intent '{query_plan.intent}' cannot run with route '{query_plan.route}'"
            )
        if query_plan.route == "graph" and query_plan.intent == "document_trace":
            if query_plan.trace_direction is None:
                raise PlannerError("Document-trace plans must declare trace_direction")
        if query_plan.route == "graph":
            has_plan_entities = any(
                entity.resolved_node_id or entity.reference
                for entity in query_plan.entities
            )
            if not has_plan_entities and not selected_node_ids and not query_plan.trace_start:
                raise PlannerError(
                    "Graph and hybrid plans must reference at least one dataset entity or selected node"
                )
        if query_plan.route in {"sql", "hybrid"} and query_plan.intent == "aggregate_analytics":
            if not query_plan.metrics:
                raise PlannerError("Aggregate analytics plans must include at least one metric")
        return query_plan
