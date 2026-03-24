from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from context_graph.graph_service import GraphService
from context_graph.schemas import CitationEdge, CitationNode, GraphResponse


@dataclass(frozen=True)
class EvidenceBundle:
    highlighted_node_ids: list[str]
    highlighted_edge_ids: list[str]
    cited_nodes: list[CitationNode]
    cited_edges: list[CitationEdge]
    graph_center_node_id: str | None


class EvidenceService:
    def __init__(self, graph_service: GraphService) -> None:
        self._graph_service = graph_service

    def from_graph_response(
        self,
        graph_response: GraphResponse,
        preferred_node_ids: list[str] | None = None,
    ) -> EvidenceBundle:
        highlighted_node_ids = [node.id for node in graph_response.nodes]
        highlighted_edge_ids = [edge.id for edge in graph_response.edges]
        cited_nodes = [
            CitationNode(
                id=node.id,
                type=node.type,
                business_key=node.business_key,
                display_label=node.display_label,
            )
            for node in graph_response.nodes[:20]
        ]
        cited_edges = [
            CitationEdge(
                id=edge.id,
                type=edge.type,
                source=edge.source,
                target=edge.target,
            )
            for edge in graph_response.edges[:20]
        ]
        graph_center_node_id = self._graph_service.infer_center_node(
            (preferred_node_ids or []) + highlighted_node_ids
        ) or graph_response.center_node_id
        return EvidenceBundle(
            highlighted_node_ids=highlighted_node_ids,
            highlighted_edge_ids=highlighted_edge_ids,
            cited_nodes=cited_nodes,
            cited_edges=cited_edges,
            graph_center_node_id=graph_center_node_id,
        )

    def from_sql_rows(
        self,
        rows: list[dict[str, Any]],
        additional_node_ids: list[str] | None = None,
    ) -> EvidenceBundle:
        ordered_node_ids = list(dict.fromkeys(additional_node_ids or []))
        ordered_edge_ids: list[str] = []
        for row in rows:
            ordered_node_ids.extend(self._node_ids_from_row(row))
            ordered_edge_ids.extend(self._edge_ids_from_row(row))
        ordered_node_ids = self._graph_service.filter_existing_node_ids(
            list(dict.fromkeys(ordered_node_ids))
        )
        ordered_edge_ids = self._graph_service.filter_existing_edge_ids(
            list(dict.fromkeys(ordered_edge_ids))
        )
        cited_nodes = [
            CitationNode(
                id=node.id,
                type=node.type,
                business_key=node.business_key,
                display_label=node.display_label,
            )
            for node in self._graph_service.get_nodes(ordered_node_ids[:24])
        ]
        cited_edges = [
            CitationEdge(
                id=edge.id,
                type=edge.type,
                source=edge.source,
                target=edge.target,
            )
            for edge in self._graph_service.get_edges(ordered_edge_ids[:24])
        ]
        return EvidenceBundle(
            highlighted_node_ids=ordered_node_ids,
            highlighted_edge_ids=ordered_edge_ids,
            cited_nodes=cited_nodes,
            cited_edges=cited_edges,
            graph_center_node_id=self._graph_service.infer_center_node(ordered_node_ids),
        )

    def _node_ids_from_row(self, row: dict[str, Any]) -> list[str]:
        found: list[str] = []
        simple_mappings = {
            "sales_order": "sales_order",
            "delivery_document": "delivery",
            "billing_document": "billing_document",
            "customer_id": "customer",
            "product_id": "product",
            "production_plant": "plant",
            "plant": "plant",
            "company_code": "company_code",
            "journal_company_code": "company_code",
            "shipping_point": "plant",
        }
        for column_name, prefix in simple_mappings.items():
            value = row.get(column_name)
            if value is not None:
                found.append(f"{prefix}:{value}")

        if row.get("sales_order") and row.get("sales_order_item"):
            found.append(f"sales_order_item:{row['sales_order']}:{row['sales_order_item']}")
        if row.get("delivery_document") and row.get("delivery_document_item"):
            found.append(f"delivery_item:{row['delivery_document']}:{row['delivery_document_item']}")
        if row.get("billing_document") and row.get("billing_document_item"):
            found.append(f"billing_item:{row['billing_document']}:{row['billing_document_item']}")
        journal_parts = [
            row.get("journal_company_code"),
            row.get("journal_fiscal_year"),
            row.get("journal_accounting_document"),
            row.get("journal_accounting_document_item"),
        ]
        if all(part is not None for part in journal_parts):
            company_code, fiscal_year, accounting_document, accounting_document_item = journal_parts
            found.append(
                f"journal_entry:{company_code}:{fiscal_year}:{accounting_document}:{accounting_document_item}"
            )
            if row.get("clearing_accounting_document") is not None:
                found.append(
                    f"payment:{company_code}:{fiscal_year}:{accounting_document}:{accounting_document_item}"
                )
        if row.get("customer_number") is not None:
            found.append(f"customer:{row['customer_number']}")
        return list(dict.fromkeys(found))

    def _edge_ids_from_row(self, row: dict[str, Any]) -> list[str]:
        edge_ids: list[str] = []
        sales_order = row.get("sales_order")
        sales_order_item = row.get("sales_order_item")
        customer_id = row.get("customer_id")
        delivery_document = row.get("delivery_document")
        delivery_document_item = row.get("delivery_document_item")
        billing_document = row.get("billing_document")
        billing_document_item = row.get("billing_document_item")
        product_id = row.get("product_id")
        production_plant = row.get("production_plant")
        plant = row.get("plant")
        journal_company_code = row.get("journal_company_code")
        journal_fiscal_year = row.get("journal_fiscal_year")
        journal_accounting_document = row.get("journal_accounting_document")
        journal_accounting_document_item = row.get("journal_accounting_document_item")
        clearing_accounting_document = row.get("clearing_accounting_document")

        if sales_order and customer_id:
            edge_ids.append(f"ordered_by:{sales_order}:{customer_id}")
        if sales_order and sales_order_item:
            edge_ids.append(f"has_item:{sales_order}:{sales_order_item}")
        if sales_order and sales_order_item and product_id:
            edge_ids.append(f"refers_to_product:{sales_order}:{sales_order_item}:{product_id}")
        if sales_order and sales_order_item and production_plant:
            edge_ids.append(
                f"shipped_from_order_item:{sales_order}:{sales_order_item}:{production_plant}"
            )
        if delivery_document and delivery_document_item:
            edge_ids.append(f"part_of_delivery:{delivery_document}:{delivery_document_item}")
        if delivery_document and customer_id:
            edge_ids.append(f"delivered_to:{delivery_document}:{customer_id}")
        if delivery_document and plant:
            edge_ids.append(f"shipped_from_delivery:{delivery_document}:{plant}")
        if sales_order and sales_order_item and delivery_document and delivery_document_item:
            edge_ids.append(
                f"fulfilled_by:{sales_order}:{sales_order_item}:{delivery_document}:{delivery_document_item}"
            )
        if billing_document and billing_document_item:
            edge_ids.append(f"part_of_billing:{billing_document}:{billing_document_item}")
        if delivery_document and delivery_document_item and billing_document and billing_document_item:
            edge_ids.append(
                f"billed_as:{delivery_document}:{delivery_document_item}:{billing_document}:{billing_document_item}"
            )
        if (
            billing_document
            and journal_company_code
            and journal_fiscal_year
            and journal_accounting_document
            and journal_accounting_document_item
        ):
            edge_ids.append(
                "posted_to:"
                f"{billing_document}:{journal_company_code}:{journal_fiscal_year}:"
                f"{journal_accounting_document}:{journal_accounting_document_item}"
            )
        if (
            journal_company_code
            and journal_fiscal_year
            and journal_accounting_document
            and journal_accounting_document_item
            and clearing_accounting_document
        ):
            edge_ids.append(
                "settled_by:"
                f"{journal_company_code}:{journal_fiscal_year}:"
                f"{journal_accounting_document}:{journal_accounting_document_item}"
            )
        return list(dict.fromkeys(edge_ids))
