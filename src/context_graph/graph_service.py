from __future__ import annotations

import json
from collections import deque
from pathlib import Path
from typing import Any

import networkx as nx
import pandas as pd
from sqlalchemy import create_engine

from context_graph.graph import HIDDEN_BY_DEFAULT, build_networkx_graph
from context_graph.schemas import EdgeDTO, GraphResponse, NodeDTO


FLOW_EDGE_TYPES = {
    "HAS_ITEM",
    "HAS_SCHEDULE_LINE",
    "FULFILLED_BY",
    "PART_OF_DELIVERY",
    "BILLED_AS",
    "PART_OF_BILLING",
    "POSTED_TO",
    "SETTLED_BY",
    "CANCELS",
}


class GraphService:
    def __init__(self, db_path: Path, max_nodes: int, max_edges: int) -> None:
        self._db_path = db_path
        self._max_nodes = max_nodes
        self._max_edges = max_edges
        engine = create_engine(f"sqlite:///{db_path}")
        self._nodes_df = pd.read_sql_table("graph_nodes", engine)
        self._edges_df = pd.read_sql_table("graph_edges", engine)
        self._graph = build_networkx_graph(self._nodes_df, self._edges_df)
        self._node_id_index = set(self._nodes_df["node_id"].tolist())
        self._edge_id_index = set(self._edges_df["edge_id"].tolist())
        self._edge_by_id = {
            row["edge_id"]: dict(row)
            for _, row in self._edges_df.iterrows()
        }

    def has_node(self, node_id: str) -> bool:
        return node_id in self._node_id_index

    def get_node(self, node_id: str) -> NodeDTO:
        if node_id not in self._graph:
            raise KeyError(f"Unknown node id: {node_id}")
        node = dict(self._graph.nodes[node_id])
        node["inbound_edge_count"] = int(self._graph.in_degree(node_id))
        node["outbound_edge_count"] = int(self._graph.out_degree(node_id))
        return self._node_dto(node)

    def get_subgraph(
        self,
        node_id: str,
        depth: int,
        include_hidden: bool = False,
        max_nodes: int | None = None,
        max_edges: int | None = None,
        cluster_mode: str | None = None,
    ) -> GraphResponse:
        node_ids = self._collect_neighborhood(
            start_nodes=[node_id],
            depth=depth,
            include_hidden=include_hidden,
            max_nodes=max_nodes or self._max_nodes,
        )
        response = self._build_graph_response(
            center_node_id=node_id,
            node_ids=node_ids,
            include_hidden=include_hidden,
            max_edges=max_edges or self._max_edges,
            depth=depth,
        )
        return self._apply_cluster_mode(response, cluster_mode)

    def get_path(
        self,
        node_id: str,
        direction: str,
        depth: int,
        cluster_mode: str | None = None,
    ) -> GraphResponse:
        if node_id not in self._graph:
            raise KeyError(f"Unknown node id: {node_id}")
        selected = self._collect_directional_flow(node_id=node_id, direction=direction, depth=depth)
        response = self._build_graph_response(
            center_node_id=node_id,
            node_ids=selected,
            include_hidden=True,
            max_edges=max(self._max_edges * 2, 120),
            depth=depth,
        )
        return self._apply_cluster_mode(response, cluster_mode)

    def get_combined_subgraph(
        self,
        node_ids: list[str],
        depth: int,
        include_hidden: bool = False,
        cluster_mode: str | None = None,
    ) -> GraphResponse | None:
        valid_nodes = [node_id for node_id in node_ids if node_id in self._graph]
        if not valid_nodes:
            return None
        combined = self._collect_neighborhood(
            start_nodes=valid_nodes,
            depth=depth,
            include_hidden=include_hidden,
            max_nodes=self._max_nodes,
        )
        combined.update(self._connect_nodes(valid_nodes))
        response = self._build_graph_response(
            center_node_id=valid_nodes[0],
            node_ids=combined,
            include_hidden=include_hidden,
            max_edges=self._max_edges,
            depth=depth,
        )
        return self._apply_cluster_mode(response, cluster_mode)

    def node_ids_from_rows(
        self,
        rows: list[dict[str, Any]],
        additional_nodes: list[str] | None = None,
    ) -> list[str]:
        found: set[str] = set(additional_nodes or [])
        for row in rows:
            found.update(self._node_ids_from_row(row))
        return [node_id for node_id in found if node_id in self._node_id_index]

    def infer_center_node(self, node_ids: list[str]) -> str | None:
        for node_id in node_ids:
            if node_id in self._node_id_index:
                return node_id
        return None

    def filter_existing_node_ids(self, node_ids: list[str]) -> list[str]:
        return [node_id for node_id in node_ids if node_id in self._node_id_index]

    def filter_existing_edge_ids(self, edge_ids: list[str]) -> list[str]:
        return [edge_id for edge_id in edge_ids if edge_id in self._edge_id_index]

    def get_nodes(self, node_ids: list[str]) -> list[NodeDTO]:
        return [self.get_node(node_id) for node_id in node_ids if node_id in self._node_id_index]

    def get_edges(self, edge_ids: list[str]) -> list[EdgeDTO]:
        return [
            self._edge_dto(self._edge_by_id[edge_id])
            for edge_id in edge_ids
            if edge_id in self._edge_by_id
        ]

    def _build_graph_response(
        self,
        center_node_id: str,
        node_ids: set[str],
        include_hidden: bool,
        max_edges: int,
        depth: int,
    ) -> GraphResponse:
        filtered_nodes = {
            node_id
            for node_id in node_ids
            if include_hidden
            or self._graph.nodes[node_id]["node_type"] not in HIDDEN_BY_DEFAULT
            or node_id == center_node_id
        }
        edges = self._edges_for_nodes(filtered_nodes, max_edges=max_edges)
        referenced_nodes = {center_node_id}
        for edge in edges:
            referenced_nodes.add(edge["source_id"])
            referenced_nodes.add(edge["target_id"])
        nodes = [self._node_dto(dict(self._graph.nodes[node_id])) for node_id in sorted(referenced_nodes)]
        edge_models = [self._edge_dto(edge) for edge in edges]
        return GraphResponse(center_node_id=center_node_id, depth=depth, nodes=nodes, edges=edge_models)

    def _apply_cluster_mode(
        self,
        response: GraphResponse,
        cluster_mode: str | None,
    ) -> GraphResponse:
        if cluster_mode != "type":
            return response
        preserved_node_ids = {response.center_node_id}
        clustered_groups: dict[str, list[NodeDTO]] = {}
        passthrough_nodes: list[NodeDTO] = []
        for node in response.nodes:
            if node.id in preserved_node_ids:
                passthrough_nodes.append(node)
                continue
            clustered_groups.setdefault(node.type, []).append(node)

        cluster_map: dict[str, str] = {}
        cluster_nodes: list[NodeDTO] = []
        for node_type, nodes in clustered_groups.items():
            if len(nodes) < 3:
                passthrough_nodes.extend(nodes)
                continue
            cluster_id = f"cluster:type:{node_type}"
            for node in nodes:
                cluster_map[node.id] = cluster_id
            cluster_nodes.append(
                NodeDTO(
                    id=cluster_id,
                    type=node_type,
                    business_key=f"{node_type.lower()}-cluster",
                    display_label=f"{node_type} ({len(nodes)})",
                    subtitle="type cluster",
                    status=None,
                    amount=None,
                    currency=None,
                    document_date=None,
                    source_tables=[],
                    metadata={
                        "is_cluster": True,
                        "cluster_mode": "type",
                        "member_count": len(nodes),
                        "member_node_ids": [node.id for node in nodes],
                    },
                    default_visible=True,
                    inbound_edge_count=None,
                    outbound_edge_count=None,
                )
            )

        if not cluster_map:
            response.cluster_mode = cluster_mode
            return response

        aggregated_edges: dict[tuple[str, str, str], dict[str, Any]] = {}
        for edge in response.edges:
            source = cluster_map.get(edge.source, edge.source)
            target = cluster_map.get(edge.target, edge.target)
            if source == target:
                continue
            key = (source, target, edge.type)
            existing = aggregated_edges.get(key)
            if existing is None:
                aggregated_edges[key] = {
                    "id": edge.id if source == edge.source and target == edge.target else f"cluster:{edge.type}:{source}:{target}",
                    "type": edge.type,
                    "source": source,
                    "target": target,
                    "link_status": edge.link_status,
                    "derivation_rule": edge.derivation_rule,
                    "provenance_columns": list(edge.provenance_columns),
                    "quantity": edge.quantity,
                    "date": edge.date,
                    "metadata": dict(edge.metadata),
                }
                continue
            existing_metadata = dict(existing["metadata"])
            existing_metadata["aggregated_edge_count"] = existing_metadata.get("aggregated_edge_count", 1) + 1
            existing["metadata"] = existing_metadata

        return GraphResponse(
            center_node_id=response.center_node_id,
            depth=response.depth,
            cluster_mode=cluster_mode,
            nodes=sorted(
                passthrough_nodes + cluster_nodes,
                key=lambda node: (node.id != response.center_node_id, node.type, node.display_label),
            ),
            edges=[EdgeDTO(**edge_payload) for edge_payload in aggregated_edges.values()],
        )

    def _collect_neighborhood(
        self,
        start_nodes: list[str],
        depth: int,
        include_hidden: bool,
        max_nodes: int,
    ) -> set[str]:
        queue: deque[tuple[str, int]] = deque()
        visited: set[str] = set()
        for node_id in start_nodes:
            if node_id in self._graph:
                queue.append((node_id, 0))
                visited.add(node_id)

        undirected = self._graph.to_undirected()
        while queue and len(visited) < max_nodes:
            node_id, current_depth = queue.popleft()
            if current_depth >= depth:
                continue
            for neighbor in undirected.neighbors(node_id):
                if neighbor in visited:
                    continue
                if not include_hidden and self._graph.nodes[neighbor]["node_type"] in HIDDEN_BY_DEFAULT:
                    continue
                visited.add(neighbor)
                queue.append((neighbor, current_depth + 1))
                if len(visited) >= max_nodes:
                    break
        return visited

    def _collect_directional_flow(self, node_id: str, direction: str, depth: int) -> set[str]:
        visited = {node_id}
        queue: deque[tuple[str, int]] = deque([(node_id, 0)])
        while queue:
            current, current_depth = queue.popleft()
            if current_depth >= depth:
                continue
            neighbors: set[str] = set()
            if direction in {"downstream", "both"}:
                for _, target, edge_data in self._graph.out_edges(current, data=True):
                    if edge_data["edge_type"] in FLOW_EDGE_TYPES:
                        neighbors.add(target)
            if direction in {"upstream", "both"}:
                for source, _, edge_data in self._graph.in_edges(current, data=True):
                    if edge_data["edge_type"] in FLOW_EDGE_TYPES:
                        neighbors.add(source)
            for neighbor in neighbors:
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                queue.append((neighbor, current_depth + 1))
        return visited

    def _connect_nodes(self, node_ids: list[str]) -> set[str]:
        if len(node_ids) < 2:
            return set(node_ids)
        undirected = self._graph.to_undirected()
        connected: set[str] = set(node_ids)
        for left_index, left_node in enumerate(node_ids):
            for right_node in node_ids[left_index + 1 :]:
                try:
                    path = nx.shortest_path(undirected, left_node, right_node)
                except (nx.NetworkXNoPath, nx.NodeNotFound):
                    continue
                connected.update(path)
        return connected

    def _edges_for_nodes(self, node_ids: set[str], max_edges: int) -> list[dict[str, Any]]:
        selected_edges = [
            dict(edge_data)
            for source, target, edge_data in self._graph.edges(data=True)
            if source in node_ids and target in node_ids
        ]
        selected_edges.sort(key=lambda edge: edge["edge_id"])
        return selected_edges[:max_edges]

    def _node_dto(self, raw: dict[str, Any]) -> NodeDTO:
        metadata = self._clean_json_value(json.loads(raw.get("metadata_json") or "{}"))
        return NodeDTO(
            id=raw["node_id"],
            type=raw["node_type"],
            business_key=str(raw["business_key"]),
            display_label=str(raw["display_label"]),
            subtitle=self._clean_json_value(raw.get("subtitle")),
            status=self._clean_json_value(raw.get("status")),
            amount=self._clean_json_value(raw.get("amount")),
            currency=self._clean_json_value(raw.get("currency")),
            document_date=self._clean_json_value(raw.get("document_date")),
            source_tables=[
                item for item in str(self._clean_json_value(raw.get("source_tables")) or "").split(",") if item
            ],
            metadata=metadata,
            default_visible=bool(raw.get("default_visible")),
            inbound_edge_count=self._clean_json_value(raw.get("inbound_edge_count")),
            outbound_edge_count=self._clean_json_value(raw.get("outbound_edge_count")),
        )

    def _edge_dto(self, raw: dict[str, Any]) -> EdgeDTO:
        metadata = self._clean_json_value(json.loads(raw.get("metadata_json") or "{}"))
        return EdgeDTO(
            id=raw["edge_id"],
            type=raw["edge_type"],
            source=raw["source_id"],
            target=raw["target_id"],
            link_status=raw["link_status"],
            derivation_rule=raw["derivation_rule"],
            provenance_columns=[
                item
                for item in str(self._clean_json_value(raw.get("provenance_columns")) or "").split(",")
                if item
            ],
            quantity=self._clean_json_value(raw.get("quantity")),
            date=self._clean_json_value(raw.get("edge_date")),
            metadata=metadata,
        )

    def _node_ids_from_row(self, row: dict[str, Any]) -> set[str]:
        found: set[str] = set()
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
        }
        for column_name, prefix in simple_mappings.items():
            value = row.get(column_name)
            if value is not None:
                found.add(f"{prefix}:{value}")

        if row.get("sales_order") and row.get("sales_order_item"):
            found.add(f"sales_order_item:{row['sales_order']}:{row['sales_order_item']}")
        if row.get("delivery_document") and row.get("delivery_document_item"):
            found.add(f"delivery_item:{row['delivery_document']}:{row['delivery_document_item']}")
        if row.get("billing_document") and row.get("billing_document_item"):
            found.add(f"billing_item:{row['billing_document']}:{row['billing_document_item']}")

        journal_parts = [
            row.get("journal_company_code"),
            row.get("journal_fiscal_year"),
            row.get("journal_accounting_document"),
            row.get("journal_accounting_document_item"),
        ]
        if all(part is not None for part in journal_parts):
            company_code, fiscal_year, accounting_document, accounting_document_item = journal_parts
            found.add(
                f"journal_entry:{company_code}:{fiscal_year}:{accounting_document}:{accounting_document_item}"
            )
            if row.get("clearing_accounting_document") is not None:
                found.add(
                    f"payment:{company_code}:{fiscal_year}:{accounting_document}:{accounting_document_item}"
                )
        return found

    def _clean_json_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self._clean_json_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._clean_json_value(item) for item in value]
        if pd.isna(value):
            return None
        return value
