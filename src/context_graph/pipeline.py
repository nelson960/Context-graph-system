from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine

from context_graph.bridges import build_all_bridges, build_bridge_coverage_report
from context_graph.config import APPROVED_VIEWS, ENTITY_CONFIGS
from context_graph.graph import build_graph_tables
from context_graph.io import profile_staging_frames, load_all_staging_frames
from context_graph.normalize import normalize_all_frames
from context_graph.semantic import build_semantic_catalog, create_sql_indexes, create_sql_views


CORE_TABLES = (
    "sales_orders",
    "sales_order_items",
    "deliveries",
    "delivery_items",
    "billing_documents",
    "billing_items",
    "journal_entries_ar",
    "payments_ar",
)

REQUIRED_NODE_TYPES = (
    "SalesOrder",
    "Delivery",
    "BillingDocument",
    "JournalEntry",
    "Payment",
    "Customer",
    "Product",
    "Address",
)

REQUIRED_EDGE_TYPES = (
    "HAS_ITEM",
    "ORDERED_BY",
    "FULFILLED_BY",
    "BILLED_AS",
    "POSTED_TO",
    "SETTLED_BY",
)

ALLOWED_PRIMARY_ANOMALIES = {
    "delivered_not_billed",
    "billed_not_posted",
    "posted_not_paid",
    "quantity_mismatch",
}


def _ensure_output_dirs(output_root: Path) -> dict[str, Path]:
    dirs = {
        "root": output_root,
        "sqlite": output_root / "sqlite",
        "graph": output_root / "graph",
        "reports": output_root / "reports",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _sql_safe_frame(frame: pd.DataFrame) -> pd.DataFrame:
    safe = frame.copy()
    for column in safe.columns:
        safe[column] = safe[column].map(
            lambda value: json.dumps(value, sort_keys=True)
            if isinstance(value, (dict, list))
            else value
        )
    return safe


def _write_frames_to_sqlite(
    db_path: Path,
    staging_frames: dict[str, pd.DataFrame],
    canonical_frames: dict[str, pd.DataFrame],
    bridges: dict[str, pd.DataFrame],
    nodes_df: pd.DataFrame,
    edges_df: pd.DataFrame,
) -> Any:
    if db_path.exists():
        db_path.unlink()
    engine = create_engine(f"sqlite:///{db_path}")

    for config in ENTITY_CONFIGS.values():
        _sql_safe_frame(staging_frames[config.canonical_name]).to_sql(
            f"stg_{config.raw_name}",
            engine,
            if_exists="replace",
            index=False,
        )
        _sql_safe_frame(canonical_frames[config.canonical_name]).to_sql(
            config.canonical_name,
            engine,
            if_exists="replace",
            index=False,
        )

    for bridge_name, bridge_frame in bridges.items():
        _sql_safe_frame(bridge_frame).to_sql(bridge_name, engine, if_exists="replace", index=False)

    _sql_safe_frame(nodes_df).to_sql("graph_nodes", engine, if_exists="replace", index=False)
    _sql_safe_frame(edges_df).to_sql("graph_edges", engine, if_exists="replace", index=False)
    create_sql_indexes(engine)
    create_sql_views(engine)
    return engine


def _build_quality_report(
    manifest_df: pd.DataFrame,
    column_profiles_df: pd.DataFrame,
    schema_variants_df: pd.DataFrame,
    dedupe_df: pd.DataFrame,
    bridge_coverage_df: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "manifest": manifest_df.to_dict(orient="records"),
        "column_profiles": column_profiles_df.to_dict(orient="records"),
        "schema_variants": schema_variants_df.to_dict(orient="records"),
        "deduplication": dedupe_df.to_dict(orient="records"),
        "bridge_coverage": bridge_coverage_df.to_dict(orient="records"),
    }


def _count_rows(engine: Any, relation_name: str) -> int:
    return int(
        pd.read_sql_query(
            f"SELECT COUNT(*) AS count FROM {relation_name}",
            engine,
        )["count"].iloc[0]
    )


def _run_acceptance_checks(engine: Any) -> dict[str, Any]:
    results: dict[str, Any] = {
        "core_tables": {},
        "approved_views": {},
        "graph_integrity": {},
        "flow_integrity": {},
        "anomaly_integrity": {},
        "product_summary": {},
    }

    for table_name in CORE_TABLES:
        row_count = _count_rows(engine, table_name)
        if row_count <= 0:
            raise AssertionError(f"Core table '{table_name}' is empty")
        results["core_tables"][table_name] = row_count

    for view_name in APPROVED_VIEWS:
        row_count = _count_rows(engine, view_name)
        if row_count <= 0:
            raise AssertionError(f"Approved analytical view '{view_name}' returned no rows")
        columns = pd.read_sql_query(f"SELECT * FROM {view_name} LIMIT 0", engine).columns.tolist()
        results["approved_views"][view_name] = {
            "row_count": row_count,
            "columns": columns,
        }

    node_count = _count_rows(engine, "graph_nodes")
    edge_count = _count_rows(engine, "graph_edges")
    if node_count <= 0 or edge_count <= 0:
        raise AssertionError("Graph projection must materialize non-empty nodes and edges")

    node_type_df = pd.read_sql_query(
        """
        SELECT node_type, COUNT(*) AS count
        FROM graph_nodes
        GROUP BY node_type
        """,
        engine,
    )
    node_type_counts = {
        row.node_type: int(row.count)
        for row in node_type_df.itertuples()
    }
    missing_node_types = [
        node_type
        for node_type in REQUIRED_NODE_TYPES
        if node_type_counts.get(node_type, 0) <= 0
    ]
    if missing_node_types:
        raise AssertionError(f"Required node types missing from graph projection: {missing_node_types}")

    edge_type_df = pd.read_sql_query(
        """
        SELECT edge_type, COUNT(*) AS count
        FROM graph_edges
        GROUP BY edge_type
        """,
        engine,
    )
    edge_type_counts = {
        row.edge_type: int(row.count)
        for row in edge_type_df.itertuples()
    }
    missing_edge_types = [
        edge_type
        for edge_type in REQUIRED_EDGE_TYPES
        if edge_type_counts.get(edge_type, 0) <= 0
    ]
    if missing_edge_types:
        raise AssertionError(f"Required edge types missing from graph projection: {missing_edge_types}")
    results["graph_integrity"] = {
        "node_count": node_count,
        "edge_count": edge_count,
        "node_type_counts": node_type_counts,
        "edge_type_counts": edge_type_counts,
    }

    flow_df = pd.read_sql_query(
        """
        SELECT
            COUNT(*) AS trace_rows,
            SUM(CASE WHEN delivery_document IS NOT NULL THEN 1 ELSE 0 END) AS with_delivery,
            SUM(CASE WHEN sales_order IS NOT NULL THEN 1 ELSE 0 END) AS with_sales_order,
            SUM(CASE WHEN journal_accounting_document IS NOT NULL THEN 1 ELSE 0 END) AS with_journal,
            SUM(CASE WHEN clearing_accounting_document IS NOT NULL THEN 1 ELSE 0 END) AS with_payment
        FROM v_billing_trace
        """,
        engine,
    )
    flow_summary = {
        key: int(value)
        for key, value in flow_df.iloc[0].to_dict().items()
    }
    incomplete_flow_segments = [
        key
        for key in ("with_delivery", "with_sales_order", "with_journal", "with_payment")
        if flow_summary[key] <= 0
    ]
    if flow_summary["trace_rows"] <= 0:
        raise AssertionError("Billing trace view did not return any rows")
    if incomplete_flow_segments:
        raise AssertionError(
            "Billing trace view does not contain any end-to-end examples for: "
            + ", ".join(incomplete_flow_segments)
        )
    results["flow_integrity"] = flow_summary

    product_summary_df = pd.read_sql_query(
        """
        SELECT product_id, distinct_billing_documents
        FROM v_product_billing_summary
        WHERE distinct_billing_documents > 0
        ORDER BY distinct_billing_documents DESC, product_id ASC
        LIMIT 5
        """,
        engine,
    )
    if product_summary_df.empty:
        raise AssertionError("Product billing summary did not return any billed products")
    results["product_summary"] = product_summary_df.to_dict(orient="records")

    anomaly_row_count = _count_rows(engine, "v_incomplete_order_flows")
    sales_order_item_count = _count_rows(engine, "sales_order_items")
    if anomaly_row_count != sales_order_item_count:
        raise AssertionError(
            "Incomplete order flow view must preserve one row per sales order item: "
            f"expected {sales_order_item_count}, got {anomaly_row_count}"
        )
    anomaly_values = pd.read_sql_query(
        """
        SELECT DISTINCT primary_anomaly
        FROM v_incomplete_order_flows
        WHERE primary_anomaly IS NOT NULL
        ORDER BY primary_anomaly
        """,
        engine,
    )["primary_anomaly"].tolist()
    unexpected_anomalies = sorted(set(anomaly_values) - ALLOWED_PRIMARY_ANOMALIES)
    if unexpected_anomalies:
        raise AssertionError(
            f"Unexpected anomaly categories found in v_incomplete_order_flows: {unexpected_anomalies}"
        )
    results["anomaly_integrity"] = {
        "row_count": anomaly_row_count,
        "sales_order_item_count": sales_order_item_count,
        "primary_anomalies": anomaly_values,
    }
    return results


def build_context_graph_artifacts(
    dataset_root: str | Path = "sap-o2c-data",
    output_root: str | Path = "artifacts",
) -> dict[str, Any]:
    dataset_root = Path(dataset_root)
    output_root = Path(output_root)
    if not dataset_root.exists():
        raise FileNotFoundError(f"Dataset root does not exist: {dataset_root}")

    dirs = _ensure_output_dirs(output_root)
    staging_frames = load_all_staging_frames(dataset_root)
    manifest_df, column_profiles_df, schema_variants_df = profile_staging_frames(staging_frames)
    canonical_frames, dedupe_df = normalize_all_frames(staging_frames)
    bridges = build_all_bridges(canonical_frames)
    bridge_coverage_df = build_bridge_coverage_report(bridges)
    nodes_df, edges_df = build_graph_tables(canonical_frames, bridges)

    db_path = dirs["sqlite"] / "context_graph.db"
    engine = _write_frames_to_sqlite(
        db_path=db_path,
        staging_frames=staging_frames,
        canonical_frames=canonical_frames,
        bridges=bridges,
        nodes_df=nodes_df,
        edges_df=edges_df,
    )

    nodes_path = dirs["graph"] / "graph_nodes.csv"
    edges_path = dirs["graph"] / "graph_edges.csv"
    nodes_df.to_csv(nodes_path, index=False)
    edges_df.to_csv(edges_path, index=False)

    manifest_df.to_csv(dirs["reports"] / "dataset_manifest.csv", index=False)
    column_profiles_df.to_csv(dirs["reports"] / "column_profiles.csv", index=False)
    schema_variants_df.to_csv(dirs["reports"] / "schema_variants.csv", index=False)
    dedupe_df.to_csv(dirs["reports"] / "deduplication_report.csv", index=False)
    bridge_coverage_df.to_csv(dirs["reports"] / "bridge_coverage.csv", index=False)

    quality_report = _build_quality_report(
        manifest_df=manifest_df,
        column_profiles_df=column_profiles_df,
        schema_variants_df=schema_variants_df,
        dedupe_df=dedupe_df,
        bridge_coverage_df=bridge_coverage_df,
    )
    _write_json(dirs["reports"] / "quality_report.json", quality_report)
    _write_json(dirs["reports"] / "semantic_catalog.json", build_semantic_catalog())

    acceptance_results = _run_acceptance_checks(engine)
    _write_json(dirs["reports"] / "acceptance_checks.json", acceptance_results)

    return {
        "dataset_root": str(dataset_root),
        "output_root": str(output_root),
        "db_path": str(db_path),
        "graph_nodes_path": str(nodes_path),
        "graph_edges_path": str(edges_path),
        "manifest_path": str(dirs["reports"] / "dataset_manifest.csv"),
        "quality_report_path": str(dirs["reports"] / "quality_report.json"),
        "semantic_catalog_path": str(dirs["reports"] / "semantic_catalog.json"),
        "acceptance_checks_path": str(dirs["reports"] / "acceptance_checks.json"),
    }
