from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine

from context_graph.bridges import build_all_bridges, build_bridge_coverage_report
from context_graph.config import ENTITY_CONFIGS
from context_graph.graph import build_graph_tables
from context_graph.io import profile_staging_frames, load_all_staging_frames
from context_graph.normalize import normalize_all_frames
from context_graph.semantic import build_semantic_catalog, create_sql_indexes, create_sql_views


EXPECTED_COUNTS = {
    "sales_orders": 100,
    "sales_order_items": 167,
    "deliveries": 86,
    "delivery_items": 137,
    "billing_documents": 163,
    "billing_items": 245,
    "journal_entries_ar": 123,
    "payments_ar": 120,
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


def _run_acceptance_checks(engine: Any) -> dict[str, Any]:
    results: dict[str, Any] = {"counts": {}, "trace_check": {}, "ranking_check": {}, "anomaly_check": {}}

    for table_name, expected_count in EXPECTED_COUNTS.items():
        actual_count = int(pd.read_sql_query(f"SELECT COUNT(*) AS count FROM {table_name}", engine)["count"].iloc[0])
        if actual_count != expected_count:
            raise AssertionError(f"Unexpected row count for {table_name}: expected {expected_count}, got {actual_count}")
        results["counts"][table_name] = actual_count

    trace_df = pd.read_sql_query(
        """
        SELECT DISTINCT
            billing_document,
            delivery_document,
            sales_order,
            journal_accounting_document,
            clearing_accounting_document
        FROM v_billing_trace
        WHERE billing_document = '90504219'
        """,
        engine,
    )
    if trace_df.empty:
        raise AssertionError("Billing trace view returned no rows for billing document 90504219")
    if set(trace_df["delivery_document"].dropna()) != {"80738051"}:
        raise AssertionError(f"Unexpected delivery trace for 90504219: {trace_df['delivery_document'].tolist()}")
    if set(trace_df["sales_order"].dropna()) != {"740520"}:
        raise AssertionError(f"Unexpected sales order trace for 90504219: {trace_df['sales_order'].tolist()}")
    if set(trace_df["journal_accounting_document"].dropna()) != {"9400000220"}:
        raise AssertionError(
            f"Unexpected journal trace for 90504219: {trace_df['journal_accounting_document'].tolist()}"
        )
    if "9400635977" not in set(trace_df["clearing_accounting_document"].dropna()):
        raise AssertionError(
            "Expected payment clearing accounting document 9400635977 for billing document 90504219"
        )
    results["trace_check"] = trace_df.to_dict(orient="records")

    ranking_df = pd.read_sql_query(
        """
        SELECT product_id, distinct_billing_documents
        FROM v_product_billing_summary
        ORDER BY distinct_billing_documents DESC, product_id ASC
        LIMIT 2
        """,
        engine,
    )
    expected_ranking = {
        ("S8907367008620", 22),
        ("S8907367039280", 22),
    }
    actual_ranking = {(row.product_id, int(row.distinct_billing_documents)) for row in ranking_df.itertuples()}
    if actual_ranking != expected_ranking:
        raise AssertionError(f"Unexpected top billed products: {actual_ranking}")
    results["ranking_check"] = ranking_df.to_dict(orient="records")

    anomaly_df = pd.read_sql_query(
        """
        SELECT DISTINCT sales_order
        FROM v_incomplete_order_flows
        WHERE primary_anomaly = 'delivered_not_billed'
        ORDER BY sales_order
        """,
        engine,
    )
    expected_anomalies = {"740506", "740507", "740508"}
    actual_anomalies = set(anomaly_df["sales_order"].astype(str))
    if not expected_anomalies.issubset(actual_anomalies):
        raise AssertionError(f"Expected delivered_not_billed orders not found: {expected_anomalies - actual_anomalies}")
    results["anomaly_check"] = anomaly_df.to_dict(orient="records")
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
