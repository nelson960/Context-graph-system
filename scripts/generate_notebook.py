from __future__ import annotations

import sys
from pathlib import Path

import nbformat as nbf


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def build_notebook() -> nbf.NotebookNode:
    notebook = nbf.v4.new_notebook()
    notebook.cells = [
        nbf.v4.new_markdown_cell(
            """# Context Graph Notebook

This notebook profiles the SAP order-to-cash dataset, normalizes it into canonical SQL tables, derives explicit bridge tables, projects the data into a graph, and demonstrates grounded business queries over the generated SQLite warehouse."""
        ),
        nbf.v4.new_code_cell(
            """from pathlib import Path
import json
import sys

import pandas as pd
from sqlalchemy import create_engine

PROJECT_ROOT = Path.cwd()
if not (PROJECT_ROOT / "src").exists():
    PROJECT_ROOT = PROJECT_ROOT.parent
if not (PROJECT_ROOT / "src").exists():
    raise FileNotFoundError("Could not locate the project root containing src/")

SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from context_graph.pipeline import build_context_graph_artifacts
from context_graph.graph import build_networkx_graph, focused_subgraph, plot_subgraph
from context_graph.semantic import run_sql_query
"""
        ),
        nbf.v4.new_markdown_cell(
            """## 1. Build the normalized warehouse, bridge tables, graph projection, and reports"""
        ),
        nbf.v4.new_code_cell(
            """artifacts = build_context_graph_artifacts(
    dataset_root=PROJECT_ROOT / "sap-o2c-data",
    output_root=PROJECT_ROOT / "artifacts",
)
artifacts"""
        ),
        nbf.v4.new_markdown_cell("""## 2. Dataset manifest and schema profiling"""),
        nbf.v4.new_code_cell(
            """manifest_df = pd.read_csv(PROJECT_ROOT / "artifacts" / "reports" / "dataset_manifest.csv")
schema_variants_df = pd.read_csv(PROJECT_ROOT / "artifacts" / "reports" / "schema_variants.csv")
column_profiles_df = pd.read_csv(PROJECT_ROOT / "artifacts" / "reports" / "column_profiles.csv")

manifest_df"""
        ),
        nbf.v4.new_code_cell(
            """schema_variants_df.head(20)"""
        ),
        nbf.v4.new_code_cell(
            """column_profiles_df[column_profiles_df["candidate_join_column"] == True].head(30)"""
        ),
        nbf.v4.new_markdown_cell("""## 3. Bridge coverage and acceptance checks"""),
        nbf.v4.new_code_cell(
            """bridge_coverage_df = pd.read_csv(PROJECT_ROOT / "artifacts" / "reports" / "bridge_coverage.csv")
acceptance_checks = json.loads((PROJECT_ROOT / "artifacts" / "reports" / "acceptance_checks.json").read_text())

bridge_coverage_df"""
        ),
        nbf.v4.new_code_cell("""acceptance_checks"""),
        nbf.v4.new_markdown_cell("""## 4. Connect to the SQLite warehouse"""),
        nbf.v4.new_code_cell(
            """engine = create_engine(f"sqlite:///{PROJECT_ROOT / 'artifacts' / 'sqlite' / 'context_graph.db'}")"""
        ),
        nbf.v4.new_markdown_cell("""## 5. Golden business queries"""),
        nbf.v4.new_code_cell(
            """top_products = run_sql_query(
    engine,
    '''
    SELECT product_id, product_description, distinct_billing_documents, billing_item_count, total_billed_amount
    FROM v_product_billing_summary
    ORDER BY distinct_billing_documents DESC, product_id ASC
    LIMIT 10
    '''
)
top_products"""
        ),
        nbf.v4.new_code_cell(
            """billing_trace = run_sql_query(
    engine,
    '''
    SELECT *
    FROM v_billing_trace
    WHERE billing_document = '90504219'
    ORDER BY billing_document_item
    '''
)
billing_trace"""
        ),
        nbf.v4.new_code_cell(
            """delivered_not_billed = run_sql_query(
    engine,
    '''
    SELECT DISTINCT sales_order, customer_id
    FROM v_incomplete_order_flows
    WHERE primary_anomaly = 'delivered_not_billed'
    ORDER BY sales_order
    '''
)
delivered_not_billed"""
        ),
        nbf.v4.new_code_cell(
            """customer_360 = run_sql_query(
    engine,
    '''
    SELECT *
    FROM v_customer_360
    ORDER BY billing_document_count DESC, customer_id ASC
    LIMIT 5
    '''
)
customer_360"""
        ),
        nbf.v4.new_markdown_cell("""## 6. Build a focused graph for visualization"""),
        nbf.v4.new_code_cell(
            """nodes_df = pd.read_csv(PROJECT_ROOT / "artifacts" / "graph" / "graph_nodes.csv")
edges_df = pd.read_csv(PROJECT_ROOT / "artifacts" / "graph" / "graph_edges.csv")
graph = build_networkx_graph(nodes_df, edges_df)

len(nodes_df), len(edges_df)"""
        ),
        nbf.v4.new_code_cell(
            """trace_highlights = {
    "billing_document:90504219",
    "delivery:80738051",
    "sales_order:740520",
    "journal_entry:ABCD:2025:9400000220:1",
    "payment:ABCD:2025:9400000220:1",
}
trace_subgraph = focused_subgraph(
    graph,
    center_node_id="billing_document:90504219",
    depth=3,
    include_item_nodes=True,
    max_nodes=30,
)
plot_subgraph(trace_subgraph, title="Billing document 90504219 focused trace", highlight_node_ids=trace_highlights)"""
        ),
        nbf.v4.new_code_cell(
            """anomaly_highlights = {"sales_order:740506"}
anomaly_subgraph = focused_subgraph(
    graph,
    center_node_id="sales_order:740506",
    depth=3,
    include_item_nodes=False,
    max_nodes=30,
)
plot_subgraph(anomaly_subgraph, title="Delivered-not-billed order 740506", highlight_node_ids=anomaly_highlights)"""
        ),
        nbf.v4.new_markdown_cell("""## 7. Semantic catalog handoff"""),
        nbf.v4.new_code_cell(
            """semantic_catalog = json.loads((PROJECT_ROOT / "artifacts" / "reports" / "semantic_catalog.json").read_text())
semantic_catalog["approved_views"]"""
        ),
    ]
    return notebook


def main() -> None:
    notebook = build_notebook()
    output_path = PROJECT_ROOT / "notebooks" / "context_graph_notebook.ipynb"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(notebook, output_path)
    print(output_path)


if __name__ == "__main__":
    main()
