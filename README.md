# Context Graph Architecture

This repository implements a grounded context graph system for an SAP order-to-cash dataset. The system is organized as a layered application: raw dataset ingestion, semantic normalization, SQL-first analytics, graph projection, query orchestration, and a graph-plus-chat UI.

All project references below use repository-relative paths only.

## Architecture Goals

- Keep the dataset as the source of truth while projecting it into a graph for exploration.
- Use SQL and curated semantic views for authoritative analytics.
- Use the model as a planner and explainer, never as the source of truth.
- Reject out-of-domain requests instead of inventing answers.
- Preserve explicit evidence, citations, and errors through the full request path.

## Architecture Decisions

The system is built around a few explicit design decisions.

- SQL is the truth layer, graph is the context layer
  Canonical business facts live in `artifacts/sqlite/context_graph.db`, while graph structures are derived from `graph_nodes` and `graph_edges` and loaded by `src/context_graph/graph_service.py`.
- Query orchestration is route-aware instead of SQL-only
  `src/context_graph/query_service.py` decides between `sql`, `graph`, and `hybrid` execution paths based on grounded entities and validated query plans.
- The model is used as a planner and explainer, not as the database
  `src/context_graph/planner.py` produces structured plans, SQL envelopes, and answer envelopes, but the returned answer must be based on executed SQL rows or graph evidence.
- Deterministic grounding is preferred when the input is obviously an entity lookup
  `src/context_graph/query_service.py` can short-circuit prompts like direct document or plant lookups before model planning when entity resolution is clear.
- Conversation memory is structured, not freeform
  `src/context_graph/conversation_store.py` persists resolved entities, highlighted nodes, filters, and prior route/intent state in SQLite so follow-up turns remain grounded.

These choices are meant to keep the system inspectable, testable, and resistant to “chatbot drift”.

## Top-Level Layout

- `src/context_graph/`
  Core backend package: ingestion pipeline, semantic model, runtime services, API, graph service, planner integration, validation, and query orchestration.
- `frontend/`
  React + TypeScript client for graph exploration and chat.
- `scripts/`
  Build and notebook generation entrypoints.
- `notebooks/`
  Notebook artifact for the notebook-first milestone.
- `tests/`
  API, routing, search, clustering, and streaming tests.
- `artifacts/`
  Generated SQLite warehouse, graph exports, semantic catalog, and reports.
- `sap-o2c-data/`
  Raw SAP-style dataset input used by the build pipeline.

## System Layers

### 1. Ingestion and Normalization

The ingestion layer scans the fragmented dataset, profiles schemas, normalizes types, and materializes canonical business tables plus bridge tables.

Primary files:

- `src/context_graph/io.py`
- `src/context_graph/normalize.py`
- `src/context_graph/bridges.py`
- `src/context_graph/semantic.py`
- `src/context_graph/pipeline.py`
- `scripts/build_context_graph.py`

Responsibilities:

- Read fragmented JSONL extracts from `sap-o2c-data/`
- Normalize identifiers, dates, amounts, quantities, and nulls
- Materialize canonical tables for sales orders, deliveries, billing, journal entries, payments, customers, products, plants, and assignments
- Derive bridge tables that resolve document flow across fragmented references
- Persist the normalized warehouse in `artifacts/sqlite/context_graph.db`

Output shape:

- Staging tables for raw ingestion provenance
- Canonical fact and dimension tables
- Bridge tables for order-to-delivery, delivery-to-billing, billing-to-journal, and journal-to-payment
- Curated SQL views used by the query layer

### 2. Semantic SQL Layer

The semantic layer is the analytical contract exposed to the planner and SQL validator. The model does not query raw ingestion tables directly.

Primary files:

- `src/context_graph/catalog_service.py`
- `src/context_graph/config.py`
- `src/context_graph/sql_guard.py`

Approved analytical surfaces:

- `v_sales_order_flow`
- `v_delivery_flow`
- `v_billing_flow`
- `v_financial_flow`
- `v_customer_360`
- `v_product_billing_summary`
- `v_incomplete_order_flows`
- `v_billing_trace`

Why this layer exists:

- Keep business joins stable and centralized
- Constrain the planner to a small approved schema
- Make SQL validation practical
- Improve reproducibility of analytics and anomaly detection

## Database Choice

The current implementation uses SQLite as the primary operational store.

Primary files:

- `src/context_graph/settings.py`
- `src/context_graph/runtime.py`
- `src/context_graph/sql_guard.py`
- `src/context_graph/graph_service.py`

Why SQLite was chosen for this system:

- The dataset size fits comfortably in a local file-backed analytical store
- The notebook-first milestone benefits from a single inspectable database artifact
- SQL validation and query safety are simpler to enforce against a constrained local engine
- Local demo setup is simpler than introducing a second service such as PostgreSQL or Neo4j

How it is used:

- Canonical normalized tables and semantic views are stored in `artifacts/sqlite/context_graph.db`
- `src/context_graph/sql_guard.py` validates and executes read-only SQL against that database
- `src/context_graph/graph_service.py` loads `graph_nodes` and `graph_edges` from SQLite and builds an in-memory NetworkX graph for traversal
- `src/context_graph/conversation_store.py` stores runtime conversation state in a separate writable SQLite file
- `src/context_graph/sqlite_utils.py` enforces a read-only analytics connection path and a distinct writable runtime connection path

Tradeoffs:

- This is the right choice for the current local-demo architecture
- The implementation is SQLAlchemy-compatible at the graph-loading layer, but the guarded SQL path and planner prompts are currently SQLite-specific
- Moving to PostgreSQL later is feasible, but would require updating SQL dialect assumptions in the planner and validator
- Deployment works best when the analytical warehouse is treated as a bundled read-only artifact and transient runtime state is redirected to a writable location such as `/tmp`

### 3. Graph Projection

The graph layer projects canonical data into a graph of typed nodes and edges. SQL remains the system of record; the graph is a derived context structure optimized for exploration and lineage.

Primary files:

- `src/context_graph/graph.py`
- `src/context_graph/graph_service.py`
- `src/context_graph/evidence_service.py`

Node families:

- Transaction nodes: sales order, sales order item, schedule line, delivery, delivery item, billing document, billing item, journal entry, payment
- Master/reference nodes: customer, address, product, plant, storage location, company code, sales area

Edge families:

- Flow edges: `HAS_ITEM`, `HAS_SCHEDULE_LINE`, `FULFILLED_BY`, `PART_OF_DELIVERY`, `BILLED_AS`, `PART_OF_BILLING`, `POSTED_TO`, `SETTLED_BY`, `CANCELS`
- Master-data edges: `ORDERED_BY`, `DELIVERED_TO`, `HAS_ADDRESS`, `REFERS_TO_PRODUCT`, `SHIPPED_FROM`, `AVAILABLE_AT`, `STORED_AT`, `ASSIGNED_TO_COMPANY`, `ASSIGNED_TO_SALES_AREA`

Graph behavior:

- Full graph is loaded in backend memory
- UI receives focused subgraphs only
- Path tracing uses directional flow traversal
- Combined subgraphs connect multiple resolved entities
- Type clustering can collapse dense same-type neighborhoods into synthetic cluster nodes

### 4. Query Orchestration

The query layer decides how a natural-language request is handled: SQL analytics, graph traversal, or hybrid behavior.

Primary files:

- `src/context_graph/query_service.py`
- `src/context_graph/plan_validator.py`
- `src/context_graph/entity_service.py`
- `src/context_graph/conversation_store.py`
- `src/context_graph/planner.py`

Execution flow:

1. Load conversation context and selected graph state
2. Ground the prompt with candidate entity matches from search
3. Short-circuit clear direct entity lookups when deterministic grounding is sufficient
4. Otherwise ask the model for a validated structured plan
5. Validate route, intent, and entity requirements
6. Execute either:
   - graph traversal
   - SQL generation + SQL validation + SQL execution
   - hybrid SQL with graph evidence projection
7. Compose the final answer from executed evidence only
8. Persist conversation state, citations, and highlights

Current route model:

- `sql`
  Aggregate analytics and anomaly-oriented queries over approved views
- `graph`
  Entity lookup, relationship exploration, and document-trace style questions
- `hybrid`
  SQL-backed analytics plus graph highlighting for returned entities

## LLM Prompting Strategy

The prompting strategy is intentionally narrow and structured.

Primary files:

- `src/context_graph/planner.py`
- `src/context_graph/catalog_service.py`
- `src/context_graph/query_service.py`

The model is used in three modes:

- Planning
  The planner prompt asks for a strict `PlannerEnvelope` JSON object describing intent, route, entities, filters, metrics, and output shape.
- SQL generation
  The SQL prompt asks for a strict `SqlEnvelope` JSON object containing read-only SQL over approved analytical views only.
- Answer composition
  The answer prompt asks for a strict `AnswerEnvelope` JSON object grounded only in executed rows or graph evidence.

Prompting decisions:

- Semantic context is injected from `src/context_graph/catalog_service.py` instead of exposing raw table internals
- Few-shot examples in `src/context_graph/planner.py` cover ranking, trace, anomaly, lookup, and out-of-domain refusal patterns
- Conversation memory and candidate entity matches are passed into planning so short follow-up prompts can stay grounded
- The SQL prompt narrows the model onto approved views such as `v_product_billing_summary`, `v_billing_trace`, and `v_incomplete_order_flows`
- The answer prompt explicitly says the SQL result is the source of truth and forbids invented entities, counts, or links
- The streaming answer prompt is plain-text only so the frontend can progressively render the response without parsing structured output mid-stream

This strategy reduces hallucination by separating planning, query generation, and explanation into independent constrained steps.

### 5. Guardrails and Validation

The system is intentionally restrictive.

Primary files:

- `src/context_graph/sql_guard.py`
- `src/context_graph/plan_validator.py`
- `src/context_graph/exceptions.py`

Guardrail boundaries:

- No raw-table SQL from the planner
- No write queries
- No hidden fallback answers
- No fabricated graph links
- No out-of-domain general knowledge responses

Validation happens at multiple layers:

- Entity resolution
- Query-plan route validation
- SQL allowlist validation
- Read-only execution constraints
- Row-bound and timeout enforcement

## Guardrails

The system guardrails are implemented in code, not just described in prompts.

Primary files:

- `src/context_graph/planner.py`
- `src/context_graph/plan_validator.py`
- `src/context_graph/sql_guard.py`
- `src/context_graph/query_service.py`

Guardrail categories:

- Domain guardrails
  The planner must return an explicit refusal for prompts outside the SAP order-to-cash dataset.
- Schema guardrails
  The model is limited to semantic context and approved analytical views rather than raw arbitrary tables.
- SQL guardrails
  `src/context_graph/sql_guard.py` rejects non-`SELECT` statements, multiple statements, `PRAGMA`, `SELECT *`, unqualified columns, unknown views, and non-allowlisted functions.
- Execution guardrails
  SQL execution is wrapped with row caps and a progress-handler timeout.
- Route guardrails
  `src/context_graph/plan_validator.py` prevents invalid route/intent pairings such as trace requests routed as plain SQL without trace semantics.
- Evidence guardrails
  `src/context_graph/query_service.py` and `src/context_graph/evidence_service.py` return explicit citations and highlights based on executed evidence.
- Error transparency
  The system does not silently hide failures. Errors are logged and surfaced instead of being replaced with fabricated fallback answers.

The practical rule is simple: no hardcoding, no fallbacks, and no hidden errors.

### 6. Conversation Memory

Conversation state is structured and persisted in SQLite rather than being kept only in transient frontend state.

Primary files:

- `src/context_graph/conversation_store.py`
- `src/context_graph/schemas.py`

Stored memory layers:

- Recent turns
- Selected node state
- Resolved entities
- Highlighted node state
- Active filter state
- Last intent and route

This supports follow-up questions like reusing the previously resolved entity even when the next turn omits the identifier explicitly.

### 7. API Layer

The API is exposed through FastAPI and serves both JSON endpoints and a streamed chat response.

Primary files:

- `src/context_graph/api.py`
- `src/context_graph/main.py`
- `src/context_graph/runtime.py`

Main endpoints:

- `GET /api/health`
- `GET /api/entities/search`
- `GET /api/entities/{node_id}`
- `GET /api/graph/subgraph`
- `GET /api/graph/path`
- `POST /api/chat/query`
- `POST /api/chat/query/stream`

Streaming contract:

- The stream returns newline-delimited JSON events
- Event types include conversation initialization, planning status, execution readiness, answer deltas, and the final structured payload

### 8. Frontend Layer

The UI is a two-pane application: graph exploration on the left and streamed conversational analysis on the right.

Primary files:

- `frontend/src/App.tsx`
- `frontend/src/api.ts`
- `frontend/src/types.ts`
- `frontend/src/styles.css`

Frontend responsibilities:

- Search for entities
- Request subgraphs and document traces
- Toggle graph clustering
- Render Cytoscape nodes and edges with selection and highlighting
- Display a streamed chat thread with user turns and assistant turns in one shared conversation space
- Open node detail in a draggable in-graph popover

## Request Lifecycle

```mermaid
flowchart LR
    A["User prompt in frontend/src/App.tsx"] --> B["POST /api/chat/query or /api/chat/query/stream"]
    B --> C["src/context_graph/query_service.py"]
    C --> D["Entity grounding via src/context_graph/entity_service.py"]
    C --> E["Planning via src/context_graph/planner.py"]
    E --> F["Route validation via src/context_graph/plan_validator.py"]
    F --> G["SQL path via src/context_graph/sql_guard.py"]
    F --> H["Graph path via src/context_graph/graph_service.py"]
    G --> I["Evidence extraction via src/context_graph/evidence_service.py"]
    H --> I
    I --> J["Conversation persistence via src/context_graph/conversation_store.py"]
    J --> K["Structured response and highlights back to frontend/src/App.tsx"]
```

## Runtime Composition

`src/context_graph/runtime.py` builds the application runtime and wires the main services together:

- `CatalogService`
- `EntityService`
- `GraphService`
- `OpenAIPlanner`
- `SqlValidator`
- `SqlExecutor`
- `ConversationStore`
- `EvidenceService`
- `QueryPlanValidator`
- `QueryService`

This keeps dependency wiring in one place and avoids mixing application bootstrap with route handlers.

## Artifact Model

Generated assets are written to `artifacts/` and consumed by both the notebook and the runtime app.

Key outputs:

- `artifacts/sqlite/context_graph.db`
  Canonical warehouse, semantic views, graph tables, and the prebuilt entity search index
- `artifacts/graph/graph_nodes.csv`
  Node export for offline inspection
- `artifacts/graph/graph_edges.csv`
  Edge export for offline inspection
- `artifacts/reports/semantic_catalog.json`
  Semantic glossary and approved-view catalog
- `artifacts/reports/quality_report.json`
  Data quality and normalization diagnostics
- `artifacts/reports/acceptance_checks.json`
  Golden data checks and expected dataset validation

Runtime-generated state is intentionally separate from those committed artifacts:

- `artifacts/sqlite/context_graph.runtime.db`
  Local writable runtime state for conversation memory when running outside Render
- `/tmp/context_graph.runtime.db`
  Writable runtime state target configured by `render.yaml` for Render deployment
- `/tmp/context_graph.query_events.jsonl`
  Render query-log target configured by `render.yaml`

## Deployment

The repository includes a single-service Render deployment configuration in `render.yaml`.

Deployment model:

- Render builds the Python package from the repository root
- The Vite frontend is built inside `frontend/`
- FastAPI serves the compiled frontend bundle from `frontend/dist`
- `artifacts/sqlite/context_graph.db` is treated as a bundled read-only analytical artifact
- Runtime writes are redirected to `/tmp` through environment variables in `render.yaml`

Required environment variables:

- `MODEL_API_KEY`
- `MODEL_PROVIDER`
- `MODEL`

Relevant deployment files:

- `render.yaml`
- `src/context_graph/settings.py`
- `src/context_graph/main.py`
- `src/context_graph/sqlite_utils.py`

## Test Coverage

Architecture-sensitive tests live under `tests/`.

Notable coverage areas:

- `tests/test_api.py`
  API health, search, graph pathing, cluster-mode response, and stream endpoint structure
- `tests/test_query_service.py`
  Graph-route execution, direct entity lookup grounding, stream event emission, and conversation memory reuse
- `tests/test_settings.py`
  Environment and model-provider configuration mapping

## Design Decisions

- SQL is the truth layer; graph is a projection layer
- The model plans and explains but does not supply authoritative data
- Short direct entity lookups can be grounded deterministically before planning
- Errors are returned explicitly instead of being hidden behind generic answers
- Relative architectural boundaries are encoded directly in `src/context_graph/` instead of being spread across the frontend and scripts
