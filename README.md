# Context Graph System

Grounded context graph explorer for an SAP order-to-cash dataset.

Live demo: [https://context-graph-system.onrender.com/](https://context-graph-system.onrender.com/)

## What It Does

This project converts fragmented SAP-style order-to-cash data into:

- a normalized SQLite analytics warehouse
- a graph of connected business entities
- a FastAPI backend for graph and query APIs
- a Vite frontend with graph exploration and chat

The model is used as a planner and explainer only. Final answers must come from executed SQL or graph evidence.

## Setup

Requirements:

- Python 3.12
- Node.js 20+
- the `ml` conda environment
- these environment variables:
  - `MODEL_API_KEY`
  - `MODEL_PROVIDER`
  - `MODEL`

Required artifact:

- `artifacts/sqlite/context_graph.db`

Install and build:

```bash
pip install .
cd frontend && npm ci && npm run build
```

Run locally:

```bash
uvicorn context_graph.main:app --host 0.0.0.0 --port 8000
```

Open:

- [http://127.0.0.1:8000](http://127.0.0.1:8000)

## Architecture Decisions

- SQL is the truth layer. The normalized warehouse in `artifacts/sqlite/context_graph.db` is the authoritative data source.
- The graph is derived, not primary. `graph_nodes` and `graph_edges` are built from the SQL model and used for traversal, highlighting, and visual exploration.
- FastAPI serves both the API and the built frontend bundle, so the demo runs as a single web service.
- Query execution is route-aware. Requests are handled through `sql`, `graph`, or `hybrid` paths depending on intent.
- Runtime state is separate from the bundled analytical database. Conversation state and logs are written to a separate writable SQLite file.

## Database Choice

SQLite is used for this project because it is the simplest fit for a local and demo deployment:

- the dataset is small enough for a single file-backed analytical store
- the notebook-first workflow benefits from an inspectable local database
- guarded SQL execution is easier to validate against a constrained SQLite schema
- Render deployment is simpler with a bundled read-only database than with a separate Postgres service

At runtime:

- `artifacts/sqlite/context_graph.db` is opened read-only
- runtime conversation state is stored separately
- graph data is loaded from SQLite into memory for traversal and focused subgraph retrieval

## LLM Prompting Strategy

The model is constrained to a narrow planner role.

- Planning: generate a structured query plan with intent, route, entities, filters, metrics, and output shape.
- SQL generation: generate read-only SQL against approved semantic views only.
- Answer composition: explain executed rows or graph evidence only.

The prompts are grounded with:

- a semantic catalog instead of raw table dumps
- few-shot examples for ranking, trace, anomaly, lookup, and refusal flows
- conversation state and resolved entity candidates when available

This reduces hallucination by separating planning, execution, and explanation into distinct steps.

## Guardrails

The system is intentionally restrictive.

- No raw-table SQL from the planner
- No write queries
- No hidden fallbacks
- No fabricated graph links
- No out-of-domain answers

Validation is enforced in code through:

- entity resolution checks
- route and intent validation
- SQL allowlists for views, columns, and functions
- read-only SQL execution
- row limits and execution timeouts

The rule for this project is simple: no hardcoding, no fallbacks, no hiding errors.

## Request Lifecycle

1. The frontend sends a prompt to the FastAPI backend.
2. The backend loads conversation context and selected graph state.
3. The request is grounded against dataset entities.
4. The planner produces a structured route-aware plan.
5. The plan is validated.
6. The system executes either:
   - SQL analytics
   - graph traversal
   - hybrid SQL plus graph evidence
7. The answer is composed from executed evidence only.
8. The response returns answer text, citations, and graph highlights.

## Render Deployment

This repo is configured for a single Render Web Service through `render.yaml`.

Build:

```bash
pip install . && cd frontend && npm ci && npm run build
```

Start:

```bash
uvicorn context_graph.main:app --host 0.0.0.0 --port $PORT
```

Deployment notes:

- the bundled SQLite file is treated as read-only demo data
- runtime state DB and query logs are redirected to `/tmp`
- no Postgres is required for this demo deployment

## Repo Layout

- `src/context_graph/` — FastAPI app, planner, graph services, SQL guardrails, runtime wiring
- `frontend/` — Vite React UI
- `artifacts/sqlite/context_graph.db` — bundled analytics database
- `artifacts/reports/semantic_catalog.json` — semantic catalog used by the planner
- `render.yaml` — Render service definition
