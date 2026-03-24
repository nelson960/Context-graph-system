from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from context_graph.exceptions import (
    AmbiguousEntityError,
    ConfigurationError,
    ContextGraphError,
    EntityResolutionError,
    PlannerError,
    QueryExecutionError,
    SqlValidationError,
)
from context_graph.runtime import AppRuntime
from context_graph.schemas import (
    ChatQueryRequest,
    ChatQueryResponse,
    EntityDetailResponse,
    EntitySearchResult,
    GraphResponse,
)


router = APIRouter(prefix="/api")


def _runtime(request: Request) -> AppRuntime:
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise HTTPException(status_code=503, detail="Application runtime is not initialized")
    return runtime


def _chat_http_exception(exc: Exception) -> HTTPException:
    if isinstance(exc, ConfigurationError):
        return HTTPException(status_code=503, detail=str(exc))
    if isinstance(exc, AmbiguousEntityError):
        return HTTPException(status_code=409, detail=str(exc))
    if isinstance(exc, EntityResolutionError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, (PlannerError, SqlValidationError)):
        return HTTPException(status_code=422, detail=str(exc))
    if isinstance(exc, QueryExecutionError):
        return HTTPException(status_code=500, detail=str(exc))
    if isinstance(exc, ContextGraphError):
        return HTTPException(status_code=500, detail=str(exc))
    raise exc


@router.get("/health")
def health(request: Request) -> dict[str, str]:
    runtime = _runtime(request)
    return {"status": "ok", "db_path": str(runtime.settings.db_path)}


@router.get("/entities/search", response_model=list[EntitySearchResult])
def search_entities(
    request: Request,
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=25),
    node_type: str | None = Query(None),
) -> list[EntitySearchResult]:
    runtime = _runtime(request)
    assert runtime.entity_service is not None
    return runtime.entity_service.search(query=q, limit=limit, node_type=node_type)


@router.get("/entities/{node_id}", response_model=EntityDetailResponse)
def get_entity(request: Request, node_id: str) -> EntityDetailResponse:
    runtime = _runtime(request)
    assert runtime.graph_service is not None
    try:
        return EntityDetailResponse(node=runtime.graph_service.get_node(node_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/graph/subgraph", response_model=GraphResponse)
def get_subgraph(
    request: Request,
    node_id: str = Query(..., min_length=1),
    depth: int = Query(1, ge=1, le=4),
    include_hidden: bool = Query(False),
    cluster_mode: str | None = Query(None, pattern="^(type)$"),
) -> GraphResponse:
    runtime = _runtime(request)
    assert runtime.graph_service is not None
    try:
        return runtime.graph_service.get_subgraph(
            node_id=node_id,
            depth=depth,
            include_hidden=include_hidden,
            cluster_mode=cluster_mode,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/graph/path", response_model=GraphResponse)
def get_path(
    request: Request,
    node_id: str = Query(..., min_length=1),
    direction: str = Query("both", pattern="^(upstream|downstream|both)$"),
    depth: int = Query(4, ge=1, le=8),
    cluster_mode: str | None = Query(None, pattern="^(type)$"),
) -> GraphResponse:
    runtime = _runtime(request)
    assert runtime.graph_service is not None
    try:
        return runtime.graph_service.get_path(
            node_id=node_id,
            direction=direction,
            depth=depth,
            cluster_mode=cluster_mode,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/chat/query", response_model=ChatQueryResponse)
def chat_query(request: Request, payload: ChatQueryRequest) -> ChatQueryResponse:
    runtime = _runtime(request)
    assert runtime.query_service is not None
    try:
        return runtime.query_service.handle_chat_request(payload)
    except Exception as exc:
        raise _chat_http_exception(exc) from exc


@router.post("/chat/query/stream")
def chat_query_stream(request: Request, payload: ChatQueryRequest) -> StreamingResponse:
    runtime = _runtime(request)
    assert runtime.query_service is not None

    def event_stream():
        for event in runtime.query_service.stream_chat_request(payload):
            yield json.dumps(event, sort_keys=True)
            yield "\n"

    return StreamingResponse(event_stream(), media_type="application/x-ndjson")
