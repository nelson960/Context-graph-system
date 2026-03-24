from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from context_graph.exceptions import PlannerError
from context_graph.planner import OpenAIPlanner
from context_graph.settings import AppSettings


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class CatalogStub:
    def compact_prompt_context(self) -> str:
        return "catalog context"


class RetryableModelError(Exception):
    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class FakeCompletions:
    def __init__(self, responses: list[object]) -> None:
        self._responses = list(responses)
        self.calls = 0

    def create(self, **kwargs):
        self.calls += 1
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeClient:
    def __init__(self, responses: list[object]) -> None:
        self.completions = FakeCompletions(responses)
        self.chat = SimpleNamespace(completions=self.completions)


def base_settings() -> AppSettings:
    frontend_root = PROJECT_ROOT / "frontend"
    return AppSettings(
        project_root=PROJECT_ROOT,
        dataset_root=PROJECT_ROOT / "sap-o2c-data",
        artifacts_root=PROJECT_ROOT / "artifacts",
        db_path=PROJECT_ROOT / "artifacts" / "sqlite" / "context_graph.db",
        state_db_path=PROJECT_ROOT / "artifacts" / "sqlite" / "context_graph.runtime.db",
        frontend_root=frontend_root,
        frontend_dist=frontend_root / "dist",
        frontend_index=frontend_root / "dist" / "index.html",
        query_log_path=PROJECT_ROOT / "artifacts" / "logs" / "query_events.jsonl",
        model_provider="openai",
        openai_api_key="test-key",
        openai_model="gpt-4.1-mini",
        openai_base_url=None,
        openai_reasoning_effort="medium",
        model_max_retries=2,
        model_retry_backoff_ms=1,
        max_query_rows=200,
        query_timeout_ms=5000,
        default_graph_depth=1,
        max_graph_nodes=40,
        max_graph_edges=80,
        api_title="Context Graph API",
    )


def chat_response(text: str):
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=text))]
    )


def test_json_completion_retries_retryable_errors(monkeypatch) -> None:
    planner = OpenAIPlanner(settings=base_settings(), catalog_service=CatalogStub())
    planner._client = FakeClient(
        [
            RetryableModelError("queue_exceeded", status_code=429),
            chat_response('{"answer":"ok","provenance_note":"p","assumptions":[]}'),
        ]
    )
    monkeypatch.setattr("context_graph.planner.time.sleep", lambda _: None)

    response = planner._json_completion("instructions", "prompt")

    assert '"answer":"ok"' in response
    assert planner._client.completions.calls == 2


def test_json_completion_raises_after_retry_budget(monkeypatch) -> None:
    settings = replace(base_settings(), model_max_retries=1, model_retry_backoff_ms=1)
    planner = OpenAIPlanner(settings=settings, catalog_service=CatalogStub())
    planner._client = FakeClient(
        [
            RetryableModelError("queue_exceeded", status_code=429),
            RetryableModelError("still overloaded", status_code=429),
        ]
    )
    monkeypatch.setattr("context_graph.planner.time.sleep", lambda _: None)

    with pytest.raises(PlannerError, match="after 2 attempt"):
        planner._json_completion("instructions", "prompt")

    assert planner._client.completions.calls == 2
