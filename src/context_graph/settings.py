from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


KNOWN_MODEL_PROVIDER_BASE_URLS = {
    "openai": None,
    "cerebras": "https://api.cerebras.ai/v1",
}


def _default_project_root() -> Path:
    cwd = Path.cwd().resolve()
    if (cwd / "pyproject.toml").exists() and (cwd / "src" / "context_graph").exists():
        return cwd
    return Path(__file__).resolve().parents[2]


def _parse_env_file(env_path: Path) -> dict[str, str]:
    if not env_path.exists():
        return {}
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(env_path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            raise ValueError(f"Invalid .env line {line_number} in {env_path}: {raw_line}")
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(f"Invalid empty env key on line {line_number} in {env_path}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _load_project_env(project_root: Path) -> None:
    for key, value in _parse_env_file(project_root / ".env").items():
        os.environ.setdefault(key, value)


def _model_base_url(provider: str, explicit_base_url: str | None) -> str | None:
    if explicit_base_url:
        return explicit_base_url
    normalized_provider = provider.strip().lower()
    if normalized_provider in KNOWN_MODEL_PROVIDER_BASE_URLS:
        return KNOWN_MODEL_PROVIDER_BASE_URLS[normalized_provider]
    raise ValueError(
        f"Unsupported MODEL_PROVIDER '{provider}'. Set MODEL_BASE_URL explicitly for this provider."
    )


@dataclass(frozen=True)
class AppSettings:
    project_root: Path
    dataset_root: Path
    artifacts_root: Path
    db_path: Path
    state_db_path: Path
    frontend_root: Path
    frontend_dist: Path
    frontend_index: Path
    query_log_path: Path
    model_provider: str
    openai_api_key: str | None
    openai_model: str
    openai_base_url: str | None
    openai_reasoning_effort: str
    model_max_retries: int
    model_retry_backoff_ms: int
    max_query_rows: int
    query_timeout_ms: int
    default_graph_depth: int
    max_graph_nodes: int
    max_graph_edges: int
    api_title: str

    @classmethod
    def from_env(cls) -> "AppSettings":
        project_root = Path(
            os.getenv("CONTEXT_GRAPH_PROJECT_ROOT", _default_project_root())
        ).resolve()
        _load_project_env(project_root)
        artifacts_root = Path(
            os.getenv("CONTEXT_GRAPH_ARTIFACTS_ROOT", project_root / "artifacts")
        ).resolve()
        frontend_root = Path(
            os.getenv("CONTEXT_GRAPH_FRONTEND_ROOT", project_root / "frontend")
        ).resolve()
        model_provider = os.getenv("MODEL_PROVIDER", "openai").strip().lower()
        return cls(
            project_root=project_root,
            dataset_root=Path(
                os.getenv("CONTEXT_GRAPH_DATASET_ROOT", project_root / "sap-o2c-data")
            ).resolve(),
            artifacts_root=artifacts_root,
            db_path=Path(
                os.getenv(
                    "CONTEXT_GRAPH_DB_PATH",
                    artifacts_root / "sqlite" / "context_graph.db",
                )
            ).resolve(),
            state_db_path=Path(
                os.getenv(
                    "CONTEXT_GRAPH_STATE_DB_PATH",
                    artifacts_root / "sqlite" / "context_graph.runtime.db",
                )
            ).resolve(),
            frontend_root=frontend_root,
            frontend_dist=Path(
                os.getenv("CONTEXT_GRAPH_FRONTEND_DIST", frontend_root / "dist")
            ).resolve(),
            frontend_index=Path(
                os.getenv(
                    "CONTEXT_GRAPH_FRONTEND_INDEX",
                    frontend_root / "dist" / "index.html",
                )
            ).resolve(),
            query_log_path=Path(
                os.getenv(
                    "CONTEXT_GRAPH_QUERY_LOG_PATH",
                    artifacts_root / "logs" / "query_events.jsonl",
                )
            ).resolve(),
            model_provider=model_provider,
            openai_api_key=os.getenv("MODEL_API_KEY")
            or os.getenv("OPENAI_API_KEY")
            or os.getenv("API_KEY"),
            openai_model=os.getenv("MODEL")
            or os.getenv("CONTEXT_GRAPH_OPENAI_MODEL", "gpt-4.1-mini"),
            openai_base_url=_model_base_url(
                model_provider,
                os.getenv("MODEL_BASE_URL") or os.getenv("CONTEXT_GRAPH_OPENAI_BASE_URL"),
            ),
            openai_reasoning_effort=os.getenv(
                "CONTEXT_GRAPH_OPENAI_REASONING_EFFORT", "medium"
            ),
            model_max_retries=int(os.getenv("CONTEXT_GRAPH_MODEL_MAX_RETRIES", "2")),
            model_retry_backoff_ms=int(
                os.getenv("CONTEXT_GRAPH_MODEL_RETRY_BACKOFF_MS", "750")
            ),
            max_query_rows=int(os.getenv("CONTEXT_GRAPH_MAX_QUERY_ROWS", "200")),
            query_timeout_ms=int(os.getenv("CONTEXT_GRAPH_QUERY_TIMEOUT_MS", "5000")),
            default_graph_depth=int(os.getenv("CONTEXT_GRAPH_DEFAULT_GRAPH_DEPTH", "1")),
            max_graph_nodes=int(os.getenv("CONTEXT_GRAPH_MAX_GRAPH_NODES", "40")),
            max_graph_edges=int(os.getenv("CONTEXT_GRAPH_MAX_GRAPH_EDGES", "80")),
            api_title=os.getenv("CONTEXT_GRAPH_API_TITLE", "Context Graph API"),
        )

    def ensure_runtime_dirs(self) -> None:
        self.artifacts_root.mkdir(parents=True, exist_ok=True)
        self.state_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.query_log_path.parent.mkdir(parents=True, exist_ok=True)
