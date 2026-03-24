from __future__ import annotations

from pathlib import Path

from context_graph.settings import AppSettings


def test_app_settings_loads_generic_model_config_from_dotenv(
    tmp_path,
    monkeypatch,
) -> None:
    for key in [
        "API_KEY",
        "MODEL_API_KEY",
        "MODEL_PROVIDER",
        "MODEL",
        "MODEL_BASE_URL",
        "OPENAI_API_KEY",
        "CONTEXT_GRAPH_OPENAI_MODEL",
        "CONTEXT_GRAPH_OPENAI_BASE_URL",
        "CONTEXT_GRAPH_PROJECT_ROOT",
    ]:
        monkeypatch.delenv(key, raising=False)

    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "MODEL_API_KEY=test-model-key",
                "MODEL_PROVIDER=cerebras",
                "MODEL=qwen-3-235b-a22b-instruct-2507",
            ]
        )
    )
    monkeypatch.setenv("CONTEXT_GRAPH_PROJECT_ROOT", str(tmp_path))

    settings = AppSettings.from_env()

    assert settings.model_provider == "cerebras"
    assert settings.openai_api_key == "test-model-key"
    assert settings.openai_model == "qwen-3-235b-a22b-instruct-2507"
    assert settings.openai_base_url == "https://api.cerebras.ai/v1"
    assert settings.db_path == (tmp_path / "artifacts" / "sqlite" / "context_graph.db").resolve()
    assert settings.state_db_path == (tmp_path / "artifacts" / "sqlite" / "context_graph.runtime.db").resolve()


def test_app_settings_accepts_api_key_alias(
    tmp_path,
    monkeypatch,
) -> None:
    for key in [
        "API_KEY",
        "MODEL_API_KEY",
        "MODEL_PROVIDER",
        "MODEL",
        "OPENAI_API_KEY",
        "CONTEXT_GRAPH_PROJECT_ROOT",
    ]:
        monkeypatch.delenv(key, raising=False)

    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "API_KEY=alias-key",
                "MODEL_PROVIDER=openai",
                "MODEL=gpt-4.1-mini",
            ]
        )
    )
    monkeypatch.setenv("CONTEXT_GRAPH_PROJECT_ROOT", str(tmp_path))

    settings = AppSettings.from_env()

    assert settings.openai_api_key == "alias-key"


def test_app_settings_defaults_project_root_from_working_tree(
    tmp_path,
    monkeypatch,
) -> None:
    for key in [
        "API_KEY",
        "MODEL_API_KEY",
        "MODEL_PROVIDER",
        "MODEL",
        "MODEL_BASE_URL",
        "OPENAI_API_KEY",
        "CONTEXT_GRAPH_OPENAI_MODEL",
        "CONTEXT_GRAPH_OPENAI_BASE_URL",
        "CONTEXT_GRAPH_PROJECT_ROOT",
    ]:
        monkeypatch.delenv(key, raising=False)

    (tmp_path / "pyproject.toml").write_text("[project]\nname='context-graph'\n")
    (tmp_path / "src" / "context_graph").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)

    settings = AppSettings.from_env()

    assert settings.project_root == Path(tmp_path).resolve()
