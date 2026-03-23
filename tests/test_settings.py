from __future__ import annotations

from context_graph.settings import AppSettings


def test_app_settings_loads_generic_model_config_from_dotenv(
    tmp_path,
    monkeypatch,
) -> None:
    for key in [
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
