from __future__ import annotations

import pytest

from app.infrastructure.tasks.celery_app import CelerySettings, create_celery_app


def test_celery_settings_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_CELERY_BROKER_URL", "  redis://redis:6379/0  ")
    monkeypatch.setenv("GRAPH_CELERY_RESULT_BACKEND", "  redis://redis:6379/1  ")
    monkeypatch.setenv("GRAPH_CELERY_TASK_ALWAYS_EAGER", "  YeS  ")

    settings = CelerySettings.from_env()

    assert settings.broker_url == "redis://redis:6379/0"
    assert settings.result_backend == "redis://redis:6379/1"
    assert settings.task_always_eager is True


def test_celery_settings_use_defaults_when_environment_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GRAPH_CELERY_BROKER_URL", raising=False)
    monkeypatch.delenv("GRAPH_CELERY_RESULT_BACKEND", raising=False)
    monkeypatch.delenv("GRAPH_CELERY_TASK_ALWAYS_EAGER", raising=False)

    settings = CelerySettings.from_env()

    assert settings.broker_url == "redis://127.0.0.1:6379/0"
    assert settings.result_backend == "redis://127.0.0.1:6379/1"
    assert settings.task_always_eager is False


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("  TRUE  ", True),
        ("  1  ", True),
        ("  YeS  ", True),
        ("  On  ", True),
        ("  FALSE  ", False),
        ("  0  ", False),
        ("  No  ", False),
        ("  Off  ", False),
    ],
)
def test_celery_settings_parse_boolean_values(
    monkeypatch: pytest.MonkeyPatch, raw_value: str, expected: bool
) -> None:
    monkeypatch.setenv("GRAPH_CELERY_TASK_ALWAYS_EAGER", raw_value)

    settings = CelerySettings.from_env()

    assert settings.task_always_eager is expected


@pytest.mark.parametrize(
    "variable_name",
    ["GRAPH_CELERY_BROKER_URL", "GRAPH_CELERY_RESULT_BACKEND"],
)
@pytest.mark.parametrize("raw_value", ["", "  \t  "])
def test_celery_settings_reject_empty_urls(
    monkeypatch: pytest.MonkeyPatch, variable_name: str, raw_value: str
) -> None:
    monkeypatch.setenv(variable_name, raw_value)

    with pytest.raises(ValueError, match=variable_name):
        CelerySettings.from_env()


def test_celery_settings_reject_invalid_boolean(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_CELERY_TASK_ALWAYS_EAGER", "sometimes")

    with pytest.raises(ValueError, match="GRAPH_CELERY_TASK_ALWAYS_EAGER"):
        CelerySettings.from_env()


def test_celery_app_uses_json_and_utc() -> None:
    app = create_celery_app(CelerySettings("memory://", "cache+memory://", True))

    assert app.conf.broker_url == "memory://"
    assert app.conf.result_backend == "cache+memory://"
    assert app.conf.task_serializer == "json"
    assert app.conf.result_serializer == "json"
    assert app.conf.accept_content == ["json"]
    assert app.conf.enable_utc is True
    assert app.conf.timezone == "UTC"
    assert app.conf.task_always_eager is True
    assert app.conf.task_eager_propagates is True
    assert app.conf.include == ["app.wiki.tasks.wiki_tasks"]
