from __future__ import annotations

import pytest

from app.infrastructure.tasks.celery_app import CelerySettings, create_celery_app


def test_celery_settings_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_CELERY_BROKER_URL", "redis://redis:6379/0")
    monkeypatch.setenv("GRAPH_CELERY_RESULT_BACKEND", "redis://redis:6379/1")
    monkeypatch.setenv("GRAPH_CELERY_TASK_ALWAYS_EAGER", "true")

    settings = CelerySettings.from_env()

    assert settings.broker_url == "redis://redis:6379/0"
    assert settings.result_backend == "redis://redis:6379/1"
    assert settings.task_always_eager is True


def test_celery_app_uses_json_and_utc() -> None:
    app = create_celery_app(CelerySettings("memory://", "cache+memory://", True))

    assert app.conf.task_serializer == "json"
    assert app.conf.result_serializer == "json"
    assert app.conf.accept_content == ["json"]
    assert app.conf.enable_utc is True
    assert app.conf.timezone == "UTC"
