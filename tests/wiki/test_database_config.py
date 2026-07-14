from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.database.config import DatabaseSettings
from app.infrastructure.database.session import create_database_engine, create_session_factory


def test_database_settings_load_postgresql_url_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "GRAPH_DATABASE_URL",
        "postgresql+asyncpg://wiki:secret@db.example.test:5432/wiki",
    )

    settings = DatabaseSettings.from_env()

    assert settings.url.host == "db.example.test"
    assert settings.url.database == "wiki"
    assert settings.echo is False


def test_database_settings_reject_non_postgresql_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_DATABASE_URL", "sqlite+aiosqlite:///wiki.db")

    with pytest.raises(ValueError, match="PostgreSQL"):
        DatabaseSettings.from_env()


def test_database_settings_read_boolean_echo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_DATABASE_ECHO", "true")

    assert DatabaseSettings.from_env().echo is True


@pytest.mark.asyncio
async def test_session_factory_builds_async_postgresql_sessions() -> None:
    settings = DatabaseSettings.from_env()
    engine = create_database_engine(settings)
    factory = create_session_factory(engine)

    try:
        async with factory() as session:
            assert isinstance(session, AsyncSession)
            assert engine.dialect.name == "postgresql"
    finally:
        await engine.dispose()
