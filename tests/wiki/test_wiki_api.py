from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.api.dependencies import get_wiki_services
from app.main import app
from app.schemas.wiki.queries import WikiStatsResponse

KB_ID = uuid4()
VIEWER_HEADERS = {
    "X-Tenant-ID": "7",
    "X-User-ID": "viewer-1",
    "X-Role": "viewer",
}


class FakeQueryService:
    async def get_stats(self, _scope):
        return WikiStatsResponse(page_count=3, link_count=2)


@pytest.fixture
def client() -> TestClient:
    services = SimpleNamespace(query=FakeQueryService(), page=SimpleNamespace(), folder=SimpleNamespace())
    app.dependency_overrides[get_wiki_services] = lambda: services
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_openapi_contains_phase_one_wiki_routes(client: TestClient) -> None:
    paths = client.get("/openapi.json").json()["paths"]
    expected = {
        "/api/v1/knowledgebase/{kb_id}/wiki/pages",
        "/api/v1/knowledgebase/{kb_id}/wiki/pages/{slug}",
        "/api/v1/knowledgebase/{kb_id}/wiki/move-page",
        "/api/v1/knowledgebase/{kb_id}/wiki/folders",
        "/api/v1/knowledgebase/{kb_id}/wiki/folders/{folder_id}",
        "/api/v1/knowledgebase/{kb_id}/wiki/index",
        "/api/v1/knowledgebase/{kb_id}/wiki/log",
        "/api/v1/knowledgebase/{kb_id}/wiki/graph",
        "/api/v1/knowledgebase/{kb_id}/wiki/stats",
        "/api/v1/knowledgebase/{kb_id}/wiki/search",
        "/api/v1/knowledgebase/{kb_id}/wiki/rebuild-links",
        "/api/v1/knowledgebase/{kb_id}/wiki/lint",
        "/api/v1/knowledgebase/{kb_id}/wiki/issues",
        "/api/v1/knowledgebase/{kb_id}/wiki/issues/{issue_id}/status",
    }

    assert expected.issubset(paths)
    assert f"/api/v1/knowledgebase/{{kb_id}}/wiki/auto-fix" not in paths


def test_openapi_exposes_all_nineteen_phase_one_operations(client: TestClient) -> None:
    paths = client.get("/openapi.json").json()["paths"]
    prefix = "/api/v1/knowledgebase/{kb_id}/wiki"
    expected_methods = {
        f"{prefix}/pages": {"get", "post"},
        f"{prefix}/pages/{{slug}}": {"get", "put", "delete"},
        f"{prefix}/move-page": {"put"},
        f"{prefix}/folders": {"get", "post"},
        f"{prefix}/folders/{{folder_id}}": {"put", "delete"},
        f"{prefix}/index": {"get"},
        f"{prefix}/log": {"get"},
        f"{prefix}/graph": {"get"},
        f"{prefix}/stats": {"get"},
        f"{prefix}/search": {"get"},
        f"{prefix}/rebuild-links": {"post"},
        f"{prefix}/lint": {"get"},
        f"{prefix}/issues": {"get"},
        f"{prefix}/issues/{{issue_id}}/status": {"put"},
    }

    assert sum(len(methods) for methods in expected_methods.values()) == 19
    for path, methods in expected_methods.items():
        assert methods.issubset(paths[path])


def test_wiki_endpoint_requires_access_headers(client: TestClient) -> None:
    response = client.get(f"/api/v1/knowledgebase/{KB_ID}/wiki/stats")

    assert response.status_code == 422


def test_stats_returns_frontend_polling_fields(client: TestClient) -> None:
    response = client.get(
        f"/api/v1/knowledgebase/{KB_ID}/wiki/stats",
        headers=VIEWER_HEADERS,
    )

    assert response.status_code == 200
    assert response.json() == {
        "page_count": 3,
        "folder_count": 0,
        "link_count": 2,
        "issue_count": 0,
        "pending_tasks": 0,
        "is_active": False,
    }


def test_viewer_cannot_create_page(client: TestClient) -> None:
    response = client.post(
        f"/api/v1/knowledgebase/{KB_ID}/wiki/pages",
        headers=VIEWER_HEADERS,
        json={"slug": "entity/acme", "title": "Acme", "page_type": "entity"},
    )

    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "WIKI_WRITE_FORBIDDEN"


def test_rejects_unknown_role_with_stable_error_code(client: TestClient) -> None:
    response = client.get(
        f"/api/v1/knowledgebase/{KB_ID}/wiki/stats",
        headers={**VIEWER_HEADERS, "X-Role": "superuser"},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "INVALID_WIKI_ROLE"
