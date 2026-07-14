from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.api.v1.endpoints import documents
from app.main import app


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


def test_health_check(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_openapi_contains_document_parse_route(client: TestClient) -> None:
    response = client.get("/openapi.json")

    assert response.status_code == 200
    assert "/api/v1/documents/parse" in response.json()["paths"]


def test_parse_markdown_upload(client: TestClient) -> None:
    content = "# API 上传测试\n\n这是正文。".encode()

    response = client.post(
        "/api/v1/documents/parse",
        files={"file": ("example.md", content, "text/markdown")},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["file_name"] == "example.md"
    assert body["content_type"] == "text/markdown"
    assert body["size_bytes"] == len(content)
    assert "API 上传测试" in body["content"]
    assert body["images"] == {}


def test_reject_unsupported_upload(client: TestClient) -> None:
    response = client.post(
        "/api/v1/documents/parse",
        files={"file": ("example.txt", b"plain text", "text/plain")},
    )

    assert response.status_code == 415
    assert "Unsupported file type" in response.json()["detail"]


def test_reject_empty_upload(client: TestClient) -> None:
    response = client.post(
        "/api/v1/documents/parse",
        files={"file": ("example.md", b"", "text/markdown")},
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "上传文件为空"


def test_report_corrupt_supported_upload(client: TestClient) -> None:
    response = client.post(
        "/api/v1/documents/parse",
        files={"file": ("broken.xlsx", b"not an excel file", "application/octet-stream")},
    )

    assert response.status_code == 422
    assert "解析文档" in response.json()["detail"]


def test_reject_oversized_upload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(documents, "MAX_UPLOAD_BYTES", 4)

    response = client.post(
        "/api/v1/documents/parse",
        files={"file": ("example.md", b"12345", "text/markdown")},
    )

    assert response.status_code == 413
