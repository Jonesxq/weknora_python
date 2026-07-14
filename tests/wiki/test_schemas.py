from __future__ import annotations

from uuid import uuid4

from app.schemas.wiki.pages import (
    WikiPageCreateRequest,
    WikiPageListItem,
    WikiPageListQuery,
    WikiPageResponse,
    WikiPageUpdateRequest,
)


def test_page_create_ignores_server_managed_fields() -> None:
    request = WikiPageCreateRequest.model_validate(
        {
            "slug": "entity/acme",
            "title": "Acme",
            "page_type": "entity",
            "content": "正文",
            "tenant_id": 999,
            "knowledge_base_id": str(uuid4()),
            "version": 88,
            "in_links": ["bad"],
            "source_refs": ["secret"],
        }
    )

    assert not hasattr(request, "tenant_id")
    assert not hasattr(request, "knowledge_base_id")
    assert not hasattr(request, "in_links")
    assert not hasattr(request, "source_refs")


def test_page_update_distinguishes_missing_field_from_explicit_null() -> None:
    missing = WikiPageUpdateRequest.model_validate({"version": 3})
    clear_summary = WikiPageUpdateRequest.model_validate({"version": 3, "summary": None})

    assert "summary" not in missing.model_fields_set
    assert "summary" in clear_summary.model_fields_set
    assert clear_summary.summary is None


def test_page_response_uses_empty_link_lists_instead_of_null() -> None:
    response = WikiPageResponse.model_validate(
        {
            "id": uuid4(),
            "slug": "entity/acme",
            "title": "Acme",
            "page_type": "entity",
            "status": "published",
            "content": "正文",
            "summary": "摘要",
            "version": 1,
        }
    )

    assert response.in_links == []
    assert response.out_links == []
    assert response.aliases == []
    assert response.category_path == []


def test_page_list_query_parses_multiple_types_and_caps_page_size() -> None:
    query = WikiPageListQuery.model_validate(
        {"page_type": "entity,concept", "page": 2, "page_size": 200}
    )

    assert query.page_types == ["entity", "concept"]
    assert query.offset == 200


def test_page_list_item_is_a_narrow_projection_without_content() -> None:
    assert "content" not in WikiPageListItem.model_fields
    assert "chunk_refs" not in WikiPageListItem.model_fields
    assert "page_metadata" not in WikiPageListItem.model_fields
