from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from app.wiki.ingest.fakes import FakeChatModel, FakeKnowledgeSource, load_fake_adapters
from app.wiki.ingest.ports import (
    ChatModelPort,
    KnowledgeSourcePort,
    PermanentModelError,
    TransientModelError,
)
from app.wiki.ingest.schemas import PageContribution, PageMergeRequest, WikiIngestConfig
from app.wiki.scope import WikiScope


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
OTHER_KB_ID = UUID("22222222-2222-2222-2222-222222222222")


FIXTURE = {
    "knowledge_bases": [
        {
            "tenant_id": 1,
            "knowledge_base_id": str(KB_ID),
            "config": {
                "wiki_enabled": True,
                "synthesis_model_id": "fake-synthesis",
                "summary_model_id": "fake-summary",
            },
        }
    ],
    "knowledge": [
        {
            "id": "knowledge-1",
            "tenant_id": 1,
            "knowledge_base_id": str(KB_ID),
            "title": "Document One",
            "op_version": "version-1",
            "status": "ready",
            "chunks": [
                {"id": "chunk-2", "chunk_index": 2, "start_at": 0, "text": "Second"},
                {
                    "id": "chunk-1",
                    "chunk_index": 1,
                    "start_at": 5,
                    "text": "First",
                    "ocr_text": "OCR",
                },
            ],
        }
    ],
    "model_responses": {
        "extract_candidates": {
            "entities": [{"name": "Acme", "slug": "entity/acme", "page_type": "entity"}],
            "concepts": [
                {"name": "Retrieval", "slug": "concept/retrieval", "page_type": "concept"}
            ],
        },
        "summaries": {
            "Document One": {"headline": "Document One", "markdown": "Summary body"}
        },
        "merges": {
            "entity/acme": {"headline": "Acme", "markdown": "Merged Acme"},
            "concept/retrieval": {"headline": "Retrieval", "markdown": "Merged Retrieval"},
        },
    },
    "transient_failures": {
        "extract_candidates": 2,
        "summarize:Document One": 1,
        "merge:entity/acme": 1,
    },
}


def write_fixture(path: Path, data: dict = FIXTURE) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def scope(tenant_id: int = 1, knowledge_base_id: UUID = KB_ID) -> WikiScope:
    return WikiScope(
        tenant_id=tenant_id,
        knowledge_base_id=knowledge_base_id,
        actor_id="test-worker",
    )


def merge_request(slug: str = "entity/acme") -> PageMergeRequest:
    page_type = "entity" if slug.startswith("entity/") else "concept"
    return PageMergeRequest(
        slug=slug,
        title="Acme",
        page_type=page_type,
        contributions=[
            PageContribution(
                pending_op_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                knowledge_id="knowledge-1",
                title="Document One",
                content="Body",
                summary="Summary",
            )
        ],
    )


def test_load_fake_adapters_validates_fixture_and_protocols(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    write_fixture(fixture)

    source, model = load_fake_adapters(fixture)

    assert isinstance(source, FakeKnowledgeSource)
    assert isinstance(model, FakeChatModel)
    assert isinstance(source, KnowledgeSourcePort)
    assert isinstance(model, ChatModelPort)


def test_fake_source_is_strictly_scoped_and_returns_copies(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    write_fixture(fixture)
    source, _ = load_fake_adapters(fixture)

    config = asyncio.run(source.get_config(scope()))
    knowledge = asyncio.run(source.get_knowledge(scope(), "knowledge-1"))
    chunks = asyncio.run(source.list_chunks(scope(), "knowledge-1"))

    assert config.wiki_enabled is True
    assert knowledge is not None
    assert knowledge.title == "Document One"
    assert [chunk.id for chunk in chunks] == ["chunk-2", "chunk-1"]
    assert asyncio.run(source.is_active(scope(), "knowledge-1", "version-1")) is True
    assert asyncio.run(source.is_active(scope(), "knowledge-1", "other-version")) is False

    assert asyncio.run(source.get_config(scope(tenant_id=2))).wiki_enabled is False
    other_config = asyncio.run(source.get_config(scope(knowledge_base_id=OTHER_KB_ID)))
    assert other_config.wiki_enabled is False
    assert asyncio.run(source.get_knowledge(scope(tenant_id=2), "knowledge-1")) is None
    assert asyncio.run(
        source.get_knowledge(scope(knowledge_base_id=OTHER_KB_ID), "knowledge-1")
    ) is None
    assert asyncio.run(source.list_chunks(scope(tenant_id=2), "knowledge-1")) == []
    assert (
        asyncio.run(source.is_active(scope(tenant_id=2), "knowledge-1", "version-1"))
        is False
    )

    config.wiki_enabled = False
    knowledge.title = "Mutated"
    chunks[0].text = "Mutated"

    assert asyncio.run(source.get_config(scope())).wiki_enabled is True
    refreshed = asyncio.run(source.get_knowledge(scope(), "knowledge-1"))
    assert refreshed is not None
    assert refreshed.title == "Document One"
    assert asyncio.run(source.list_chunks(scope(), "knowledge-1"))[0].text == "Second"


def test_fake_model_uses_exact_transient_failure_counts(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    write_fixture(fixture)
    _, model = load_fake_adapters(fixture)
    config = WikiIngestConfig()

    for _ in range(2):
        with pytest.raises(TransientModelError, match="extract_candidates"):
            asyncio.run(model.extract_candidates("Body", config))
    extraction = asyncio.run(model.extract_candidates("Body", config))

    with pytest.raises(TransientModelError, match="summarize:Document One"):
        asyncio.run(model.summarize("Document One", "Body"))
    summary = asyncio.run(model.summarize("Document One", "Body"))

    request = merge_request()
    with pytest.raises(TransientModelError, match="merge:entity/acme"):
        asyncio.run(model.merge_page(request))
    merged = asyncio.run(model.merge_page(request))

    assert extraction.entities[0].slug == "entity/acme"
    assert summary.headline == "Document One"
    assert merged.markdown == "Merged Acme"
    assert model.calls.count("extract_candidates") == 3
    assert model.calls.count("summarize:Document One") == 2
    assert model.calls.count("merge:entity/acme") == 2
    assert len(model.merge_requests) == 2
    assert model.merge_requests[0] is not request


def test_fake_model_raises_permanent_error_for_missing_response(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["transient_failures"] = {}
    write_fixture(fixture, data)
    _, model = load_fake_adapters(fixture)

    with pytest.raises(PermanentModelError, match="summarize:Missing Document"):
        asyncio.run(model.summarize("Missing Document", "Body"))

    with pytest.raises(PermanentModelError, match="merge:concept/missing"):
        asyncio.run(model.merge_page(merge_request("concept/missing")))


def test_load_fake_adapters_does_not_swallow_fixture_validation(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["transient_failures"] = {"extract_candidates": -1}
    write_fixture(fixture, data)

    with pytest.raises(ValidationError):
        load_fake_adapters(fixture)


def test_example_fixture_loads() -> None:
    fixture = Path(__file__).parents[2] / "examples" / "wiki_fake_data.json"

    source, model = load_fake_adapters(fixture)

    assert isinstance(source, FakeKnowledgeSource)
    assert isinstance(model, FakeChatModel)
