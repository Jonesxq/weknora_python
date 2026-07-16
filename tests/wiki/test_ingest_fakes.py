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
            "knowledge-1": {
                "entities": [
                    {"name": "Acme", "slug": "entity/acme", "page_type": "entity"}
                ],
                "concepts": [
                    {
                        "name": "Retrieval",
                        "slug": "concept/retrieval",
                        "page_type": "concept",
                    }
                ],
            }
        },
        "summaries": {
            "knowledge-1": {"headline": "Document One", "markdown": "Summary body"}
        },
        "merges": {
            "entity/acme": {"headline": "Acme", "markdown": "Merged Acme"},
            "concept/retrieval": {"headline": "Retrieval", "markdown": "Merged Retrieval"},
        },
    },
    "transient_failures": {
        "extract_candidates:knowledge-1": 2,
        "summarize:knowledge-1": 1,
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
        with pytest.raises(TransientModelError, match="extract_candidates:knowledge-1"):
            asyncio.run(model.extract_candidates("knowledge-1", "Body", config))
    extraction = asyncio.run(model.extract_candidates("knowledge-1", "Body", config))

    with pytest.raises(TransientModelError, match="summarize:knowledge-1"):
        asyncio.run(model.summarize("knowledge-1", "Document One", "Body"))
    summary = asyncio.run(model.summarize("knowledge-1", "Document One", "Body"))

    request = merge_request()
    with pytest.raises(TransientModelError, match="merge:entity/acme"):
        asyncio.run(model.merge_page(request))
    merged = asyncio.run(model.merge_page(request))

    assert extraction.entities[0].slug == "entity/acme"
    assert summary.headline == "Document One"
    assert merged.markdown == "Merged Acme"
    assert model.calls.count("extract_candidates:knowledge-1") == 3
    assert model.calls.count("summarize:knowledge-1") == 2
    assert model.calls.count("merge:entity/acme") == 2
    assert len(model.merge_requests) == 2
    assert model.merge_requests[0] is not request


def test_fake_model_raises_permanent_error_for_missing_response(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["transient_failures"] = {}
    write_fixture(fixture, data)
    _, model = load_fake_adapters(fixture)

    with pytest.raises(PermanentModelError, match="summarize:missing"):
        asyncio.run(model.summarize("missing", "Missing Document", "Body"))

    with pytest.raises(PermanentModelError, match="merge:concept/missing"):
        asyncio.run(model.merge_page(merge_request("concept/missing")))


def test_load_fake_adapters_does_not_swallow_fixture_validation(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["transient_failures"] = {"extract_candidates:knowledge-1": -1}
    write_fixture(fixture, data)

    with pytest.raises(ValidationError):
        load_fake_adapters(fixture)


def test_example_fixture_loads() -> None:
    fixture = Path(__file__).parents[2] / "examples" / "wiki_fake_data.json"

    source, model = load_fake_adapters(fixture)

    assert isinstance(source, FakeKnowledgeSource)
    assert isinstance(model, FakeChatModel)


def test_fixture_rejects_unknown_fields_and_duplicate_identities(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["unknown"] = True
    write_fixture(fixture, data)
    with pytest.raises(ValidationError):
        load_fake_adapters(fixture)

    data = json.loads(json.dumps(FIXTURE))
    data["knowledge"].append(json.loads(json.dumps(data["knowledge"][0])))
    write_fixture(fixture, data)
    with pytest.raises(ValidationError, match="重复"):
        load_fake_adapters(fixture)


def test_fixture_rejects_same_knowledge_id_across_scopes(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["knowledge_bases"].append(
        {
            "tenant_id": 2,
            "knowledge_base_id": str(OTHER_KB_ID),
            "config": {"wiki_enabled": True},
        }
    )
    duplicate = json.loads(json.dumps(data["knowledge"][0]))
    duplicate["tenant_id"] = 2
    duplicate["knowledge_base_id"] = str(OTHER_KB_ID)
    data["knowledge"].append(duplicate)
    write_fixture(fixture, data)

    with pytest.raises(ValidationError, match="全局唯一"):
        load_fake_adapters(fixture)


def test_fixture_rejects_knowledge_from_undeclared_kb(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["knowledge"][0]["knowledge_base_id"] = str(OTHER_KB_ID)
    write_fixture(fixture, data)

    with pytest.raises(ValidationError, match="已声明"):
        load_fake_adapters(fixture)


def test_fixture_rejects_transient_failure_for_unknown_identity(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["transient_failures"] = {"summarize:missing": 1}
    write_fixture(fixture, data)

    with pytest.raises(ValidationError, match="未知"):
        load_fake_adapters(fixture)


def test_fixture_rejects_legacy_or_unknown_model_response_keys(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["model_responses"]["extract_candidates"] = {
        "entities": [],
        "concepts": [],
    }
    write_fixture(fixture, data)
    with pytest.raises(ValidationError):
        load_fake_adapters(fixture)

    data = json.loads(json.dumps(FIXTURE))
    data["model_responses"]["summaries"] = {
        "Document One": {"headline": "Legacy", "markdown": "Legacy body"}
    }
    write_fixture(fixture, data)
    with pytest.raises(ValidationError, match="未知"):
        load_fake_adapters(fixture)


@pytest.mark.parametrize("slug", ["ENTITY/ACME", "entity//acme", "entity/acme/"])
def test_fixture_rejects_noncanonical_merge_slug(tmp_path: Path, slug: str) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    response = data["model_responses"]["merges"].pop("entity/acme")
    data["model_responses"]["merges"][slug] = response
    data["transient_failures"].pop("merge:entity/acme")
    write_fixture(fixture, data)

    with pytest.raises(ValidationError):
        load_fake_adapters(fixture)


def test_fake_model_indexes_duplicate_titles_by_knowledge_id(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    second = json.loads(json.dumps(data["knowledge"][0]))
    second["id"] = "knowledge-2"
    data["knowledge"].append(second)
    extraction = data["model_responses"].pop("extract_candidates")["knowledge-1"]
    data["model_responses"]["extract_candidates"] = {
        "knowledge-1": extraction,
        "knowledge-2": {"entities": [], "concepts": []},
    }
    data["model_responses"]["summaries"] = {
        "knowledge-1": {"headline": "First", "markdown": "First body"},
        "knowledge-2": {"headline": "Second", "markdown": "Second body"},
    }
    data["transient_failures"] = {}
    write_fixture(fixture, data)
    _, model = load_fake_adapters(fixture)

    first = asyncio.run(model.summarize("knowledge-1", "Document One", "Body"))
    second_result = asyncio.run(model.summarize("knowledge-2", "Document One", "Body"))
    first_candidates = asyncio.run(
        model.extract_candidates("knowledge-1", "Body", WikiIngestConfig())
    )
    second_candidates = asyncio.run(
        model.extract_candidates("knowledge-2", "Body", WikiIngestConfig())
    )

    assert first.headline == "First"
    assert second_result.headline == "Second"
    assert len(first_candidates.entities) == 1
    assert second_candidates.entities == []
    assert model.calls == [
        "summarize:knowledge-1",
        "summarize:knowledge-2",
        "extract_candidates:knowledge-1",
        "extract_candidates:knowledge-2",
    ]
