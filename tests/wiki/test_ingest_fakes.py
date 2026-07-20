from __future__ import annotations

import asyncio
from copy import deepcopy
import json
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from app.wiki.ingest.fakes import (
    FakeChatModel,
    FakeDataset,
    FakeEmbeddingModel,
    FakeKnowledgeSource,
    load_fake_adapters,
    load_fake_runtime_adapters,
)
from app.wiki.ingest.ports import (
    ChatModelPort,
    CitationModelPort,
    DedupModelPort,
    EmbeddingModelPort,
    KnowledgeSourcePort,
    PermanentModelError,
    TransientModelError,
    TombstonePort,
    TaxonomyModelPort,
    WikiIngestModelPort,
)
from app.wiki.ingest.schemas import (
    CitationBatchChunk,
    CitationBatchOutput,
    CitationBatchRequest,
    DedupCandidateRequest,
    DedupPageCandidate,
    DedupRequest,
    EmbeddingItem,
    EmbeddingRequest,
    IndexIntroChange,
    IndexIntroOutput,
    IndexIntroRequest,
    IndexSummaryItem,
    PageContribution,
    PageMergeRequest,
    TopicCandidate,
    TaxonomyRequest,
    TaxonomyTopic,
    WikiIngestConfig,
)
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


def taxonomy_request(*slugs: str) -> TaxonomyRequest:
    return TaxonomyRequest(
        topics=tuple(
            TaxonomyTopic(
                slug=slug,
                title=slug.rsplit("/", 1)[-1].title(),
                page_type=slug.split("/", 1)[0],
                summary="Summary",
            )
            for slug in slugs
        ),
        allowed_bases=(),
    )


def index_intro_create_request(*knowledge_ids: str) -> IndexIntroRequest:
    return IndexIntroRequest(
        mode="create",
        summaries=tuple(
            IndexSummaryItem(
                slug=f"summary/{knowledge_id}",
                title=f"Document {knowledge_id}",
                summary=f"Summary {knowledge_id}",
            )
            for knowledge_id in knowledge_ids
        ),
    )


def index_intro_update_request(*changes: tuple[str, str]) -> IndexIntroRequest:
    return IndexIntroRequest(
        mode="update",
        existing_intro="Existing intro",
        changes=tuple(
            IndexIntroChange(action=action, knowledge_id=knowledge_id)
            for action, knowledge_id in changes
        ),
    )


@pytest.mark.asyncio
async def test_fake_embedding_returns_exact_requested_vectors() -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["embeddings"] = {
        "topic:entity/acme": (1.0, 0.0),
        "folder:00000000-0000-0000-0000-000000000001": (0.5, 0.5),
    }
    dataset = FakeDataset.model_validate(payload)
    model = FakeEmbeddingModel(dataset)
    request = EmbeddingRequest(items=(EmbeddingItem(key="topic:entity/acme", text="Acme"),))

    output = await model.embed(request)

    assert output.vectors == {"topic:entity/acme": (1.0, 0.0)}
    assert model.calls == ["embedding:topic:entity/acme"]


@pytest.mark.asyncio
async def test_fake_taxonomy_requires_explicit_batch_response() -> None:
    model = FakeChatModel(FakeDataset.model_validate(deepcopy(FIXTURE)))
    request = taxonomy_request("entity/acme")

    with pytest.raises(PermanentModelError, match="taxonomy:entity/acme"):
        await model.plan_folders(request)


@pytest.mark.asyncio
async def test_fake_index_intro_uses_request_order_and_isolates_snapshots() -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["index_intros"] = {
        "index_intro:create:summary/knowledge-1": {"intro": "Created intro"},
        "index_intro:update:ingest:knowledge-1,retract:knowledge-2": {
            "intro": "Updated intro"
        },
    }
    dataset = FakeDataset.model_validate(payload)
    first = FakeChatModel(dataset)
    second = FakeChatModel(dataset)
    create = index_intro_create_request("knowledge-1")
    update = index_intro_update_request(("ingest", "knowledge-1"), ("retract", "knowledge-2"))

    created = await first.generate_index_intro(create)
    updated = await first.generate_index_intro(update)
    repeated = await second.generate_index_intro(create)

    assert created.intro == "Created intro"
    assert updated.intro == "Updated intro"
    assert first.calls == [
        "index_intro:create:summary/knowledge-1",
        "index_intro:update:ingest:knowledge-1,retract:knowledge-2",
    ]
    assert [snapshot.model_dump() for snapshot in first.index_intro_requests] == [
        create.model_dump(),
        update.model_dump(),
    ]
    assert first.index_intro_requests[0] is not create
    assert created is not repeated
    assert first.responses["index_intros"] is not first._responses.index_intros
    first.responses["index_intros"]["index_intro:create:summary/knowledge-1"] = IndexIntroOutput(
        intro="Mutated public snapshot"
    )
    assert (await first.generate_index_intro(create)).intro == "Created intro"


@pytest.mark.asyncio
async def test_fake_index_intro_records_transient_attempts_and_missing_responses() -> None:
    payload = deepcopy(FIXTURE)
    key = "index_intro:create:summary/knowledge-1"
    payload["model_responses"]["index_intros"] = {key: {"intro": "Created intro"}}
    payload["transient_failures"] = {key: 1}
    model = FakeChatModel(FakeDataset.model_validate(payload))
    request = index_intro_create_request("knowledge-1")

    with pytest.raises(TransientModelError, match=key):
        await model.generate_index_intro(request)
    assert (await model.generate_index_intro(request)).intro == "Created intro"
    assert model.calls == [key, key]
    assert len(model.index_intro_requests) == 2
    assert all(snapshot.model_dump() == request.model_dump() for snapshot in model.index_intro_requests)
    assert model.index_intro_requests[0] is not request
    assert model.index_intro_requests[0] is not model.index_intro_requests[1]

    missing = index_intro_create_request("knowledge-2")
    with pytest.raises(
        PermanentModelError, match="index_intro:create:summary/knowledge-2"
    ):
        await model.generate_index_intro(missing)


@pytest.mark.parametrize(
    "key",
    [
        "index_intro:create:",
        "index_intro:create:summary/knowledge-1,summary/knowledge-1",
        "index_intro:create:summary/Knowledge-1",
        "index_intro:create:entity/knowledge-1",
        "index_intro:update:",
        "index_intro:update:retract:knowledge-2,ingest:knowledge-1",
        "index_intro:update:ingest:knowledge-1,ingest:knowledge-1",
        "index_intro:update:ingest:knowledge,1",
        "index_intro:update:ingest:knowledge:1",
        "index_intro:unknown:summary/knowledge-1",
        "not_index_intro:create:summary/knowledge-1",
    ],
)
def test_fixture_rejects_malformed_index_intro_response_keys(key: str) -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["index_intros"] = {key: {"intro": "Index intro"}}

    with pytest.raises(ValidationError):
        FakeDataset.model_validate(payload)


def test_fixture_accepts_empty_and_canonical_index_intro_responses() -> None:
    empty = deepcopy(FIXTURE)
    empty["model_responses"]["index_intros"] = {}
    assert FakeDataset.model_validate(empty).model_responses.index_intros == {}

    payload = deepcopy(FIXTURE)
    payload["model_responses"]["index_intros"] = {
        "index_intro:create:summary/knowledge-2,summary/knowledge-1": {"intro": "Created"},
        "index_intro:update:ingest:knowledge-1,retract:knowledge-2": {"intro": "Updated"},
    }
    assert set(FakeDataset.model_validate(payload).model_responses.index_intros) == set(
        payload["model_responses"]["index_intros"]
    )


def test_fixture_rejects_unknown_index_intro_transient_response_key() -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["index_intros"] = {
        "index_intro:create:summary/knowledge-1": {"intro": "Created"}
    }
    payload["transient_failures"] = {
        "index_intro:create:summary/knowledge-missing": 1
    }

    with pytest.raises(ValidationError, match="未知"):
        FakeDataset.model_validate(payload)


@pytest.mark.asyncio
async def test_fake_embedding_transient_failure_uses_existing_failure_counter() -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["embeddings"] = {"topic:entity/acme": (1.0, 0.0)}
    payload["transient_failures"]["embedding:topic:entity/acme"] = 1
    dataset = FakeDataset.model_validate(payload)
    model = FakeEmbeddingModel(dataset)
    request = EmbeddingRequest(items=(EmbeddingItem(key="topic:entity/acme", text="Acme"),))

    with pytest.raises(TransientModelError):
        await model.embed(request)
    assert (await model.embed(request)).vectors["topic:entity/acme"] == (1.0, 0.0)


def test_embedding_and_taxonomy_ports_are_runtime_checkable() -> None:
    class IncompleteModel:
        pass

    payload = deepcopy(FIXTURE)
    payload["model_responses"]["embeddings"] = {"topic:entity/acme": (1.0, 0.0)}
    payload["model_responses"]["taxonomies"] = {
        "entity/acme": {"decisions": [{"slug": "entity/acme"}]}
    }
    dataset = FakeDataset.model_validate(payload)

    assert isinstance(FakeEmbeddingModel(dataset), EmbeddingModelPort)
    assert isinstance(FakeChatModel(dataset), TaxonomyModelPort)
    assert isinstance(FakeChatModel(dataset), WikiIngestModelPort)
    assert not isinstance(IncompleteModel(), EmbeddingModelPort)
    assert not isinstance(IncompleteModel(), TaxonomyModelPort)


@pytest.mark.asyncio
async def test_fake_embedding_missing_vectors_and_mutations_are_isolated() -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["embeddings"] = {"topic:entity/acme": (1.0, 0.0)}
    model = FakeEmbeddingModel(FakeDataset.model_validate(payload))
    missing = EmbeddingRequest(items=(EmbeddingItem(key="topic:entity/missing", text="Missing"),))
    request = EmbeddingRequest(items=(EmbeddingItem(key="topic:entity/acme", text="Acme"),))

    with pytest.raises(PermanentModelError, match="embedding:topic:entity/missing"):
        await model.embed(missing)
    output = await model.embed(request)

    assert model.requests[1] is not request
    assert tuple(output.vectors) == ("topic:entity/acme",)
    with pytest.raises(TypeError):
        output.vectors["topic:entity/acme"] = (0.0, 1.0)  # type: ignore[index]


@pytest.mark.asyncio
async def test_fake_taxonomy_returns_snapshot_and_retries_exactly() -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["taxonomies"] = {
        "concept/retrieval,entity/acme": {
            "decisions": [
                {"slug": "concept/retrieval", "new_segments": ["Research"]},
                {"slug": "entity/acme", "new_segments": ["Organizations"]},
            ]
        }
    }
    payload["transient_failures"]["taxonomy:concept/retrieval,entity/acme"] = 1
    model = FakeChatModel(FakeDataset.model_validate(payload))
    request = taxonomy_request("concept/retrieval", "entity/acme")

    with pytest.raises(TransientModelError, match="taxonomy:concept/retrieval,entity/acme"):
        await model.plan_folders(request)
    output = await model.plan_folders(request)

    assert [decision.slug for decision in output.decisions] == ["concept/retrieval", "entity/acme"]
    assert model.taxonomy_requests[0] is not request
    with pytest.raises(ValidationError):
        output.decisions = ()


@pytest.mark.asyncio
async def test_fake_taxonomy_canonicalizes_unsorted_request_batch_without_reordering_snapshot() -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["taxonomies"] = {
        "concept/z,entity/a": {
            "decisions": [
                {"slug": "concept/z", "new_segments": ["Concepts"]},
                {"slug": "entity/a", "new_segments": ["Entities"]},
            ]
        }
    }
    model = FakeChatModel(FakeDataset.model_validate(payload))
    request = taxonomy_request("entity/a", "concept/z")

    output = await model.plan_folders(request)

    assert [decision.slug for decision in output.decisions] == ["concept/z", "entity/a"]
    assert [topic.slug for topic in model.taxonomy_requests[0].topics] == [
        "entity/a",
        "concept/z",
    ]
    assert model.calls == ["taxonomy:concept/z,entity/a"]


@pytest.mark.asyncio
async def test_fake_taxonomy_unsorted_request_uses_canonical_transient_failure_key() -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["taxonomies"] = {
        "concept/z,entity/a": {
            "decisions": [{"slug": "concept/z"}, {"slug": "entity/a"}]
        }
    }
    payload["transient_failures"]["taxonomy:concept/z,entity/a"] = 1
    model = FakeChatModel(FakeDataset.model_validate(payload))
    request = taxonomy_request("entity/a", "concept/z")

    with pytest.raises(TransientModelError, match="taxonomy:concept/z,entity/a"):
        await model.plan_folders(request)
    output = await model.plan_folders(request)

    assert [decision.slug for decision in output.decisions] == ["concept/z", "entity/a"]
    assert [tuple(topic.slug for topic in snapshot.topics) for snapshot in model.taxonomy_requests] == [
        ("entity/a", "concept/z"),
        ("entity/a", "concept/z"),
    ]
    assert model.calls == ["taxonomy:concept/z,entity/a", "taxonomy:concept/z,entity/a"]


@pytest.mark.parametrize(
    ("embeddings", "taxonomies", "failures"),
    [
        ({" topic:entity/acme ": (1.0, 0.0)}, {}, {}),
        ({"topic:entity/acme": (1.0, 0.0), " topic:entity/acme ": (0.0, 1.0)}, {}, {}),
        ({"topic:entity/acme,other": (1.0, 0.0)}, {}, {}),
        ({"topic:entity/acme": ()}, {}, {}),
        ({"topic:entity/acme": (1.0,), "folder:x": (1.0, 0.0)}, {}, {}),
        ({"topic:entity/acme": (float("nan"), 0.0)}, {}, {}),
        ({"topic:entity/acme": (1.0, 0.0)}, {}, {"embedding:topic:entity/missing": 1}),
        ({"topic:entity/acme": (1.0, 0.0)}, {}, {"embedding:topic:entity/acme,topic:entity/acme": 1}),
        ({}, {"entity/acme,concept/retrieval": {"decisions": []}}, {}),
        ({}, {"entity/acme,entity/acme": {"decisions": []}}, {}),
        ({}, {"entity/acme": {"decisions": [{"slug": "concept/retrieval"}]}}, {}),
        ({}, {"entity/acme": {"decisions": []}}, {}),
        ({}, {"concept/retrieval,entity/acme": {"decisions": [{"slug": "entity/acme"}, {"slug": "concept/retrieval"}]}}, {}),
        ({}, {"entity/acme": {"decisions": [{"slug": "entity/acme"}]}}, {"taxonomy:concept/missing": 1}),
    ],
)
def test_fixture_rejects_invalid_embedding_taxonomy_contracts(
    embeddings: dict[str, tuple[float, ...]],
    taxonomies: dict[str, dict],
    failures: dict[str, int],
) -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["embeddings"] = embeddings
    payload["model_responses"]["taxonomies"] = taxonomies
    payload["transient_failures"] = failures

    with pytest.raises(ValidationError):
        FakeDataset.model_validate(payload)


@pytest.mark.parametrize(
    "embeddings",
    [
        {"": (1.0, 0.0)},
        {"x" * 513: (1.0, 0.0)},
        {"topic:entity/acme": (float("inf"), 0.0)},
        {"topic:entity/acme": (float("-inf"), 0.0)},
    ],
)
def test_fixture_rejects_embedding_identity_and_finite_value_boundaries(
    embeddings: dict[str, tuple[float, ...]],
) -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["embeddings"] = embeddings

    with pytest.raises(ValidationError, match="embedding"):
        FakeDataset.model_validate(payload)


def test_fixture_accepts_explicit_empty_embeddings_with_independent_state() -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["embeddings"] = {}

    first = FakeDataset.model_validate(payload)
    second = FakeDataset.model_validate(payload)

    assert first.model_responses.embeddings == second.model_responses.embeddings == {}
    assert first.model_responses.embeddings is not second.model_responses.embeddings
    assert first.model_responses.embeddings is not payload["model_responses"]["embeddings"]


@pytest.mark.parametrize("batch_key", ["", ",entity/acme", "entity/acme,"])
def test_fixture_rejects_empty_taxonomy_batch_key_segments(batch_key: str) -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["taxonomies"] = {batch_key: {"decisions": []}}

    with pytest.raises(ValidationError, match=r"taxonomies.*batch key"):
        FakeDataset.model_validate(payload)


@pytest.mark.asyncio
async def test_load_fake_runtime_adapters_preserves_legacy_shape_and_isolates_state(
    tmp_path: Path,
) -> None:
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["embeddings"] = {"topic:entity/acme": (1.0, 0.0)}
    payload["transient_failures"] = {"embedding:topic:entity/acme": 1}
    fixture = tmp_path / "wiki.json"
    write_fixture(fixture, payload)

    source, chat = load_fake_adapters(fixture)
    first_source, first_chat, first_embedding = load_fake_runtime_adapters(fixture)
    second_source, second_chat, second_embedding = load_fake_runtime_adapters(fixture)
    request = EmbeddingRequest(items=(EmbeddingItem(key="topic:entity/acme", text="Acme"),))

    with pytest.raises(TransientModelError):
        await first_embedding.embed(request)
    with pytest.raises(TransientModelError):
        await second_embedding.embed(request)
    first_output = await first_embedding.embed(request)
    second_output = await second_embedding.embed(request)

    assert isinstance(source, FakeKnowledgeSource)
    assert isinstance(chat, FakeChatModel)
    assert isinstance(first_source, FakeKnowledgeSource)
    assert isinstance(first_chat, FakeChatModel)
    assert isinstance(first_embedding, FakeEmbeddingModel)
    assert isinstance(second_source, FakeKnowledgeSource)
    assert isinstance(second_chat, FakeChatModel)
    assert isinstance(second_embedding, FakeEmbeddingModel)
    assert first_output.vectors == second_output.vectors == {"topic:entity/acme": (1.0, 0.0)}
    assert first_chat.calls == second_chat.calls == []
    assert first_embedding.calls == second_embedding.calls == [
        "embedding:topic:entity/acme",
        "embedding:topic:entity/acme",
    ]
    assert len(first_embedding.requests) == len(second_embedding.requests) == 2
    assert first_embedding.calls is not second_embedding.calls
    assert first_embedding.requests is not second_embedding.requests
    assert first_embedding.requests[0] is not second_embedding.requests[0]


def test_load_fake_adapters_validates_fixture_and_protocols(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    write_fixture(fixture)

    source, model = load_fake_adapters(fixture)

    assert isinstance(source, FakeKnowledgeSource)
    assert isinstance(model, FakeChatModel)
    assert isinstance(source, KnowledgeSourcePort)
    assert isinstance(model, ChatModelPort)
    assert isinstance(model, CitationModelPort)
    assert isinstance(model, DedupModelPort)
    assert isinstance(model, WikiIngestModelPort)


def test_tombstone_port_is_runtime_checkable() -> None:
    class MemoryTombstones:
        async def mark_deleted(self, scope: WikiScope, knowledge_id: str) -> None:
            return None

        async def is_deleted(self, scope: WikiScope, knowledge_id: str) -> bool:
            return False

    assert isinstance(MemoryTombstones(), TombstonePort)


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


def test_fake_model_returns_citations_and_dedup_decisions_as_copies(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["model_responses"]["citations"] = {
        "knowledge-1": [{"refs_by_slug": {"entity/acme": ["c001"]}}]
    }
    data["model_responses"]["deduplications"] = {
        "entity/acme": {"candidate_slug": "entity/acme", "canonical_slug": "entity/existing"}
    }
    data["transient_failures"] = {}
    write_fixture(fixture, data)
    _, model = load_fake_adapters(fixture)
    citation_request = CitationBatchRequest(
        knowledge_id="knowledge-1", batch_index=0,
        candidates=[TopicCandidate(name="Acme", slug="entity/acme", page_type="entity")],
        chunks=[CitationBatchChunk(alias="c001", text="Body")],
    )
    dedup_request = DedupRequest(candidates=[DedupCandidateRequest(
        candidate=TopicCandidate(name="Acme", slug="entity/acme", page_type="entity"),
        allowed_targets=[DedupPageCandidate(slug="entity/existing", title="Existing", page_type="entity")],
    )])

    citations = asyncio.run(model.classify_chunks(citation_request))
    decisions = asyncio.run(model.resolve_duplicates(dedup_request))
    with pytest.raises(AttributeError):
        citations.refs_by_slug["entity/acme"].append("c002")
    with pytest.raises(ValidationError):
        citation_request.chunks[0].text = "Mutated"

    assert decisions.decisions[0].canonical_slug == "entity/existing"
    assert model.citation_requests[0].chunks[0].text == "Body"
    assert asyncio.run(model.classify_chunks(citation_request)).refs_by_slug == {"entity/acme": ("c001",)}


def test_fake_model_dedup_defaults_orders_calls_and_isolates_copies(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["model_responses"]["deduplications"] = {
        "entity/acme": {"candidate_slug": "entity/acme", "canonical_slug": "entity/existing"}
    }
    data["transient_failures"] = {}
    write_fixture(fixture, data)
    _, model = load_fake_adapters(fixture)
    first = TopicCandidate(name="Acme", slug="entity/acme", page_type="entity", aliases=["Acme"])
    second = TopicCandidate(name="Other", slug="entity/other", page_type="entity")
    request = DedupRequest(candidates=[
        DedupCandidateRequest(candidate=first, allowed_targets=[]),
        DedupCandidateRequest(candidate=second, allowed_targets=[]),
    ])

    output = asyncio.run(model.resolve_duplicates(request))
    with pytest.raises(ValidationError):
        output.decisions[0].canonical_slug = None
    first.aliases.append("Mutated")
    next_output = asyncio.run(model.resolve_duplicates(request))

    assert [decision.canonical_slug for decision in next_output.decisions] == ["entity/existing", None]
    assert next_output is not output
    assert model.calls == ["dedup:entity/acme", "dedup:entity/other", "dedup:entity/acme", "dedup:entity/other"]
    assert model.dedup_requests[0].candidates[0].candidate.aliases == ("Acme",)


def test_fake_model_dedup_retries_transient_failure(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["model_responses"]["deduplications"] = {
        "entity/acme": {"candidate_slug": "entity/acme", "canonical_slug": "entity/existing"}
    }
    data["transient_failures"] = {"dedup:entity/acme": 1}
    write_fixture(fixture, data)
    _, model = load_fake_adapters(fixture)
    request = DedupRequest(candidates=[DedupCandidateRequest(
        candidate=TopicCandidate(name="Acme", slug="entity/acme", page_type="entity"), allowed_targets=[]
    )])

    with pytest.raises(TransientModelError, match="dedup:entity/acme"):
        asyncio.run(model.resolve_duplicates(request))
    assert asyncio.run(model.resolve_duplicates(request)).decisions[0].canonical_slug == "entity/existing"
    assert model.calls == ["dedup:entity/acme", "dedup:entity/acme"]


def test_fake_model_citation_missing_and_transient_failure_are_distinct(tmp_path: Path) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["model_responses"]["citations"] = {"knowledge-1": [{"refs_by_slug": {}}]}
    data["transient_failures"] = {"citation:knowledge-1:0": 1}
    write_fixture(fixture, data)
    _, model = load_fake_adapters(fixture)
    request = CitationBatchRequest(
        knowledge_id="knowledge-1", batch_index=0, candidates=[],
        chunks=[CitationBatchChunk(alias="c001", text="Body")],
    )

    with pytest.raises(TransientModelError, match="citation:knowledge-1:0"):
        asyncio.run(model.classify_chunks(request))
    assert asyncio.run(model.classify_chunks(request)).refs_by_slug == {}
    with pytest.raises(PermanentModelError, match="citation:knowledge-1:1"):
        asyncio.run(model.classify_chunks(request.model_copy(update={"batch_index": 1})))


@pytest.mark.parametrize(
    ("key", "response"),
    [
        ("missing", {"candidate_slug": "entity/missing"}),
        ("ENTITY/ACME", {"candidate_slug": "entity/acme"}),
    ],
)
def test_fixture_rejects_invalid_dedup_response_keys(tmp_path: Path, key: str, response: dict) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["model_responses"]["deduplications"] = {key: response}
    write_fixture(fixture, data)
    with pytest.raises(ValidationError):
        load_fake_adapters(fixture)


@pytest.mark.parametrize("batch", ["00", "01", "-1", "١", "1.0"])
def test_fixture_rejects_noncanonical_citation_transient_batch(tmp_path: Path, batch: str) -> None:
    fixture = tmp_path / "wiki.json"
    data = json.loads(json.dumps(FIXTURE))
    data["transient_failures"] = {f"citation:knowledge-1:{batch}": 1}
    write_fixture(fixture, data)

    with pytest.raises(ValidationError):
        load_fake_adapters(fixture)


def test_fake_citation_mappings_are_deeply_isolated() -> None:
    data = json.loads(json.dumps(FIXTURE))
    data["model_responses"]["citations"] = {
        "knowledge-1": [{"refs_by_slug": {"entity/acme": ["c001"]}}]
    }
    dataset = FakeDataset.model_validate(data)
    model = FakeChatModel(dataset)
    request = CitationBatchRequest(
        knowledge_id="knowledge-1", batch_index=0, candidates=[],
        chunks=[CitationBatchChunk(alias="c001", text="Body")],
    )

    dataset_refs = dataset.model_responses.citations["knowledge-1"][0].refs_by_slug
    model_refs = model._responses.citations["knowledge-1"][0].refs_by_slug
    first = asyncio.run(model.classify_chunks(request)).refs_by_slug
    second = asyncio.run(model.classify_chunks(request)).refs_by_slug

    assert len({id(dataset_refs), id(model_refs), id(first), id(second)}) == 4
    before_dump = CitationBatchOutput(refs_by_slug=first).model_dump()
    with pytest.raises((AttributeError, TypeError)):
        first._items = (("bad/key", ("bad",)),)  # type: ignore[attr-defined]
    with pytest.raises((AttributeError, TypeError)):
        first._lookup = {}  # type: ignore[attr-defined]
    with pytest.raises(TypeError):
        first._lookup["entity/acme"] = ("c999",)  # type: ignore[attr-defined]
    assert CitationBatchOutput(refs_by_slug=first).model_dump() == before_dump
    assert dataset_refs == {"entity/acme": ("c001",)}
    assert model_refs == {"entity/acme": ("c001",)}
    assert second == {"entity/acme": ("c001",)}
