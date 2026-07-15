from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from app.wiki.ingest.schemas import (
    BatchResult,
    CandidateExtraction,
    DocumentSummary,
    FinalizationRequest,
    PageContribution,
    PageMergeOutput,
    PageMergeRequest,
    ReducedPage,
    SlugUpdate,
    SourceChunk,
    SourceKnowledge,
    TopicCandidate,
    WikiIngestConfig,
    WikiWorkerOptions,
)
from app.wiki.scope import WikiScope


KB_ID = UUID("11111111-1111-1111-1111-111111111111")


def test_ingest_config_and_worker_options_use_safe_defaults() -> None:
    config = WikiIngestConfig()
    options = WikiWorkerOptions()

    assert config.model_dump() == {
        "wiki_enabled": True,
        "synthesis_model_id": None,
        "summary_model_id": None,
        "extraction_granularity": "standard",
        "max_pages_per_ingest": 0,
    }
    assert options.model_dump() == {
        "batch_size": 5,
        "map_parallel": 10,
        "reduce_parallel": 10,
        "claim_timeout_seconds": 600,
        "max_pages_per_ingest": 0,
        "extraction_granularity": "standard",
    }


def test_worker_options_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_WIKI_INGEST_BATCH_SIZE", "7")
    monkeypatch.setenv("GRAPH_WIKI_INGEST_MAP_PARALLEL", "3")
    monkeypatch.setenv("GRAPH_WIKI_INGEST_REDUCE_PARALLEL", "4")
    monkeypatch.setenv("GRAPH_WIKI_CLAIM_TIMEOUT_SECONDS", "900")
    monkeypatch.setenv("GRAPH_WIKI_MAX_PAGES_PER_INGEST", "12")
    monkeypatch.setenv("GRAPH_WIKI_EXTRACTION_GRANULARITY", "focused")

    options = WikiWorkerOptions.from_env()

    assert options.batch_size == 7
    assert options.map_parallel == 3
    assert options.reduce_parallel == 4
    assert options.claim_timeout_seconds == 900
    assert options.max_pages_per_ingest == 12
    assert options.extraction_granularity == "focused"


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("GRAPH_WIKI_INGEST_BATCH_SIZE", "0"),
        ("GRAPH_WIKI_INGEST_MAP_PARALLEL", "101"),
        ("GRAPH_WIKI_INGEST_REDUCE_PARALLEL", "not-an-integer"),
        ("GRAPH_WIKI_CLAIM_TIMEOUT_SECONDS", "59"),
        ("GRAPH_WIKI_MAX_PAGES_PER_INGEST", "-1"),
        ("GRAPH_WIKI_EXTRACTION_GRANULARITY", "broad"),
    ],
)
def test_worker_options_reject_invalid_environment(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValidationError):
        WikiWorkerOptions.from_env()


def test_source_models_validate_identity_and_activity() -> None:
    knowledge = SourceKnowledge(
        id="knowledge-1",
        tenant_id=1,
        knowledge_base_id=KB_ID,
        title="Document One",
        op_version="version-1",
    )

    assert knowledge.is_active is True
    assert knowledge.model_copy(update={"status": "deleting"}).is_active is False
    assert SourceChunk(id="chunk-1").model_dump() == {
        "id": "chunk-1",
        "chunk_index": 0,
        "start_at": 0,
        "text": "",
        "ocr_text": "",
        "image_caption": "",
    }

    for model, values in (
        (SourceChunk, {"id": ""}),
        (
            SourceKnowledge,
            {
                "id": "",
                "tenant_id": 1,
                "knowledge_base_id": KB_ID,
                "title": "Document One",
                "op_version": "version-1",
            },
        ),
    ):
        with pytest.raises(ValidationError):
            model.model_validate(values)


def test_candidate_normalizes_slug_name_and_aliases_stably() -> None:
    candidate = TopicCandidate(
        name="  Acme  ",
        slug="  ENTITY/ACME  ",
        page_type="entity",
        aliases=[" ACME ", "ACME", "", "  ", "Acme Corp"],
    )

    assert candidate.name == "Acme"
    assert candidate.slug == "entity/acme"
    assert candidate.aliases == ["ACME", "Acme Corp"]
    assert candidate.description == ""
    assert candidate.details == ""


@pytest.mark.parametrize("slug", ["entity/acme corp", "entity/acme?x=1", "entity/acme.company"])
def test_candidate_rejects_illegal_slug_characters(slug: str) -> None:
    with pytest.raises(ValidationError):
        TopicCandidate(name="Acme", slug=slug, page_type="entity")


def test_candidate_requires_matching_type_prefix() -> None:
    with pytest.raises(ValidationError):
        TopicCandidate(name="Acme", slug="concept/acme", page_type="entity")


@pytest.mark.parametrize("name", ["", "  ", "\t\n"])
def test_candidate_rejects_blank_name(name: str) -> None:
    with pytest.raises(ValidationError):
        TopicCandidate(name=name, slug="entity/acme", page_type="entity")


def test_candidate_extraction_rejects_candidates_in_wrong_group() -> None:
    with pytest.raises(ValidationError):
        CandidateExtraction(
            entities=[
                TopicCandidate(name="Retrieval", slug="concept/retrieval", page_type="concept")
            ]
        )

    with pytest.raises(ValidationError):
        CandidateExtraction(
            concepts=[TopicCandidate(name="Acme", slug="entity/acme", page_type="entity")]
        )


@pytest.mark.parametrize("model", [DocumentSummary, PageMergeOutput])
def test_model_text_outputs_strip_and_reject_blank_values(model: type) -> None:
    output = model(headline="  Headline  ", markdown="  Body  ")

    assert output.headline == "Headline"
    assert output.markdown == "Body"

    with pytest.raises(ValidationError):
        model(headline="  ", markdown="Body")


def test_merge_request_requires_at_least_one_contribution() -> None:
    with pytest.raises(ValidationError):
        PageMergeRequest(
            slug="entity/acme",
            title="Acme",
            page_type="entity",
            contributions=[],
        )


def test_slug_update_requires_matching_page_type_prefix() -> None:
    with pytest.raises(ValidationError):
        SlugUpdate(
            pending_op_id=uuid4(),
            knowledge_id="knowledge-1",
            slug="concept/acme",
            title="Acme",
            page_type="entity",
        )


def test_reduced_page_supports_summary_pages() -> None:
    page = ReducedPage(
        slug="summary/knowledge-1",
        title="Document One",
        page_type="summary",
        content="Body",
        summary="Summary",
    )

    assert page.page_type == "summary"


def test_finalization_request_uses_scope_and_source_version() -> None:
    scope = WikiScope(tenant_id=1, knowledge_base_id=KB_ID, actor_id="worker")
    knowledge = SourceKnowledge(
        id="knowledge-1",
        tenant_id=1,
        knowledge_base_id=KB_ID,
        title="Document One",
        op_version="version-1",
    )

    request = FinalizationRequest.from_knowledge(scope, knowledge)

    assert request.model_dump() == {
        "tenant_id": 1,
        "knowledge_base_id": KB_ID,
        "knowledge_id": "knowledge-1",
        "attempt": "version-1",
        "subtask_name": "wiki",
    }


def test_batch_result_exposes_read_only_counts() -> None:
    result = BatchResult(completed_op_ids=[uuid4(), uuid4()], failed_op_ids=[uuid4()])

    assert result.completed_ops == 2
    assert result.failed_ops == 1

    with pytest.raises(AttributeError):
        result.completed_ops = 99


def test_batch_result_from_ids_classifies_pending_ids_stably() -> None:
    pending_a, pending_b, pending_c = uuid4(), uuid4(), uuid4()

    result = BatchResult.from_ids(
        [pending_a, pending_b, pending_c],
        [pending_b],
    )

    assert result.completed_op_ids == [pending_a, pending_c]
    assert result.failed_op_ids == [pending_b]
    assert result.completed_ops == 2
    assert result.failed_ops == 1


def test_batch_result_from_ids_deduplicates_and_rejects_unknown_failures() -> None:
    pending_a, pending_b, pending_c, unknown = uuid4(), uuid4(), uuid4(), uuid4()

    result = BatchResult.from_ids(
        [pending_a, pending_b, pending_a, pending_c],
        [pending_b, pending_b],
    )

    assert result.completed_op_ids == [pending_a, pending_c]
    assert result.failed_op_ids == [pending_b]

    with pytest.raises(ValueError, match="pending"):
        BatchResult.from_ids([pending_a, pending_b], [pending_b, unknown])


def test_page_contribution_uses_independent_list_defaults() -> None:
    first = PageContribution(
        pending_op_id=uuid4(),
        knowledge_id="knowledge-1",
        title="Document One",
        content="Body",
        summary="Summary",
    )
    second = PageContribution(
        pending_op_id=uuid4(),
        knowledge_id="knowledge-2",
        title="Document Two",
        content="Body",
        summary="Summary",
    )

    first.aliases.append("Acme")

    assert second.aliases == []
    assert second.source_refs == []
    assert second.chunk_refs == []
