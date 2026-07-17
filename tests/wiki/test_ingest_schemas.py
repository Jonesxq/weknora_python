from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from app.wiki.ingest.schemas import (
    BatchApplyRequest,
    BatchResult,
    CandidateExtraction,
    CitationBatchChunk,
    CitationBatchOutput,
    CitationBatchRequest,
    ContributionDelta,
    DedupCandidateRequest,
    DedupDecision,
    DedupOutput,
    DedupPageCandidate,
    DedupRequest,
    DocumentSummary,
    FinalizationRequest,
    MapDocumentResult,
    PageContribution,
    PageMergeOutput,
    PageMergeRequest,
    OperationFailure,
    PageExpectation,
    ReducedPage,
    SlugUpdate,
    SourceChunk,
    SourceKnowledge,
    StoredContributionRecord,
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
        "citation_batch_chars": 12000,
        "citation_parallel": 4,
        "dedup_candidate_limit": 20,
        "tombstone_ttl_seconds": 3600,
    }


def test_worker_options_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_WIKI_INGEST_BATCH_SIZE", "7")
    monkeypatch.setenv("GRAPH_WIKI_INGEST_MAP_PARALLEL", "3")
    monkeypatch.setenv("GRAPH_WIKI_INGEST_REDUCE_PARALLEL", "4")
    monkeypatch.setenv("GRAPH_WIKI_CLAIM_TIMEOUT_SECONDS", "900")
    monkeypatch.setenv("GRAPH_WIKI_MAX_PAGES_PER_INGEST", "12")
    monkeypatch.setenv("GRAPH_WIKI_EXTRACTION_GRANULARITY", "focused")
    monkeypatch.setenv("GRAPH_WIKI_CITATION_BATCH_CHARS", "13000")
    monkeypatch.setenv("GRAPH_WIKI_CITATION_PARALLEL", "5")
    monkeypatch.setenv("GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT", "19")
    monkeypatch.setenv("GRAPH_WIKI_TOMBSTONE_TTL_SECONDS", "7200")

    options = WikiWorkerOptions.from_env()

    assert options.batch_size == 7
    assert options.map_parallel == 3
    assert options.reduce_parallel == 4
    assert options.claim_timeout_seconds == 900
    assert options.max_pages_per_ingest == 12
    assert options.extraction_granularity == "focused"
    assert options.citation_batch_chars == 13000
    assert options.citation_parallel == 5
    assert options.dedup_candidate_limit == 19
    assert options.tombstone_ttl_seconds == 7200


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


@pytest.mark.parametrize("slug", ["entity//acme", "entity/acme/", "entity/acme//detail"])
def test_slug_rejects_empty_path_segments(slug: str) -> None:
    with pytest.raises(ValidationError):
        TopicCandidate(name="Acme", slug=slug, page_type="entity")


def test_finalization_request_rejects_cross_scope_knowledge() -> None:
    knowledge = SourceKnowledge(
        id="knowledge-1",
        tenant_id=2,
        knowledge_base_id=KB_ID,
        title="Document One",
        op_version="version-1",
    )
    with pytest.raises(ValueError, match="租户"):
        FinalizationRequest.from_knowledge(
            WikiScope(tenant_id=1, knowledge_base_id=KB_ID, actor_id="worker"),
            knowledge,
        )


def test_batch_result_rejects_duplicate_or_overlapping_ids() -> None:
    op_id = uuid4()
    with pytest.raises(ValidationError):
        BatchResult(completed_op_ids=[op_id, op_id])
    with pytest.raises(ValidationError):
        BatchResult(completed_op_ids=[op_id], failed_op_ids=[op_id])


def test_identity_fields_strip_and_reject_blank_values() -> None:
    assert SourceChunk(id=" chunk-1 ").id == "chunk-1"
    with pytest.raises(ValidationError):
        SourceChunk(id="   ")
    with pytest.raises(ValidationError):
        SourceChunk(id="chunk-1", chunk_index=-1)
    with pytest.raises(ValidationError):
        SourceChunk(id="chunk-1", start_at=-1)
    with pytest.raises(ValidationError):
        MapDocumentResult(
            pending_op_id=uuid4(),
            knowledge_id="   ",
        )


def test_worker_options_include_incremental_safe_defaults() -> None:
    options = WikiWorkerOptions()

    assert options.citation_batch_chars == 12000
    assert options.citation_parallel == 4
    assert options.dedup_candidate_limit == 20
    assert options.tombstone_ttl_seconds == 3600


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("GRAPH_WIKI_CITATION_BATCH_CHARS", "999"),
        ("GRAPH_WIKI_CITATION_BATCH_CHARS", "100001"),
        ("GRAPH_WIKI_CITATION_PARALLEL", "0"),
        ("GRAPH_WIKI_CITATION_PARALLEL", "33"),
        ("GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT", "0"),
        ("GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT", "21"),
        ("GRAPH_WIKI_TOMBSTONE_TTL_SECONDS", "59"),
        ("GRAPH_WIKI_TOMBSTONE_TTL_SECONDS", "86401"),
    ],
)
def test_worker_options_reject_invalid_incremental_environment(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        WikiWorkerOptions.from_env()


def test_citation_dtos_normalize_and_reject_hidden_chunk_ids() -> None:
    candidate = TopicCandidate(name="Acme", slug="entity/acme", page_type="entity")
    request = CitationBatchRequest(
        knowledge_id=" knowledge-1 ",
        batch_index=0,
        candidates=[candidate],
        chunks=[CitationBatchChunk(alias="c001", text=" Body ")],
    )
    input_refs = {" entity/acme ": [" c001 "]}
    output = CitationBatchOutput(refs_by_slug=input_refs)

    assert request.knowledge_id == "knowledge-1"
    assert request.chunks[0].text == "Body"
    assert output.refs_by_slug == {"entity/acme": ["c001"]}
    assert input_refs == {" entity/acme ": [" c001 "]}
    with pytest.raises(ValidationError):
        CitationBatchChunk(alias="c001", text="Body", chunk_id="internal")
    with pytest.raises(ValidationError):
        CitationBatchRequest(
            knowledge_id="knowledge-1", batch_index=0, candidates=[candidate],
            chunks=[CitationBatchChunk(alias="c001", text="A"), CitationBatchChunk(alias="c001", text="B")],
        )
    for aliases in ([" "], [" c001 ", "c001"]):
        with pytest.raises(ValidationError):
            CitationBatchOutput(refs_by_slug={"entity/acme": aliases})


def test_dedup_dtos_enforce_static_target_and_decision_contracts() -> None:
    candidate = TopicCandidate(name="Acme", slug="entity/acme", page_type="entity")
    input_aliases = [" Existing alias "]
    target = DedupPageCandidate(slug=" ENTITY/existing ", title=" Existing ", page_type="entity", aliases=input_aliases)
    request = DedupRequest(candidates=[DedupCandidateRequest(candidate=candidate, allowed_targets=[target])])
    decision = DedupDecision(candidate_slug=" ENTITY/ACME ", canonical_slug=" entity/existing ")

    assert request.candidates[0].allowed_targets[0].aliases == ["Existing alias"]
    assert input_aliases == [" Existing alias "]
    assert decision.candidate_slug == "entity/acme"
    assert decision.canonical_slug == "entity/existing"
    with pytest.raises(ValidationError):
        DedupCandidateRequest(candidate=candidate, allowed_targets=[DedupPageCandidate(slug="concept/x", title="X", page_type="concept")])
    with pytest.raises(ValidationError):
        DedupOutput(decisions=[decision, decision])
    for aliases in ([" "], [" A ", "A"]):
        with pytest.raises(ValidationError):
            DedupPageCandidate(slug="entity/existing", title="Existing", page_type="entity", aliases=aliases)


def _record(
    *, slug: str = "entity/acme", knowledge_id: str = "knowledge-1", state: str = "active",
    tenant_id: int = 1, knowledge_base_id: UUID = KB_ID, page_type: str = "entity",
) -> StoredContributionRecord:
    return StoredContributionRecord(
        id=None, tenant_id=tenant_id, knowledge_base_id=knowledge_base_id, slug=slug, knowledge_id=knowledge_id,
        op_version="v1", page_type=page_type, state=state, title="Acme", content="Body", summary="Summary",
        aliases=[" Acme "], chunk_refs=[" c001 "],
    )


@pytest.mark.parametrize(
    ("action", "previous", "current"),
    [
        ("add", None, _record()),
        ("replace", _record(), _record()),
        ("retract_stale", _record(), None),
        ("retract", _record(state="retract_pending"), None),
    ],
)
def test_contribution_delta_accepts_each_action_contract(
    action: str, previous: StoredContributionRecord | None, current: StoredContributionRecord | None
) -> None:
    delta = ContributionDelta(pending_op_id=uuid4(), action=action, slug="entity/acme", knowledge_id="knowledge-1", previous=previous, current=current)
    assert delta.action == action


@pytest.mark.parametrize(
    ("action", "previous", "current"),
    [
        ("add", _record(), _record()),
        ("replace", None, _record()),
        ("retract_stale", _record(state="retract_pending"), None),
        ("retract", _record(), None),
    ],
)
def test_contribution_delta_rejects_invalid_action_states(
    action: str, previous: StoredContributionRecord | None, current: StoredContributionRecord | None
) -> None:
    with pytest.raises(ValidationError):
        ContributionDelta(pending_op_id=uuid4(), action=action, slug="entity/acme", knowledge_id="knowledge-1", previous=previous, current=current)


@pytest.mark.parametrize(
    "record",
    [
        _record(slug="entity/other"),
        _record(knowledge_id="other"),
    ],
)
def test_contribution_delta_rejects_mismatched_delta_identity(record: StoredContributionRecord) -> None:
    with pytest.raises(ValidationError):
        ContributionDelta(pending_op_id=uuid4(), action="add", slug="entity/acme", knowledge_id="knowledge-1", previous=None, current=record)


@pytest.mark.parametrize(
    "current",
    [
        _record(tenant_id=2),
        _record(knowledge_base_id=UUID("22222222-2222-2222-2222-222222222222")),
        _record(slug="concept/acme", page_type="concept"),
    ],
)
def test_contribution_delta_rejects_mismatched_previous_current_scope_or_type(current: StoredContributionRecord) -> None:
    with pytest.raises(ValidationError):
        ContributionDelta(pending_op_id=uuid4(), action="replace", slug="entity/acme", knowledge_id="knowledge-1", previous=_record(), current=current)


def test_contribution_delta_rejects_mismatched_previous_current_page_type() -> None:
    invalid_current = _record().model_copy(update={"page_type": "concept"})
    with pytest.raises(ValidationError):
        ContributionDelta(pending_op_id=uuid4(), action="replace", slug="entity/acme", knowledge_id="knowledge-1", previous=_record(), current=invalid_current)


@pytest.mark.parametrize("field", ["aliases", "chunk_refs"])
@pytest.mark.parametrize("values", [[" "], [" value ", "value"]])
def test_stored_contribution_record_rejects_empty_or_duplicate_arrays(field: str, values: list[str]) -> None:
    payload = _record().model_dump()
    payload[field] = values
    with pytest.raises(ValidationError):
        StoredContributionRecord(**payload)


def test_stored_contribution_record_strips_arrays_without_mutating_input() -> None:
    aliases = [" Acme alias "]
    chunk_refs = [" c001 "]
    payload = _record().model_dump()
    payload.update(aliases=aliases, chunk_refs=chunk_refs)
    record = StoredContributionRecord(**payload)

    assert record.aliases == ["Acme alias"]
    assert record.chunk_refs == ["c001"]
    assert aliases == [" Acme alias "]
    assert chunk_refs == [" c001 "]


def test_apply_request_enforces_operation_and_page_expectation_invariants() -> None:
    op_id, other_id = uuid4(), uuid4()
    page = ReducedPage(slug="entity/acme", title="Acme", page_type="entity", content="Body", summary="Summary")
    request = BatchApplyRequest(
        claim_token=uuid4(), pages=[page], contribution_deltas=[], completed_op_ids=[op_id],
        superseded_op_ids=[], failures=[OperationFailure(pending_op_id=other_id, error_code=" CODE ", error_summary=" broken\n now ")],
        expected_pages=[PageExpectation(slug="entity/acme", page_id=uuid4(), version=1)], operation_id=uuid4(),
    )
    assert request.failures[0].error_code == "CODE"
    assert request.failures[0].error_summary == "broken now"
    with pytest.raises(ValidationError):
        PageExpectation(slug="entity/acme", page_id=uuid4(), version=None)
    with pytest.raises(ValidationError):
        PageExpectation(slug="entity/acme", page_id=None, version=1)
    with pytest.raises(ValidationError):
        BatchApplyRequest(claim_token=uuid4(), pages=[page], contribution_deltas=[], completed_op_ids=[op_id], superseded_op_ids=[op_id], failures=[], expected_pages=[], operation_id=uuid4())


@pytest.mark.parametrize("field", ["completed_op_ids", "superseded_op_ids", "failures"])
def test_apply_request_rejects_duplicate_operation_ids(field: str) -> None:
    op_id = uuid4()
    payload = dict(claim_token=uuid4(), pages=[], contribution_deltas=[], completed_op_ids=[], superseded_op_ids=[], failures=[], expected_pages=[], operation_id=uuid4())
    payload[field] = ([op_id, op_id] if field != "failures" else [OperationFailure(pending_op_id=op_id, error_code="code", error_summary="failed")] * 2)
    with pytest.raises(ValidationError):
        BatchApplyRequest(**payload)


@pytest.mark.parametrize(
    ("completed", "superseded", "failures"),
    [(["shared"], ["shared"], []), (["shared"], [], ["shared"]), ([], ["shared"], ["shared"])],
)
def test_apply_request_rejects_cross_group_operation_id_overlap(
    completed: list[str], superseded: list[str], failures: list[str]
) -> None:
    op_id = uuid4()
    make_ids = lambda values: [op_id for _ in values]
    with pytest.raises(ValidationError):
        BatchApplyRequest(claim_token=uuid4(), pages=[], contribution_deltas=[], completed_op_ids=make_ids(completed), superseded_op_ids=make_ids(superseded), failures=[OperationFailure(pending_op_id=op_id, error_code="code", error_summary="failed") for _ in failures], expected_pages=[], operation_id=uuid4())


@pytest.mark.parametrize("field", ["pages", "expected_pages"])
def test_apply_request_rejects_duplicate_page_slugs(field: str) -> None:
    page = ReducedPage(slug="entity/acme", title="Acme", page_type="entity", content="Body", summary="Summary")
    expected = PageExpectation(slug="entity/acme")
    payload = dict(claim_token=uuid4(), pages=[], contribution_deltas=[], completed_op_ids=[], superseded_op_ids=[], failures=[], expected_pages=[], operation_id=uuid4())
    payload[field] = [page, page] if field == "pages" else [expected, expected]
    with pytest.raises(ValidationError):
        BatchApplyRequest(**payload)
