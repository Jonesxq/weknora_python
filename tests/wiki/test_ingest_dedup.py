from __future__ import annotations

import pytest

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.dedup import deduplicate_candidates, validate_dedup_output
from app.wiki.ingest.schemas import (
    DedupDecision,
    DedupCandidateRequest,
    DedupRequest,
    DedupOutput,
    DedupPageCandidate,
    TopicCandidate,
)
from app.wiki.ingest.store import ExistingPageRecord
from app.wiki.ingest.schemas import ReducedPage
from app.wiki.ingest.ports import PermanentModelError, TransientModelError
from uuid import uuid4
from app.wiki.scope import WikiScope


SCOPE = WikiScope(tenant_id=7, knowledge_base_id="11111111-1111-1111-1111-111111111111", actor_id="worker")


class Store:
    def __init__(self, pages: dict[str, list[DedupPageCandidate]]) -> None:
        self.pages = pages
        self.calls: list[str] = []

    async def find_existing_pages(self, _scope, slugs):
        return {
            slug: ExistingPageRecord(
                page_id=uuid4(), version=1,
                page=ReducedPage(slug=slug, title="Existing", page_type="entity", content="", summary=""),
            )
            for slug in slugs if slug == "entity/existing"
        }

    async def find_dedup_candidates(self, _scope, candidate, limit=20):
        self.calls.append(candidate.slug)
        return self.pages.get(candidate.slug, [])[:limit]


class Model:
    def __init__(self, output: DedupOutput) -> None:
        self.output = output
        self.calls = 0

    async def resolve_duplicates(self, _request):
        self.calls += 1
        return self.output


@pytest.mark.asyncio
async def test_exact_existing_slug_skips_model_and_returns_canonical() -> None:
    model = Model(DedupOutput())
    result, mapping = await deduplicate_candidates(
        SCOPE, [TopicCandidate(name="Acme", slug="entity/existing", page_type="entity")], Store({}), model
    )
    assert model.calls == 0
    assert [item.slug for item in result] == ["entity/existing"]
    assert mapping == {"entity/existing": "entity/existing"}


@pytest.mark.asyncio
async def test_model_canonical_merges_metadata_and_maps_all_inputs() -> None:
    existing = DedupPageCandidate(slug="entity/acme", title="ACME", page_type="entity", aliases=["Company"])
    model = Model(DedupOutput(decisions=[
        DedupDecision(candidate_slug="entity/acme-new", canonical_slug="entity/acme"),
        DedupDecision(candidate_slug="entity/acme-alt", canonical_slug="entity/acme"),
    ]))
    result, mapping = await deduplicate_candidates(
        SCOPE,
        [
            TopicCandidate(name="Acme", slug="entity/acme-new", page_type="entity", aliases=["A"], description="First"),
            TopicCandidate(name="Acme Ltd", slug="entity/acme-alt", page_type="entity", aliases=["A"], details="Second"),
        ],
        Store({"entity/acme-new": [existing], "entity/acme-alt": [existing]}), model,
    )
    assert model.calls == 1
    assert [item.slug for item in result] == ["entity/acme"]
    assert result[0].aliases == ["Company", "Acme", "A", "Acme Ltd"]
    assert result[0].description == "First"
    assert result[0].details == "Second"
    assert mapping == {"entity/acme-new": "entity/acme", "entity/acme-alt": "entity/acme"}


@pytest.mark.asyncio
async def test_rejects_model_target_outside_store_whitelist() -> None:
    model = Model(DedupOutput(decisions=[DedupDecision(candidate_slug="entity/new", canonical_slug="entity/forged")]))
    with pytest.raises(WikiValidationError, match="allowed"):
        await deduplicate_candidates(
            SCOPE, [TopicCandidate(name="New", slug="entity/new", page_type="entity")],
            Store({"entity/new": [DedupPageCandidate(slug="entity/valid", title="Valid", page_type="entity")]}), model,
        )


@pytest.mark.asyncio
async def test_document_dedup_closes_slug_name_bridge_and_skips_empty_model_requests() -> None:
    model = Model(DedupOutput())
    result, mapping = await deduplicate_candidates(
        SCOPE,
        [
            TopicCandidate(name="Alpha", slug="entity/a", page_type="entity", description="A"),
            TopicCandidate(name="Beta", slug="entity/b", page_type="entity", details="B"),
            TopicCandidate(name="Alpha", slug="entity/b", page_type="entity", aliases=["Alias"]),
        ],
        Store({}), model,
    )
    assert model.calls == 0
    assert [item.slug for item in result] == ["entity/a"]
    assert result[0].aliases == ["Beta", "Alpha", "Alias"]
    assert mapping == {"entity/a": "entity/a", "entity/b": "entity/a"}


@pytest.mark.asyncio
async def test_empty_targets_are_not_sent_to_model_in_mixed_batch() -> None:
    target = DedupPageCandidate(slug="entity/existing-target", title="Target", page_type="entity")
    model = Model(DedupOutput(decisions=[DedupDecision(candidate_slug="entity/with", canonical_slug="entity/existing-target")]))
    result, mapping = await deduplicate_candidates(
        SCOPE,
        [TopicCandidate(name="With", slug="entity/with", page_type="entity"), TopicCandidate(name="Without", slug="entity/without", page_type="entity")],
        Store({"entity/with": [target], "entity/without": []}), model,
    )
    assert model.calls == 1
    assert [item.slug for item in result] == ["entity/existing-target", "entity/without"]
    assert mapping["entity/without"] == "entity/without"


def test_validate_output_rejects_any_generated_canonical_even_if_whitelisted() -> None:
    request = DedupRequest(candidates=[
        DedupCandidateRequest(
            candidate=TopicCandidate(name="A", slug="entity/a", page_type="entity"),
            allowed_targets=[DedupPageCandidate(slug="entity/b", title="B", page_type="entity")],
        ),
        DedupCandidateRequest(candidate=TopicCandidate(name="B", slug="entity/b", page_type="entity"), allowed_targets=[]),
    ])
    output = DedupOutput.model_construct(decisions=(
        DedupDecision(candidate_slug="entity/a", canonical_slug="entity/b"),
        DedupDecision(candidate_slug="entity/b"),
    ))
    with pytest.raises(WikiValidationError, match="generated"):
        validate_dedup_output(request, output)


@pytest.mark.asyncio
async def test_exact_existing_can_be_model_canonical_and_aliases_keep_first_seen_order() -> None:
    class ExactStore(Store):
        async def find_existing_pages(self, _scope, slugs):
            return {
                "entity/existing": ExistingPageRecord(
                    page_id=uuid4(), version=1,
                    page=ReducedPage(slug="entity/existing", title="Existing", page_type="entity", content="", summary="", aliases=["DB"]),
                )
            }
    model = Model(DedupOutput(decisions=[DedupDecision(candidate_slug="entity/new", canonical_slug="entity/existing")]))
    result, mapping = await deduplicate_candidates(
        SCOPE,
        [
            TopicCandidate(name="Existing", slug="entity/existing", page_type="entity", aliases=["Input exact"]),
            TopicCandidate(name="New", slug="entity/new", page_type="entity", aliases=["N"]),
        ],
        ExactStore({"entity/new": [DedupPageCandidate(slug="entity/existing", title="Existing", page_type="entity", aliases=["DB"])]}), model,
    )
    assert model.calls == 1
    assert [item.slug for item in result] == ["entity/existing"]
    assert result[0].aliases == ["DB", "Existing", "Input exact", "New", "N"]
    assert mapping == {"entity/existing": "entity/existing", "entity/new": "entity/existing"}


@pytest.mark.asyncio
async def test_rejects_absorbed_generated_slug_as_store_target() -> None:
    model = Model(DedupOutput())
    with pytest.raises(WikiValidationError, match="目标不合法"):
        await deduplicate_candidates(
            SCOPE,
            [
                TopicCandidate(name="Alpha", slug="entity/a", page_type="entity"),
                TopicCandidate(name="alpha", slug="entity/b", page_type="entity"),
                TopicCandidate(name="Gamma", slug="entity/c", page_type="entity"),
            ],
            Store({"entity/c": [DedupPageCandidate(slug="entity/b", title="Alpha", page_type="entity")]}), model,
        )


def _request() -> DedupRequest:
    return DedupRequest(candidates=[DedupCandidateRequest(
        candidate=TopicCandidate(name="A", slug="entity/a", page_type="entity"),
        allowed_targets=[DedupPageCandidate(slug="entity/db", title="DB", page_type="entity")],
    )])


def test_validate_accepts_canonical_and_null() -> None:
    request = DedupRequest(candidates=[
        _request().candidates[0],
        DedupCandidateRequest(candidate=TopicCandidate(name="B", slug="entity/b", page_type="entity"), allowed_targets=[]),
    ])
    assert validate_dedup_output(request, DedupOutput(decisions=[
        DedupDecision(candidate_slug="entity/a", canonical_slug="entity/db"), DedupDecision(candidate_slug="entity/b"),
    ])) == {"entity/a": "entity/db", "entity/b": None}


@pytest.mark.parametrize("decisions", [
    [DedupDecision(candidate_slug="entity/unknown")],
    [],
])
def test_validate_rejects_unknown_or_missing_source(decisions) -> None:
    with pytest.raises(WikiValidationError):
        validate_dedup_output(_request(), DedupOutput.model_construct(decisions=tuple(decisions)))


@pytest.mark.parametrize("canonical", ["entity/unknown", "entity/a", "summary/x", "concept/x"])
def test_validate_rejects_invalid_canonical_contract(canonical: str) -> None:
    output = DedupOutput.model_construct(decisions=(DedupDecision.model_construct(candidate_slug="entity/a", canonical_slug=canonical),))
    with pytest.raises(WikiValidationError):
        validate_dedup_output(_request(), output)


@pytest.mark.parametrize("decisions", [
    (DedupDecision.model_construct(candidate_slug="entity/a"), DedupDecision.model_construct(candidate_slug="entity/a")),
    (DedupDecision.model_construct(candidate_slug="entity/a"), DedupDecision.model_construct(candidate_slug="entity/z")),
    (DedupDecision.model_construct(candidate_slug="entity/z"),),
    (),
])
def test_validate_rejects_duplicate_unknown_or_incomplete_runtime_output(decisions) -> None:
    with pytest.raises(WikiValidationError):
        validate_dedup_output(_request(), DedupOutput.model_construct(decisions=decisions))


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [PermanentModelError("permanent"), TransientModelError("transient"), RuntimeError("runtime")])
async def test_model_errors_propagate_unchanged(error: Exception) -> None:
    class RaisingModel:
        async def resolve_duplicates(self, _request):
            raise error
    with pytest.raises(type(error)) as caught:
        await deduplicate_candidates(
            SCOPE, [TopicCandidate(name="A", slug="entity/a", page_type="entity")],
            Store({"entity/a": [DedupPageCandidate(slug="entity/db", title="DB", page_type="entity")]}), RaisingModel(),
        )
    assert caught.value is error


@pytest.mark.asyncio
@pytest.mark.parametrize("record_slug,record_type", [("entity/other", "entity"), ("entity/existing", "concept")])
async def test_exact_record_mismatch_is_rejected(record_slug: str, record_type: str) -> None:
    class BadStore(Store):
        async def find_existing_pages(self, _scope, _slugs):
            return {"entity/existing": ExistingPageRecord(
                page_id=uuid4(), version=1,
                page=ReducedPage.model_construct(slug=record_slug, title="Bad", page_type=record_type, content="", summary="", aliases=[]),
            )}
    with pytest.raises(WikiValidationError, match="已有页面"):
        await deduplicate_candidates(SCOPE, [TopicCandidate(name="E", slug="entity/existing", page_type="entity")], BadStore({}), Model(DedupOutput()))


@pytest.mark.asyncio
async def test_same_normalized_name_cross_type_is_not_merged() -> None:
    model = Model(DedupOutput())
    result, mapping = await deduplicate_candidates(
        SCOPE,
        [TopicCandidate(name="Shared", slug="entity/shared", page_type="entity"), TopicCandidate(name=" shared ", slug="concept/shared", page_type="concept")],
        Store({}), model,
    )
    assert [item.slug for item in result] == ["entity/shared", "concept/shared"]
    assert mapping == {"entity/shared": "entity/shared", "concept/shared": "concept/shared"}
    assert model.calls == 0


@pytest.mark.asyncio
async def test_store_targets_are_stably_deduplicated_and_limited_before_model() -> None:
    targets = [DedupPageCandidate(slug=f"entity/db-{index}", title=f"DB {index}", page_type="entity") for index in range(21)]
    targets.insert(1, targets[0])
    class InspectingModel:
        def __init__(self): self.request = None
        async def resolve_duplicates(self, request):
            self.request = request
            return DedupOutput(decisions=[DedupDecision(candidate_slug="entity/a")])
    class OverflowStore(Store):
        async def find_dedup_candidates(self, _scope, candidate, limit=20):
            self.calls.append(candidate.slug)
            return self.pages[candidate.slug]
    model = InspectingModel()
    await deduplicate_candidates(SCOPE, [TopicCandidate(name="A", slug="entity/a", page_type="entity")], OverflowStore({"entity/a": targets}), model)
    assert [target.slug for target in model.request.candidates[0].allowed_targets] == [f"entity/db-{index}" for index in range(20)]


@pytest.mark.parametrize("slug,page_type", [("concept/bad", "concept"), ("summary/bad", "summary")])
def test_validate_rejects_runtime_cross_type_and_summary_before_whitelist(slug: str, page_type: str) -> None:
    request = _request().model_copy(deep=True)
    bad = DedupPageCandidate.model_construct(slug=slug, title="Bad", page_type=page_type, aliases=())
    item = DedupCandidateRequest.model_construct(candidate=request.candidates[0].candidate, allowed_targets=(bad,))
    forged = DedupRequest.model_construct(candidates=(item,))
    output = DedupOutput.model_construct(decisions=(DedupDecision.model_construct(candidate_slug="entity/a", canonical_slug=slug),))
    with pytest.raises(WikiValidationError, match="类型"):
        validate_dedup_output(forged, output)


@pytest.mark.asyncio
async def test_service_input_model_output_and_return_value_are_isolated() -> None:
    candidate = TopicCandidate(name="A", slug="entity/a", page_type="entity", aliases=["Input"])
    target = DedupPageCandidate(slug="entity/db", title="DB", page_type="entity", aliases=["Target"])
    output = DedupOutput(decisions=[DedupDecision(candidate_slug="entity/a", canonical_slug="entity/db")])
    model = Model(output)
    store = Store({"entity/a": [target]})
    result, _ = await deduplicate_candidates(SCOPE, [candidate], store, model)
    result[0].aliases.append("returned only")
    result[0].name = "Editable"
    again, _ = await deduplicate_candidates(SCOPE, [candidate], store, model)
    assert candidate.aliases == ["Input"]
    assert target.aliases == ("Target",)
    assert output.decisions[0].canonical_slug == "entity/db"
    assert again[0].name == "DB" and "returned only" not in again[0].aliases
