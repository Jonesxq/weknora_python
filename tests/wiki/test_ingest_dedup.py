from __future__ import annotations

import pytest

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.dedup import deduplicate_candidates
from app.wiki.ingest.schemas import (
    DedupDecision,
    DedupOutput,
    DedupPageCandidate,
    TopicCandidate,
)
from app.wiki.ingest.store import ExistingPageRecord
from app.wiki.ingest.schemas import ReducedPage
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
            SCOPE, [TopicCandidate(name="New", slug="entity/new", page_type="entity")], Store({"entity/new": []}), model
        )
