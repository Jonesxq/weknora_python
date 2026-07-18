from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.retract import (
    plan_ingest_deltas,
    plan_retract_deltas,
    project_active_refs,
    project_aliases,
)
from app.wiki.ingest.schemas import StoredContributionRecord


KB = UUID("11111111-1111-1111-1111-111111111111")


def record(
    *,
    slug: str = "entity/acme",
    knowledge_id: str = "knowledge-a",
    version: str = "v1",
    state: str = "active",
    aliases: tuple[str, ...] = ("Acme",),
    refs: tuple[str, ...] = ("chunk-a",),
    **changes: object,
) -> StoredContributionRecord:
    values = dict(
        id=None,
        tenant_id=1,
        knowledge_base_id=KB,
        slug=slug,
        knowledge_id=knowledge_id,
        op_version=version,
        page_type="entity",
        state=state,
        title="Acme",
        content="Body",
        summary="Summary",
        aliases=aliases,
        chunk_refs=refs,
    )
    values.update(changes)
    return StoredContributionRecord(**values)


def test_ingest_plans_add_replace_unchanged_and_stale_in_required_order() -> None:
    previous = [
        record(slug="entity/keep"),
        record(slug="entity/replace"),
        record(slug="entity/stale"),
    ]
    current = [
        record(slug="entity/add"),
        record(slug="entity/replace", content="New body"),
        record(slug="entity/keep"),
    ]

    result = plan_ingest_deltas(uuid4(), previous, current)

    assert [(item.action, item.slug) for item in result] == [
        ("add", "entity/add"),
        ("replace", "entity/replace"),
        ("retract_stale", "entity/stale"),
    ]
    assert result[0].previous is None and result[0].current == current[0]
    assert result[1].previous == previous[1] and result[1].current == current[1]


@pytest.mark.parametrize(
    "field,value",
    [
        ("op_version", "v2"),
        ("page_type", "concept"),
        ("title", "Changed title"),
        ("content", "changed"),
        ("summary", "Changed summary"),
        ("aliases", ("Other",)),
        ("chunk_refs", ("other",)),
    ],
)
def test_ingest_replaces_when_contribution_content_changes(
    field: str, value: object
) -> None:
    old = record()
    new = (
        record(**{field: value})
        if field != "page_type"
        else StoredContributionRecord.model_construct(
            **{**record().model_dump(), "page_type": value}
        )
    )
    if field == "page_type":
        with pytest.raises(WikiValidationError):
            plan_ingest_deltas(uuid4(), [old], [new])
    else:
        assert [item.action for item in plan_ingest_deltas(uuid4(), [old], [new])] == [
            "replace"
        ]


def test_ingest_ignores_id_only_change_and_does_not_mutate_inputs() -> None:
    old = record()
    new = record(id=uuid4())
    before = (old.model_dump(), new.model_dump())
    assert plan_ingest_deltas(uuid4(), [old], [new]) == []
    assert (old.model_dump(), new.model_dump()) == before


@pytest.mark.parametrize(
    "records",
    [
        [record(), record()],
        [record(), record(slug="entity/other", tenant_id=2)],
        [
            record(page_type="entity", slug="entity/a"),
            record(slug="entity/b", knowledge_id="other"),
        ],
    ],
)
def test_ingest_rejects_duplicate_or_mixed_scope_or_knowledge(
    records: list[StoredContributionRecord],
) -> None:
    with pytest.raises(WikiValidationError):
        plan_ingest_deltas(uuid4(), [], records)


def test_ingest_rejects_previous_and_current_from_different_operations() -> None:
    with pytest.raises(WikiValidationError):
        plan_ingest_deltas(
            uuid4(),
            [record()],
            [record(slug="entity/other", knowledge_id="knowledge-other")],
        )


@pytest.mark.parametrize(
    "change",
    [
        {"tenant_id": 2},
        {"knowledge_base_id": UUID("22222222-2222-2222-2222-222222222222")},
        {"knowledge_id": "knowledge-other"},
    ],
)
def test_ingest_rejects_cross_collection_scope_mismatch(
    change: dict[str, object],
) -> None:
    with pytest.raises(WikiValidationError):
        plan_ingest_deltas(uuid4(), [record()], [record(slug="entity/other", **change)])


@pytest.mark.parametrize(
    "field,value", [("version", "v2"), ("state", "retract_pending")]
)
@pytest.mark.parametrize("position", ["previous", "current"])
def test_ingest_rejects_mixed_versions_and_states_in_each_collection(
    field: str, value: str, position: str
) -> None:
    records = [record(slug="entity/a"), record(slug="entity/b", **{field: value})]
    previous, current = (records, []) if position == "previous" else ([], records)
    with pytest.raises(WikiValidationError):
        plan_ingest_deltas(uuid4(), previous, current)


def test_empty_sequences_have_empty_delta_and_projection_results() -> None:
    assert plan_ingest_deltas(uuid4(), [], []) == []
    assert plan_retract_deltas(uuid4(), []) == []
    assert project_active_refs([]) == ([], [])
    assert project_aliases([]) == []


def test_ingest_rejects_pending_id_and_malicious_record_without_leaking_pydantic_details() -> (
    None
):
    payload = record().model_dump()
    payload["aliases"] = ("",)
    malicious = StoredContributionRecord.model_construct(**payload)
    for pending in (True, "not-a-uuid"):
        with pytest.raises(WikiValidationError) as caught:
            plan_ingest_deltas(pending, [], [record()])  # type: ignore[arg-type]
        assert caught.value.code == "WIKI_CONTRIBUTION_INVALID_PENDING_OP"
    with pytest.raises(WikiValidationError) as caught:
        plan_ingest_deltas(uuid4(), [], [malicious])
    assert caught.value.code == "WIKI_CONTRIBUTION_INVALID_RECORD"
    assert "ValidationError" not in caught.value.message


def test_retract_plans_input_order_and_rejects_active_mix() -> None:
    pending = [
        record(slug="entity/b", state="retract_pending"),
        record(slug="entity/a", state="retract_pending"),
    ]
    result = plan_retract_deltas(uuid4(), pending)
    assert [(item.action, item.slug, item.current) for item in result] == [
        ("retract", "entity/b", None),
        ("retract", "entity/a", None),
    ]
    with pytest.raises(WikiValidationError):
        plan_retract_deltas(uuid4(), [pending[0], record(slug="entity/x")])


def test_active_projections_exclude_pending_and_are_deterministic_and_isolated() -> (
    None
):
    records = [
        record(
            knowledge_id="z",
            version="v2",
            aliases=("Z", "Shared"),
            refs=("z1", "shared"),
        ),
        record(
            knowledge_id="a",
            version="v1",
            aliases=("A", "Shared"),
            refs=("a1", "shared"),
        ),
        record(
            knowledge_id="0",
            version="v0",
            state="retract_pending",
            aliases=("Hidden",),
            refs=("hidden",),
        ),
    ]
    sources, chunks = project_active_refs(records)
    aliases = project_aliases(records)
    assert sources == ["a", "z"]
    assert chunks == ["a1", "shared", "z1"]
    assert aliases == ["A", "Shared", "Z"]
    sources.append("mutated")
    assert project_active_refs(records)[0] == ["a", "z"]


def test_projection_rejects_mixed_target_and_malicious_records() -> None:
    with pytest.raises(WikiValidationError):
        project_aliases([record(), record(slug="entity/other")])
    payload = record().model_dump()
    payload["chunk_refs"] = ("",)
    bad = StoredContributionRecord.model_construct(**payload)
    with pytest.raises(WikiValidationError):
        project_active_refs([bad])


@pytest.mark.parametrize(
    "change",
    [
        {"tenant_id": 2},
        {"knowledge_base_id": UUID("22222222-2222-2222-2222-222222222222")},
        {"slug": "entity/other"},
        {"page_type": "concept", "slug": "concept/acme"},
    ],
)
def test_projection_rejects_mixed_target_even_when_pending(
    change: dict[str, object],
) -> None:
    with pytest.raises(WikiValidationError):
        project_active_refs([record(), record(state="retract_pending", **change)])


@pytest.mark.parametrize(
    "records",
    [
        [record(), record(id=uuid4(), version="v2")],
        [record(id=uuid4(), version="v2"), record()],
    ],
)
def test_projection_rejects_duplicate_active_source_regardless_of_input_order(
    records: list[StoredContributionRecord],
) -> None:
    with pytest.raises(WikiValidationError) as caught:
        project_aliases(records)
    assert caught.value.code == "WIKI_CONTRIBUTION_DUPLICATE_ACTIVE"


def test_projection_is_stable_for_none_and_uuid_ids_and_returned_lists_are_isolated() -> (
    None
):
    records = [
        record(knowledge_id="z", id=uuid4(), aliases=("Z",), refs=("z",)),
        record(knowledge_id="a", id=None, aliases=("A",), refs=("a",)),
    ]
    expected_refs = (["a", "z"], ["a", "z"])
    assert project_active_refs(records) == expected_refs
    assert project_active_refs(list(reversed(records))) == expected_refs
    assert project_aliases(records) == ["A", "Z"]
    sources, chunks = project_active_refs(records)
    aliases = project_aliases(records)
    sources.append("mutated")
    chunks.append("mutated")
    aliases.append("mutated")
    assert project_active_refs(records) == expected_refs
    assert project_aliases(records) == ["A", "Z"]


def test_large_input_uses_all_records_without_quadratic_deduplication() -> None:
    records = [
        record(
            knowledge_id=f"knowledge-{index:04d}",
            refs=(f"chunk-{index}",),
            aliases=(f"Alias {index}",),
        )
        for index in range(1000)
    ]
    assert len(project_active_refs(records)[0]) == 1000
    assert len(project_aliases(records)) == 1000
    assert project_active_refs(records) == project_active_refs(list(reversed(records)))
