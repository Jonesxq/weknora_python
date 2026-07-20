from __future__ import annotations

import copy
from uuid import UUID

import pytest
from pydantic import ValidationError

from app.wiki.ingest.index_intro import (
    DEFAULT_INDEX_INTRO,
    INDEX_INTRO_MAX_CHARS,
    build_index_intro_request,
    build_success_index_intro_plan,
    clean_index_intro,
    fallback_index_intro_plan,
)
from app.wiki.ingest.schemas import (
    ContributionDelta,
    IndexIntroContext,
    IndexIntroChange,
    IndexIntroOutput,
    IndexPageSnapshot,
    IndexIntroRequest,
    IndexSummaryItem,
    ReducedPage,
    StoredContributionRecord,
)


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
OP_1 = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
OP_2 = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
INDEX_ID = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")


def _summary(slug: str, title: str = "Title", summary: str = "Summary") -> IndexSummaryItem:
    return IndexSummaryItem(slug=slug, title=title, summary=summary)


def _page(slug: str, title: str = "Title", summary: str = "Summary", *, deleted: bool = False) -> ReducedPage:
    return ReducedPage(
        slug=slug,
        title=title,
        page_type="summary",
        content="Body",
        summary=summary,
        deleted=deleted,
    )


def _record(
    slug: str,
    knowledge_id: str,
    title: str = "Record title",
    summary: str = "Record summary",
    *,
    state: str = "active",
) -> StoredContributionRecord:
    return StoredContributionRecord(
        tenant_id=1,
        knowledge_base_id=KB_ID,
        slug=slug,
        knowledge_id=knowledge_id,
        op_version="v1",
        page_type="summary",
        state=state,
        title=title,
        content="Body",
        summary=summary,
    )


def _delta(
    op_id: UUID,
    action: str,
    slug: str,
    knowledge_id: str,
    *,
    title: str = "Record title",
    summary: str = "Record summary",
) -> ContributionDelta:
    if action in {"add", "replace"}:
        previous = _record(slug, knowledge_id, state="active") if action == "replace" else None
        current = _record(slug, knowledge_id, title, summary)
    else:
        previous = _record(
            slug,
            knowledge_id,
            title,
            summary,
            state="retract_pending" if action == "retract" else "active",
        )
        current = None
    return ContributionDelta(
        pending_op_id=op_id,
        action=action,
        slug=slug,
        knowledge_id=knowledge_id,
        previous=previous,
        current=current,
    )


def _context(content: str | None = None) -> IndexIntroContext:
    return IndexIntroContext(
        index=None
        if content is None
        else IndexPageSnapshot(id=INDEX_ID, version=7, content=content, summary=""),
        recent_summaries=(
            _summary("summary/history", "History", "History summary"),
            _summary("summary/shared", "Old", "Old shared"),
        ),
    )


def test_create_request_prefers_current_batch_then_history_caps_and_preserves_inputs() -> None:
    pages = [_page("summary/shared", "Batch", "Batch shared")]
    pages.extend(_page(f"summary/p{index:03d}", f"P{index}", "S") for index in range(201))
    pages.append(
        ReducedPage(
            slug="entity/ignored",
            title="Ignored",
            page_type="entity",
            content="Body",
            summary="Ignored",
        )
    )
    pages_before = copy.deepcopy(pages)
    context = _context()
    request = build_index_intro_request(
        context,
        completed_op_ids=(OP_1,),
        pages=pages,
        contribution_deltas=(),
        operation_actions=(("ingest", "knowledge-1"),),
    )

    assert request is not None
    assert request.mode == "create"
    assert request.existing_intro == ""
    assert request.changes == ()
    assert request.summaries[0] == _summary("summary/shared", "Batch", "Batch shared")
    assert len(request.summaries) == 200
    assert request.summaries[-1].slug == "summary/p198"
    assert pages == pages_before
    assert context == _context()


@pytest.mark.parametrize("content", [None, " ", "Wiki Index", "知识库索引"])
def test_missing_or_placeholder_index_builds_create_request(content: str | None) -> None:
    request = build_index_intro_request(
        _context(content),
        completed_op_ids=(OP_1,),
        pages=(),
        contribution_deltas=(),
        operation_actions=(("ingest", "knowledge-1"),),
    )
    assert request is not None
    assert request.mode == "create"
    assert request.summaries == (_summary("summary/history", "History", "History summary"), _summary("summary/shared", "Old", "Old shared"))


def test_update_request_contains_only_old_intro_and_sorted_changes() -> None:
    request = build_index_intro_request(
        _context("Existing intro"),
        completed_op_ids=(OP_1, OP_2),
        pages=(),
        contribution_deltas=(
            _delta(OP_2, "retract", "summary/b", "knowledge-b", title="Old B"),
            _delta(OP_1, "add", "summary/a", "knowledge-a", title="A"),
        ),
        operation_actions=(
            ("retract", " knowledge-b "),
            ("ingest", "knowledge-a"),
            ("ingest", "knowledge-a"),
        ),
    )
    assert request is not None
    assert request.mode == "update"
    assert request.existing_intro == "Existing intro"
    assert request.summaries == ()
    assert [(item.action, item.knowledge_id) for item in request.changes] == [
        ("ingest", "knowledge-a"),
        ("retract", "knowledge-b"),
    ]


def test_changes_select_current_or_previous_snapshot_and_final_page_wins() -> None:
    request = build_index_intro_request(
        _context("Existing intro"),
        completed_op_ids=(OP_1, OP_2),
        pages=(
            _page("summary/a", "Final A", "Final summary"),
            _page("summary/c", "Deleted C", "Deleted", deleted=True),
        ),
        contribution_deltas=(
            _delta(OP_1, "add", "summary/a", "knowledge-a", title="Current A"),
            _delta(OP_2, "retract", "summary/b", "knowledge-b", title="Previous B"),
        ),
        operation_actions=(
            ("retract", "knowledge-b"),
            ("ingest", "knowledge-a"),
            ("ingest", "knowledge-c"),
        ),
    )
    assert request is not None
    changes = {(change.action, change.knowledge_id): change.pages for change in request.changes}
    assert changes[("ingest", "knowledge-a")] == (_summary("summary/a", "Final A", "Final summary"),)
    assert changes[("retract", "knowledge-b")] == (_summary("summary/b", "Previous B", "Record summary"),)
    assert changes[("ingest", "knowledge-c")] == ()


def test_request_is_none_without_completed_ids_or_effective_actions() -> None:
    context = _context()
    common = dict(pages=(), contribution_deltas=())
    assert build_index_intro_request(context, completed_op_ids=(), operation_actions=(("ingest", "k"),), **common) is None
    assert build_index_intro_request(context, completed_op_ids=(OP_1,), operation_actions=(), **common) is None
    with pytest.raises(ValueError):
        build_index_intro_request(context, completed_op_ids=(OP_1,), operation_actions=(("delete", "k"),), **common)
    with pytest.raises(ValueError):
        build_index_intro_request(context, completed_op_ids=(OP_1,), operation_actions=(("ingest", " "),), **common)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("  Intro\n## Contents\n- x", "Intro"),
        ("Intro\r\n## Contents\r\n- x", "Intro"),
    ],
)
def test_clean_index_intro_removes_directory_at_first_heading(value: str, expected: str) -> None:
    assert clean_index_intro(value) == expected


@pytest.mark.parametrize("value", [" ", "\n## Contents", "x" * (INDEX_INTRO_MAX_CHARS + 1)])
def test_clean_index_intro_rejects_empty_or_oversized_values(value: str) -> None:
    with pytest.raises(ValueError):
        clean_index_intro(value)


def test_success_plan_preserves_cas_for_create_placeholder_and_update() -> None:
    output = IndexIntroOutput(intro=" Generated\n## Contents\n- summary ")
    create = build_success_index_intro_plan(
        _context("Wiki Index"),
        build_index_intro_request(
            _context("Wiki Index"),
            completed_op_ids=(OP_1,), pages=(), contribution_deltas=(), operation_actions=(("ingest", "k"),)
        ),
        output,
    )
    update = build_success_index_intro_plan(
        _context("Existing intro"),
        build_index_intro_request(
            _context("Existing intro"),
            completed_op_ids=(OP_1,), pages=(), contribution_deltas=(), operation_actions=(("ingest", "k"),)
        ),
        output,
    )
    assert create.mode == "create"
    assert (create.expected_page_id, create.expected_version) == (INDEX_ID, 7)
    assert create.intro == "Generated"
    assert (update.mode, update.expected_page_id, update.expected_version) == ("update", INDEX_ID, 7)
    assert update.model_status == "generated"
    assert update.error_code is None


def test_success_plan_create_missing_and_fallback_modes_validate_context() -> None:
    missing_context = _context()
    request = build_index_intro_request(
        missing_context, completed_op_ids=(OP_1,), pages=(), contribution_deltas=(), operation_actions=(("ingest", "k"),)
    )
    plan = build_success_index_intro_plan(missing_context, request, IndexIntroOutput(intro="Generated"))
    defaulted = fallback_index_intro_plan(missing_context, request, error_code=" MODEL_FAILED ")
    placeholder_context = _context("Wiki Index")
    placeholder_request = build_index_intro_request(
        placeholder_context,
        completed_op_ids=(OP_1,),
        pages=(),
        contribution_deltas=(),
        operation_actions=(("ingest", "k"),),
    )
    defaulted_with_snapshot = fallback_index_intro_plan(
        placeholder_context, placeholder_request, error_code="MODEL_FAILED"
    )
    kept = fallback_index_intro_plan(
        _context("Existing intro"),
        build_index_intro_request(
            _context("Existing intro"), completed_op_ids=(OP_1,), pages=(), contribution_deltas=(), operation_actions=(("ingest", "k"),)
        ),
        error_code="MODEL_FAILED",
    )
    assert (plan.expected_page_id, plan.expected_version) == (None, None)
    assert (defaulted.intro, defaulted.model_status, defaulted.error_code) == (DEFAULT_INDEX_INTRO, "defaulted", "MODEL_FAILED")
    assert (defaulted_with_snapshot.expected_page_id, defaulted_with_snapshot.expected_version) == (INDEX_ID, 7)
    assert (kept.intro, kept.model_status, kept.expected_page_id) == ("Existing intro", "kept_after_error", INDEX_ID)
    mismatched_request = IndexIntroRequest(
        mode="update",
        existing_intro="Existing intro",
        changes=(IndexIntroChange(action="ingest", knowledge_id="k"),),
    )
    with pytest.raises(ValueError):
        fallback_index_intro_plan(_context(), mismatched_request, error_code="FAILED")
    with pytest.raises(ValueError):
        build_success_index_intro_plan(
            _context(), mismatched_request, IndexIntroOutput(intro="Generated")
        )


def test_requests_and_plans_are_deterministic_and_frozen_snapshots() -> None:
    arguments = dict(
        completed_op_ids=(OP_1,),
        pages=[_page("summary/a")],
        contribution_deltas=[_delta(OP_1, "add", "summary/a", "knowledge-a")],
        operation_actions=[("ingest", "knowledge-a")],
    )
    first = build_index_intro_request(_context(), **arguments)
    second = build_index_intro_request(_context(), **arguments)
    assert first == second
    assert first is not second
    assert first is not None
    with pytest.raises(ValidationError):
        first.mode = "update"
    with pytest.raises(ValidationError):
        first.model_copy(update={"mode": "update"})
