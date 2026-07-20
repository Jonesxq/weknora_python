from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from collections.abc import Sequence
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from app.wiki.ingest import schemas as ingest_schemas
from app.wiki.ingest import store as ingest_store
from app.wiki.ingest.schemas import (
    BatchApplyRequest,
    ContributionDelta,
    FinalizationRequest,
    FolderAssignment,
    IndexIntroContext,
    OperationFailure,
    PageExpectation,
    ReducedPage,
    SourceKnowledge,
    StoredContributionRecord,
)
from app.wiki.ingest.store import (
    ClaimLost,
    EnqueueRecord,
    ExistingPageRecord,
    IngestStore,
    InvariantError,
    PageConflict,
    SqlAlchemyIngestStore,
    SqlFinalizationPort,
    build_claim_recovery_dedup_key,
    build_claim_outbox_statement,
    build_claim_pending_ops_statement,
    build_finalization_register_statement,
    build_finalization_release_statement,
    build_outbox_dedup_key,
    _cancel_claim_recovery,
    _enqueue_follow_up,
    _finalization_request,
    _pending_record,
    build_dedup_candidate_statement,
)
from app.wiki.ingest.schemas import TopicCandidate
from app.wiki.models import (
    TaskOutbox,
    WikiLogEntry,
    WikiDeadLetter,
    WikiFolder,
    WikiPage,
    WikiPageContribution,
    WikiPendingOp,
)
from app.wiki.scope import WikiScope


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
SCOPE = WikiScope(tenant_id=7, knowledge_base_id=KB_ID, actor_id="worker")
NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
_UNSET = object()


def _batch_request(
    *, folder_assignments: object = _UNSET, **updates: object
) -> BatchApplyRequest:
    values = {
        "claim_token": uuid4(),
        "pages": (),
        "contribution_deltas": (),
        "completed_op_ids": (uuid4(),),
        "superseded_op_ids": (),
        "failures": (),
        "expected_pages": (),
        "operation_id": uuid4(),
    }
    values.update(updates)
    if folder_assignments is _UNSET:
        expectations = {
            expectation.slug: expectation
            for expectation in values["expected_pages"]
            if isinstance(expectation, PageExpectation)
        }
        folder_assignments = tuple(
            FolderAssignment(
                slug=page.slug,
                contributor_op_ids=tuple(page.contributor_op_ids),
            )
            for page in values["pages"]
            if isinstance(page, ReducedPage)
            and page.page_type in {"entity", "concept"}
            and not page.deleted
            and page.contributor_op_ids
            and (expectation := expectations.get(page.slug)) is not None
            and expectation.page_id is None
        )
    values["folder_assignments"] = folder_assignments
    return BatchApplyRequest(**values)


def test_batch_apply_outcome_is_frozen_and_partitions_operation_ids() -> None:
    outcome_type = getattr(ingest_schemas, "BatchApplyOutcome", None)
    assert outcome_type is not None
    completed, superseded, failed = uuid4(), uuid4(), uuid4()
    outcome = outcome_type(
        applied=True,
        completed_op_ids=(completed,),
        superseded_op_ids=(superseded,),
        failed_op_ids=(failed,),
    )

    assert outcome.completed_op_ids == (completed,)
    assert outcome.superseded_op_ids == (superseded,)
    assert outcome.failed_op_ids == (failed,)
    with pytest.raises(ValidationError):
        outcome.applied = False
    for kwargs in (
        {"completed_op_ids": (completed, completed)},
        {"superseded_op_ids": (superseded, superseded)},
        {"failed_op_ids": (failed, failed)},
        {"completed_op_ids": (completed,), "superseded_op_ids": (completed,)},
        {"completed_op_ids": (completed,), "failed_op_ids": (completed,)},
        {"superseded_op_ids": (completed,), "failed_op_ids": (completed,)},
    ):
        with pytest.raises(ValidationError):
            outcome_type(applied=True, **kwargs)


def test_validate_batch_request_returns_an_immutable_snapshot() -> None:
    validator = getattr(ingest_store, "_validate_batch_request", None)
    assert validator is not None
    request = _batch_request()

    assert validator(request) == request


def _stored_contribution(
    *, knowledge_id: str = "knowledge-1", version: str = "version-1"
) -> StoredContributionRecord:
    return StoredContributionRecord(
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        slug="entity/acme",
        knowledge_id=knowledge_id,
        op_version=version,
        page_type="entity",
        state="active",
        title="Acme",
        content="正文",
        summary="摘要",
    )


def _result_page() -> ReducedPage:
    return ReducedPage(
        slug="entity/acme",
        title="Acme",
        page_type="entity",
        content="正文",
        summary="摘要",
    )


def _delta_for_page(op_id: UUID, page: ReducedPage) -> ContributionDelta:
    record = _stored_contribution().model_copy(
        update={
            "slug": page.slug,
            "page_type": page.page_type,
            "title": page.title,
            "content": page.content,
            "summary": page.summary,
        }
    )
    return ContributionDelta(
        pending_op_id=op_id,
        action="retract_stale" if page.deleted else "add",
        slug=page.slug,
        knowledge_id=record.knowledge_id,
        previous=record if page.deleted else None,
        current=None if page.deleted else record,
    )


def test_validate_batch_request_requires_matching_page_expectation_slugs() -> None:
    request = _batch_request(pages=(_result_page(),), expected_pages=())

    with pytest.raises(InvariantError, match="expected_pages"):
        ingest_store._validate_batch_request(request)


def test_validate_batch_request_rejects_delta_outside_terminal_claim_set() -> None:
    current = _stored_contribution()
    request = _batch_request(
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=uuid4(),
                action="add",
                slug=current.slug,
                knowledge_id=current.knowledge_id,
                previous=None,
                current=current,
            ),
        )
    )

    with pytest.raises(InvariantError, match="pending_op"):
        ingest_store._validate_batch_request(request)


def test_validate_batch_request_rejects_two_current_active_source_versions() -> None:
    first_id, second_id = uuid4(), uuid4()
    first = _stored_contribution(version="version-1")
    second = _stored_contribution(version="version-2")
    request = _batch_request(
        completed_op_ids=(first_id, second_id),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=first_id,
                action="add",
                slug=first.slug,
                knowledge_id=first.knowledge_id,
                previous=None,
                current=first,
            ),
            ContributionDelta(
                pending_op_id=second_id,
                action="add",
                slug=second.slug,
                knowledge_id=second.knowledge_id,
                previous=None,
                current=second,
            ),
        ),
    )

    with pytest.raises(InvariantError, match="current active"):
        ingest_store._validate_batch_request(request)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["failure", "superseded"])
async def test_noncompleted_page_contributor_is_rejected_before_session(
    terminal: str,
) -> None:
    op_id = uuid4()
    page = _result_page().model_copy(update={"contributor_op_ids": [op_id]})
    request = _batch_request(
        pages=(page,),
        completed_op_ids=(),
        superseded_op_ids=(op_id,) if terminal == "superseded" else (),
        failures=(
            OperationFailure(
                pending_op_id=op_id,
                error_code="MODEL_FAILURE",
                error_summary="失败",
            ),
        )
        if terminal == "failure"
        else (),
        expected_pages=(PageExpectation(slug=page.slug),),
    )

    class CountingFactory:
        calls = 0

        def __call__(self):
            self.calls += 1
            raise AssertionError("非法页面结果不应打开 session")

    factory = CountingFactory()
    store = SqlAlchemyIngestStore(factory, SqlFinalizationPort())  # type: ignore[arg-type]
    error: BaseException | None = None
    try:
        await store.apply_results(SCOPE, request)
    except BaseException as exc:
        error = exc

    assert isinstance(error, InvariantError)
    assert factory.calls == 0


def test_completed_current_delta_requires_a_matching_page() -> None:
    op_id = uuid4()
    current = _stored_contribution()
    request = _batch_request(
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=op_id,
                action="add",
                slug=current.slug,
                knowledge_id=current.knowledge_id,
                previous=None,
                current=current,
            ),
        ),
        completed_op_ids=(op_id,),
    )

    with pytest.raises(InvariantError, match="page"):
        ingest_store._validate_batch_request(request)


def test_page_contributor_requires_a_same_slug_delta() -> None:
    op_id = uuid4()
    page = _result_page().model_copy(update={"contributor_op_ids": [op_id]})
    request = _batch_request(
        pages=(page,),
        completed_op_ids=(op_id,),
        expected_pages=(PageExpectation(slug=page.slug),),
    )

    with pytest.raises(InvariantError, match="delta"):
        ingest_store._validate_batch_request(request)


@pytest.mark.asyncio
async def test_same_slug_deltas_require_exact_page_contributor_coverage_before_session() -> (
    None
):
    first_id, second_id = uuid4(), uuid4()
    first = _stored_contribution(knowledge_id="knowledge-1")
    second = _stored_contribution(knowledge_id="knowledge-2")
    page = _result_page().model_copy(update={"contributor_op_ids": [first_id]})
    request = _batch_request(
        pages=(page,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=first_id,
                action="add",
                slug=page.slug,
                knowledge_id=first.knowledge_id,
                previous=None,
                current=first,
            ),
            ContributionDelta(
                pending_op_id=second_id,
                action="add",
                slug=page.slug,
                knowledge_id=second.knowledge_id,
                previous=None,
                current=second,
            ),
        ),
        completed_op_ids=(first_id, second_id),
        expected_pages=(PageExpectation(slug=page.slug),),
    )

    class CountingFactory:
        calls = 0

        def __call__(self):
            self.calls += 1
            raise AssertionError("非法批次不应打开数据库 session")

    factory = CountingFactory()
    store = SqlAlchemyIngestStore(factory, SqlFinalizationPort())  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="contributor"):
        await store.apply_results_with_outcome(SCOPE, request)
    assert factory.calls == 0


def test_same_slug_deltas_accept_exact_page_contributor_coverage() -> None:
    first_id, second_id = uuid4(), uuid4()
    first = _stored_contribution(knowledge_id="knowledge-1")
    second = _stored_contribution(knowledge_id="knowledge-2")
    page = _result_page().model_copy(
        update={"contributor_op_ids": [first_id, second_id]}
    )
    request = _batch_request(
        pages=(page,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=first_id,
                action="add",
                slug=page.slug,
                knowledge_id=first.knowledge_id,
                previous=None,
                current=first,
            ),
            ContributionDelta(
                pending_op_id=second_id,
                action="add",
                slug=page.slug,
                knowledge_id=second.knowledge_id,
                previous=None,
                current=second,
            ),
        ),
        completed_op_ids=(first_id, second_id),
        expected_pages=(PageExpectation(slug=page.slug),),
    )

    assert ingest_store._validate_batch_request(request) == request


def test_modern_page_rejects_empty_contributor_ids() -> None:
    page = _result_page()
    request = _batch_request(
        pages=(page,),
        expected_pages=(PageExpectation(slug=page.slug),),
    )

    with pytest.raises(InvariantError, match="contributor"):
        ingest_store._validate_batch_request(request)


def test_new_topic_page_gets_root_assignment_unless_explicitly_disabled() -> None:
    op_id = uuid4()
    page = _result_page().model_copy(update={"contributor_op_ids": [op_id]})
    expectation = PageExpectation(slug=page.slug)

    request = _batch_request(
        pages=(page,),
        completed_op_ids=(op_id,),
        expected_pages=(expectation,),
    )
    explicit_empty = _batch_request(
        pages=(page,),
        completed_op_ids=(op_id,),
        expected_pages=(expectation,),
        folder_assignments=(),
    )

    assert request.folder_assignments == (
        FolderAssignment(slug=page.slug, contributor_op_ids=(op_id,)),
    )
    assert explicit_empty.folder_assignments == ()


def _assignment_request(
    *,
    page: ReducedPage | None = None,
    page_op_id: UUID | None = None,
    assignment_op_ids: tuple[UUID, ...] | None = None,
    completed_op_ids: tuple[UUID, ...] | None = None,
    expectation: PageExpectation | None = None,
) -> BatchApplyRequest:
    page_op_id = page_op_id or uuid4()
    page = (page or _result_page()).model_copy(
        update={"contributor_op_ids": [page_op_id]}
    )
    assignment_values = {
        "slug": page.slug,
        "contributor_op_ids": assignment_op_ids or (page_op_id,),
    }
    assignment = (
        FolderAssignment.model_construct(
            **assignment_values,
            base_folder_id=None,
            base_path=None,
            base_depth=0,
            new_segments=(),
        )
        if page.page_type == "summary"
        else FolderAssignment(**assignment_values)
    )
    return _batch_request(
        pages=(page,),
        contribution_deltas=(_delta_for_page(page_op_id, page),),
        completed_op_ids=completed_op_ids or (page_op_id,),
        expected_pages=(expectation or PageExpectation(slug=page.slug),),
        folder_assignments=(assignment,),
    )


def test_folder_assignment_rejects_an_existing_page() -> None:
    request = _assignment_request(
        expectation=PageExpectation(slug="entity/acme", page_id=uuid4(), version=1)
    )

    with pytest.raises(InvariantError, match="新页面"):
        ingest_store._validate_batch_request(request)


@pytest.mark.parametrize("deleted", [False, True])
def test_folder_assignment_rejects_summary_or_deleted_page(deleted: bool) -> None:
    page = (
        _result_page().model_copy(update={"deleted": True})
        if deleted
        else ReducedPage(
            slug="summary/overview",
            title="Overview",
            page_type="summary",
            content="Body",
            summary="Summary",
        )
    )

    with pytest.raises(InvariantError, match="未删除 topic 页面"):
        ingest_store._validate_batch_request(_assignment_request(page=page))


def test_folder_assignment_contributors_must_match_result_page() -> None:
    page_op_id, other_op_id = uuid4(), uuid4()
    request = _assignment_request(
        page_op_id=page_op_id,
        assignment_op_ids=(other_op_id,),
        completed_op_ids=(page_op_id, other_op_id),
    )

    with pytest.raises(InvariantError, match="contributor"):
        ingest_store._validate_batch_request(request)


def test_folder_assignment_contributors_must_be_completed() -> None:
    completed_id, other_id = uuid4(), uuid4()
    request = _assignment_request(
        page_op_id=completed_id,
        assignment_op_ids=(other_id,),
        completed_op_ids=(completed_id,),
    )

    with pytest.raises(InvariantError, match="completed"):
        ingest_store._validate_batch_request(request)


def test_folder_assignment_requires_a_result_page_and_expectation() -> None:
    op_id = uuid4()
    request = _batch_request(
        completed_op_ids=(op_id,),
        folder_assignments=(
            FolderAssignment(slug="entity/missing", contributor_op_ids=(op_id,)),
        ),
    )

    with pytest.raises(InvariantError, match="结果页面"):
        ingest_store._validate_batch_request(request)


def test_validate_batch_request_rejects_polluted_duplicate_assignments() -> None:
    request = _assignment_request()
    assignment = request.folder_assignments[0]
    object.__setattr__(request, "folder_assignments", (assignment, assignment))

    with pytest.raises(InvariantError, match="重复"):
        ingest_store._validate_batch_request(request)


@pytest.mark.parametrize("terminal", ["failure", "superseded"])
def test_noncompleted_operation_cannot_own_a_contribution_delta(terminal: str) -> None:
    op_id = uuid4()
    current = _stored_contribution()
    request = _batch_request(
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=op_id,
                action="add",
                slug=current.slug,
                knowledge_id=current.knowledge_id,
                previous=None,
                current=current,
            ),
        ),
        completed_op_ids=(),
        superseded_op_ids=(op_id,) if terminal == "superseded" else (),
        failures=(
            OperationFailure(
                pending_op_id=op_id,
                error_code="MODEL_FAILURE",
                error_summary="失败",
            ),
        )
        if terminal == "failure"
        else (),
    )

    with pytest.raises(InvariantError, match="completed"):
        ingest_store._validate_batch_request(request)


def test_completed_noop_without_delta_or_page_remains_valid() -> None:
    request = _batch_request(pages=(), contribution_deltas=(), expected_pages=())

    assert ingest_store._validate_batch_request(request) == request


@pytest.mark.parametrize(
    ("pending_op", "action", "previous", "current"),
    [
        ("ingest", "add", None, _stored_contribution(version="version-2")),
        (
            "ingest",
            "replace",
            _stored_contribution(version="version-1"),
            _stored_contribution(version="version-2"),
        ),
        (
            "ingest",
            "retract_stale",
            _stored_contribution(version="version-1"),
            None,
        ),
        (
            "retract",
            "retract",
            _stored_contribution(version="version-1").model_copy(
                update={"state": "retract_pending"}
            ),
            None,
        ),
    ],
)
def test_pending_delta_accepts_operation_specific_actions(
    pending_op: str,
    action: str,
    previous: StoredContributionRecord | None,
    current: StoredContributionRecord | None,
) -> None:
    pending = _pending(op=pending_op, version="version-2")
    delta = ContributionDelta(
        pending_op_id=pending.id,
        action=action,
        slug="entity/acme",
        knowledge_id=pending.knowledge_id,
        previous=previous,
        current=current,
    )

    ingest_store._validate_delta_for_pending(pending, delta)


@pytest.mark.parametrize(
    ("pending_op", "action", "previous", "current"),
    [
        (
            "retract",
            "add",
            None,
            _stored_contribution(version="delete-1"),
        ),
        (
            "ingest",
            "retract",
            _stored_contribution(version="version-1").model_copy(
                update={"state": "retract_pending"}
            ),
            None,
        ),
    ],
)
def test_pending_delta_rejects_operation_specific_action_mismatch(
    pending_op: str,
    action: str,
    previous: StoredContributionRecord | None,
    current: StoredContributionRecord | None,
) -> None:
    pending = _pending(op=pending_op, version="version-2")
    delta = ContributionDelta(
        pending_op_id=pending.id,
        action=action,
        slug="entity/acme",
        knowledge_id=pending.knowledge_id,
        previous=previous,
        current=current,
    )

    with pytest.raises(InvariantError, match="action"):
        ingest_store._validate_delta_for_pending(pending, delta)


def test_pending_delta_rejects_ingest_identity_and_current_version_mismatch() -> None:
    pending = _pending(op="ingest", version="version-2")
    current = _stored_contribution(version="version-1")
    delta = ContributionDelta(
        pending_op_id=pending.id,
        action="add",
        slug=current.slug,
        knowledge_id=current.knowledge_id,
        previous=None,
        current=current,
    )

    with pytest.raises(InvariantError, match="版本"):
        ingest_store._validate_delta_for_pending(pending, delta)

    mismatched = delta.model_copy(update={"knowledge_id": "other-knowledge"})
    with pytest.raises(InvariantError, match="来源"):
        ingest_store._validate_delta_for_pending(pending, mismatched)


def test_pending_delta_rejects_retract_previous_identity_and_state_mismatch() -> None:
    pending = _pending(op="retract", version="delete-1")
    previous = _stored_contribution(version="version-1")
    valid_previous = previous.model_copy(update={"state": "retract_pending"})
    valid_delta = ContributionDelta(
        pending_op_id=pending.id,
        action="retract",
        slug=previous.slug,
        knowledge_id=previous.knowledge_id,
        previous=valid_previous,
        current=None,
    )

    invalid_state = valid_delta.model_copy(update={"previous": previous})
    with pytest.raises(InvariantError, match="状态"):
        ingest_store._validate_delta_for_pending(pending, invalid_state)

    mismatched = valid_delta.model_copy(update={"knowledge_id": "other-knowledge"})
    with pytest.raises(InvariantError, match="来源"):
        ingest_store._validate_delta_for_pending(pending, mismatched)


@pytest.mark.parametrize(
    "marker",
    [
        "ClAiM ToKeN",
        "CLAIM_TOKEN",
        "claim-token",
        "claimToken",
        "CLAIM.TOKEN",
        "ClAiMtOkEn",
        "TrAcEbAcK",
        "ChUnK TeXt",
        "chunk_text",
        "chunk-text",
        "RaW ChUnK",
        "raw_chunk",
        "raw-chunk",
        "MoDeL OuTpUt",
        "model_output",
        "model-output",
        "RaW OuTpUt",
        "raw_output",
        "raw-output",
    ],
)
def test_safe_error_summary_truncates_separator_and_case_variants(marker: str) -> None:
    assert ingest_store._safe_error_summary(f"安全摘要 {marker} secret") == "安全摘要"


def test_safe_error_summary_does_not_truncate_claimant() -> None:
    summary = "claimant completed successfully"

    assert ingest_store._safe_error_summary(summary) == summary


@pytest.mark.asyncio
async def test_retract_pending_add_delta_is_rejected_before_any_writes() -> None:
    token = uuid4()
    pending = _pending(op="retract", version="delete-1", claimed=True)
    pending.claim_token = token
    page = _result_page().model_copy(
        update={"contributor_op_ids": [pending.id], "deleted": True}
    )
    current = _stored_contribution(version=pending.op_version)
    request = _batch_request(
        claim_token=token,
        pages=(page,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=pending.id,
                action="add",
                slug=page.slug,
                knowledge_id=pending.knowledge_id,
                previous=None,
                current=current,
            ),
        ),
        completed_op_ids=(pending.id,),
        expected_pages=(PageExpectation(slug=page.slug),),
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[pending]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(rowcount=1),
            _ScriptedResult(),
            _ScriptedResult(scalar=0),
        ]
    )
    finalization = _RecordingFinalization(session.events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    error: BaseException | None = None
    try:
        await store.apply_results(SCOPE, request)
    except BaseException as exc:
        error = exc

    assert isinstance(error, InvariantError)
    assert session.added == []
    assert finalization.requests == []
    assert not any(
        _sql(statement).startswith(("INSERT", "UPDATE", "DELETE"))
        for statement in session.statements
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("detailed", [False, True])
async def test_modern_apply_results_returns_idempotent_noop_for_same_scope_log(
    detailed: bool,
) -> None:
    operation_id = uuid4()
    existing_log = WikiLogEntry(
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        operation_id=operation_id,
        action="wiki_ingest_batch",
        message="已完成",
        pages_affected=[],
        actor_id=SCOPE.actor_id,
    )

    class Result:
        def scalar_one_or_none(self):
            return existing_log

    class Session:
        def __init__(self) -> None:
            self.begin_count = 0
            self.execute_count = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def begin(self):
            self.begin_count += 1
            return self

        async def execute(self, _statement):
            self.execute_count += 1
            return Result()

    session = Session()
    store = SqlAlchemyIngestStore(lambda: session, SqlFinalizationPort())  # type: ignore[arg-type]
    request = _batch_request(operation_id=operation_id)
    assert existing_log.result_outcome is None

    if detailed:
        outcome = await store.apply_results_with_outcome(SCOPE, request)
        assert outcome.applied is False
        assert outcome.completed_op_ids == request.completed_op_ids
        assert outcome.superseded_op_ids == ()
        assert outcome.failed_op_ids == ()
    else:
        assert await store.apply_results(SCOPE, request) is False  # type: ignore[call-arg]
    assert session.begin_count == 1
    assert session.execute_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result_outcome",
    [
        {
            "completed_op_ids": [],
            "superseded_op_ids": [],
            "failed_op_ids": [],
            "unexpected": [],
        },
        {
            "completed_op_ids": ["not-a-uuid"],
            "superseded_op_ids": [],
            "failed_op_ids": [],
        },
        {
            "completed_op_ids": [
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            ],
            "superseded_op_ids": [],
            "failed_op_ids": [],
        },
        {
            "completed_op_ids": ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"],
            "superseded_op_ids": ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"],
            "failed_op_ids": [],
        },
        {
            "completed_op_ids": ("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",),
            "superseded_op_ids": [],
            "failed_op_ids": [],
        },
        {
            "completed_op_ids": ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"],
            "superseded_op_ids": [],
            "failed_op_ids": [],
        },
    ],
)
async def test_idempotent_replay_rejects_corrupt_persisted_outcome(
    result_outcome: object,
) -> None:
    operation_id = uuid4()
    existing_log = WikiLogEntry(
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        operation_id=operation_id,
        action="wiki_ingest_batch",
        message="已完成",
        pages_affected=[],
        result_outcome=result_outcome,  # type: ignore[arg-type]
        actor_id=SCOPE.actor_id,
    )
    session = _ScriptedSession([_ScriptedResult(scalar=existing_log)])
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), SqlFinalizationPort())  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="终态"):
        await store.apply_results_with_outcome(
            SCOPE, _batch_request(operation_id=operation_id)
        )

    assert len(session.statements) == 2


@pytest.mark.asyncio
async def test_operation_lock_is_first_stable_global_sql_for_operation_id() -> None:
    operation_id = uuid4()

    async def execute(scope: WikiScope, op_id: UUID) -> int:
        existing_log = WikiLogEntry(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            operation_id=op_id,
            action="wiki_ingest_batch",
            message="已完成",
            pages_affected=[],
            actor_id=scope.actor_id,
        )
        session = _ScriptedSession([_ScriptedResult(scalar=existing_log)])
        store = SqlAlchemyIngestStore(
            _OneSessionFactory(session), SqlFinalizationPort()
        )  # type: ignore[arg-type]

        assert (
            await store.apply_results(scope, _batch_request(operation_id=op_id))
            is False
        )
        assert _sql(session.statements[0]).startswith("SELECT pg_advisory_xact_lock")
        assert _sql(session.statements[1]).startswith("SELECT wiki_log_entries.")
        params = session.statements[0].compile(dialect=postgresql.dialect()).params
        assert len(params) == 1
        return next(iter(params.values()))

    other_scope = WikiScope(
        tenant_id=SCOPE.tenant_id + 1,
        knowledge_base_id=uuid4(),
        actor_id="other-worker",
    )
    first = await execute(SCOPE, operation_id)
    same_operation_other_scope = await execute(other_scope, operation_id)
    different_operation = await execute(SCOPE, uuid4())

    assert first == same_operation_other_scope
    assert first != different_operation
    assert -(2**63) <= first < 2**63


def test_taxonomy_scope_lock_key_is_stable_and_scope_specific() -> None:
    other_tenant = WikiScope(
        tenant_id=SCOPE.tenant_id + 1,
        knowledge_base_id=SCOPE.knowledge_base_id,
        actor_id="other",
    )
    other_kb = WikiScope(
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=uuid4(),
        actor_id="other",
    )

    assert ingest_store._taxonomy_scope_lock_key(SCOPE) == 7561626728492530037
    assert ingest_store._taxonomy_scope_lock_key(SCOPE) == (
        ingest_store._taxonomy_scope_lock_key(SCOPE)
    )
    assert ingest_store._taxonomy_scope_lock_key(other_tenant) != (
        ingest_store._taxonomy_scope_lock_key(SCOPE)
    )
    assert ingest_store._taxonomy_scope_lock_key(other_kb) != (
        ingest_store._taxonomy_scope_lock_key(SCOPE)
    )


@pytest.mark.asyncio
async def test_modern_apply_results_rejects_an_incompletely_covered_claim() -> None:
    token = uuid4()
    requested_id, extra_id = uuid4(), uuid4()
    pending_rows = [
        WikiPendingOp(
            id=op_id,
            tenant_id=SCOPE.tenant_id,
            knowledge_base_id=SCOPE.knowledge_base_id,
            knowledge_id=f"knowledge-{index}",
            op="ingest",
            op_version="version-1",
            payload={"knowledge_id": f"knowledge-{index}"},
            fail_count=0,
            enqueued_at=NOW,
            claimed_at=NOW,
            claim_token=token,
        )
        for index, op_id in enumerate((requested_id, extra_id), start=1)
    ]

    class Result:
        def __init__(self, *, scalar=None, rows=()) -> None:
            self.scalar = scalar
            self.rows = list(rows)

        def scalar_one_or_none(self):
            return self.scalar

        def scalars(self):
            return self.rows

    class Session:
        def __init__(self) -> None:
            self.statements = []
            self.results = [Result(), Result(rows=pending_rows)]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def begin(self):
            return self

        async def execute(self, statement):
            self.statements.append(statement)
            if "pg_advisory_xact_lock" in _sql(statement):
                return Result()
            return self.results.pop(0)

    session = Session()
    store = SqlAlchemyIngestStore(lambda: session, SqlFinalizationPort())  # type: ignore[arg-type]
    request = _batch_request(
        claim_token=token,
        completed_op_ids=(requested_id,),
    )

    with pytest.raises(ClaimLost, match="完整覆盖"):
        await store.apply_results(SCOPE, request)
    assert "FOR UPDATE" in _sql(session.statements[2])


def _sql(statement) -> str:
    return " ".join(str(statement.compile(dialect=postgresql.dialect())).split())


@pytest.mark.asyncio
async def test_load_taxonomy_context_filters_history_and_returns_stable_narrow_scope() -> (
    None
):
    root_id, child_id = uuid4(), uuid4()
    session = _ScriptedSession(
        [
            _ScriptedResult(
                rows=[
                    (root_id, None, "Engineering", "/Engineering", 1),
                    (
                        child_id,
                        root_id,
                        "Databases",
                        "/Engineering/Databases",
                        2,
                    ),
                ]
            ),
            _ScriptedResult(rows=["concept/deleted-history", "entity/existing"]),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), SqlFinalizationPort()
    )  # type: ignore[arg-type]

    context = await store.load_taxonomy_context(
        SCOPE,
        [
            " entity/Zeta ",
            "concept/new",
            "ENTITY/zeta",
            "entity/existing",
            "concept/deleted-history",
            "summary/overview",
            "other/ignored",
            7,  # type: ignore[list-item]
        ],
    )

    assert [(folder.id, folder.path) for folder in context.folders] == [
        (root_id, "/Engineering"),
        (child_id, "/Engineering/Databases"),
    ]
    assert context.classifiable_slugs == ("concept/new", "entity/zeta")
    folder_sql, page_sql = map(_sql, session.statements)
    assert "wiki_folders.content" not in folder_sql
    assert "wiki_pages.content" not in page_sql
    assert "wiki_folders.tenant_id" in folder_sql
    assert "wiki_folders.knowledge_base_id" in folder_sql
    assert "wiki_folders.deleted_at IS NULL" in folder_sql
    assert (
        "ORDER BY wiki_folders.depth, wiki_folders.path, wiki_folders.id"
        in folder_sql
    )
    assert "wiki_pages.tenant_id" in page_sql
    assert "wiki_pages.knowledge_base_id" in page_sql
    assert "wiki_pages.deleted_at" not in page_sql
    for statement in session.statements:
        params = statement.compile(dialect=postgresql.dialect()).params.values()
        assert SCOPE.tenant_id in params
        assert SCOPE.knowledge_base_id in params


@pytest.mark.asyncio
async def test_load_taxonomy_context_binds_large_slug_iterable_as_one_parameter() -> (
    None
):
    slug_count = 32_766
    session = _ScriptedSession(
        [_ScriptedResult(rows=[]), _ScriptedResult(rows=[])]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), SqlFinalizationPort()
    )  # type: ignore[arg-type]

    context = await store.load_taxonomy_context(
        SCOPE, (f"entity/item-{index:05d}" for index in range(slug_count))
    )

    assert len(context.classifiable_slugs) == slug_count
    assert context.classifiable_slugs[0] == "entity/item-00000"
    assert context.classifiable_slugs[-1] == "entity/item-32765"
    page_statement = session.statements[1]
    compiled = page_statement.compile(
        dialect=postgresql.dialect(),
        compile_kwargs={"render_postcompile": True},
    )
    assert len(compiled.params) == 3
    sql = " ".join(str(compiled).split())
    assert "wiki_pages.slug = ANY" in sql
    assert "wiki_pages.tenant_id" in sql
    assert "wiki_pages.knowledge_base_id" in sql
    assert "wiki_pages.deleted_at" not in sql


@pytest.mark.asyncio
async def test_load_taxonomy_context_uses_topic_candidate_slug_boundary() -> None:
    session = _ScriptedSession([])
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), SqlFinalizationPort()
    )  # type: ignore[arg-type]

    with pytest.raises(ValidationError, match="slug"):
        await store.load_taxonomy_context(SCOPE, ["entity/not valid"])

    assert session.statements == []


@pytest.mark.asyncio
async def test_load_taxonomy_context_wraps_dirty_dto_rows_only() -> None:
    bad_folder = (uuid4(), None, "Wrong", "/Different", 1)
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[bad_folder]),
            _ScriptedResult(rows=[]),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), SqlFinalizationPort()
    )  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="taxonomy context 查询返回脏数据"):
        await store.load_taxonomy_context(SCOPE, ["entity/new"])


@pytest.mark.asyncio
async def test_load_index_intro_context_uses_scoped_narrow_queries_and_snapshots() -> None:
    index_row = [uuid4(), "index", "index", "published", 3, " Index body ", " Index summary "]
    summary_row = ["summary/recent", " Recent ", " Summary "]
    session = _ScriptedSession(
        [_ScriptedResult(rows=[index_row]), _ScriptedResult(rows=[summary_row])]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), SqlFinalizationPort()
    )  # type: ignore[arg-type]

    context = await store.load_index_intro_context(SCOPE)

    assert isinstance(store, IngestStore)
    assert isinstance(context, IndexIntroContext)
    assert context.index is not None
    assert context.index.id == index_row[0]
    assert context.index.content == "Index body"
    assert [(item.slug, item.title, item.summary) for item in context.recent_summaries] == [
        ("summary/recent", "Recent", "Summary")
    ]
    index_row[5] = "mutated"
    summary_row[2] = "mutated"
    assert context.index.content == "Index body"
    assert context.recent_summaries[0].summary == "Summary"
    with pytest.raises(ValidationError):
        context.index.content = "mutated"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        context.recent_summaries[0].summary = "mutated"  # type: ignore[misc]

    identity_sql, summary_sql = map(_sql, session.statements)
    assert "wiki_pages.id" in identity_sql
    assert "wiki_pages.slug" in identity_sql
    assert "wiki_pages.page_type" in identity_sql
    assert "wiki_pages.status" in identity_sql
    assert "wiki_pages.version" in identity_sql
    assert "wiki_pages.content" in identity_sql
    assert "wiki_pages.summary" in identity_sql
    assert "wiki_pages.tenant_id" in identity_sql
    assert "wiki_pages.knowledge_base_id" in identity_sql
    assert "wiki_pages.deleted_at IS NULL" in identity_sql
    assert "wiki_pages.slug =" in identity_sql
    assert " OR wiki_pages.page_type =" in identity_sql
    assert 2 in session.statements[0].compile(dialect=postgresql.dialect()).params.values()
    assert "wiki_pages.slug" in summary_sql
    assert "wiki_pages.title" in summary_sql
    assert "wiki_pages.summary" in summary_sql
    assert "wiki_pages.content" not in summary_sql
    assert "wiki_pages.aliases" not in summary_sql
    assert "wiki_pages.tenant_id" in summary_sql
    assert "wiki_pages.knowledge_base_id" in summary_sql
    assert "wiki_pages.deleted_at IS NULL" in summary_sql
    assert "wiki_pages.status =" in summary_sql
    assert "wiki_pages.page_type =" in summary_sql
    assert "ORDER BY wiki_pages.updated_at DESC, wiki_pages.id DESC" in summary_sql
    assert 200 in session.statements[1].compile(dialect=postgresql.dialect()).params.values()


@pytest.mark.asyncio
async def test_load_index_intro_context_returns_empty_index_and_summaries() -> None:
    session = _ScriptedSession([_ScriptedResult(rows=[]), _ScriptedResult(rows=[])])
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), SqlFinalizationPort()
    )  # type: ignore[arg-type]

    context = await store.load_index_intro_context(SCOPE)

    assert context.index is None
    assert context.recent_summaries == ()
    assert len(session.statements) == 2


@pytest.mark.asyncio
async def test_load_index_intro_context_rejects_identity_conflict_without_summary_query() -> None:
    session = _ScriptedSession(
        [
            _ScriptedResult(
                rows=[
                    (uuid4(), "index", "index", "published", 1, "", ""),
                    (uuid4(), "summary/index", "index", "published", 1, "", ""),
                ]
            )
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), SqlFinalizationPort()
    )  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="canonical Index 身份冲突"):
        await store.load_index_intro_context(SCOPE)

    assert len(session.statements) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity_row",
    [
        (uuid4(), "index", "summary", "published", 1, "", ""),
        (uuid4(), "summary/index", "index", "published", 1, "", ""),
        (uuid4(), "index", "index", "archived", 1, "", ""),
    ],
)
async def test_load_index_intro_context_rejects_noncanonical_identity_row(
    identity_row: tuple[object, ...],
) -> None:
    session = _ScriptedSession([_ScriptedResult(rows=[identity_row])])
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), SqlFinalizationPort()
    )  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="canonical Index 身份冲突"):
        await store.load_index_intro_context(SCOPE)

    assert len(session.statements) == 1


@pytest.mark.asyncio
async def test_load_index_intro_context_preserves_recent_summary_sql_order() -> None:
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(
                rows=[
                    ("summary/newest", "Newest", "First"),
                    ("summary/older", "Older", "Second"),
                ]
            ),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), SqlFinalizationPort()
    )  # type: ignore[arg-type]

    context = await store.load_index_intro_context(SCOPE)

    assert [item.slug for item in context.recent_summaries] == [
        "summary/newest",
        "summary/older",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identity_rows", "summary_rows"),
    [
        ([(uuid4(), "index", "index", "published", 0, "", "")], []),
        (
            [(uuid4(), "index", "index", "published", 1, "", "")],
            [("summary/not valid", "Title", "Summary")],
        ),
        (
            [(uuid4(), "index", "index", "published", 1, "", "")],
            [
                ("summary/duplicate", "First", "One"),
                ("summary/duplicate", "Second", "Two"),
            ],
        ),
    ],
)
async def test_load_index_intro_context_wraps_dirty_database_rows(
    identity_rows: list[tuple[object, ...]],
    summary_rows: list[tuple[object, ...]],
) -> None:
    session = _ScriptedSession(
        [_ScriptedResult(rows=identity_rows), _ScriptedResult(rows=summary_rows)]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), SqlFinalizationPort()
    )  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="Index intro 上下文包含无效数据库记录"):
        await store.load_index_intro_context(SCOPE)


@pytest.mark.asyncio
async def test_load_index_intro_context_propagates_database_execution_error() -> None:
    error = RuntimeError("database unavailable")
    session = _ScriptedSession([error])
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), SqlFinalizationPort()
    )  # type: ignore[arg-type]

    with pytest.raises(RuntimeError) as raised:
        await store.load_index_intro_context(SCOPE)

    assert raised.value is error


def test_claim_pending_sql_is_scoped_ordered_and_skip_locked() -> None:
    sql = _sql(
        build_claim_pending_ops_statement(
            SCOPE, limit=5, stale_before=NOW - timedelta(minutes=10)
        )
    )

    assert "wiki_pending_ops.tenant_id" in sql
    assert "wiki_pending_ops.knowledge_base_id" in sql
    assert "wiki_pending_ops.claimed_at IS NULL" in sql
    assert "wiki_pending_ops.claimed_at <=" in sql
    assert "ORDER BY wiki_pending_ops.enqueued_at, wiki_pending_ops.id" in sql
    assert "LIMIT" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql


def test_dedup_candidate_sql_is_scoped_index_equivalent_and_limited() -> None:
    statement = build_dedup_candidate_statement(
        SCOPE,
        TopicCandidate(
            name="Acme", slug="entity/acme", page_type="entity", aliases=["ACME Corp"]
        ),
        limit=20,
    )
    sql = _sql(statement)

    for fragment in (
        "wiki_pages.tenant_id",
        "wiki_pages.knowledge_base_id",
        "wiki_pages.deleted_at IS NULL",
        "wiki_pages.status =",
        "wiki_pages.page_type =",
        "lower(wiki_pages.title)",
        "coalesce(CAST(wiki_pages.aliases AS TEXT),",
        " <-> ",
        "ORDER BY",
        "wiki_pages.slug",
        "LIMIT",
    ):
        assert fragment in sql
    assert "|| ' '" in sql
    assert "coalesce(CAST(wiki_pages.aliases AS TEXT), '')" in sql
    assert "least(" not in sql.lower()


def test_dedup_single_name_sql_has_no_least_and_aliases_do_not_add_empty_query() -> (
    None
):
    sql = _sql(
        build_dedup_candidate_statement(
            SCOPE,
            TopicCandidate(
                name="Acme", slug="entity/acme", page_type="entity", aliases=[]
            ),
        )
    )
    assert "LEAST" not in sql
    assert sql.count(" <-> ") == 2  # select distance + KNN order


def _dedup_row(**updates) -> WikiPage:
    values = {
        "id": uuid4(),
        "tenant_id": SCOPE.tenant_id,
        "knowledge_base_id": KB_ID,
        "slug": "entity/db",
        "title": "Database",
        "page_type": "entity",
        "status": "published",
        "deleted_at": None,
        "aliases": ["DB"],
    }
    values.update(updates)
    return WikiPage(**values)


class _DedupSession:
    def __init__(self, rows):
        self.rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def execute(self, _statement):
        class Result:
            def __init__(self, rows):
                self.rows = rows

            def all(self):
                return [(row, float(index)) for index, row in enumerate(self.rows)]

        return Result(self.rows)


class _QuerySession:
    def __init__(self, batches):
        self.batches = list(batches)
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def execute(self, _statement):
        batch = self.batches[self.calls]
        self.calls += 1

        class Result:
            def __init__(self, rows):
                self.rows = rows

            def all(self):
                return self.rows

        return Result(batch)


class _RowLike(Sequence[object]):
    def __init__(self, page: WikiPage, distance: object):
        self.values = (page, distance)

    def __getitem__(self, index):
        return self.values[index]

    def __len__(self):
        return 2


@pytest.mark.asyncio
async def test_dedup_query_aggregates_min_distance_and_global_top_limit() -> None:
    main = [
        (_dedup_row(slug=f"entity/main-{index}"), float(index + 1))
        for index in range(20)
    ]
    alias_best = _dedup_row(slug="entity/alias-best")
    duplicate = main[5][0]
    row_like = _RowLike(alias_best, 0.01)
    assert not isinstance(row_like, tuple)
    session = _QuerySession([main, [row_like, _RowLike(duplicate, 0.1)]])
    store = SqlAlchemyIngestStore(lambda: session, SqlFinalizationPort())  # type: ignore[arg-type]
    result = await store.find_dedup_candidates(
        SCOPE,
        TopicCandidate(
            name="Main", slug="entity/new", page_type="entity", aliases=["Alias"]
        ),
    )
    assert result[0].slug == "entity/alias-best"
    assert "entity/main-19" not in [item.slug for item in result]
    assert session.calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("distance", [None, "bad", float("nan"), float("inf"), -0.1])
async def test_dedup_query_rejects_invalid_distance(distance) -> None:
    store = SqlAlchemyIngestStore(
        lambda: _QuerySession([[(_dedup_row(), distance)]]), SqlFinalizationPort()
    )  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await store.find_dedup_candidates(
            SCOPE, TopicCandidate(name="A", slug="entity/new", page_type="entity")
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad", [[_dedup_row()], [(_dedup_row(), 1.0, "extra")], [(object(), 1.0)]]
)
async def test_dedup_query_rejects_bad_result_shape(bad) -> None:
    store = SqlAlchemyIngestStore(lambda: _QuerySession([bad]), SqlFinalizationPort())  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await store.find_dedup_candidates(
            SCOPE, TopicCandidate(name="A", slug="entity/new", page_type="entity")
        )


@pytest.mark.asyncio
async def test_dedup_query_validates_late_alias_batch_after_limit_is_full() -> None:
    first = [(_dedup_row(slug=f"entity/{index}"), float(index)) for index in range(20)]
    bad = _dedup_row(slug="entity/bad", tenant_id=99)
    store = SqlAlchemyIngestStore(
        lambda: _QuerySession([first, [(bad, 1.0)]]), SqlFinalizationPort()
    )  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await store.find_dedup_candidates(
            SCOPE,
            TopicCandidate(
                name="A", slug="entity/new", page_type="entity", aliases=["B"]
            ),
        )


@pytest.mark.asyncio
async def test_find_dedup_candidates_returns_detached_frozen_snapshots() -> None:
    first, second = (
        _dedup_row(slug="entity/a", aliases=["A"]),
        _dedup_row(slug="entity/b", aliases=["B"]),
    )
    store = SqlAlchemyIngestStore(
        lambda: _DedupSession([first, second]), SqlFinalizationPort()
    )  # type: ignore[arg-type]
    result = await store.find_dedup_candidates(
        SCOPE, TopicCandidate(name="A", slug="entity/new", page_type="entity")
    )
    first.aliases.append("mutated")
    assert [(item.slug, item.aliases) for item in result] == [
        ("entity/a", ("A",)),
        ("entity/b", ("B",)),
    ]
    with pytest.raises(ValidationError) as exc:
        result[0].aliases += ("nope",)
    assert exc.value.errors()[0]["type"] == "frozen_instance"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "updates",
    [
        {"tenant_id": 8},
        {"knowledge_base_id": uuid4()},
        {"deleted_at": NOW},
        {"status": "draft"},
        {"page_type": "concept", "slug": "concept/db"},
    ],
)
async def test_find_dedup_candidates_rejects_polluted_rows(updates) -> None:
    store = SqlAlchemyIngestStore(
        lambda: _DedupSession([_dedup_row(**updates)]), SqlFinalizationPort()
    )  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await store.find_dedup_candidates(
            SCOPE, TopicCandidate(name="A", slug="entity/new", page_type="entity")
        )


@pytest.mark.asyncio
async def test_find_dedup_candidates_rejects_overflow_and_dirty_aliases() -> None:
    overflow = SqlAlchemyIngestStore(
        lambda: _DedupSession(
            [_dedup_row(slug=f"entity/{index}") for index in range(2)]
        ),
        SqlFinalizationPort(),
    )  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await overflow.find_dedup_candidates(
            SCOPE,
            TopicCandidate(name="A", slug="entity/new", page_type="entity"),
            limit=1,
        )
    dirty = SqlAlchemyIngestStore(
        lambda: _DedupSession([_dedup_row(aliases=["", "A", "A"])]),
        SqlFinalizationPort(),
    )  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await dirty.find_dedup_candidates(
            SCOPE, TopicCandidate(name="A", slug="entity/new", page_type="entity")
        )


@pytest.mark.parametrize("limit", [True, 0, 21])
def test_dedup_candidate_statement_rejects_invalid_limit(limit: int) -> None:
    with pytest.raises(ValueError):
        build_dedup_candidate_statement(
            SCOPE,
            TopicCandidate(name="Acme", slug="entity/acme", page_type="entity"),
            limit=limit,
        )


def test_claim_outbox_sql_excludes_sent_and_future_events() -> None:
    sql = _sql(
        build_claim_outbox_statement(
            limit=10,
            now=NOW,
            stale_before=NOW - timedelta(minutes=10),
        )
    )

    assert "task_outbox.sent_at IS NULL" in sql
    assert "task_outbox.available_at <=" in sql
    assert "task_outbox.claimed_at IS NULL" in sql
    assert "task_outbox.claimed_at <" in sql
    assert (
        "ORDER BY task_outbox.available_at, task_outbox.created_at, task_outbox.id"
        in sql
    )
    assert "FOR UPDATE SKIP LOCKED" in sql


@pytest.mark.parametrize("limit", [0, -1])
def test_claim_builders_reject_non_positive_limit(limit: int) -> None:
    with pytest.raises(ValueError, match="正整数"):
        build_claim_pending_ops_statement(SCOPE, limit=limit, stale_before=NOW)
    with pytest.raises(ValueError, match="正整数"):
        build_claim_outbox_statement(limit=limit, now=NOW, stale_before=NOW)


def test_outbox_dedup_is_stable_scoped_sha256() -> None:
    key = build_outbox_dedup_key(
        7, KB_ID, "wiki.batch.trigger", "knowledge-1", "version-1"
    )
    same = build_outbox_dedup_key(
        7, KB_ID, "wiki.batch.trigger", "knowledge-1", "version-1"
    )

    assert key == same
    assert len(key) == 64
    int(key, 16)
    variants = {
        build_outbox_dedup_key(
            8, KB_ID, "wiki.batch.trigger", "knowledge-1", "version-1"
        ),
        build_outbox_dedup_key(
            7, uuid4(), "wiki.batch.trigger", "knowledge-1", "version-1"
        ),
        build_outbox_dedup_key(7, KB_ID, "other", "knowledge-1", "version-1"),
        build_outbox_dedup_key(
            7, KB_ID, "wiki.batch.trigger", "knowledge-2", "version-1"
        ),
        build_outbox_dedup_key(
            7, KB_ID, "wiki.batch.trigger", "knowledge-1", "version-2"
        ),
    }
    assert key not in variants
    assert len(variants) == 5


def test_operation_outbox_identity_is_canonical_stable_and_strict() -> None:
    ingest = ingest_store.build_operation_outbox_identity("ingest", "retract:doc")
    retract = ingest_store.build_operation_outbox_identity("retract", "doc")
    special = ingest_store.build_operation_outbox_identity("ingest", "文档:一")

    assert ingest == '["ingest","retract:doc"]'
    assert retract == '["retract","doc"]'
    assert special == '["ingest","\\u6587\\u6863:\\u4e00"]'
    assert special == ingest_store.build_operation_outbox_identity("ingest", "文档:一")
    for invalid in ("", "INGEST", "delete", True, None):
        with pytest.raises(ValueError, match="op"):
            ingest_store.build_operation_outbox_identity(  # type: ignore[attr-defined,arg-type]
                invalid, "doc"
            )


def test_operation_outbox_identity_prevents_prefix_collision() -> None:
    ingest_key = build_outbox_dedup_key(
        7,
        KB_ID,
        "wiki.batch.trigger",
        ingest_store.build_operation_outbox_identity("ingest", "retract:doc"),
        "version-1",
    )
    retract_key = build_outbox_dedup_key(
        7,
        KB_ID,
        "wiki.batch.trigger",
        ingest_store.build_operation_outbox_identity("retract", "doc"),
        "version-1",
    )

    assert ingest_key != retract_key
    assert ingest_key == build_outbox_dedup_key(
        7,
        KB_ID,
        "wiki.batch.trigger",
        ingest_store.build_operation_outbox_identity("ingest", "retract:doc"),
        "version-1",
    )


def test_finalization_sql_uses_named_conflict_and_strict_release_identity() -> None:
    request = FinalizationRequest(
        tenant_id=7,
        knowledge_base_id=KB_ID,
        knowledge_id="knowledge-1",
        attempt="version-1",
        subtask_name="wiki",
    )
    register_sql = _sql(build_finalization_register_statement(request))
    release_sql = _sql(build_finalization_release_statement(request, released_at=NOW))

    assert (
        "ON CONFLICT ON CONSTRAINT uq_wiki_finalization_markers_attempt DO NOTHING"
        in register_sql
    )
    assert "RETURNING wiki_finalization_markers.id" in register_sql
    for column in (
        "tenant_id",
        "knowledge_base_id",
        "knowledge_id",
        "attempt",
        "subtask_name",
    ):
        assert f"wiki_finalization_markers.{column}" in release_sql
    assert "wiki_finalization_markers.released_at IS NULL" in release_sql
    assert "RETURNING wiki_finalization_markers.id" in release_sql


def test_enqueue_record_is_frozen_and_exposes_pending_id() -> None:
    pending_id = uuid4()
    record = EnqueueRecord(
        id=pending_id,
        tenant_id=7,
        knowledge_base_id=KB_ID,
        knowledge_id="knowledge-1",
        op_version="version-1",
        payload={"knowledge_id": "knowledge-1"},
        outbox_event_id=uuid4(),
        deduplicated=False,
    )

    assert record.pending_op_id == pending_id
    with pytest.raises(FrozenInstanceError):
        record.tenant_id = 8  # type: ignore[misc]


def test_existing_page_record_is_frozen() -> None:
    record = ExistingPageRecord(
        page_id=uuid4(),
        version=3,
        page=ReducedPage(
            slug="entity/acme",
            title="Acme",
            page_type="entity",
            content="正文",
            summary="摘要",
        ),
    )
    with pytest.raises(FrozenInstanceError):
        record.version = 4  # type: ignore[misc]


def test_pending_record_deep_copies_nested_payload() -> None:
    row = WikiPendingOp(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        knowledge_id="knowledge-1",
        op="ingest",
        op_version="version-1",
        payload={"nested": {"values": ["original"]}},
        fail_count=0,
        enqueued_at=NOW,
    )
    record = _pending_record(row)
    record.payload["nested"]["values"].append("record-only")  # type: ignore[index,union-attr]
    assert row.payload == {"nested": {"values": ["original"]}}


@pytest.mark.asyncio
async def test_claim_pending_atomically_schedules_timeout_recovery() -> None:
    pending = WikiPendingOp(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        knowledge_id="knowledge-recovery",
        op="ingest",
        op_version="version-1",
        payload={"knowledge_id": "knowledge-recovery"},
        fail_count=0,
        enqueued_at=NOW,
    )

    class Rows:
        def scalars(self):
            return [pending]

    class Session:
        def __init__(self) -> None:
            self.statements = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def begin(self):
            return self

        async def execute(self, statement):
            self.statements.append(statement)
            return Rows()

        async def flush(self) -> None:
            return None

    session = Session()

    class Factory:
        def __call__(self):
            return session

    store = SqlAlchemyIngestStore(Factory(), SqlFinalizationPort())  # type: ignore[arg-type]
    claimed = await store.claim_pending(SCOPE, 1, 600)

    assert len(claimed) == 1
    assert len(session.statements) == 2
    recovery = session.statements[1]
    sql = _sql(recovery)
    assert "INSERT INTO task_outbox" in sql
    params = recovery.compile(dialect=postgresql.dialect()).params
    assert params["event_type"] == "wiki.batch.trigger"
    assert params["dedup_key"] == build_claim_recovery_dedup_key(
        SCOPE, claimed[0].claim_token
    )


@pytest.mark.asyncio
async def test_follow_up_and_recovery_cancel_emit_one_statement_each() -> None:
    class Session:
        def __init__(self) -> None:
            self.statements = []

        async def execute(self, statement):
            self.statements.append(statement)

    follow_up_session = Session()
    await _enqueue_follow_up(follow_up_session, SCOPE, uuid4())  # type: ignore[arg-type]
    assert len(follow_up_session.statements) == 1
    assert "INSERT INTO task_outbox" in _sql(follow_up_session.statements[0])

    cancel_session = Session()
    await _cancel_claim_recovery(cancel_session, SCOPE, uuid4())  # type: ignore[arg-type]
    assert len(cancel_session.statements) == 1
    assert _sql(cancel_session.statements[0]).startswith("UPDATE task_outbox")


@pytest.mark.asyncio
async def test_non_empty_runtime_operations_reject_missing_claim_token() -> None:
    class ExplodingFactory:
        def __call__(self):
            raise AssertionError("不应打开数据库 session")

    store = SqlAlchemyIngestStore(ExplodingFactory(), SqlFinalizationPort())  # type: ignore[arg-type]
    item_id = uuid4()

    with pytest.raises(ClaimLost, match="非空 UUID"):
        await store.release_failed(SCOPE, [item_id], None)
    with pytest.raises(ClaimLost, match="非空 UUID"):
        await store.mark_outbox_sent([item_id], None)
    with pytest.raises(ClaimLost, match="非空 UUID"):
        await store.release_outbox([item_id], None)
    with pytest.raises(ClaimLost, match="非空 UUID"):
        await store.apply_results(
            SCOPE,
            None,
            [],
            [item_id],
            uuid4(),
            expected_pages={},
        )

    await store.release_failed(SCOPE, [], None)
    await store.mark_outbox_sent([], None)
    await store.release_outbox([], None)


@pytest.mark.asyncio
async def test_legacy_empty_batch_still_validates_expected_pages() -> None:
    class ExplodingFactory:
        def __call__(self):
            raise AssertionError("非法空批不应打开数据库 session")

    store = SqlAlchemyIngestStore(ExplodingFactory(), SqlFinalizationPort())  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="expected_pages"):
        await store.apply_results(
            SCOPE,
            None,
            [],
            [],
            uuid4(),
            expected_pages={"entity/unexpected": None},
        )


@pytest.mark.asyncio
async def test_legacy_valid_empty_batch_is_noop_without_claim_or_operation() -> None:
    class ExplodingFactory:
        def __call__(self):
            raise AssertionError("合法空批不应打开数据库 session")

    store = SqlAlchemyIngestStore(ExplodingFactory(), SqlFinalizationPort())  # type: ignore[arg-type]

    assert (
        await store.apply_results(
            SCOPE,
            None,
            [],
            [],
            None,  # type: ignore[arg-type]
            expected_pages={},
        )
        is False
    )


def test_legacy_page_coverage_remains_compatible_without_deltas() -> None:
    op_id = uuid4()
    page = _result_page().model_copy(update={"contributor_op_ids": [op_id]})

    request = ingest_store._legacy_batch_request(
        uuid4(),
        [page],
        [op_id],
        uuid4(),
        [],
        {page.slug: None},
    )

    assert [item.slug for item in request.pages] == [page.slug]
    assert request.pages[0].contributor_op_ids == (op_id,)
    assert request.contribution_deltas == ()
    assert request.folder_assignments == ()


@pytest.mark.asyncio
async def test_apply_results_routes_taxonomy_requirement_by_request_style() -> None:
    class RecordingStore(SqlAlchemyIngestStore):
        def __init__(self) -> None:
            super().__init__(lambda: None, SqlFinalizationPort())  # type: ignore[arg-type]
            self.require_taxonomy_flags: list[bool] = []

        async def _apply_batch_results(
            self,
            scope: WikiScope,
            request: BatchApplyRequest,
            *,
            require_taxonomy: bool,
        ):
            self.require_taxonomy_flags.append(require_taxonomy)
            return ingest_store._batch_apply_outcome(request, applied=True)

    store = RecordingStore()
    await store.apply_results(SCOPE, _batch_request())
    await store.apply_results(SCOPE, uuid4(), [], [uuid4()], uuid4())
    await store.apply_results_with_outcome(SCOPE, _batch_request())

    assert store.require_taxonomy_flags == [True, False, True]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "legacy_kwargs",
    [
        {"pages": ()},
        {"completed_op_ids": ()},
        {"operation_id": uuid4()},
        {"failed_op_ids": ()},
        {"expected_pages": {}},
    ],
)
async def test_modern_apply_results_rejects_even_empty_legacy_arguments(
    legacy_kwargs: dict[str, object],
) -> None:
    class ExplodingFactory:
        def __call__(self):
            raise AssertionError("混用参数不应打开数据库 session")

    store = SqlAlchemyIngestStore(ExplodingFactory(), SqlFinalizationPort())  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="不能混用"):
        await store.apply_results(SCOPE, _batch_request(), **legacy_kwargs)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_sql_store_rejects_forged_scope_before_opening_session() -> None:
    class ExplodingFactory:
        def __call__(self):
            raise AssertionError("不应打开数据库 session")

    store = SqlAlchemyIngestStore(ExplodingFactory(), SqlFinalizationPort())  # type: ignore[arg-type]
    knowledge = SourceKnowledge(
        id="knowledge-1",
        tenant_id=8,
        knowledge_base_id=KB_ID,
        title="伪造来源",
        op_version="version-1",
    )

    with pytest.raises(ValueError, match="不一致"):
        await store.enqueue(
            SCOPE, knowledge, {"knowledge_id": knowledge.id}, delay_seconds=0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("completed_count", [0, 1])
async def test_result_pages_require_completed_contributors(
    completed_count: int,
) -> None:
    class ExplodingFactory:
        def __call__(self):
            raise AssertionError("非法批次不应打开数据库 session")

    store = SqlAlchemyIngestStore(ExplodingFactory(), SqlFinalizationPort())  # type: ignore[arg-type]
    completed_ids = [uuid4()] if completed_count else []
    failed_ids = [uuid4()]
    page = ReducedPage(
        slug="entity/no-contributor",
        title="无贡献页面",
        page_type="entity",
        content="正文",
        summary="摘要",
        contributor_op_ids=[],
    )

    with pytest.raises(InvariantError, match="contributor"):
        await store.apply_results(
            SCOPE,
            uuid4(),
            [page],
            completed_ids,
            uuid4(),
            failed_op_ids=failed_ids,
            expected_pages={page.slug: None},
        )


class _ScriptedResult:
    def __init__(self, *, rows=(), scalar=None, rowcount: int | None = None) -> None:
        self._rows = list(rows)
        self._scalar = scalar
        self.rowcount = rowcount

    def scalars(self):
        return list(self._rows)

    def scalar_one_or_none(self):
        return self._scalar

    def scalar_one(self):
        return self._scalar

    def all(self):
        return list(self._rows)


class _ScriptedSession:
    def __init__(self, results, events: list[str] | None = None) -> None:
        self.results = list(results)
        self.statements = []
        self.events = events if events is not None else []
        self.rolled_back = False
        self.added = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, *_args):
        if exc_type is not None:
            self.rolled_back = True
        return None

    def begin(self):
        return self

    async def execute(self, statement):
        self.statements.append(statement)
        sql = _sql(statement)
        self.events.append(sql)
        if "pg_advisory_xact_lock" in sql:
            return _ScriptedResult()
        if not self.results:
            raise AssertionError(f"unexpected SQL: {sql}")
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    async def flush(self) -> None:
        self.events.append("FLUSH")
        return None

    def add(self, value) -> None:
        self.added.append(value)
        self.events.append(f"ADD:{type(value).__name__}")

    def add_all(self, values) -> None:
        for value in values:
            self.add(value)


class _OneSessionFactory:
    def __init__(self, session: _ScriptedSession) -> None:
        self.session = session

    def __call__(self):
        return self.session


class _RecordingFinalization:
    def __init__(
        self,
        events: list[str],
        *,
        registered: bool = True,
        release_ok: bool = True,
    ) -> None:
        self.events = events
        self.registered = registered
        self.release_ok = release_ok
        self.requests: list[tuple[str, FinalizationRequest]] = []

    async def register(self, _session, request: FinalizationRequest) -> bool:
        self.events.append(f"finalization.register:{request.attempt}")
        self.requests.append(("register", request))
        return self.registered

    async def release(self, _session, request: FinalizationRequest) -> bool:
        self.events.append(f"finalization.release:{request.attempt}")
        self.requests.append(("release", request))
        return self.release_ok


@pytest.mark.asyncio
async def test_resolve_folder_assignment_keeps_root_without_sql() -> None:
    session = _ScriptedSession([])
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), SqlFinalizationPort())  # type: ignore[arg-type]
    assignment = FolderAssignment(
        slug="entity/acme",
        contributor_op_ids=(uuid4(),),
    )

    placement = await store._resolve_folder_assignment(session, SCOPE, assignment)

    assert placement == (None, [], "/entity/acme", 0)
    assert session.statements == []


@pytest.mark.asyncio
async def test_resolve_folder_assignment_creates_and_locks_each_segment() -> None:
    root_id, child_id = uuid4(), uuid4()
    root = WikiFolder(
        id=root_id,
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        parent_id=None,
        name="Engineering",
        path="/Engineering",
        depth=1,
    )
    child = WikiFolder(
        id=child_id,
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        parent_id=root_id,
        name="Databases",
        path="/Engineering/Databases",
        depth=2,
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(scalar=root_id),
            _ScriptedResult(scalar=root),
            _ScriptedResult(scalar=child_id),
            _ScriptedResult(scalar=child),
        ]
    )
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), SqlFinalizationPort())  # type: ignore[arg-type]
    assignment = FolderAssignment(
        slug="concept/postgresql",
        contributor_op_ids=(uuid4(),),
        new_segments=("Engineering", "Databases"),
    )

    placement = await store._resolve_folder_assignment(session, SCOPE, assignment)

    assert placement == (
        child_id,
        ["Engineering", "Databases"],
        "/Engineering/Databases/concept/postgresql",
        2,
    )
    assert len(session.statements) == 4
    for statement in session.statements[::2]:
        sql = _sql(statement)
        assert sql.startswith("INSERT INTO wiki_folders")
        assert "ON CONFLICT (knowledge_base_id, parent_id, name)" in sql
        assert "WHERE deleted_at IS NULL DO NOTHING" in sql
        assert "RETURNING wiki_folders.id" in sql
    for statement in session.statements[1::2]:
        sql = _sql(statement)
        assert "wiki_folders.tenant_id" in sql
        assert "wiki_folders.knowledge_base_id" in sql
        assert "wiki_folders.deleted_at IS NULL" in sql
        assert "FOR UPDATE" in sql


@pytest.mark.asyncio
async def test_resolve_folder_assignment_rejects_changed_base_snapshot() -> None:
    base_id = uuid4()
    moved = WikiFolder(
        id=base_id,
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        parent_id=None,
        name="Moved",
        path="/Moved",
        depth=1,
    )
    session = _ScriptedSession([_ScriptedResult(scalar=moved)])
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), SqlFinalizationPort())  # type: ignore[arg-type]
    assignment = FolderAssignment(
        slug="entity/acme",
        contributor_op_ids=(uuid4(),),
        base_folder_id=base_id,
        base_path="/Engineering",
        base_depth=1,
    )

    with pytest.raises(PageConflict, match="taxonomy base 目录已移动或失效"):
        await store._resolve_folder_assignment(session, SCOPE, assignment)

    assert len(session.statements) == 1
    assert "FOR UPDATE" in _sql(session.statements[0])


@pytest.mark.asyncio
async def test_resolve_folder_assignment_rejects_locked_path_depth_mismatch() -> None:
    folder_id = uuid4()
    corrupted = WikiFolder(
        id=folder_id,
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        parent_id=None,
        name="Engineering",
        path="/unexpected",
        depth=2,
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(scalar=None),
            _ScriptedResult(scalar=corrupted),
        ]
    )
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), SqlFinalizationPort())  # type: ignore[arg-type]
    assignment = FolderAssignment(
        slug="entity/acme",
        contributor_op_ids=(uuid4(),),
        new_segments=("Engineering",),
    )

    with pytest.raises(InvariantError, match="path.*depth"):
        await store._resolve_folder_assignment(session, SCOPE, assignment)


def test_folder_assignments_must_exactly_cover_truly_new_topics() -> None:
    op_id = uuid4()
    new_topic = _result_page().model_copy(update={"contributor_op_ids": [op_id]})
    restored_history = _page(slug="concept/history", page_type="concept")
    restored_history.deleted_at = NOW
    restored = ReducedPage(
        slug=restored_history.slug,
        title="Restored",
        page_type="concept",
        content="Restored",
        summary="Restored",
        contributor_op_ids=[op_id],
    )

    with pytest.raises(
        InvariantError,
        match="folder assignments 必须精确覆盖真正新建 topic 页面",
    ):
        ingest_store._folder_assignments_for_new_topics(
            [(None, new_topic), (restored_history, restored)],
            (),
            {op_id},
            require_taxonomy=True,
        )

    extra = FolderAssignment(
        slug=restored.slug,
        contributor_op_ids=(op_id,),
        new_segments=("Ignored",),
    )
    with pytest.raises(
        InvariantError,
        match="folder assignments 必须精确覆盖真正新建 topic 页面",
    ):
        ingest_store._folder_assignments_for_new_topics(
            [(None, new_topic), (restored_history, restored)],
            (
                FolderAssignment(
                    slug=new_topic.slug,
                    contributor_op_ids=(op_id,),
                ),
                extra,
            ),
            {op_id},
            require_taxonomy=True,
        )


def test_folder_assignments_filter_superseded_contributors_before_coverage() -> None:
    superseded_id = uuid4()
    assignment = FolderAssignment(
        slug="entity/superseded",
        contributor_op_ids=(superseded_id,),
        new_segments=("MustNotExist",),
    )

    assert (
        ingest_store._folder_assignments_for_new_topics(
            [],
            (assignment,),
            set(),
            require_taxonomy=True,
        )
        == ()
    )


def test_folder_assignments_are_returned_in_stable_slug_order() -> None:
    op_id = uuid4()
    pages = [
        ReducedPage(
            slug=slug,
            title=slug,
            page_type="entity",
            content=slug,
            summary=slug,
            contributor_op_ids=[op_id],
        )
        for slug in ("entity/zeta", "entity/alpha")
    ]
    assignments = tuple(
        FolderAssignment(slug=page.slug, contributor_op_ids=(op_id,)) for page in pages
    )

    selected = ingest_store._folder_assignments_for_new_topics(
        [(None, page) for page in pages],
        assignments,
        {op_id},
        require_taxonomy=True,
    )

    assert [assignment.slug for assignment in selected] == [
        "entity/alpha",
        "entity/zeta",
    ]


def _pending(
    *,
    knowledge_id: str = "knowledge-1",
    op: str = "ingest",
    version: str = "version-1",
    claimed: bool = False,
) -> WikiPendingOp:
    return WikiPendingOp(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        knowledge_id=knowledge_id,
        op=op,
        op_version=version,
        payload={"knowledge_id": knowledge_id},
        fail_count=0,
        enqueued_at=NOW,
        claimed_at=NOW if claimed else None,
        claim_token=uuid4() if claimed else None,
    )


def _outbox() -> TaskOutbox:
    return TaskOutbox(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        event_type="wiki.batch.trigger",
        dedup_key="a" * 64,
        payload={"tenant_id": SCOPE.tenant_id},
        available_at=NOW,
    )


def _contribution(
    *,
    knowledge_id: str,
    slug: str = "entity/acme",
    version: str = "version-1",
    page_type: str = "entity",
    refs: list[str] | None = None,
) -> WikiPageContribution:
    return WikiPageContribution(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        slug=slug,
        knowledge_id=knowledge_id,
        op_version=version,
        page_type=page_type,
        state="active",
        title="Acme",
        content=f"content:{knowledge_id}",
        summary=f"summary:{knowledge_id}",
        aliases=[knowledge_id],
        chunk_refs=refs or [f"chunk:{knowledge_id}"],
        created_at=NOW,
        updated_at=NOW,
    )


def _page(
    *,
    slug: str = "entity/acme",
    page_type: str = "entity",
    sources: list[str] | None = None,
    chunks: list[str] | None = None,
) -> WikiPage:
    return WikiPage(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        slug=slug,
        title="Stable title",
        page_type=page_type,
        status="published",
        content="Stable [[entity/linked]] content",
        summary="Stable summary",
        aliases=["Stable alias"],
        source_refs=sources or [],
        chunk_refs=chunks or [],
        version=4,
        created_at=NOW,
        updated_at=NOW,
        deleted_at=None,
    )


def _knowledge(version: str = "version-2") -> SourceKnowledge:
    return SourceKnowledge(
        id="knowledge-1",
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        title="Document",
        op_version=version,
    )


@pytest.mark.asyncio
async def test_modern_apply_results_writes_resolved_folder_page_cache() -> None:
    token = uuid4()
    pending = _pending(claimed=True)
    pending.claim_token = token
    page = _result_page().model_copy(update={"contributor_op_ids": [pending.id]})
    current = _stored_contribution(version=pending.op_version)
    assignment = FolderAssignment(
        slug=page.slug,
        contributor_op_ids=(pending.id,),
        new_segments=("Engineering", "Databases"),
    )
    request = _batch_request(
        claim_token=token,
        pages=(page,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=pending.id,
                action="add",
                slug=page.slug,
                knowledge_id=pending.knowledge_id,
                previous=None,
                current=current,
            ),
        ),
        completed_op_ids=(pending.id,),
        expected_pages=(PageExpectation(slug=page.slug),),
        folder_assignments=(assignment,),
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[pending]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(),
            _ScriptedResult(),
            _ScriptedResult(rowcount=1),
            _ScriptedResult(),
            _ScriptedResult(scalar=0),
        ]
    )
    folder_id = uuid4()

    class RecordingStore(SqlAlchemyIngestStore):
        def __init__(self) -> None:
            super().__init__(
                _OneSessionFactory(session),  # type: ignore[arg-type]
                _RecordingFinalization(session.events),
            )
            self.resolved: list[FolderAssignment] = []

        async def _resolve_folder_assignment(
            self,
            actual_session: AsyncSession,
            scope: WikiScope,
            actual_assignment: FolderAssignment,
        ) -> tuple[UUID | None, list[str], str, int]:
            assert actual_session is session
            assert scope == SCOPE
            session.events.append(f"RESOLVE:{actual_assignment.slug}")
            self.resolved.append(actual_assignment)
            return (
                folder_id,
                ["Engineering", "Databases"],
                "/Engineering/Databases/entity/acme",
                2,
            )

    store = RecordingStore()

    assert await store.apply_results_with_outcome(SCOPE, request)

    inserted_page = next(item for item in session.added if isinstance(item, WikiPage))
    assert store.resolved == [assignment]
    assert inserted_page.folder_id == folder_id
    assert inserted_page.category_path == ["Engineering", "Databases"]
    assert inserted_page.wiki_path == "/Engineering/Databases/entity/acme"
    assert inserted_page.depth == 2
    assert inserted_page.version == 1
    advisory_indexes = [
        index
        for index, event in enumerate(session.events)
        if "pg_advisory_xact_lock" in event
    ]
    assert len(advisory_indexes) == 2
    assert advisory_indexes[-1] < session.events.index(
        f"RESOLVE:{assignment.slug}"
    )
    advisory_statements = [
        statement
        for statement in session.statements
        if "pg_advisory_xact_lock" in _sql(statement)
    ]
    taxonomy_params = advisory_statements[-1].compile(
        dialect=postgresql.dialect()
    ).params
    assert next(iter(taxonomy_params.values())) == (
        ingest_store._taxonomy_scope_lock_key(SCOPE)
    )


@pytest.mark.asyncio
async def test_legacy_new_page_does_not_take_taxonomy_scope_lock() -> None:
    token = uuid4()
    pending = _pending(claimed=True)
    pending.claim_token = token
    page = _result_page().model_copy(
        update={"contributor_op_ids": [pending.id]}
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[pending]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(),
            _ScriptedResult(),
            _ScriptedResult(rowcount=1),
            _ScriptedResult(),
            _ScriptedResult(scalar=0),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization(session.events)
    )  # type: ignore[arg-type]

    assert await store.apply_results(
        SCOPE,
        token,
        [page],
        [pending.id],
        uuid4(),
        expected_pages={page.slug: None},
    )

    inserted_page = next(item for item in session.added if isinstance(item, WikiPage))
    assert inserted_page.wiki_path == f"/{page.slug}"
    assert (
        sum("pg_advisory_xact_lock" in _sql(item) for item in session.statements)
        == 1
    )


@pytest.mark.asyncio
async def test_modern_apply_results_atomically_replaces_contribution_and_page() -> None:
    token = uuid4()
    pending = _pending(claimed=True)
    pending.claim_token = token
    old = _contribution(knowledge_id=pending.knowledge_id)
    page = _page(sources=[pending.knowledge_id], chunks=["chunk:old"])
    previous = _stored_contribution().model_copy(update={"id": old.id})
    current = _stored_contribution(version=pending.op_version).model_copy(
        update={
            "content": "新贡献正文",
            "summary": "新贡献摘要",
            "chunk_refs": ("chunk:new",),
        }
    )
    reduced = ReducedPage(
        slug=page.slug,
        title="新标题",
        page_type="entity",
        content="新聚合正文",
        summary="新聚合摘要",
        aliases=["New alias"],
        source_refs=[pending.knowledge_id],
        chunk_refs=["chunk:new"],
        contributor_op_ids=[pending.id],
    )
    request = _batch_request(
        claim_token=token,
        pages=(reduced,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=pending.id,
                action="replace",
                slug=page.slug,
                knowledge_id=pending.knowledge_id,
                previous=previous,
                current=current,
            ),
        ),
        completed_op_ids=(pending.id,),
        expected_pages=(
            PageExpectation(slug=page.slug, page_id=page.id, version=page.version),
        ),
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[pending]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[page]),
            _ScriptedResult(rowcount=1),
            _ScriptedResult(),
            _ScriptedResult(),
            _ScriptedResult(rowcount=1),
            _ScriptedResult(),
            _ScriptedResult(scalar=0),
        ]
    )
    finalization = _RecordingFinalization(session.events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    assert await store.apply_results(SCOPE, request) is True

    inserted = [
        item for item in session.added if isinstance(item, WikiPageContribution)
    ]
    logs = [item for item in session.added if isinstance(item, WikiLogEntry)]
    assert len(inserted) == 1
    assert inserted[0].op_version == pending.op_version
    assert inserted[0].state == "active"
    assert page.version == 5
    assert page.title == "新标题" and page.content == "新聚合正文"
    assert page.source_refs == [pending.knowledge_id]
    assert page.chunk_refs == ["chunk:new"]
    assert len(logs) == 1 and logs[0].action == "wiki_ingest_batch"
    assert [(kind, item.subtask_name) for kind, item in finalization.requests] == [
        ("release", "wiki")
    ]
    contribution_delete = next(
        statement
        for statement in session.statements
        if _sql(statement).startswith("DELETE FROM wiki_page_contributions")
    )
    assert "wiki_page_contributions.state" in _sql(contribution_delete)


@pytest.mark.asyncio
async def test_modern_apply_results_keeps_the_fourth_failure_pending() -> None:
    token = uuid4()
    pending = _pending(claimed=True)
    pending.claim_token = token
    pending.fail_count = 3
    request = _batch_request(
        claim_token=token,
        completed_op_ids=(),
        failures=(
            OperationFailure(
                pending_op_id=pending.id,
                error_code="MODEL_TEMPORARY",
                error_summary="模型暂时不可用",
            ),
        ),
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[pending]),
            _ScriptedResult(),
            _ScriptedResult(scalar=1),
            _ScriptedResult(),
        ]
    )
    finalization = _RecordingFinalization(session.events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    assert await store.apply_results(SCOPE, request) is True

    assert pending.fail_count == 4
    assert pending.claimed_at is None and pending.claim_token is None
    assert finalization.requests == []
    assert not any(type(item).__name__ == "WikiDeadLetter" for item in session.added)


@pytest.mark.asyncio
async def test_modern_apply_results_moves_the_fifth_failure_to_dead_letter() -> None:
    token = uuid4()
    pending = _pending(claimed=True)
    pending.claim_token = token
    pending.fail_count = 4
    pending.payload = {
        "knowledge_id": pending.knowledge_id,
        "safe": {"attempt": 5},
        "claim_token": str(token),
        "traceback": "Traceback secret",
        "chunk_text": "chunk raw text",
        "nested": {"model_output": "raw output", "kept": True},
    }
    request = _batch_request(
        claim_token=token,
        completed_op_ids=(),
        failures=(
            OperationFailure(
                pending_op_id=pending.id,
                error_code="MODEL_PERMANENT",
                error_summary=" 模型失败\r\n  Traceback raw stack ",
            ),
        ),
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[pending]),
            _ScriptedResult(),
            _ScriptedResult(rowcount=1),
            _ScriptedResult(),
            _ScriptedResult(scalar=0),
        ]
    )
    finalization = _RecordingFinalization(session.events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    assert await store.apply_results(SCOPE, request) is True

    dead_insert = next(
        statement
        for statement in session.statements
        if _sql(statement).startswith("INSERT INTO wiki_dead_letters")
    )
    dead_sql = _sql(dead_insert)
    params = dead_insert.compile(dialect=postgresql.dialect()).params
    assert (
        "ON CONFLICT ON CONSTRAINT uq_wiki_dead_letters_pending_op DO NOTHING"
        in dead_sql
    )
    assert params["fail_count"] == 5
    assert params["last_error_code"] == "MODEL_PERMANENT"
    assert params["last_error_summary"] == "模型失败"
    assert params["payload"] == {"knowledge_id": pending.knowledge_id}
    assert [(kind, item.subtask_name) for kind, item in finalization.requests] == [
        ("release", "wiki")
    ]
    pending_delete = next(
        statement
        for statement in session.statements
        if _sql(statement).startswith("DELETE FROM wiki_pending_ops")
    )
    assert "wiki_pending_ops.claim_token" in _sql(pending_delete)


@pytest.mark.asyncio
async def test_list_dead_letters_returns_scoped_immutable_snapshots() -> None:
    rows = [
        WikiDeadLetter(
            id=uuid4(),
            pending_op_id=uuid4(),
            tenant_id=SCOPE.tenant_id,
            knowledge_base_id=SCOPE.knowledge_base_id,
            knowledge_id=f"knowledge-{index}",
            op="ingest",
            op_version="version-1",
            payload={"knowledge_id": f"knowledge-{index}"},
            fail_count=5,
            last_error_code="MODEL_PERMANENT",
            last_error_summary="重试耗尽",
            dead_at=NOW + timedelta(seconds=index),
        )
        for index in (1, 2)
    ]

    class Rows:
        def scalars(self):
            return rows

    class Session:
        def __init__(self) -> None:
            self.statement = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def execute(self, statement):
            self.statement = statement
            return Rows()

    session = Session()
    store = SqlAlchemyIngestStore(lambda: session, SqlFinalizationPort())  # type: ignore[arg-type]

    records = await store.list_dead_letters(SCOPE, limit=2)  # type: ignore[attr-defined]

    assert [record.id for record in records] == [row.id for row in rows]
    rows[0].payload["knowledge_id"] = "mutated"
    assert records[0].payload["knowledge_id"] == "knowledge-1"
    with pytest.raises(FrozenInstanceError):
        records[0].fail_count = 6  # type: ignore[misc]
    with pytest.raises(TypeError):
        records[0].payload["new"] = True  # type: ignore[index]
    sql = _sql(session.statement)
    assert "wiki_dead_letters.tenant_id" in sql
    assert "wiki_dead_letters.knowledge_base_id" in sql
    assert "ORDER BY wiki_dead_letters.dead_at, wiki_dead_letters.id" in sql
    assert "LIMIT" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "updates",
    [
        {"payload": {"knowledge_id": "knowledge-1", "claim_token": "secret"}},
        {"payload": {"knowledge_id": "knowledge-1", "traceback": "raw"}},
        {
            "payload": {
                "knowledge_id": "knowledge-1",
                "nested": {"model_output": "raw"},
            }
        },
        {"payload": {"knowledge_id": "other"}},
        {"tenant_id": SCOPE.tenant_id + 1},
        {"knowledge_base_id": UUID("22222222-2222-2222-2222-222222222222")},
        {"op": "delete"},
        {"fail_count": 4},
        {"last_error_code": ""},
        {"last_error_code": "x" * 129},
        {"last_error_summary": ""},
        {"last_error_summary": "x" * 2001},
        {"last_error_summary": "line one\r\nline two"},
        {"last_error_summary": "safe MODEL_OUTPUT raw"},
    ],
)
async def test_list_dead_letters_rejects_polluted_rows(updates) -> None:
    values = {
        "id": uuid4(),
        "pending_op_id": uuid4(),
        "tenant_id": SCOPE.tenant_id,
        "knowledge_base_id": SCOPE.knowledge_base_id,
        "knowledge_id": "knowledge-1",
        "op": "ingest",
        "op_version": "version-1",
        "payload": {"knowledge_id": "knowledge-1"},
        "fail_count": 5,
        "last_error_code": "MODEL_PERMANENT",
        "last_error_summary": "重试耗尽",
        "dead_at": NOW,
    }
    values.update(updates)
    row = WikiDeadLetter(**values)
    session = _ScriptedSession([_ScriptedResult(rows=[row])])
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), SqlFinalizationPort())  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="dead-letter"):
        await store.list_dead_letters(SCOPE)


@pytest.mark.asyncio
async def test_release_claim_only_clears_the_owned_scoped_claim() -> None:
    token = uuid4()
    op_ids = [uuid4(), uuid4()]
    session = _ScriptedSession([_ScriptedResult(rowcount=2)])
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), SqlFinalizationPort())  # type: ignore[arg-type]

    await store.release_claim(SCOPE, op_ids, token)  # type: ignore[attr-defined]

    assert len(session.statements) == 1
    statement = session.statements[0]
    sql = _sql(statement)
    assert sql.startswith("UPDATE wiki_pending_ops")
    assert "wiki_pending_ops.tenant_id" in sql
    assert "wiki_pending_ops.knowledge_base_id" in sql
    assert "wiki_pending_ops.id IN" in sql
    assert "wiki_pending_ops.claim_token" in sql
    assert "fail_count" not in sql
    params = statement.compile(dialect=postgresql.dialect()).params
    assert params["claimed_at"] is None
    assert params["claim_token"] is None


@pytest.mark.asyncio
async def test_auto_superseded_shared_slug_rolls_back_before_any_writes() -> None:
    token = uuid4()
    superseded = _pending(claimed=True)
    superseded.claim_token = token
    other = _pending(knowledge_id="knowledge-2", version="version-2", claimed=True)
    other.claim_token = token
    retract = _pending(
        knowledge_id=superseded.knowledge_id,
        op="retract",
        version="delete-1",
    )
    retract.enqueued_at = NOW + timedelta(seconds=1)
    superseded_current = _stored_contribution(version=superseded.op_version)
    other_current = _stored_contribution(
        knowledge_id=other.knowledge_id, version=other.op_version
    )
    page = ReducedPage(
        slug="entity/acme",
        title="混合页面",
        page_type="entity",
        content="同时包含两个来源的正文",
        summary="混合摘要",
        source_refs=[superseded.knowledge_id, other.knowledge_id],
        contributor_op_ids=[superseded.id, other.id],
    )
    request = _batch_request(
        claim_token=token,
        pages=(page,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=superseded.id,
                action="add",
                slug=page.slug,
                knowledge_id=superseded.knowledge_id,
                previous=None,
                current=superseded_current,
            ),
            ContributionDelta(
                pending_op_id=other.id,
                action="add",
                slug=page.slug,
                knowledge_id=other.knowledge_id,
                previous=None,
                current=other_current,
            ),
        ),
        completed_op_ids=(superseded.id, other.id),
        expected_pages=(PageExpectation(slug=page.slug),),
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[superseded, other]),
            _ScriptedResult(rows=[retract]),
            _ScriptedResult(rowcount=2),
            _ScriptedResult(),
            _ScriptedResult(scalar=0),
        ]
    )
    finalization = _RecordingFinalization(session.events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    with pytest.raises(ingest_store.PageConflict, match="superseded"):
        await store.apply_results(SCOPE, request)

    assert session.rolled_back is True
    assert session.added == []
    assert finalization.requests == []
    assert not any(
        _sql(statement).startswith(("INSERT", "UPDATE", "DELETE"))
        for statement in session.statements
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("detailed", [False, True])
async def test_later_retract_supersedes_ingest_without_writing_its_slug(
    detailed: bool,
) -> None:
    token = uuid4()
    ingest = _pending(claimed=True)
    ingest.claim_token = token
    retract = _pending(
        knowledge_id=ingest.knowledge_id,
        op="retract",
        version="delete-1",
    )
    retract.enqueued_at = NOW + timedelta(seconds=1)
    current = _stored_contribution(version=ingest.op_version)
    page = _result_page().model_copy(update={"contributor_op_ids": [ingest.id]})
    request = _batch_request(
        claim_token=token,
        pages=(page,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=ingest.id,
                action="add",
                slug=current.slug,
                knowledge_id=current.knowledge_id,
                previous=None,
                current=current,
            ),
        ),
        completed_op_ids=(ingest.id,),
        expected_pages=(PageExpectation(slug=page.slug),),
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[ingest]),
            _ScriptedResult(rows=[retract]),
            _ScriptedResult(rowcount=1),
            _ScriptedResult(),
            _ScriptedResult(scalar=1),
            _ScriptedResult(),
        ]
    )
    finalization = _RecordingFinalization(session.events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    if detailed:
        outcome = await store.apply_results_with_outcome(SCOPE, request)
        assert outcome.applied is True
        assert outcome.completed_op_ids == outcome.failed_op_ids == ()
        assert outcome.superseded_op_ids == (ingest.id,)
    else:
        assert await store.apply_results(SCOPE, request) is True

    assert (
        sum("pg_advisory_xact_lock" in _sql(item) for item in session.statements) == 1
    )
    assert not any(
        isinstance(item, (WikiPage, WikiPageContribution)) for item in session.added
    )
    assert ingest.fail_count == 0
    assert [(kind, item.subtask_name) for kind, item in finalization.requests] == [
        ("release", "wiki")
    ]
    log = next(item for item in session.added if isinstance(item, WikiLogEntry))
    assert "跳过 1" in log.message
    expected_outcome = {
        "completed_op_ids": [],
        "superseded_op_ids": [str(ingest.id)],
        "failed_op_ids": [],
    }
    assert log.result_outcome == expected_outcome
    if detailed:
        replay_session = _ScriptedSession([_ScriptedResult(scalar=log)])
        replay_store = SqlAlchemyIngestStore(
            _OneSessionFactory(replay_session), SqlFinalizationPort()
        )  # type: ignore[arg-type]

        replay = await replay_store.apply_results_with_outcome(SCOPE, request)

        assert replay.applied is False
        assert replay.completed_op_ids == replay.failed_op_ids == ()
        assert replay.superseded_op_ids == (ingest.id,)


@pytest.mark.asyncio
async def test_modern_retract_soft_deletes_page_and_clears_visible_links() -> None:
    token = uuid4()
    pending = _pending(op="retract", version="delete-1", claimed=True)
    pending.claim_token = token
    contribution = _contribution(knowledge_id=pending.knowledge_id)
    contribution.state = "retract_pending"
    previous = _stored_contribution().model_copy(
        update={"id": contribution.id, "state": "retract_pending"}
    )
    page = _page(sources=[pending.knowledge_id], chunks=["chunk:knowledge-1"])
    reduced = ReducedPage(
        slug=page.slug,
        title=page.title,
        page_type="entity",
        content=page.content,
        summary=page.summary,
        aliases=page.aliases,
        contributor_op_ids=[pending.id],
        deleted=True,
    )
    request = _batch_request(
        claim_token=token,
        pages=(reduced,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=pending.id,
                action="retract",
                slug=page.slug,
                knowledge_id=pending.knowledge_id,
                previous=previous,
                current=None,
            ),
        ),
        completed_op_ids=(pending.id,),
        expected_pages=(
            PageExpectation(slug=page.slug, page_id=page.id, version=page.version),
        ),
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[pending]),
            _ScriptedResult(rows=[page]),
            _ScriptedResult(rowcount=1),
            _ScriptedResult(),
            _ScriptedResult(),
            _ScriptedResult(rowcount=1),
            _ScriptedResult(),
            _ScriptedResult(scalar=0),
        ]
    )
    finalization = _RecordingFinalization(session.events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    assert await store.apply_results(SCOPE, request) is True

    assert page.deleted_at is not None
    assert page.source_refs == [] and page.chunk_refs == []
    assert page.version == 5
    link_sql = [
        _sql(statement)
        for statement in session.statements
        if "wiki_links" in _sql(statement)
    ]
    assert link_sql[0].startswith("DELETE FROM wiki_links")
    assert link_sql[1].startswith("UPDATE wiki_links")
    log = next(item for item in session.added if isinstance(item, WikiLogEntry))
    assert log.action == "wiki_retract_batch"
    assert [(kind, item.subtask_name) for kind, item in finalization.requests] == [
        ("release", "wiki-retract")
    ]


@pytest.mark.asyncio
async def test_modern_operation_id_reuse_across_scope_rolls_back() -> None:
    foreign_log = WikiLogEntry(
        tenant_id=SCOPE.tenant_id + 1,
        knowledge_base_id=SCOPE.knowledge_base_id,
        operation_id=uuid4(),
        action="wiki_ingest_batch",
        message="foreign",
        pages_affected=[],
        result_outcome={"corrupt": True},
    )
    session = _ScriptedSession([_ScriptedResult(scalar=foreign_log)])
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), SqlFinalizationPort())  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="其他 scope"):
        await store.apply_results(
            SCOPE, _batch_request(operation_id=foreign_log.operation_id)
        )

    assert session.rolled_back is True
    assert len(session.statements) == 2


@pytest.mark.asyncio
async def test_modern_cas_conflict_rolls_back_before_contribution_writes() -> None:
    token = uuid4()
    pending = _pending(claimed=True)
    pending.claim_token = token
    page = _page(sources=[pending.knowledge_id])
    current = _stored_contribution(version=pending.op_version)
    reduced = _result_page().model_copy(update={"contributor_op_ids": [pending.id]})
    request = _batch_request(
        claim_token=token,
        pages=(reduced,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=pending.id,
                action="add",
                slug=page.slug,
                knowledge_id=pending.knowledge_id,
                previous=None,
                current=current,
            ),
        ),
        completed_op_ids=(pending.id,),
        expected_pages=(
            PageExpectation(slug=page.slug, page_id=page.id, version=page.version - 1),
        ),
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[pending]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[page]),
        ]
    )
    finalization = _RecordingFinalization(session.events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    with pytest.raises(ingest_store.PageConflict):
        await store.apply_results(SCOPE, request)

    assert session.rolled_back is True
    assert session.added == []
    assert finalization.requests == []
    assert not any(
        _sql(statement).startswith("DELETE FROM wiki_page_contributions")
        for statement in session.statements
    )


@pytest.mark.asyncio
async def test_modern_contribution_conflict_rolls_back_before_page_writes() -> None:
    token = uuid4()
    pending = _pending(claimed=True)
    pending.claim_token = token
    page = _page(sources=[pending.knowledge_id])
    old = _contribution(knowledge_id=pending.knowledge_id)
    previous = _stored_contribution().model_copy(update={"id": old.id})
    current = _stored_contribution(version=pending.op_version)
    reduced = _result_page().model_copy(update={"contributor_op_ids": [pending.id]})
    request = _batch_request(
        claim_token=token,
        pages=(reduced,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=pending.id,
                action="replace",
                slug=page.slug,
                knowledge_id=pending.knowledge_id,
                previous=previous,
                current=current,
            ),
        ),
        completed_op_ids=(pending.id,),
        expected_pages=(
            PageExpectation(slug=page.slug, page_id=page.id, version=page.version),
        ),
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[pending]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[page]),
            _ScriptedResult(rowcount=0),
        ]
    )
    finalization = _RecordingFinalization(session.events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="previous contribution"):
        await store.apply_results(SCOPE, request)

    assert session.rolled_back is True
    assert session.added == []
    assert finalization.requests == []
    assert page.version == 4


@pytest.mark.asyncio
async def test_modern_finalization_failure_rolls_back_and_stops_pending_delete() -> (
    None
):
    token = uuid4()
    pending = _pending(claimed=True)
    pending.claim_token = token
    page = _page(sources=[pending.knowledge_id])
    reduced = ReducedPage(
        slug=page.slug,
        title="最终写入前失败",
        page_type="entity",
        content="无链接正文",
        summary=page.summary,
        source_refs=[pending.knowledge_id],
        contributor_op_ids=[pending.id],
    )
    request = _batch_request(
        claim_token=token,
        pages=(reduced,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=pending.id,
                action="add",
                slug=page.slug,
                knowledge_id=pending.knowledge_id,
                previous=None,
                current=_stored_contribution(version=pending.op_version),
            ),
        ),
        completed_op_ids=(pending.id,),
        expected_pages=(
            PageExpectation(slug=page.slug, page_id=page.id, version=page.version),
        ),
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(),
            _ScriptedResult(rows=[pending]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[page]),
            _ScriptedResult(),
            _ScriptedResult(),
        ]
    )
    finalization = _RecordingFinalization(session.events, release_ok=False)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="finalization"):
        await store.apply_results(SCOPE, request)

    assert session.rolled_back is True
    assert len(finalization.requests) == 1
    assert not any(
        _sql(statement).startswith("DELETE FROM wiki_pending_ops")
        for statement in session.statements
    )
    assert not any(
        _sql(statement).startswith("INSERT INTO task_outbox")
        for statement in session.statements
    )


@pytest.mark.asyncio
async def test_ingest_and_retract_share_scope_lock_before_reads_across_sources() -> (
    None
):
    ingest_sessions: list[_ScriptedSession] = []
    ingest_calls = []
    for version in ("version-1", "version-2"):
        pending = _pending(knowledge_id="knowledge-a", version=version)
        session = _ScriptedSession(
            [
                _ScriptedResult(rows=[]),
                _ScriptedResult(scalar=pending.id),
                _ScriptedResult(),
                _ScriptedResult(scalar=pending),
                _ScriptedResult(scalar=_outbox()),
            ]
        )
        store = SqlAlchemyIngestStore(
            _OneSessionFactory(session), _RecordingFinalization(session.events)
        )  # type: ignore[arg-type]
        knowledge = _knowledge(version).model_copy(update={"id": "knowledge-a"})
        ingest_sessions.append(session)
        ingest_calls.append(
            store.enqueue_ingest(
                SCOPE,
                knowledge,
                {"knowledge_id": "knowledge-a"},
                delay_seconds=0,
            )
        )

    retract_pending = _pending(
        knowledge_id="knowledge-b", op="retract", version="delete-1"
    )
    retract_session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(scalar=retract_pending.id),
            _ScriptedResult(),
            _ScriptedResult(scalar=retract_pending),
            _ScriptedResult(scalar=_outbox()),
        ]
    )
    retract_store = SqlAlchemyIngestStore(
        _OneSessionFactory(retract_session),
        _RecordingFinalization(retract_session.events),
    )  # type: ignore[arg-type]

    await asyncio.gather(
        *ingest_calls,
        retract_store.enqueue_retract(
            SCOPE,
            "knowledge-b",
            "delete-1",
            {"knowledge_id": "knowledge-b"},
            delay_seconds=0,
        ),
    )

    sessions = [*ingest_sessions, retract_session]
    first_sql = [_sql(session.statements[0]) for session in sessions]
    assert all("pg_advisory_xact_lock" in sql for sql in first_sql)
    lock_values = [
        tuple(
            session.statements[0].compile(dialect=postgresql.dialect()).params.values()
        )
        for session in sessions
    ]
    assert lock_values[0] == lock_values[1] == lock_values[2]
    assert all(
        _sql(session.statements[1]).startswith("SELECT wiki_pending_ops.")
        for session in sessions
    )


@pytest.mark.asyncio
async def test_enqueue_ingest_releases_and_deletes_only_old_unclaimed_versions() -> (
    None
):
    old = _pending(version="version-1")
    new = _pending(version="version-2")
    outbox = _outbox()
    events: list[str] = []
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[old]),
            _ScriptedResult(),
            _ScriptedResult(scalar=new.id),
            _ScriptedResult(),
            _ScriptedResult(scalar=new),
            _ScriptedResult(scalar=outbox),
        ],
        events,
    )
    finalization = _RecordingFinalization(events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    result = await store.enqueue_ingest(
        SCOPE, _knowledge(), {"knowledge_id": "knowledge-1"}, delay_seconds=0
    )

    assert result.id == new.id and result.deduplicated is False
    first_sql = _sql(session.statements[0])
    assert "pg_advisory_xact_lock" in first_sql
    pending_sql = _sql(session.statements[1])
    assert "wiki_pending_ops.claimed_at IS NULL" in pending_sql
    assert "wiki_pending_ops.op_version !=" in pending_sql
    assert "FOR UPDATE" in pending_sql
    delete_sql = _sql(session.statements[2])
    assert delete_sql.startswith("DELETE FROM wiki_pending_ops")
    assert "wiki_pending_ops.claimed_at IS NULL" in delete_sql
    assert [(kind, request.attempt) for kind, request in finalization.requests] == [
        ("release", "version-1"),
        ("register", "version-2"),
    ]
    assert events.index("finalization.release:version-1") < events.index(
        "finalization.register:version-2"
    )


@pytest.mark.asyncio
async def test_enqueue_ingest_duplicate_does_not_insert_release_or_emit_outbox() -> (
    None
):
    pending = _pending(version="version-2")
    outbox = _outbox()
    events: list[str] = []
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=outbox),
        ],
        events,
    )
    finalization = _RecordingFinalization(events, registered=False)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    result = await store.enqueue(
        SCOPE, _knowledge(), {"knowledge_id": "knowledge-1"}, delay_seconds=0
    )

    assert result.id == pending.id and result.deduplicated is True
    assert not any(sql.startswith("INSERT INTO wiki_pending_ops") for sql in events)
    assert not any(sql.startswith("INSERT INTO task_outbox") for sql in events)
    assert [kind for kind, _request in finalization.requests] == ["register"]


@pytest.mark.asyncio
async def test_enqueue_ingest_release_failure_rolls_back_before_new_insert() -> None:
    events: list[str] = []
    session = _ScriptedSession([_ScriptedResult(rows=[_pending()])], events)
    finalization = _RecordingFinalization(events, release_ok=False)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="finalization"):
        await store.enqueue_ingest(SCOPE, _knowledge(), {"knowledge_id": "knowledge-1"})

    assert session.rolled_back is True
    assert len(session.statements) == 2
    assert "pg_advisory_xact_lock" in _sql(session.statements[0])
    assert _sql(session.statements[1]).startswith("SELECT wiki_pending_ops.")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload,delay",
    [({"knowledge_id": "knowledge-1"}, True), ([], 0), ({}, 0)],
)
async def test_enqueue_ingest_rejects_invalid_payload_and_delay_before_session(
    payload, delay
) -> None:
    class ExplodingFactory:
        def __call__(self):
            raise AssertionError("invalid boundary must not open a session")

    store = SqlAlchemyIngestStore(ExplodingFactory(), SqlFinalizationPort())  # type: ignore[arg-type]
    with pytest.raises((TypeError, ValueError)):
        await store.enqueue_ingest(SCOPE, _knowledge(), payload, delay_seconds=delay)


@pytest.mark.asyncio
async def test_enqueue_retract_removes_only_target_refs_and_keeps_visible_page() -> (
    None
):
    target = _contribution(knowledge_id="knowledge-1", refs=["chunk:a"])
    remaining = _contribution(knowledge_id="knowledge-2", refs=["chunk:b"])
    page = _page(sources=["knowledge-1", "knowledge-2"], chunks=["chunk:a", "chunk:b"])
    pending = _pending(op="retract", version="delete-1")
    outbox = _outbox()
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[target]),
            _ScriptedResult(scalar=page),
            _ScriptedResult(rows=[remaining]),
            _ScriptedResult(scalar=pending.id),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=outbox),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization(session.events)
    )  # type: ignore[arg-type]
    before = (page.title, page.content, page.summary, list(page.aliases), page.status)

    result = await store.enqueue_retract(
        SCOPE,
        "knowledge-1",
        "delete-1",
        {"knowledge_id": "knowledge-1"},
        delay_seconds=0,
    )

    assert result.id == pending.id and result.deduplicated is False
    assert target.state == "retract_pending"
    assert page.source_refs == ["knowledge-2"]
    assert page.chunk_refs == ["chunk:b"]
    assert page.version == 5 and page.deleted_at is None
    assert (page.title, page.content, page.summary, page.aliases, page.status) == before
    assert not any("wiki_links" in _sql(statement) for statement in session.statements)
    first_page_select = next(
        index
        for index, event in enumerate(session.events)
        if event.startswith("SELECT wiki_pages.")
    )
    assert session.events.index("FLUSH") < first_page_select


@pytest.mark.asyncio
async def test_enqueue_retract_unique_source_soft_deletes_and_clears_links() -> None:
    target = _contribution(knowledge_id="knowledge-1", refs=["chunk:a"])
    page = _page(sources=["knowledge-1"], chunks=["chunk:a"])
    pending = _pending(op="retract", version="delete-1")
    outbox = _outbox()
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[target]),
            _ScriptedResult(scalar=page),
            _ScriptedResult(rows=[]),
            _ScriptedResult(),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending.id),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=outbox),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization(session.events)
    )  # type: ignore[arg-type]

    await store.enqueue_retract(
        SCOPE,
        "knowledge-1",
        "delete-1",
        {"knowledge_id": "knowledge-1"},
        delay_seconds=0,
    )

    assert target.state == "retract_pending"
    assert page.deleted_at is not None
    assert page.source_refs == [] and page.chunk_refs == []
    assert page.version == 5
    link_sql = [
        _sql(statement)
        for statement in session.statements
        if "wiki_links" in _sql(statement)
    ]
    assert link_sql[0].startswith("DELETE FROM wiki_links")
    assert link_sql[1].startswith("UPDATE wiki_links")
    assert all("wiki_links.tenant_id" in sql for sql in link_sql)


@pytest.mark.asyncio
async def test_enqueue_retract_repeat_and_no_contributions_only_return_pending() -> (
    None
):
    pending = _pending(op="retract", version="delete-1")
    outbox = _outbox()
    events: list[str] = []
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=outbox),
        ],
        events,
    )
    finalization = _RecordingFinalization(events, registered=False)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    result = await store.enqueue_retract(
        SCOPE, "knowledge-1", "delete-1", {"knowledge_id": "knowledge-1"}
    )

    assert result.id == pending.id and result.deduplicated is True
    assert "pg_advisory_xact_lock" in _sql(session.statements[0])
    assert len(session.statements) == 5
    assert not any(sql.startswith("INSERT") for sql in events)


def test_finalization_identity_distinguishes_retract_from_ingest() -> None:
    ingest = _finalization_request(SCOPE, "knowledge-1", "version-1", "ingest")
    retract = _finalization_request(SCOPE, "knowledge-1", "version-1", "retract")

    assert ingest.subtask_name == "wiki"
    assert retract.subtask_name == "wiki-retract"
    assert ingest != retract


@pytest.mark.asyncio
async def test_enqueue_retract_keeps_claimed_ingest_and_releases_unclaimed() -> None:
    unclaimed = _pending(version="version-1")
    pending = _pending(op="retract", version="delete-1")
    outbox = _outbox()
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[unclaimed]),
            _ScriptedResult(),
            _ScriptedResult(rows=[]),
            _ScriptedResult(scalar=pending.id),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=outbox),
        ]
    )
    finalization = _RecordingFinalization(session.events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    await store.enqueue_retract(
        SCOPE, "knowledge-1", "delete-1", {"knowledge_id": "knowledge-1"}
    )

    select_sql = next(
        _sql(statement)
        for statement in session.statements
        if _sql(statement).startswith("SELECT wiki_pending_ops.")
    )
    assert "wiki_pending_ops.claimed_at IS NULL" in select_sql
    assert "FOR UPDATE" in select_sql
    assert ("release", "version-1") in [
        (kind, request.attempt) for kind, request in finalization.requests
    ]
    assert any(
        _sql(statement).startswith("DELETE FROM wiki_pending_ops")
        for statement in session.statements
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["missing", "scope", "type", "refs"])
async def test_enqueue_retract_dirty_or_missing_page_rolls_back(problem: str) -> None:
    target = _contribution(knowledge_id="knowledge-1")
    page = _page(sources=["knowledge-1"], chunks=["chunk:knowledge-1"])
    if problem == "scope":
        page.tenant_id = SCOPE.tenant_id + 1
    elif problem == "type":
        page.page_type = "concept"
    elif problem == "refs":
        page.source_refs = ["polluted"]
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[target]),
            _ScriptedResult(scalar=None if problem == "missing" else page),
            _ScriptedResult(rows=[]),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization(session.events)
    )  # type: ignore[arg-type]

    with pytest.raises(InvariantError):
        await store.enqueue_retract(
            SCOPE, "knowledge-1", "delete-1", {"knowledge_id": "knowledge-1"}
        )

    assert session.rolled_back is True


@pytest.mark.asyncio
async def test_enqueue_retract_locks_slugs_in_stable_order() -> None:
    second = _contribution(knowledge_id="knowledge-1", slug="entity/z")
    first = _contribution(knowledge_id="knowledge-1", slug="entity/a")
    page_a = _page(
        slug="entity/a", sources=["knowledge-1"], chunks=["chunk:knowledge-1"]
    )
    page_z = _page(
        slug="entity/z", sources=["knowledge-1"], chunks=["chunk:knowledge-1"]
    )
    pending = _pending(op="retract", version="delete-1")
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[second, first]),
            _ScriptedResult(scalar=page_a),
            _ScriptedResult(rows=[]),
            _ScriptedResult(),
            _ScriptedResult(),
            _ScriptedResult(scalar=page_z),
            _ScriptedResult(rows=[]),
            _ScriptedResult(),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending.id),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=_outbox()),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization(session.events)
    )  # type: ignore[arg-type]

    await store.enqueue_retract(
        SCOPE, "knowledge-1", "delete-1", {"knowledge_id": "knowledge-1"}
    )

    page_selects = [
        statement
        for statement in session.statements
        if _sql(statement).startswith("SELECT wiki_pages.")
    ]
    assert [
        statement.compile(dialect=postgresql.dialect()).params["slug_1"]
        for statement in page_selects
    ] == [
        "entity/a",
        "entity/z",
    ]
