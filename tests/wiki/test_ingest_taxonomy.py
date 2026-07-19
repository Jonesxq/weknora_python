import asyncio
import math
from dataclasses import FrozenInstanceError
from uuid import UUID
import warnings

import pytest
from pydantic import ValidationError

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.schemas import (
    AllowedFolderBase,
    ContributionDelta,
    EmbeddingOutput,
    EmbeddingRequest,
    FolderAssignment,
    FolderCatalogEntry,
    StoredContributionRecord,
    TaxonomyDecision,
    TaxonomyOutput,
    TaxonomyRequest,
    TaxonomyTopic,
)
from app.wiki.ingest.taxonomy import (
    TaxonomyWorkItem,
    build_folder_assignment,
    build_taxonomy_requests,
    build_taxonomy_work_items,
    cosine_similarity,
    recover_taxonomy_output,
    select_allowed_bases,
)


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
OP_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
OP_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
OP_C = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
BASE_ID = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
ROOT_ALPHA_ID = UUID("10000000-0000-0000-0000-000000000001")
ROOT_BETA_ID = UUID("20000000-0000-0000-0000-000000000001")
ALPHA_APPS_ID = UUID("10000000-0000-0000-0000-000000000002")
ALPHA_CLOUD_ID = UUID("10000000-0000-0000-0000-000000000003")
BETA_SERVICES_ID = UUID("20000000-0000-0000-0000-000000000002")


class RecordingEmbedding:
    def __init__(self, response: object) -> None:
        self.response = response
        self.requests: list[EmbeddingRequest] = []

    async def embed(self, request: EmbeddingRequest) -> EmbeddingOutput:
        snapshot = EmbeddingRequest.model_validate(
            request.model_dump(mode="python", warnings="error")
        )
        self.requests.append(snapshot)
        if isinstance(self.response, BaseException):
            raise self.response
        if callable(self.response):
            return self.response(snapshot)  # type: ignore[no-any-return]
        return self.response  # type: ignore[return-value]


def _folders() -> tuple[FolderCatalogEntry, ...]:
    return (
        FolderCatalogEntry(
            id=ROOT_ALPHA_ID,
            parent_id=None,
            name="Alpha",
            path="/Alpha",
            depth=1,
        ),
        FolderCatalogEntry(
            id=ROOT_BETA_ID,
            parent_id=None,
            name="Beta",
            path="/Beta",
            depth=1,
        ),
        FolderCatalogEntry(
            id=ALPHA_APPS_ID,
            parent_id=ROOT_ALPHA_ID,
            name="Apps",
            path="/Alpha/Apps",
            depth=2,
        ),
        FolderCatalogEntry(
            id=ALPHA_CLOUD_ID,
            parent_id=ALPHA_APPS_ID,
            name="Cloud",
            path="/Alpha/Apps/Cloud",
            depth=3,
        ),
        FolderCatalogEntry(
            id=BETA_SERVICES_ID,
            parent_id=ROOT_BETA_ID,
            name="Services",
            path="/Beta/Services",
            depth=2,
        ),
    )


def _topic(slug: str = "entity/acme", title: str = "Acme") -> TaxonomyTopic:
    return TaxonomyTopic(
        slug=slug,
        title=title,
        page_type=slug.split("/", maxsplit=1)[0],
        summary="Summary",
    )


def _work_items(count: int) -> tuple[TaxonomyWorkItem, ...]:
    return tuple(
        TaxonomyWorkItem(
            topic=_topic(f"concept/topic-{index:03d}", f"Topic {index:03d}"),
            contributor_op_ids=(OP_A,),
        )
        for index in range(count)
    )


def _vectors(values: dict[str, tuple[float, ...]]) -> object:
    def response(request: EmbeddingRequest) -> EmbeddingOutput:
        return EmbeddingOutput(
            vectors={item.key: values[item.key] for item in request.items}
        )

    return response


def _add_delta(
    *,
    slug: str = "entity/acme",
    knowledge_id: str = "knowledge-a",
    op_version: str = "v1",
    pending_op_id: UUID = OP_A,
    page_type: str = "entity",
    title: str = "Acme",
    summary: str = "Summary",
) -> ContributionDelta:
    current = StoredContributionRecord(
        tenant_id=1,
        knowledge_base_id=KB_ID,
        slug=slug,
        knowledge_id=knowledge_id,
        op_version=op_version,
        page_type=page_type,
        state="active",
        title=title,
        content="正文",
        summary=summary,
    )
    return ContributionDelta(
        pending_op_id=pending_op_id,
        action="add",
        slug=slug,
        knowledge_id=knowledge_id,
        previous=None,
        current=current,
    )


def _request(*topics: TaxonomyTopic, allowed_bases: tuple[AllowedFolderBase, ...] = ()) -> TaxonomyRequest:
    return TaxonomyRequest(topics=topics, allowed_bases=allowed_bases)


def _invalid_output(request: TaxonomyRequest, output: TaxonomyOutput, message: str) -> None:
    with pytest.raises(WikiValidationError, match=message) as exc_info:
        recover_taxonomy_output(request, output)
    assert exc_info.value.code == "TAXONOMY_OUTPUT_INVALID"


def test_build_taxonomy_work_items_is_stable_and_summarizes_contributors() -> None:
    first = _add_delta(knowledge_id="knowledge-a", summary=" Summary ", pending_op_id=OP_B)
    second = _add_delta(
        knowledge_id="knowledge-b",
        summary="Second summary",
        pending_op_id=OP_A,
        title="Later title",
    )

    items = build_taxonomy_work_items(
        [second, first], classifiable_slugs=("entity/acme",)
    )

    assert len(items) == 1
    assert isinstance(items[0], TaxonomyWorkItem)
    assert items[0].topic == TaxonomyTopic(
        slug="entity/acme",
        title="Acme",
        page_type="entity",
        summary="Summary\n\nSecond summary",
    )
    assert items[0].contributor_op_ids == (OP_A, OP_B)
    assert items[0].__dataclass_params__.frozen is True
    with pytest.raises(FrozenInstanceError):
        items[0].topic = items[0].topic  # type: ignore[misc]


def test_build_taxonomy_work_items_filters_and_has_stable_topic_order() -> None:
    ignored_summary = _add_delta(
        slug="summary/ignored", page_type="summary", title="Ignored", summary="Ignored"
    )
    ignored_slug = _add_delta(slug="entity/ignored", title="Ignored")
    no_current = ContributionDelta(
        pending_op_id=OP_A,
        action="retract",
        slug="entity/acme",
        knowledge_id="knowledge-a",
        previous=_add_delta().current.model_copy(update={"state": "retract_pending"}),
        current=None,
    )
    beta = _add_delta(slug="concept/beta", page_type="concept", title="Beta")
    alpha = _add_delta(slug="entity/alpha", title="Alpha")

    items = build_taxonomy_work_items(
        (ignored_summary, beta, no_current, ignored_slug, alpha),
        classifiable_slugs={"concept/beta", "entity/alpha"},
    )

    assert [item.topic.slug for item in items] == ["concept/beta", "entity/alpha"]


def test_build_taxonomy_work_items_deduplicates_truncates_and_tie_breaks() -> None:
    tied_later = _add_delta(
        knowledge_id="same",
        op_version="v1",
        pending_op_id=OP_C,
        title="Later",
        summary="  repeated  ",
    )
    tied_first = _add_delta(
        knowledge_id="same",
        op_version="v1",
        pending_op_id=OP_B,
        title="First",
        summary="repeated",
    )
    long = _add_delta(
        knowledge_id="z",
        summary="x" * 4000,
        pending_op_id=OP_B,
    )

    items = build_taxonomy_work_items(
        [tied_later, long, tied_first], classifiable_slugs=["entity/acme"]
    )

    assert items[0].topic.title == "First"
    assert items[0].topic.summary == ("repeated\n\n" + "x" * 4000)[:4000]
    assert items[0].contributor_op_ids == (OP_B, OP_C)


def test_build_taxonomy_work_items_ignores_empty_summaries_and_input_container_order() -> None:
    first = _add_delta(knowledge_id="a", summary="   ")
    second = _add_delta(knowledge_id="b", summary="Summary", pending_op_id=OP_B)

    list_items = build_taxonomy_work_items([second, first], classifiable_slugs=["entity/acme"])
    tuple_items = build_taxonomy_work_items((first, second), classifiable_slugs=("entity/acme",))

    assert list_items == tuple_items
    assert list_items[0].topic.summary == "Summary"


def test_build_taxonomy_work_items_stabilizes_fully_tied_identity_records() -> None:
    later = _add_delta(
        knowledge_id="same",
        op_version="v1",
        pending_op_id=OP_A,
        title="Zulu",
        summary=" Zulu summary ",
    )
    first = _add_delta(
        knowledge_id="same",
        op_version="v1",
        pending_op_id=OP_A,
        title="Alpha",
        summary="Alpha summary",
    )

    forward = build_taxonomy_work_items(
        [later, first], classifiable_slugs=("entity/acme",)
    )
    reversed_items = build_taxonomy_work_items(
        [first, later], classifiable_slugs=("entity/acme",)
    )

    assert forward == reversed_items
    assert forward[0].topic.title == "Alpha"
    assert forward[0].topic.summary == "Alpha summary\n\nZulu summary"


def test_build_taxonomy_requests_sorts_and_batches_all_topics() -> None:
    work_items = _work_items(61)
    bases = [
        AllowedFolderBase(id=ROOT_BETA_ID, path="/Beta", depth=1),
        AllowedFolderBase(id=ROOT_ALPHA_ID, path="/Alpha", depth=1),
    ]

    requests = build_taxonomy_requests(reversed(work_items), bases, batch_size=60)
    bases.clear()

    assert isinstance(requests, tuple)
    assert [len(request.topics) for request in requests] == [60, 1]
    assert [
        topic.slug for request in requests for topic in request.topics
    ] == [item.topic.slug for item in work_items]
    assert [request.allowed_bases for request in requests] == [
        (
            AllowedFolderBase(id=ROOT_BETA_ID, path="/Beta", depth=1),
            AllowedFolderBase(id=ROOT_ALPHA_ID, path="/Alpha", depth=1),
        ),
        (
            AllowedFolderBase(id=ROOT_BETA_ID, path="/Beta", depth=1),
            AllowedFolderBase(id=ROOT_ALPHA_ID, path="/Alpha", depth=1),
        ),
    ]
    assert requests[0].allowed_bases[0] is not requests[1].allowed_bases[0]
    with pytest.raises(ValidationError, match="frozen"):
        requests[0].topics = ()  # type: ignore[misc]


@pytest.mark.parametrize("batch_size", [1, 60])
def test_build_taxonomy_requests_accepts_batch_size_boundaries(batch_size: int) -> None:
    requests = build_taxonomy_requests(_work_items(1), (), batch_size=batch_size)

    assert len(requests) == 1
    assert requests[0].topics[0].slug == "concept/topic-000"


@pytest.mark.parametrize("batch_size", [0, 61, True, False])
def test_build_taxonomy_requests_rejects_invalid_batch_size(batch_size: int) -> None:
    with pytest.raises(ValueError):
        build_taxonomy_requests(_work_items(1), (), batch_size=batch_size)


def test_build_taxonomy_requests_handles_empty_and_rejects_duplicate_slugs() -> None:
    assert build_taxonomy_requests((), (), batch_size=60) == ()
    duplicate = TaxonomyWorkItem(
        topic=_topic("concept/topic-000", "Duplicate"), contributor_op_ids=(OP_B,)
    )

    with pytest.raises(ValueError, match="slug"):
        build_taxonomy_requests((*_work_items(1), duplicate), (), batch_size=60)


def test_build_folder_assignment_snapshots_root_decision_and_contributors() -> None:
    work_item = TaxonomyWorkItem(
        topic=_topic("concept/root", "Root"), contributor_op_ids=(OP_B, OP_A)
    )
    decision = TaxonomyDecision(slug="concept/root", new_segments=("General",))

    assignment = build_folder_assignment(work_item, decision, {})

    assert assignment == FolderAssignment(
        slug="concept/root",
        contributor_op_ids=(OP_B, OP_A),
        base_folder_id=None,
        base_path=None,
        base_depth=0,
        new_segments=("General",),
    )
    assert assignment.new_segments is not decision.new_segments


def test_build_folder_assignment_snapshots_base_and_contributors() -> None:
    work_item = TaxonomyWorkItem(
        topic=_topic("concept/cloud", "Cloud"), contributor_op_ids=(OP_A, OP_B)
    )
    base = AllowedFolderBase(id=BASE_ID, path="/Root/Products", depth=2)
    decision = TaxonomyDecision(
        slug="concept/cloud", base_folder_id=BASE_ID, new_segments=("Cloud",)
    )

    assignment = build_folder_assignment(work_item, decision, {BASE_ID: base})

    assert assignment.base_folder_id == BASE_ID
    assert assignment.base_path == "/Root/Products"
    assert assignment.base_depth == 2
    assert assignment.contributor_op_ids == (OP_A, OP_B)
    assert assignment.new_segments == ("Cloud",)
    assert assignment is not decision
    assert assignment.new_segments is not decision.new_segments


@pytest.mark.parametrize(
    ("decision", "allowed_by_id", "message"),
    [
        (TaxonomyDecision(slug="concept/other"), {}, "slug不一致"),
        (
            TaxonomyDecision(slug="concept/root", base_folder_id=BASE_ID),
            {},
            "白名单",
        ),
        (
            TaxonomyDecision(slug="concept/root", base_folder_id=BASE_ID),
            {
                BASE_ID: AllowedFolderBase(
                    id=ROOT_ALPHA_ID, path="/Alpha", depth=1
                )
            },
            "白名单",
        ),
    ],
)
def test_build_folder_assignment_rejects_mismatched_or_untrusted_decisions(
    decision: TaxonomyDecision,
    allowed_by_id: dict[UUID, AllowedFolderBase],
    message: str,
) -> None:
    work_item = TaxonomyWorkItem(
        topic=_topic("concept/root", "Root"), contributor_op_ids=(OP_A,)
    )

    with pytest.raises(WikiValidationError, match=message) as exc_info:
        build_folder_assignment(work_item, decision, allowed_by_id)

    assert exc_info.value.code == "TAXONOMY_OUTPUT_INVALID"


def test_build_folder_assignment_normalizes_final_dto_invariants() -> None:
    deep_work_item = TaxonomyWorkItem(
        topic=_topic("concept/root", "Root"), contributor_op_ids=(OP_A,)
    )
    base = AllowedFolderBase(id=BASE_ID, path="/One/Two/Three", depth=3)
    deep_decision = TaxonomyDecision(
        slug="concept/root", base_folder_id=BASE_ID, new_segments=("Four",)
    )

    with pytest.raises(WikiValidationError, match="目录总深度") as exc_info:
        build_folder_assignment(deep_work_item, deep_decision, {BASE_ID: base})
    assert exc_info.value.code == "TAXONOMY_OUTPUT_INVALID"

    empty_contributor_work_item = TaxonomyWorkItem(
        topic=_topic("concept/root", "Root"), contributor_op_ids=()
    )
    root_decision = TaxonomyDecision(slug="concept/root")
    with pytest.raises(WikiValidationError, match="contributor") as exc_info:
        build_folder_assignment(empty_contributor_work_item, root_decision, {})
    assert exc_info.value.code == "TAXONOMY_OUTPUT_INVALID"


def test_recover_taxonomy_output_requires_complete_exact_coverage() -> None:
    request = _request(
        TaxonomyTopic(slug="entity/acme", title="Acme", page_type="entity"),
        TaxonomyTopic(slug="concept/beta", title="Beta", page_type="concept"),
    )

    _invalid_output(request, TaxonomyOutput(), "完整且恰好覆盖")
    _invalid_output(
        request,
        TaxonomyOutput(decisions=(TaxonomyDecision(slug="entity/acme"),)),
        "完整且恰好覆盖",
    )
    _invalid_output(
        request,
        TaxonomyOutput(
            decisions=(
                TaxonomyDecision(slug="entity/acme"),
                TaxonomyDecision(slug="concept/unknown"),
            )
        ),
        "完整且恰好覆盖",
    )


def test_recover_taxonomy_output_rejects_non_whitelisted_base() -> None:
    request = _request(TaxonomyTopic(slug="entity/acme", title="Acme", page_type="entity"))
    output = TaxonomyOutput(
        decisions=(TaxonomyDecision(slug="entity/acme", base_folder_id=BASE_ID),)
    )

    _invalid_output(request, output, "白名单")


def test_recover_taxonomy_output_accepts_root_and_allowed_base_depth_three() -> None:
    root_request = _request(
        TaxonomyTopic(slug="entity/acme", title="Acme", page_type="entity")
    )
    root = recover_taxonomy_output(
        root_request, TaxonomyOutput(decisions=(TaxonomyDecision(slug="entity/acme"),))
    )
    assert root["entity/acme"].base_folder_id is None

    base = AllowedFolderBase(id=BASE_ID, path="/Root/Products", depth=2)
    based_request = _request(
        TaxonomyTopic(slug="entity/acme", title="Acme", page_type="entity"),
        allowed_bases=(base,),
    )
    based = recover_taxonomy_output(
        based_request,
        TaxonomyOutput(
            decisions=(
                TaxonomyDecision(
                    slug="entity/acme", base_folder_id=BASE_ID, new_segments=("Cloud",)
                ),
            )
        ),
    )
    assert based["entity/acme"].new_segments == ("Cloud",)


def test_recover_taxonomy_output_rejects_casefold_duplicate_at_base_boundary() -> None:
    base = AllowedFolderBase(id=BASE_ID, path="/Root", depth=1)
    request = _request(
        TaxonomyTopic(slug="entity/acme", title="Acme", page_type="entity"),
        allowed_bases=(base,),
    )
    output = TaxonomyOutput(
        decisions=(
            TaxonomyDecision(
                slug="entity/acme",
                base_folder_id=BASE_ID,
                new_segments=("root",),
            ),
        )
    )

    _invalid_output(request, output, "相邻")


def test_recover_taxonomy_output_rejects_excess_depth_and_wiki_path() -> None:
    base = AllowedFolderBase(id=BASE_ID, path="/One/Two/Three", depth=3)
    deep_request = _request(
        TaxonomyTopic(slug="entity/acme", title="Acme", page_type="entity"),
        allowed_bases=(base,),
    )
    _invalid_output(
        deep_request,
        TaxonomyOutput(
            decisions=(
                TaxonomyDecision(slug="entity/acme", base_folder_id=BASE_ID, new_segments=("Four",)),
            )
        ),
        "目录总深度",
    )

    slug = "entity/" + "s" * 248
    path_request = _request(
        TaxonomyTopic(slug=slug, title="Acme", page_type="entity"),
        allowed_bases=(AllowedFolderBase(id=BASE_ID, path="/" + "a" * 512, depth=1),),
    )
    accepted = recover_taxonomy_output(
        path_request,
        TaxonomyOutput(
            decisions=(
                TaxonomyDecision(slug=slug, base_folder_id=BASE_ID, new_segments=("b" * 254,)),
            )
        ),
    )
    assert len(f"/{'a' * 512}/{'b' * 254}/{slug}") == 1024
    assert accepted[slug].new_segments == ("b" * 254,)
    _invalid_output(
        path_request,
        TaxonomyOutput(
            decisions=(
                TaxonomyDecision(slug=slug, base_folder_id=BASE_ID, new_segments=("b" * 255,)),
            )
        ),
        "wiki_path",
    )


def test_recover_taxonomy_output_rebuilds_strict_snapshots() -> None:
    topics = [TaxonomyTopic(slug="entity/acme", title="Acme", page_type="entity")]
    decisions = [TaxonomyDecision(slug="entity/acme")]
    request = TaxonomyRequest(topics=topics)
    output = TaxonomyOutput(decisions=decisions)
    topics.clear()
    decisions.clear()

    recovered = recover_taxonomy_output(request, output)

    assert recovered == {"entity/acme": TaxonomyDecision(slug="entity/acme")}
    assert isinstance(recovered["entity/acme"], TaxonomyDecision)
    assert recovered["entity/acme"] is not output.decisions[0]
    assert recovered["entity/acme"] == output.decisions[0]
    with pytest.raises(ValidationError, match="frozen"):
        recovered["entity/acme"].slug = "entity/other"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("request_value", "output_value"),
    [
        (None, "valid"),
        (object(), "valid"),
        ("valid", None),
        ("valid", object()),
    ],
)
def test_recover_taxonomy_output_normalizes_wrong_boundary_types(
    request_value: object, output_value: object
) -> None:
    request = _request(
        TaxonomyTopic(slug="entity/acme", title="Acme", page_type="entity")
    )
    output = TaxonomyOutput(
        decisions=(TaxonomyDecision(slug="entity/acme"),)
    )

    with pytest.raises(
        WikiValidationError, match="taxonomy 请求或输出结构无效"
    ) as exc_info:
        recover_taxonomy_output(
            request if request_value == "valid" else request_value,  # type: ignore[arg-type]
            output if output_value == "valid" else output_value,  # type: ignore[arg-type]
        )

    assert exc_info.value.code == "TAXONOMY_OUTPUT_INVALID"


@pytest.mark.parametrize("polluted_field", ["decisions", "topics", "allowed_bases"])
def test_recover_taxonomy_output_normalizes_polluted_constructs_without_warnings(
    polluted_field: str,
) -> None:
    topic = TaxonomyTopic(slug="entity/acme", title="Acme", page_type="entity")
    request = _request(topic)
    output = TaxonomyOutput(decisions=(TaxonomyDecision(slug=topic.slug),))
    if polluted_field == "decisions":
        output = TaxonomyOutput.model_construct(decisions=object())
    elif polluted_field == "topics":
        request = TaxonomyRequest.model_construct(
            topics=object(), allowed_bases=()
        )
    else:
        request = TaxonomyRequest.model_construct(
            topics=(topic,), allowed_bases=object()
        )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(
            WikiValidationError, match="taxonomy 请求或输出结构无效"
        ) as exc_info:
            recover_taxonomy_output(request, output)

    assert exc_info.value.code == "TAXONOMY_OUTPUT_INVALID"
    assert caught == []


def test_recover_taxonomy_output_does_not_hide_programming_value_errors() -> None:
    class BrokenTaxonomyOutput(TaxonomyOutput):
        def model_dump(self, *args: object, **kwargs: object) -> dict[str, object]:
            raise ValueError("programming error")

    request = _request(
        TaxonomyTopic(slug="entity/acme", title="Acme", page_type="entity")
    )
    output = BrokenTaxonomyOutput(
        decisions=(TaxonomyDecision(slug="entity/acme"),)
    )

    with pytest.raises(ValueError, match="programming error"):
        recover_taxonomy_output(request, output)


@pytest.mark.asyncio
async def test_select_allowed_bases_returns_small_catalog_without_embedding() -> None:
    embedding = RecordingEmbedding(object())

    selected = await select_allowed_bases(
        (_topic(),),
        tuple(reversed(_folders())),
        embedding,
        full_catalog_limit=5,
        related_limit=1,
    )

    assert [(base.depth, base.path, base.id) for base in selected] == [
        (1, "/Alpha", ROOT_ALPHA_ID),
        (1, "/Beta", ROOT_BETA_ID),
        (2, "/Alpha/Apps", ALPHA_APPS_ID),
        (2, "/Beta/Services", BETA_SERVICES_ID),
        (3, "/Alpha/Apps/Cloud", ALPHA_CLOUD_ID),
    ]
    assert embedding.requests == []


@pytest.mark.asyncio
async def test_select_allowed_bases_keeps_roots_and_selected_ancestors() -> None:
    embedding = RecordingEmbedding(
        _vectors(
            {
                "topic:entity/acme": (1.0, 0.0),
                f"folder:{ALPHA_APPS_ID}": (0.7, 0.7),
                f"folder:{ALPHA_CLOUD_ID}": (0.99, 0.1),
                f"folder:{BETA_SERVICES_ID}": (0.1, 0.99),
            }
        )
    )

    selected = await select_allowed_bases(
        (_topic(),), _folders(), embedding, full_catalog_limit=2, related_limit=1
    )

    assert [base.id for base in selected] == [
        ROOT_ALPHA_ID,
        ROOT_BETA_ID,
        ALPHA_APPS_ID,
        ALPHA_CLOUD_ID,
    ]


@pytest.mark.asyncio
async def test_select_allowed_bases_tie_breaks_and_requests_are_input_order_independent() -> None:
    vectors = {
        "topic:concept/beta": (1.0, 0.0),
        "topic:entity/acme": (1.0, 0.0),
        f"folder:{ALPHA_APPS_ID}": (1.0, 0.0),
        f"folder:{ALPHA_CLOUD_ID}": (1.0, 0.0),
        f"folder:{BETA_SERVICES_ID}": (1.0, 0.0),
    }
    topics = (_topic("entity/acme"), _topic("concept/beta", "Beta"))
    first_embedding = RecordingEmbedding(_vectors(vectors))
    second_embedding = RecordingEmbedding(_vectors(vectors))

    first = await select_allowed_bases(
        topics, _folders(), first_embedding, full_catalog_limit=2, related_limit=2
    )
    second = await select_allowed_bases(
        tuple(reversed(topics)),
        tuple(reversed(_folders())),
        second_embedding,
        full_catalog_limit=2,
        related_limit=2,
    )

    assert first == second
    assert [base.id for base in first] == [
        ROOT_ALPHA_ID,
        ROOT_BETA_ID,
        ALPHA_APPS_ID,
        BETA_SERVICES_ID,
    ]
    assert [item.key for item in first_embedding.requests[0].items] == [
        "topic:concept/beta",
        "topic:entity/acme",
        f"folder:{ALPHA_APPS_ID}",
        f"folder:{BETA_SERVICES_ID}",
        f"folder:{ALPHA_CLOUD_ID}",
    ]
    assert first_embedding.requests == second_embedding.requests


@pytest.mark.asyncio
async def test_select_allowed_bases_preserves_nearby_score_order_before_path() -> None:
    embedding = RecordingEmbedding(
        _vectors(
            {
                "topic:entity/acme": (1.0, 0.0),
                f"folder:{ALPHA_APPS_ID}": (1.0, 2e-5),
                f"folder:{BETA_SERVICES_ID}": (1.0, 1e-5),
                f"folder:{ALPHA_CLOUD_ID}": (0.0, 1.0),
            }
        )
    )

    selected = await select_allowed_bases(
        (_topic(),), _folders(), embedding, full_catalog_limit=2, related_limit=1
    )

    assert [base.id for base in selected] == [
        ROOT_ALPHA_ID,
        ROOT_BETA_ID,
        BETA_SERVICES_ID,
    ]


def test_cosine_similarity_handles_zero_orthogonal_and_equal_vectors() -> None:
    assert cosine_similarity((0.0, 0.0), (1.0, 0.0)) == 0.0
    assert cosine_similarity((1.0, 0.0), (0.0, 1.0)) == 0.0
    assert cosine_similarity((1.0, 2.0), (1.0, 2.0)) == pytest.approx(1.0)


def test_cosine_similarity_preserves_distinct_near_one_scores() -> None:
    closer = cosine_similarity((1.0, 0.0), (1.0, 1e-5))
    farther = cosine_similarity((1.0, 0.0), (1.0, 2e-5))

    assert math.isfinite(closer)
    assert math.isfinite(farther)
    assert farther < closer <= 1.0


def test_cosine_similarity_is_stable_for_large_finite_vectors() -> None:
    vector = (1e308, 0.0)

    same = cosine_similarity(vector, vector)
    orthogonal = cosine_similarity(vector, (0.0, 1e308))
    opposite = cosine_similarity(vector, (-1e308, 0.0))

    assert math.isfinite(same)
    assert same == pytest.approx(1.0)
    assert orthogonal == pytest.approx(0.0)
    assert opposite == pytest.approx(-1.0)


def test_cosine_similarity_preserves_tiny_nonzero_vectors() -> None:
    similarity = cosine_similarity((1e-308, 0.0), (1e-308, 0.0))

    assert math.isfinite(similarity)
    assert similarity == pytest.approx(1.0)


def test_cosine_similarity_stays_bounded_for_mixed_scales() -> None:
    similarity = cosine_similarity(
        (1e308, 1.0, 1e-308),
        (1e308, -1.0, 1e-308),
    )

    assert math.isfinite(similarity)
    assert -1.0 <= similarity <= 1.0


@pytest.mark.parametrize(
    ("left", "right"),
    [((1.0,), (1.0, 0.0)), ((), (1.0,)), ((math.nan,), (1.0,)), ((math.inf,), (1.0,))],
)
def test_cosine_similarity_rejects_invalid_vectors(
    left: tuple[float, ...], right: tuple[float, ...]
) -> None:
    with pytest.raises(WikiValidationError) as exc_info:
        cosine_similarity(left, right)

    assert exc_info.value.code == "EMBEDDING_OUTPUT_INVALID"


@pytest.mark.asyncio
async def test_select_allowed_bases_scores_each_folder_against_best_topic() -> None:
    embedding = RecordingEmbedding(
        _vectors(
            {
                "topic:concept/beta": (0.0, 1.0),
                "topic:entity/acme": (1.0, 0.0),
                f"folder:{ALPHA_APPS_ID}": (0.2, 0.1),
                f"folder:{ALPHA_CLOUD_ID}": (0.1, 0.2),
                f"folder:{BETA_SERVICES_ID}": (0.0, 1.0),
            }
        )
    )

    selected = await select_allowed_bases(
        (_topic(), _topic("concept/beta", "Beta")),
        _folders(),
        embedding,
        full_catalog_limit=2,
        related_limit=1,
    )

    assert BETA_SERVICES_ID in {base.id for base in selected}


@pytest.mark.asyncio
async def test_select_allowed_bases_requests_only_topics_and_deep_folders_with_text() -> None:
    embedding = RecordingEmbedding(
        _vectors(
            {
                "topic:entity/acme": (1.0, 0.0),
                f"folder:{ALPHA_APPS_ID}": (1.0, 0.0),
                f"folder:{ALPHA_CLOUD_ID}": (1.0, 0.0),
                f"folder:{BETA_SERVICES_ID}": (1.0, 0.0),
            }
        )
    )

    await select_allowed_bases(
        (_topic(),), _folders(), embedding, full_catalog_limit=2, related_limit=3
    )

    assert [(item.key, item.text) for item in embedding.requests[0].items] == [
        ("topic:entity/acme", "Acme\nSummary"),
        (f"folder:{ALPHA_APPS_ID}", "/Alpha/Apps"),
        (f"folder:{BETA_SERVICES_ID}", "/Beta/Services"),
        (f"folder:{ALPHA_CLOUD_ID}", "/Alpha/Apps/Cloud"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["missing", "extra", "wrong_type", "polluted"])
async def test_select_allowed_bases_normalizes_invalid_model_outputs_without_warnings(
    mode: str,
) -> None:
    def invalid_response(request: EmbeddingRequest) -> object:
        keys = [item.key for item in request.items]
        vectors = {key: (1.0, 0.0) for key in keys}
        if mode == "missing":
            vectors.pop(keys[-1])
            return EmbeddingOutput(vectors=vectors)
        if mode == "extra":
            vectors["extra"] = (1.0, 0.0)
            return EmbeddingOutput(vectors=vectors)
        if mode == "wrong_type":
            return object()
        return EmbeddingOutput.model_construct(vectors=object())

    embedding = RecordingEmbedding(invalid_response)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(WikiValidationError) as exc_info:
            await select_allowed_bases(
                (_topic(),), _folders(), embedding, full_catalog_limit=2, related_limit=1
            )

    assert exc_info.value.code == "EMBEDDING_OUTPUT_INVALID"
    assert caught == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("transient"), ValueError("permanent")])
async def test_select_allowed_bases_propagates_model_failures(failure: Exception) -> None:
    with pytest.raises(type(failure), match=str(failure)):
        await select_allowed_bases(
            (_topic(),),
            _folders(),
            RecordingEmbedding(failure),
            full_catalog_limit=2,
            related_limit=1,
        )


@pytest.mark.asyncio
async def test_select_allowed_bases_propagates_cancellation() -> None:
    with pytest.raises(asyncio.CancelledError):
        await select_allowed_bases(
            (_topic(),),
            _folders(),
            RecordingEmbedding(asyncio.CancelledError()),
            full_catalog_limit=2,
            related_limit=1,
        )


@pytest.mark.asyncio
async def test_select_allowed_bases_handles_roots_only_and_invalid_arguments() -> None:
    roots = _folders()[:2]
    embedding = RecordingEmbedding(object())

    selected = await select_allowed_bases(
        (), roots, embedding, full_catalog_limit=1, related_limit=1
    )

    assert [base.id for base in selected] == [ROOT_ALPHA_ID, ROOT_BETA_ID]
    assert embedding.requests == []
    with pytest.raises(WikiValidationError) as exc_info:
        await select_allowed_bases(
            (), _folders(), embedding, full_catalog_limit=2, related_limit=1
        )
    assert exc_info.value.code == "EMBEDDING_OUTPUT_INVALID"
    for full_catalog_limit, related_limit in ((0, 1), (1, 0)):
        with pytest.raises(ValueError):
            await select_allowed_bases(
                (_topic(),),
                _folders(),
                embedding,
                full_catalog_limit=full_catalog_limit,
                related_limit=related_limit,
            )


@pytest.mark.asyncio
async def test_select_allowed_bases_rejects_incomplete_folder_tree() -> None:
    with pytest.raises(WikiValidationError) as exc_info:
        await select_allowed_bases(
            (_topic(),),
            (_folders()[2],),
            RecordingEmbedding(object()),
            full_catalog_limit=2,
            related_limit=1,
        )

    assert exc_info.value.code == "EMBEDDING_OUTPUT_INVALID"
