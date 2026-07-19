from dataclasses import FrozenInstanceError
from uuid import UUID
import warnings

import pytest
from pydantic import ValidationError

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.schemas import (
    AllowedFolderBase,
    ContributionDelta,
    StoredContributionRecord,
    TaxonomyDecision,
    TaxonomyOutput,
    TaxonomyRequest,
    TaxonomyTopic,
)
from app.wiki.ingest.taxonomy import (
    TaxonomyWorkItem,
    build_taxonomy_work_items,
    recover_taxonomy_output,
)


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
OP_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
OP_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
OP_C = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
BASE_ID = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")


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
