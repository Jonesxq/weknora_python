from __future__ import annotations

from uuid import UUID

import pytest

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.reduce_slug import reduce_slug
from app.wiki.ingest.schemas import (
    PageMergeOutput,
    PageMergeRequest,
    ReducedPage,
    SlugUpdate,
)


OP_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
OP_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


class RecordingModel:
    def __init__(self) -> None:
        self.merge_requests: list[PageMergeRequest] = []
        self.error: Exception | None = None

    async def merge_page(self, request: PageMergeRequest) -> PageMergeOutput:
        self.merge_requests.append(request.model_copy(deep=True))
        if self.error is not None:
            raise self.error
        return PageMergeOutput(headline="合并后的 Acme", markdown="合并后的正文")


def topic_update(
    *,
    pending_op_id: UUID = OP_A,
    knowledge_id: str = "knowledge-a",
    slug: str = "entity/acme",
    page_type: str = "entity",
    title: str = "Acme",
    content: str = "本次贡献正文",
    summary: str = "本次贡献摘要",
    aliases: list[str] | None = None,
    source_refs: list[str] | None = None,
    chunk_refs: list[str] | None = None,
) -> SlugUpdate:
    return SlugUpdate(
        pending_op_id=pending_op_id,
        knowledge_id=knowledge_id,
        slug=slug,
        title=title,
        page_type=page_type,
        content=content,
        summary=summary,
        aliases=aliases or [],
        source_refs=source_refs or [knowledge_id],
        chunk_refs=chunk_refs or [],
    )


def existing_page(**overrides: object) -> ReducedPage:
    values: dict[str, object] = {
        "slug": "entity/acme",
        "title": "旧 Acme",
        "page_type": "entity",
        "content": "旧正文",
        "summary": "旧摘要",
        "aliases": ["旧别名"],
        "source_refs": ["knowledge-old"],
        "chunk_refs": ["chunk-old"],
        "contributor_op_ids": [UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")],
    }
    values.update(overrides)
    return ReducedPage.model_validate(values)


@pytest.mark.asyncio
async def test_reduce_summary_replaces_page_without_model_call() -> None:
    model = RecordingModel()
    update = SlugUpdate(
        pending_op_id=OP_A,
        knowledge_id="knowledge-a",
        slug="summary/knowledge-a",
        title="文档 A 摘要",
        page_type="summary",
        content="完整摘要正文",
        summary="一句话摘要",
        aliases=["应被忽略"],
        source_refs=["错误来源", "knowledge-a"],
        chunk_refs=["应被忽略"],
    )

    page = await reduce_slug(update.slug, [update], None, model)

    assert page == ReducedPage(
        slug="summary/knowledge-a",
        title="文档 A 摘要",
        page_type="summary",
        content="完整摘要正文",
        summary="一句话摘要",
        aliases=[],
        source_refs=["knowledge-a"],
        chunk_refs=[],
        contributor_op_ids=[OP_A],
    )
    assert model.merge_requests == []


@pytest.mark.asyncio
async def test_reduce_topic_merges_two_contributions_once() -> None:
    model = RecordingModel()
    updates = [
        topic_update(
            aliases=["Acme Inc.", "共同别名"],
            source_refs=["knowledge-a"],
            chunk_refs=["chunk-a"],
        ),
        topic_update(
            pending_op_id=OP_B,
            knowledge_id="knowledge-b",
            title="ACME",
            content="第二份正文",
            summary="第二份摘要",
            aliases=["共同别名", "ACME Corp."],
            source_refs=["knowledge-b"],
            chunk_refs=["chunk-b"],
        ),
    ]

    page = await reduce_slug("entity/acme", updates, None, model)

    assert len(model.merge_requests) == 1
    request = model.merge_requests[0]
    assert (request.slug, request.title, request.page_type) == (
        "entity/acme",
        "Acme",
        "entity",
    )
    assert [item.pending_op_id for item in request.contributions] == [OP_A, OP_B]
    assert [item.knowledge_id for item in request.contributions] == [
        "knowledge-a",
        "knowledge-b",
    ]
    assert page.title == "合并后的 Acme"
    assert page.content == "合并后的正文"
    assert page.summary == "本次贡献摘要\n\n第二份摘要"
    assert page.aliases == ["Acme Inc.", "共同别名", "ACME Corp."]
    assert page.source_refs == ["knowledge-a", "knowledge-b"]
    assert page.chunk_refs == ["chunk-a", "chunk-b"]
    assert page.contributor_op_ids == [OP_A, OP_B]


@pytest.mark.asyncio
async def test_reduce_topic_puts_batch_metadata_before_existing_page() -> None:
    model = RecordingModel()
    update_a = topic_update(
        aliases=[" 新别名 ", "重复别名", ""],
        source_refs=["knowledge-a", "knowledge-a", " "],
        chunk_refs=["chunk-a", "chunk-shared"],
    )
    update_b = topic_update(
        pending_op_id=OP_B,
        knowledge_id="knowledge-b",
        aliases=["重复别名", "第二别名"],
        source_refs=["knowledge-b", "knowledge-a"],
        chunk_refs=["chunk-shared", "chunk-b"],
        summary="本次贡献摘要",
    )
    old = existing_page(
        aliases=["重复别名", "旧别名", " "],
        source_refs=["knowledge-a", "knowledge-old", ""],
        chunk_refs=["chunk-b", "chunk-old"],
    )

    page = await reduce_slug("entity/acme", [update_a, update_b], old, model)

    request = model.merge_requests[0]
    assert request.aliases == ["新别名", "重复别名", "第二别名", "旧别名"]
    assert request.existing_content == "旧正文"
    assert request.existing_summary == "旧摘要"
    assert page.aliases == request.aliases
    assert page.source_refs == ["knowledge-a", "knowledge-b", "knowledge-old"]
    assert page.chunk_refs == ["chunk-a", "chunk-shared", "chunk-b", "chunk-old"]
    assert page.summary == "本次贡献摘要\n\n旧摘要"
    assert page.contributor_op_ids == [OP_A, OP_B]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("slug", "updates", "message"),
    [
        ("entity/acme", [], "不能为空"),
        (
            "entity/acme",
            [topic_update(slug="entity/other")],
            "slug",
        ),
        (
            "entity/acme",
            [topic_update(), topic_update(page_type="concept", slug="concept/acme")],
            "slug",
        ),
    ],
)
async def test_reduce_rejects_empty_or_mismatched_updates(
    slug: str, updates: list[SlugUpdate], message: str
) -> None:
    with pytest.raises(WikiValidationError, match=message):
        await reduce_slug(slug, updates, None, RecordingModel())


@pytest.mark.asyncio
async def test_reduce_rejects_prefix_type_mismatch_even_for_constructed_input() -> None:
    invalid = SlugUpdate.model_construct(
        pending_op_id=OP_A,
        knowledge_id="knowledge-a",
        slug="entity/acme",
        title="Acme",
        page_type="concept",
        content="正文",
        summary="摘要",
        aliases=[],
        source_refs=[],
        chunk_refs=[],
    )

    with pytest.raises(WikiValidationError, match="类型"):
        await reduce_slug("entity/acme", [invalid], None, RecordingModel())


@pytest.mark.asyncio
async def test_reduce_rejects_mixed_topic_types_for_same_constructed_slug() -> None:
    concept = topic_update().model_copy(deep=True)
    object.__setattr__(concept, "page_type", "concept")

    with pytest.raises(WikiValidationError, match="类型"):
        await reduce_slug(
            "entity/acme", [topic_update(), concept], None, RecordingModel()
        )


@pytest.mark.asyncio
async def test_reduce_rejects_multiple_summary_updates() -> None:
    first = SlugUpdate(
        pending_op_id=OP_A,
        knowledge_id="knowledge-a",
        slug="summary/knowledge-a",
        title="摘要",
        page_type="summary",
    )
    second = first.model_copy(update={"pending_op_id": OP_B})

    with pytest.raises(WikiValidationError, match="恰好一个"):
        await reduce_slug(first.slug, [first, second], None, RecordingModel())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old",
    [
        existing_page(slug="entity/other"),
        existing_page(
            slug="concept/acme",
            page_type="concept",
        ),
    ],
)
async def test_reduce_rejects_existing_page_mismatch(old: ReducedPage) -> None:
    with pytest.raises(WikiValidationError, match="已有页面"):
        await reduce_slug("entity/acme", [topic_update()], old, RecordingModel())


@pytest.mark.asyncio
async def test_reduce_propagates_model_error_unchanged() -> None:
    expected = RuntimeError("merge failed")
    model = RecordingModel()
    model.error = expected

    with pytest.raises(RuntimeError) as caught:
        await reduce_slug("entity/acme", [topic_update()], None, model)

    assert caught.value is expected
    assert len(model.merge_requests) == 1


@pytest.mark.asyncio
async def test_reduce_does_not_modify_inputs() -> None:
    model = RecordingModel()
    updates = [
        topic_update(
            aliases=[" A ", "A"],
            source_refs=[" knowledge-a ", "knowledge-a"],
            chunk_refs=[" chunk-a ", "chunk-a"],
        )
    ]
    old = existing_page()
    updates_before = [item.model_dump() for item in updates]
    old_before = old.model_dump()

    await reduce_slug("entity/acme", updates, old, model)

    assert [item.model_dump() for item in updates] == updates_before
    assert old.model_dump() == old_before
