from __future__ import annotations

import asyncio

import pytest

from app.wiki.ingest.citations import classify_citations, prepare_citation_batches
from app.wiki.ingest.ports import PermanentModelError, TransientModelError
from app.wiki.ingest.schemas import CitationBatchOutput, SourceChunk, TopicCandidate


def chunk(identifier: str, text: str = "text", index: int = 0, start: int = 0) -> SourceChunk:
    return SourceChunk(id=identifier, text=text, chunk_index=index, start_at=start)


def candidate(slug: str = "entity/acme") -> TopicCandidate:
    return TopicCandidate(name="Acme", slug=slug, page_type=slug.split("/", 1)[0])


def test_prepare_batches_joins_content_sorts_and_does_not_mutate_inputs() -> None:
    chunks = [
        SourceChunk(id="later", chunk_index=2, text=" z ", ocr_text="ocr", image_caption="cap "),
        SourceChunk(id="first", chunk_index=1, start_at=3, text=" first "),
        SourceChunk(id="empty", chunk_index=0, text=" ", ocr_text="", image_caption=" "),
    ]
    before = [item.model_dump() for item in chunks]

    batches = prepare_citation_batches(chunks, max_chars=11)

    assert [[(piece.alias, piece.text) for piece in batch.chunks] for batch in batches] == [
        [("c000", "first")], [("c000", "z\nocr\ncap")]
    ]
    assert [dict(batch.alias_to_chunk_id) for batch in batches] == [{"c000": "first"}, {"c000": "later"}]
    assert [item.model_dump() for item in chunks] == before
    with pytest.raises(TypeError):
        batches[0].alias_to_chunk_id["c999"] = "other"  # type: ignore[index]


def test_prepare_batches_honors_boundary_splits_long_chunks_and_alias_limit() -> None:
    assert len(prepare_citation_batches([chunk("one", "x" * 12000)])) == 1
    assert [len(batch.chunks) for batch in prepare_citation_batches([chunk("one", "x" * 12001)])] == [1, 1]
    split = prepare_citation_batches([chunk("one", "abcdefgh")], max_chars=3)
    assert [[piece.text for piece in batch.chunks] for batch in split] == [["abc"], ["def"], ["gh"]]
    assert [dict(batch.alias_to_chunk_id) for batch in split] == [{"c000": "one"}] * 3

    many = prepare_citation_batches([chunk(f"id-{i}", "x") for i in range(1001)], max_chars=5000)
    assert [len(batch.chunks) for batch in many] == [1000, 1]
    assert many[0].chunks[-1].alias == "c999"
    assert many[1].chunks[0].alias == "c000"
    with pytest.raises(ValueError):
        prepare_citation_batches([], max_chars=0)


def test_prepare_batches_empty_input_is_empty() -> None:
    assert prepare_citation_batches([]) == []


@pytest.mark.parametrize(("text", "max_chars", "expected"), [
    ("a    b", 2, ["a", "b"]),
    ("a  b", 1, ["a", "b"]),
])
def test_prepare_batches_skips_whitespace_only_slices(text: str, max_chars: int, expected: list[str]) -> None:
    batches = prepare_citation_batches([chunk("one", text)], max_chars=max_chars)
    assert [piece.text for batch in batches for piece in batch.chunks] == expected
    assert [piece.alias for batch in batches for piece in batch.chunks] == ["c000", "c000"]


class ScriptedModel:
    def __init__(self, responses: dict[int, list[object]]) -> None:
        self.responses = responses
        self.requests = []
        self.active = 0
        self.peak = 0

    async def classify_chunks(self, request):  # type: ignore[no-untyped-def]
        self.requests.append(request)
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            response = self.responses[request.batch_index].pop(0)
            if isinstance(response, BaseException):
                raise response
            if response == "yield":
                await asyncio.sleep(0.01)
                return CitationBatchOutput(refs_by_slug={"entity/acme": (request.chunks[0].alias,)})
            return response
        finally:
            self.active -= 1


def test_classify_restores_ids_validates_current_batch_and_source_order() -> None:
    model = ScriptedModel({
        0: [CitationBatchOutput(refs_by_slug={"entity/acme": ("c001", "c000")})],
        1: [CitationBatchOutput(refs_by_slug={"entity/acme": ("c000",)})],
    })
    chunks = [chunk("b", "bbb", 1), chunk("a", "aaa", 0), chunk("c", "ccc", 2)]

    refs, supplements = asyncio.run(classify_citations(
        knowledge_id="knowledge", chunks=chunks, candidates=[candidate()], model=model, max_chars=6
    ))

    assert refs == {"entity/acme": ["a", "b", "c"]}
    assert supplements == []
    assert [[part.alias for part in request.chunks] for request in model.requests] == [["c000", "c001"], ["c000"]]


def test_classify_degrades_only_invalid_or_permanent_batches() -> None:
    model = ScriptedModel({
        0: [CitationBatchOutput(refs_by_slug={"entity/acme": ("c999",)})],
        1: [PermanentModelError("bad")],
        2: [CitationBatchOutput(refs_by_slug={"entity/acme": ("c000",)})],
    })
    refs, supplements = asyncio.run(classify_citations(
        knowledge_id="k", chunks=[chunk("a", "a"), chunk("b", "b"), chunk("c", "c")],
        candidates=[candidate()], model=model, max_chars=1,
    ))
    assert refs == {"entity/acme": ["c"]}
    assert supplements == []


def test_classify_rejects_alias_that_only_exists_in_another_batch() -> None:
    model = ScriptedModel({
        0: [CitationBatchOutput(refs_by_slug={"entity/acme": ("c000", "c001")})],
        1: [CitationBatchOutput(refs_by_slug={"entity/acme": ("c001",)})],
    })
    refs, supplements = asyncio.run(classify_citations(
        knowledge_id="k", chunks=[chunk("a", "a"), chunk("b", "b"), chunk("c", "cc")],
        candidates=[candidate()], model=model, max_chars=2,
    ))
    assert [[part.alias for part in request.chunks] for request in model.requests] == [["c000", "c001"], ["c000"]]
    assert refs == {"entity/acme": ["a", "b"]}
    assert supplements == []


def test_classify_rejects_cross_batch_supplement_conflicts() -> None:
    first = candidate("concept/extra")
    conflicting = TopicCandidate(name="Other", slug="concept/extra", page_type="concept")
    model = ScriptedModel({
        0: [CitationBatchOutput(refs_by_slug={"concept/extra": ("c000",)}, supplemental_candidates=(first,))],
        1: [CitationBatchOutput(refs_by_slug={"concept/extra": ("c000",)}, supplemental_candidates=(conflicting,))],
    })
    refs, supplements = asyncio.run(classify_citations(
        knowledge_id="k", chunks=[chunk("a", "a"), chunk("b", "b")],
        candidates=[candidate()], model=model, max_chars=1,
    ))
    assert refs == {"concept/extra": ["a"]}
    assert [item.slug for item in supplements] == ["concept/extra"]


def test_classify_deduplicates_real_id_across_split_batches_in_source_order() -> None:
    model = ScriptedModel({
        0: [CitationBatchOutput(refs_by_slug={"entity/acme": ("c000",)})],
        1: [CitationBatchOutput(refs_by_slug={"entity/acme": ("c000",)})],
        2: [CitationBatchOutput(refs_by_slug={"entity/acme": ("c000",)})],
    })
    refs, _ = asyncio.run(classify_citations(
        knowledge_id="k", chunks=[chunk("same-id", "abcdef", 0), chunk("later", "xyz", 1)],
        candidates=[candidate()], model=model, max_chars=3,
    ))
    assert refs == {"entity/acme": ["same-id", "later"]}
    assert all(not value.startswith("c") for value in refs["entity/acme"])


def test_classify_retries_transient_twice_and_merges_supplements_stably() -> None:
    extra = candidate("concept/extra")
    model = ScriptedModel({
        0: [TransientModelError("one"), TransientModelError("two"), CitationBatchOutput(
            refs_by_slug={"concept/extra": ("c000",)}, supplemental_candidates=(extra,)
        )],
        1: [CitationBatchOutput(supplemental_candidates=(extra,))],
    })
    waits: list[int] = []

    async def wait(seconds: int) -> None:
        waits.append(seconds)

    refs, supplements = asyncio.run(classify_citations(
        knowledge_id="k", chunks=[chunk("a", "a"), chunk("b", "b")], candidates=[candidate()],
        model=model, max_chars=1, retry_wait=wait,
    ))
    assert waits == [2, 4]
    assert refs == {"concept/extra": ["a"]}
    assert [item.slug for item in supplements] == ["concept/extra"]


def test_classify_transient_exhaustion_and_all_permanent_failures_return_empty() -> None:
    model = ScriptedModel({
        0: [TransientModelError("one"), TransientModelError("two"), TransientModelError("three")],
        1: [PermanentModelError("bad")],
    })
    waits: list[int] = []

    async def wait(seconds: int) -> None:
        waits.append(seconds)

    result = asyncio.run(classify_citations(
        knowledge_id="k", chunks=[chunk("a", "a"), chunk("b", "b")], candidates=[candidate()],
        model=model, max_chars=1, retry_wait=wait,
    ))
    assert result == ({}, [])
    assert waits == [2, 4]


def test_classify_degrades_batch_with_conflicting_duplicate_supplements() -> None:
    first = candidate("concept/extra")
    conflicting = TopicCandidate(name="Different", slug="concept/extra", page_type="concept")
    model = ScriptedModel({
        0: [CitationBatchOutput(refs_by_slug={"concept/extra": ("c000",)}, supplemental_candidates=(first, conflicting))]
    })

    refs, supplements = asyncio.run(classify_citations(
        knowledge_id="k", chunks=[chunk("a", "a")], candidates=[candidate()], model=model, max_chars=1,
    ))

    assert refs == {}
    assert supplements == []


def test_classify_runs_all_batches_with_bounded_workers_and_concurrency(monkeypatch: pytest.MonkeyPatch) -> None:
    class BarrierModel:
        def __init__(self) -> None:
            self.entered = asyncio.Event()
            self.release = asyncio.Event()
            self.requests = []
            self.active = 0
            self.peak = 0

        async def classify_chunks(self, request):  # type: ignore[no-untyped-def]
            self.requests.append(request)
            self.active += 1
            self.peak = max(self.peak, self.active)
            if len(self.requests) == 4:
                self.entered.set()
            try:
                await self.release.wait()
                return CitationBatchOutput(refs_by_slug={"entity/acme": (request.chunks[0].alias,)})
            finally:
                self.active -= 1

    async def exercise() -> tuple[BarrierModel, int]:
        import app.wiki.ingest.citations as citations

        model = BarrierModel()
        created = 0
        original = asyncio.create_task

        def count_workers(coro):  # type: ignore[no-untyped-def]
            nonlocal created
            created += 1
            return original(coro)

        monkeypatch.setattr(citations.asyncio, "create_task", count_workers)
        operation = original(classify_citations(
            knowledge_id="k", chunks=[chunk(str(i), "x", i) for i in range(1000)], candidates=[candidate()],
            model=model, max_chars=1, max_parallel=4,
        ))
        try:
            try:
                await asyncio.wait_for(model.entered.wait(), timeout=1)
            except TimeoutError:
                pytest.fail("citation workers did not enter the four-call barrier within one second")
            assert model.peak == 4
            assert len(model.requests) == 4
            assert created == 4
            model.release.set()
            try:
                refs, _ = await asyncio.wait_for(operation, timeout=2)
            except TimeoutError:
                pytest.fail("citation workers did not finish all batches within two seconds after release")
            assert refs["entity/acme"] == [str(i) for i in range(1000)]
            return model, created
        finally:
            model.release.set()
            if not operation.done():
                operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)

    model, created = asyncio.run(exercise())
    assert model.peak <= 4
    assert len(model.requests) == 1000
    assert created == 4
    with pytest.raises(ValueError):
        asyncio.run(classify_citations(knowledge_id="k", chunks=[], candidates=[], model=model, max_parallel=0))


def test_classify_rejects_unknown_exceptions_after_cleaning_siblings() -> None:
    model = ScriptedModel({0: [RuntimeError("programming error")], 1: ["yield"]})
    with pytest.raises(RuntimeError, match="programming error"):
        asyncio.run(classify_citations(
            knowledge_id="k", chunks=[chunk("a", "a"), chunk("b", "b")], candidates=[candidate()],
            model=model, max_chars=1,
        ))


@pytest.mark.parametrize("error", [ValueError("bad value"), TypeError("bad type"), RuntimeError("bad runtime")])
def test_classify_propagates_unknown_model_exceptions(error: BaseException) -> None:
    model = ScriptedModel({0: [error], 1: ["yield"]})
    with pytest.raises(type(error), match=str(error)):
        asyncio.run(classify_citations(
            knowledge_id="k", chunks=[chunk("a", "a"), chunk("b", "b")], candidates=[candidate()],
            model=model, max_chars=1,
        ))
    assert model.active == 0


def test_classify_propagates_child_cancellation_and_collects_siblings() -> None:
    class SelfCancellingModel:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.start_count = 0
            self.active = 0
            self.sibling_cancelled = False

        async def classify_chunks(self, request):  # type: ignore[no-untyped-def]
            self.active += 1
            self.start_count += 1
            if self.start_count == 2:
                self.started.set()
            try:
                await self.release.wait()
                if request.batch_index == 0:
                    raise asyncio.CancelledError()
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                if request.batch_index == 1:
                    self.sibling_cancelled = True
                raise
            finally:
                self.active -= 1

    async def assert_cancelled_while_loop_is_running() -> None:
        model = SelfCancellingModel()
        operation = asyncio.create_task(classify_citations(
            knowledge_id="k", chunks=[chunk("a", "a"), chunk("b", "b")], candidates=[candidate()],
            model=model, max_chars=1,
        ))
        await model.started.wait()
        model.release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert model.active == 0
        assert model.sibling_cancelled
        assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()]

    asyncio.run(assert_cancelled_while_loop_is_running())


def test_classify_merges_reversed_completion_in_batch_order() -> None:
    first = candidate("concept/first")
    duplicate = candidate("concept/first")
    second = candidate("concept/second")

    class ReversedModel:
        def __init__(self) -> None:
            self.later_done = asyncio.Event()

        async def classify_chunks(self, request):  # type: ignore[no-untyped-def]
            if request.batch_index == 0:
                await self.later_done.wait()
                return CitationBatchOutput(supplemental_candidates=(first,))
            self.later_done.set()
            return CitationBatchOutput(supplemental_candidates=(duplicate, second))

    _, supplements = asyncio.run(classify_citations(
        knowledge_id="k", chunks=[chunk("a", "a"), chunk("b", "b")], candidates=[candidate()],
        model=ReversedModel(), max_chars=1,
    ))
    assert [item.slug for item in supplements] == ["concept/first", "concept/second"]


def test_classify_keeps_inputs_model_output_and_return_containers_isolated() -> None:
    sources = [chunk("source-id", "a")]
    candidates = [candidate()]
    output = CitationBatchOutput(
        refs_by_slug={"entity/acme": ("c000",)},
        supplemental_candidates=(candidate("concept/extra"),),
    )
    source_before = [item.model_dump() for item in sources]
    candidate_before = [item.model_dump() for item in candidates]
    output_before = output.model_dump()
    model = ScriptedModel({0: [output, output]})

    refs, supplements = asyncio.run(classify_citations(
        knowledge_id="k", chunks=sources, candidates=candidates, model=model, max_chars=1,
    ))
    assert type(refs) is dict
    assert type(refs["entity/acme"]) is list
    assert type(supplements) is list
    assert [item.model_dump() for item in sources] == source_before
    assert [item.model_dump() for item in candidates] == candidate_before
    assert output.model_dump() == output_before
    assert all(not hasattr(part, "chunk_id") for part in model.requests[0].chunks)
    assert "source-id" not in str(model.requests[0].model_dump())

    refs["entity/acme"].append("forged")
    refs["forged"] = ["source-id"]
    supplements[0].aliases.append("mutated")
    supplements[0].details = "mutated"
    again, again_supplements = asyncio.run(classify_citations(
        knowledge_id="k", chunks=sources, candidates=candidates, model=model, max_chars=1,
    ))
    assert again == {"entity/acme": ["source-id"]}
    assert again_supplements[0].aliases == []
    assert again_supplements[0].details == ""
    assert [item.model_dump() for item in sources] == source_before
    assert output.model_dump() == output_before


def test_classify_propagates_parent_cancellation_and_collects_siblings() -> None:
    class BlockingModel:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.active = 0
            self.cancelled = 0

        async def classify_chunks(self, request):  # type: ignore[no-untyped-def]
            self.active += 1
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled += 1
                raise
            finally:
                self.active -= 1

    async def cancel_parent() -> BlockingModel:
        model = BlockingModel()
        task = asyncio.create_task(classify_citations(
            knowledge_id="k", chunks=[chunk("a", "a"), chunk("b", "b")], candidates=[candidate()],
            model=model, max_chars=1,
        ))
        await model.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return model

    model = asyncio.run(cancel_parent())
    assert model.active == 0
    assert model.cancelled >= 1
