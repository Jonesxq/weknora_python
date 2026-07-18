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
    assert len(prepare_citation_batches([chunk("one", "x" * 12)], max_chars=12)) == 1
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


def test_classify_runs_all_batches_with_concurrency_limit() -> None:
    model = ScriptedModel({index: ["yield"] for index in range(9)})
    refs, _ = asyncio.run(classify_citations(
        knowledge_id="k", chunks=[chunk(str(i), "x") for i in range(9)], candidates=[candidate()],
        model=model, max_chars=1, max_parallel=4,
    ))
    assert model.peak <= 4
    assert len(model.requests) == 9
    assert refs["entity/acme"] == [str(i) for i in range(9)]
    with pytest.raises(ValueError):
        asyncio.run(classify_citations(knowledge_id="k", chunks=[], candidates=[], model=model, max_parallel=0))


def test_classify_rejects_unknown_exceptions_after_cleaning_siblings() -> None:
    model = ScriptedModel({0: [RuntimeError("programming error")], 1: ["yield"]})
    with pytest.raises(RuntimeError, match="programming error"):
        asyncio.run(classify_citations(
            knowledge_id="k", chunks=[chunk("a", "a"), chunk("b", "b")], candidates=[candidate()],
            model=model, max_chars=1,
        ))
