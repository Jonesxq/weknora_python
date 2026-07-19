from __future__ import annotations

import copy
from collections.abc import ItemsView
import json
import math
import pickle
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from app.wiki.ingest.schemas import (
    AllowedFolderBase,
    BatchApplyRequest,
    BatchResult,
    CandidateExtraction,
    CitationBatchChunk,
    CitationBatchOutput,
    CitationBatchRequest,
    ContributionDelta,
    DedupCandidateRequest,
    DedupDecision,
    DedupOutput,
    DedupPageCandidate,
    DedupRequest,
    DocumentSummary,
    EmbeddingItem,
    EmbeddingOutput,
    EmbeddingRequest,
    FinalizationRequest,
    FolderAssignment,
    FolderCatalogEntry,
    MapDocumentResult,
    PageContribution,
    PageMergeOutput,
    PageMergeRequest,
    OperationFailure,
    PageExpectation,
    ReducedPage,
    SlugUpdate,
    SourceChunk,
    SourceKnowledge,
    StoredContributionRecord,
    TaxonomyDecision,
    TaxonomyContext,
    TaxonomyOutput,
    TaxonomyRequest,
    TaxonomyTopic,
    TopicCandidate,
    WikiIngestConfig,
    WikiWorkerOptions,
)
from app.wiki.scope import WikiScope


KB_ID = UUID("11111111-1111-1111-1111-111111111111")


def test_ingest_config_and_worker_options_use_safe_defaults() -> None:
    config = WikiIngestConfig()
    options = WikiWorkerOptions()

    assert config.model_dump() == {
        "wiki_enabled": True,
        "synthesis_model_id": None,
        "summary_model_id": None,
        "extraction_granularity": "standard",
        "max_pages_per_ingest": 0,
    }
    assert options.model_dump() == {
        "batch_size": 5,
        "map_parallel": 10,
        "reduce_parallel": 10,
        "claim_timeout_seconds": 600,
        "max_pages_per_ingest": 0,
        "extraction_granularity": "standard",
        "citation_batch_chars": 12000,
        "citation_parallel": 4,
        "dedup_candidate_limit": 20,
        "tombstone_ttl_seconds": 3600,
        "taxonomy_topic_batch_size": 60,
        "taxonomy_parallel": 4,
        "taxonomy_full_catalog_limit": 120,
        "taxonomy_related_folder_limit": 40,
    }


def test_phase_four_a_options_defaults_and_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE", "40")
    monkeypatch.setenv("GRAPH_WIKI_TAXONOMY_PARALLEL", "3")
    monkeypatch.setenv("GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT", "80")
    monkeypatch.setenv("GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT", "24")

    options = WikiWorkerOptions.from_env()

    assert options.taxonomy_topic_batch_size == 40
    assert options.taxonomy_parallel == 3
    assert options.taxonomy_full_catalog_limit == 80
    assert options.taxonomy_related_folder_limit == 24


def test_embedding_output_is_complete_finite_and_deeply_immutable() -> None:
    request = EmbeddingRequest(
        items=(EmbeddingItem(key="topic:entity/acme", text="Acme"),)
    )
    output = EmbeddingOutput(vectors={"topic:entity/acme": (1.0, 0.0)})

    assert tuple(output.vectors) == ("topic:entity/acme",)
    with pytest.raises(TypeError):
        output.vectors["topic:entity/acme"] = (0.0, 1.0)  # type: ignore[index]
    with pytest.raises(ValidationError):
        EmbeddingOutput(vectors={"topic:entity/acme": (math.nan, 0.0)})
    assert request.items[0].key == "topic:entity/acme"
    assert output.model_dump() == {"vectors": {"topic:entity/acme": (1.0, 0.0)}}


def test_taxonomy_request_and_assignment_require_canonical_identities() -> None:
    folder_id = uuid4()
    op_id = uuid4()
    base = AllowedFolderBase(id=folder_id, path="/Organizations", depth=1)
    request = TaxonomyRequest(
        topics=(
            TaxonomyTopic(
                slug="entity/acme",
                title="Acme",
                page_type="entity",
                summary="Organization",
            ),
        ),
        allowed_bases=(base,),
    )
    output = TaxonomyOutput(
        decisions=(
            TaxonomyDecision(
                slug="entity/acme",
                base_folder_id=folder_id,
                new_segments=("Products",),
            ),
        )
    )
    assignment = FolderAssignment(
        slug="entity/acme",
        contributor_op_ids=(op_id,),
        base_folder_id=folder_id,
        base_path="/Organizations",
        base_depth=1,
        new_segments=("Products",),
    )

    assert request.topics[0].slug == output.decisions[0].slug == assignment.slug
    with pytest.raises(ValidationError):
        FolderAssignment(
            slug="entity/acme",
            contributor_op_ids=(op_id,),
            base_folder_id=None,
            base_path="/forged",
            base_depth=0,
        )


def test_folder_catalog_rejects_invalid_path_depth_and_name() -> None:
    with pytest.raises(ValidationError):
        FolderCatalogEntry(
            id=uuid4(), parent_id=None, name="bad/name", path="/bad/name", depth=1
        )
    with pytest.raises(ValidationError, match="相邻"):
        TaxonomyDecision(
            slug="entity/acme", new_segments=("Products", "products")
        )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE", "0"),
        ("GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE", "61"),
        ("GRAPH_WIKI_TAXONOMY_PARALLEL", "0"),
        ("GRAPH_WIKI_TAXONOMY_PARALLEL", "17"),
        ("GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT", "0"),
        ("GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT", "5001"),
        ("GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT", "0"),
        ("GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT", "501"),
    ],
)
def test_phase_four_a_options_reject_out_of_range_environment(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValidationError):
        WikiWorkerOptions.from_env()


def test_embedding_output_normalizes_keys_and_rejects_normalized_collisions() -> None:
    output = EmbeddingOutput(vectors={" a ": (1.0, 0.0)})

    assert tuple(output.vectors) == ("a",)
    for invalid_key in ("", " a,b "):
        with pytest.raises(ValidationError):
            EmbeddingOutput(vectors={invalid_key: (1.0, 0.0)})
    with pytest.raises(ValidationError):
        EmbeddingOutput(vectors={"a": (1.0, 0.0), " a ": (0.0, 1.0)})


def test_embedding_output_round_trips_and_rejects_invalid_vector_shapes() -> None:
    output = EmbeddingOutput(
        vectors={"first": (1.0, 0.0), "second": (0.0, 1.0)}
    )

    copied = copy.deepcopy(output)
    restored = EmbeddingOutput.model_validate(output.model_dump())

    assert copied is not output
    assert copied.vectors is not output.vectors
    assert restored == output
    with pytest.raises(ValidationError):
        EmbeddingOutput(vectors={"first": (1.0,), "second": (0.0, 1.0)})
    with pytest.raises(ValidationError):
        EmbeddingOutput(vectors={"first": (math.inf, 0.0)})


def test_taxonomy_context_requires_a_complete_unique_catalog_tree() -> None:
    root_id, child_id, grandchild_id = uuid4(), uuid4(), uuid4()
    root = FolderCatalogEntry(
        id=root_id, parent_id=None, name="Organizations", path="/Organizations", depth=1
    )
    child = FolderCatalogEntry(
        id=child_id,
        parent_id=root_id,
        name="Products",
        path="/Organizations/Products",
        depth=2,
    )
    grandchild = FolderCatalogEntry(
        id=grandchild_id,
        parent_id=child_id,
        name="Catalog",
        path="/Organizations/Products/Catalog",
        depth=3,
    )

    context = TaxonomyContext(
        folders=(root, child, grandchild), classifiable_slugs=(" ENTITY/acme ",)
    )

    assert context.classifiable_slugs == ("entity/acme",)
    with pytest.raises(ValidationError):
        TaxonomyContext(folders=(root, grandchild))
    with pytest.raises(ValidationError):
        TaxonomyContext(folders=(root, root))
    duplicate_path = child.model_copy(update={"id": uuid4()})
    with pytest.raises(ValidationError):
        TaxonomyContext(folders=(root, child, duplicate_path))


def test_folder_paths_must_be_canonical_and_names_reject_unicode_controls() -> None:
    with pytest.raises(ValidationError):
        FolderCatalogEntry(
            id=uuid4(), parent_id=None, name="Catalog", path="/ Catalog", depth=1
        )
    with pytest.raises(ValidationError):
        AllowedFolderBase(id=uuid4(), path="/Catalog ", depth=1)
    with pytest.raises(ValidationError):
        FolderCatalogEntry(
            id=uuid4(), parent_id=None, name="good\u0085name", path="/good\u0085name", depth=1
        )
    folder = FolderCatalogEntry(
        id=uuid4(), parent_id=None, name="产品", path="/产品", depth=1
    )

    assert folder.path == "/产品"
    with pytest.raises(ValidationError):
        FolderCatalogEntry(
            id=uuid4(), parent_id=None, name="Catalog", path="/Catalog/Child", depth=1
        )


def test_folder_assignment_keeps_none_root_and_validates_base_segment_boundary() -> None:
    op_id = uuid4()
    root_assignment = FolderAssignment(
        slug="entity/acme",
        contributor_op_ids=(op_id,),
        base_folder_id=None,
        base_path=None,
        base_depth=0,
    )

    assert root_assignment.base_path is None
    assert root_assignment.wiki_path == "/entity/acme"
    with pytest.raises(ValidationError):
        FolderAssignment(
            slug="entity/acme",
            contributor_op_ids=(op_id,),
            base_folder_id=uuid4(),
            base_path="/Products ",
            base_depth=1,
        )
    with pytest.raises(ValidationError, match="相邻"):
        FolderAssignment(
            slug="entity/acme",
            contributor_op_ids=(op_id,),
            base_folder_id=uuid4(),
            base_path="/Products",
            base_depth=1,
            new_segments=("products",),
        )


def test_batch_apply_request_round_trips_folder_assignments() -> None:
    assignment = FolderAssignment(
        slug="entity/acme",
        contributor_op_ids=(uuid4(),),
        base_folder_id=None,
        base_path=None,
        base_depth=0,
    )
    request = BatchApplyRequest(
        claim_token=uuid4(),
        pages=(),
        contribution_deltas=(),
        completed_op_ids=(),
        superseded_op_ids=(),
        failures=(),
        expected_pages=(),
        operation_id=uuid4(),
        folder_assignments=(assignment,),
    )

    assert BatchApplyRequest.model_validate(request.model_dump()) == request


def test_folder_assignment_root_identity_requires_none_base_path() -> None:
    assignment = FolderAssignment(
        slug="entity/acme",
        contributor_op_ids=(uuid4(),),
        base_folder_id=None,
        base_path=None,
        base_depth=0,
    )

    assert assignment.base_path is None
    assert FolderAssignment.model_validate(assignment.model_dump()) == assignment
    with pytest.raises(ValidationError):
        FolderAssignment(
            slug="entity/acme",
            contributor_op_ids=(uuid4(),),
            base_folder_id=None,
            base_path="",
            base_depth=0,
        )


def test_folder_name_limits_and_maximum_reachable_path() -> None:
    max_name = "n" * 512
    max_path = "/" + "/".join(("a" * 512, "b" * 512, "c" * 512))

    assert FolderCatalogEntry(
        id=uuid4(), parent_id=None, name=max_name, path=f"/{max_name}", depth=1
    ).name == max_name
    assert AllowedFolderBase(id=uuid4(), path=max_path, depth=3).path == max_path
    with pytest.raises(ValidationError):
        FolderCatalogEntry(
            id=uuid4(), parent_id=None, name="n" * 513, path=f"/{'n' * 513}", depth=1
        )
    # depth<=3 且每段<=512，使 1539 成为可达的合法 path 最大长度。
    with pytest.raises(ValidationError):
        AllowedFolderBase(id=uuid4(), path=f"{max_path}d", depth=3)
    # 2048 上限被上述更严格约束支配，不存在合法的 2048 长度 path。
    with pytest.raises(ValidationError):
        AllowedFolderBase(id=uuid4(), path=f"/{'x' * 2048}", depth=1)


def test_embedding_item_length_boundaries() -> None:
    assert EmbeddingItem(key="k" * 512, text="t" * 8000).key == "k" * 512
    with pytest.raises(ValidationError):
        EmbeddingItem(key="k" * 513, text="valid")
    with pytest.raises(ValidationError):
        EmbeddingItem(key="valid", text="t" * 8001)


def test_taxonomy_topic_length_boundaries() -> None:
    assert TaxonomyTopic(
        slug="entity/acme", title="t" * 512, page_type="entity", summary="s" * 4000
    ).title == "t" * 512
    with pytest.raises(ValidationError):
        TaxonomyTopic(
            slug="entity/acme", title="t" * 513, page_type="entity", summary=""
        )
    with pytest.raises(ValidationError):
        TaxonomyTopic(
            slug="entity/acme", title="valid", page_type="entity", summary="s" * 4001
        )


def test_taxonomy_request_topic_count_and_uniqueness_boundaries() -> None:
    def topics(count: int) -> tuple[TaxonomyTopic, ...]:
        return tuple(
            TaxonomyTopic(
                slug=f"entity/topic-{index}",
                title=f"Topic {index}",
                page_type="entity",
            )
            for index in range(count)
        )

    assert len(TaxonomyRequest(topics=topics(1)).topics) == 1
    assert len(TaxonomyRequest(topics=topics(60)).topics) == 60
    with pytest.raises(ValidationError):
        TaxonomyRequest(topics=())
    with pytest.raises(ValidationError):
        TaxonomyRequest(topics=topics(61))
    with pytest.raises(ValidationError):
        TaxonomyRequest(topics=(topics(1)[0], topics(1)[0]))


def test_taxonomy_decision_segment_count_boundaries() -> None:
    assert TaxonomyDecision(
        slug="entity/acme", new_segments=("First", "Second")
    ).new_segments == ("First", "Second")
    with pytest.raises(ValidationError):
        TaxonomyDecision(slug="entity/acme", new_segments=("First", "Second", "Third"))


def test_folder_assignment_depth_and_wiki_path_boundaries() -> None:
    base_id = uuid4()
    assert FolderAssignment(
        slug="entity/acme",
        contributor_op_ids=(uuid4(),),
        base_folder_id=base_id,
        base_path="/First/Second",
        base_depth=2,
        new_segments=("Third",),
    ).folder_path == "/First/Second/Third"
    with pytest.raises(ValidationError):
        FolderAssignment(
            slug="entity/acme",
            contributor_op_ids=(uuid4(),),
            base_folder_id=base_id,
            base_path="/First/Second/Third",
            base_depth=3,
            new_segments=("Fourth",),
        )

    slug = "entity/" + "s" * 248
    base_segment = "a" * 512
    accepted = FolderAssignment(
        slug=slug,
        contributor_op_ids=(uuid4(),),
        base_folder_id=base_id,
        base_path=f"/{base_segment}",
        base_depth=1,
        new_segments=("b" * 254,),
    )

    assert len(accepted.wiki_path) == 1024
    with pytest.raises(ValidationError):
        FolderAssignment(
            slug=slug,
            contributor_op_ids=(uuid4(),),
            base_folder_id=base_id,
            base_path=f"/{base_segment}",
            base_depth=1,
            new_segments=("b" * 255,),
        )


def test_worker_options_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_WIKI_INGEST_BATCH_SIZE", "7")
    monkeypatch.setenv("GRAPH_WIKI_INGEST_MAP_PARALLEL", "3")
    monkeypatch.setenv("GRAPH_WIKI_INGEST_REDUCE_PARALLEL", "4")
    monkeypatch.setenv("GRAPH_WIKI_CLAIM_TIMEOUT_SECONDS", "900")
    monkeypatch.setenv("GRAPH_WIKI_MAX_PAGES_PER_INGEST", "12")
    monkeypatch.setenv("GRAPH_WIKI_EXTRACTION_GRANULARITY", "focused")
    monkeypatch.setenv("GRAPH_WIKI_CITATION_BATCH_CHARS", "13000")
    monkeypatch.setenv("GRAPH_WIKI_CITATION_PARALLEL", "5")
    monkeypatch.setenv("GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT", "19")
    monkeypatch.setenv("GRAPH_WIKI_TOMBSTONE_TTL_SECONDS", "7200")

    options = WikiWorkerOptions.from_env()

    assert options.batch_size == 7
    assert options.map_parallel == 3
    assert options.reduce_parallel == 4
    assert options.claim_timeout_seconds == 900
    assert options.max_pages_per_ingest == 12
    assert options.extraction_granularity == "focused"
    assert options.citation_batch_chars == 13000
    assert options.citation_parallel == 5
    assert options.dedup_candidate_limit == 19
    assert options.tombstone_ttl_seconds == 7200


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("GRAPH_WIKI_INGEST_BATCH_SIZE", "0"),
        ("GRAPH_WIKI_INGEST_MAP_PARALLEL", "101"),
        ("GRAPH_WIKI_INGEST_REDUCE_PARALLEL", "not-an-integer"),
        ("GRAPH_WIKI_CLAIM_TIMEOUT_SECONDS", "59"),
        ("GRAPH_WIKI_MAX_PAGES_PER_INGEST", "-1"),
        ("GRAPH_WIKI_EXTRACTION_GRANULARITY", "broad"),
    ],
)
def test_worker_options_reject_invalid_environment(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValidationError):
        WikiWorkerOptions.from_env()


def test_source_models_validate_identity_and_activity() -> None:
    knowledge = SourceKnowledge(
        id="knowledge-1",
        tenant_id=1,
        knowledge_base_id=KB_ID,
        title="Document One",
        op_version="version-1",
    )

    assert knowledge.is_active is True
    assert knowledge.model_copy(update={"status": "deleting"}).is_active is False
    assert SourceChunk(id="chunk-1").model_dump() == {
        "id": "chunk-1",
        "chunk_index": 0,
        "start_at": 0,
        "text": "",
        "ocr_text": "",
        "image_caption": "",
    }

    for model, values in (
        (SourceChunk, {"id": ""}),
        (
            SourceKnowledge,
            {
                "id": "",
                "tenant_id": 1,
                "knowledge_base_id": KB_ID,
                "title": "Document One",
                "op_version": "version-1",
            },
        ),
    ):
        with pytest.raises(ValidationError):
            model.model_validate(values)


def test_candidate_normalizes_slug_name_and_aliases_stably() -> None:
    candidate = TopicCandidate(
        name="  Acme  ",
        slug="  ENTITY/ACME  ",
        page_type="entity",
        aliases=[" ACME ", "ACME", "", "  ", "Acme Corp"],
    )

    assert candidate.name == "Acme"
    assert candidate.slug == "entity/acme"
    assert candidate.aliases == ["ACME", "Acme Corp"]
    assert candidate.description == ""
    assert candidate.details == ""


@pytest.mark.parametrize("slug", ["entity/acme corp", "entity/acme?x=1", "entity/acme.company"])
def test_candidate_rejects_illegal_slug_characters(slug: str) -> None:
    with pytest.raises(ValidationError):
        TopicCandidate(name="Acme", slug=slug, page_type="entity")


def test_candidate_requires_matching_type_prefix() -> None:
    with pytest.raises(ValidationError):
        TopicCandidate(name="Acme", slug="concept/acme", page_type="entity")


@pytest.mark.parametrize("name", ["", "  ", "\t\n"])
def test_candidate_rejects_blank_name(name: str) -> None:
    with pytest.raises(ValidationError):
        TopicCandidate(name=name, slug="entity/acme", page_type="entity")


def test_candidate_extraction_rejects_candidates_in_wrong_group() -> None:
    with pytest.raises(ValidationError):
        CandidateExtraction(
            entities=[
                TopicCandidate(name="Retrieval", slug="concept/retrieval", page_type="concept")
            ]
        )

    with pytest.raises(ValidationError):
        CandidateExtraction(
            concepts=[TopicCandidate(name="Acme", slug="entity/acme", page_type="entity")]
        )


@pytest.mark.parametrize("model", [DocumentSummary, PageMergeOutput])
def test_model_text_outputs_strip_and_reject_blank_values(model: type) -> None:
    output = model(headline="  Headline  ", markdown="  Body  ")

    assert output.headline == "Headline"
    assert output.markdown == "Body"

    with pytest.raises(ValidationError):
        model(headline="  ", markdown="Body")


def test_merge_request_requires_at_least_one_contribution() -> None:
    with pytest.raises(ValidationError):
        PageMergeRequest(
            slug="entity/acme",
            title="Acme",
            page_type="entity",
            contributions=[],
        )


def test_slug_update_requires_matching_page_type_prefix() -> None:
    with pytest.raises(ValidationError):
        SlugUpdate(
            pending_op_id=uuid4(),
            knowledge_id="knowledge-1",
            slug="concept/acme",
            title="Acme",
            page_type="entity",
        )


def test_reduced_page_supports_summary_pages() -> None:
    page = ReducedPage(
        slug="summary/knowledge-1",
        title="Document One",
        page_type="summary",
        content="Body",
        summary="Summary",
    )

    assert page.page_type == "summary"


def test_finalization_request_uses_scope_and_source_version() -> None:
    scope = WikiScope(tenant_id=1, knowledge_base_id=KB_ID, actor_id="worker")
    knowledge = SourceKnowledge(
        id="knowledge-1",
        tenant_id=1,
        knowledge_base_id=KB_ID,
        title="Document One",
        op_version="version-1",
    )

    request = FinalizationRequest.from_knowledge(scope, knowledge)

    assert request.model_dump() == {
        "tenant_id": 1,
        "knowledge_base_id": KB_ID,
        "knowledge_id": "knowledge-1",
        "attempt": "version-1",
        "subtask_name": "wiki",
    }


def test_batch_result_exposes_read_only_counts() -> None:
    result = BatchResult(completed_op_ids=[uuid4(), uuid4()], failed_op_ids=[uuid4()])

    assert result.completed_ops == 2
    assert result.failed_ops == 1

    with pytest.raises(AttributeError):
        result.completed_ops = 99


def test_batch_result_from_ids_classifies_pending_ids_stably() -> None:
    pending_a, pending_b, pending_c = uuid4(), uuid4(), uuid4()

    result = BatchResult.from_ids(
        [pending_a, pending_b, pending_c],
        [pending_b],
    )

    assert result.completed_op_ids == [pending_a, pending_c]
    assert result.failed_op_ids == [pending_b]
    assert result.completed_ops == 2
    assert result.failed_ops == 1


def test_batch_result_from_ids_deduplicates_and_rejects_unknown_failures() -> None:
    pending_a, pending_b, pending_c, unknown = uuid4(), uuid4(), uuid4(), uuid4()

    result = BatchResult.from_ids(
        [pending_a, pending_b, pending_a, pending_c],
        [pending_b, pending_b],
    )

    assert result.completed_op_ids == [pending_a, pending_c]
    assert result.failed_op_ids == [pending_b]

    with pytest.raises(ValueError, match="pending"):
        BatchResult.from_ids([pending_a, pending_b], [pending_b, unknown])


def test_page_contribution_uses_independent_list_defaults() -> None:
    first = PageContribution(
        pending_op_id=uuid4(),
        knowledge_id="knowledge-1",
        title="Document One",
        content="Body",
        summary="Summary",
    )
    second = PageContribution(
        pending_op_id=uuid4(),
        knowledge_id="knowledge-2",
        title="Document Two",
        content="Body",
        summary="Summary",
    )

    first.aliases.append("Acme")

    assert second.aliases == []
    assert second.source_refs == []
    assert second.chunk_refs == []


@pytest.mark.parametrize("slug", ["entity//acme", "entity/acme/", "entity/acme//detail"])
def test_slug_rejects_empty_path_segments(slug: str) -> None:
    with pytest.raises(ValidationError):
        TopicCandidate(name="Acme", slug=slug, page_type="entity")


def test_finalization_request_rejects_cross_scope_knowledge() -> None:
    knowledge = SourceKnowledge(
        id="knowledge-1",
        tenant_id=2,
        knowledge_base_id=KB_ID,
        title="Document One",
        op_version="version-1",
    )
    with pytest.raises(ValueError, match="租户"):
        FinalizationRequest.from_knowledge(
            WikiScope(tenant_id=1, knowledge_base_id=KB_ID, actor_id="worker"),
            knowledge,
        )


def test_batch_result_rejects_duplicate_or_overlapping_ids() -> None:
    op_id = uuid4()
    with pytest.raises(ValidationError):
        BatchResult(completed_op_ids=[op_id, op_id])
    with pytest.raises(ValidationError):
        BatchResult(completed_op_ids=[op_id], failed_op_ids=[op_id])


def test_identity_fields_strip_and_reject_blank_values() -> None:
    assert SourceChunk(id=" chunk-1 ").id == "chunk-1"
    with pytest.raises(ValidationError):
        SourceChunk(id="   ")
    with pytest.raises(ValidationError):
        SourceChunk(id="chunk-1", chunk_index=-1)
    with pytest.raises(ValidationError):
        SourceChunk(id="chunk-1", start_at=-1)
    with pytest.raises(ValidationError):
        MapDocumentResult(
            pending_op_id=uuid4(),
            knowledge_id="   ",
        )


def test_worker_options_include_incremental_safe_defaults() -> None:
    options = WikiWorkerOptions()

    assert options.citation_batch_chars == 12000
    assert options.citation_parallel == 4
    assert options.dedup_candidate_limit == 20
    assert options.tombstone_ttl_seconds == 3600


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("GRAPH_WIKI_CITATION_BATCH_CHARS", "999"),
        ("GRAPH_WIKI_CITATION_BATCH_CHARS", "100001"),
        ("GRAPH_WIKI_CITATION_PARALLEL", "0"),
        ("GRAPH_WIKI_CITATION_PARALLEL", "33"),
        ("GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT", "0"),
        ("GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT", "21"),
        ("GRAPH_WIKI_TOMBSTONE_TTL_SECONDS", "59"),
        ("GRAPH_WIKI_TOMBSTONE_TTL_SECONDS", "86401"),
    ],
)
def test_worker_options_reject_invalid_incremental_environment(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        WikiWorkerOptions.from_env()


def test_citation_dtos_normalize_and_reject_hidden_chunk_ids() -> None:
    candidate = TopicCandidate(name="Acme", slug="entity/acme", page_type="entity")
    request = CitationBatchRequest(
        knowledge_id=" knowledge-1 ",
        batch_index=0,
        candidates=[candidate],
        chunks=[CitationBatchChunk(alias="c001", text=" Body ")],
    )
    input_refs = {" entity/acme ": [" c001 "]}
    output = CitationBatchOutput(refs_by_slug=input_refs)

    assert request.knowledge_id == "knowledge-1"
    assert request.chunks[0].text == "Body"
    assert output.refs_by_slug == {"entity/acme": ("c001",)}
    assert input_refs == {" entity/acme ": [" c001 "]}
    with pytest.raises(ValidationError):
        CitationBatchChunk(alias="c001", text="Body", chunk_id="internal")
    with pytest.raises(ValidationError):
        CitationBatchRequest(
            knowledge_id="knowledge-1", batch_index=0, candidates=[candidate],
            chunks=[CitationBatchChunk(alias="c001", text="A"), CitationBatchChunk(alias="c001", text="B")],
        )
    for aliases in ([], [" "], [" c001 ", "c001"]):
        with pytest.raises(ValidationError):
            CitationBatchOutput(refs_by_slug={"entity/acme": aliases})


def test_dedup_dtos_enforce_static_target_and_decision_contracts() -> None:
    candidate = TopicCandidate(name="Acme", slug="entity/acme", page_type="entity")
    input_aliases = [" Existing alias "]
    target = DedupPageCandidate(slug=" ENTITY/existing ", title=" Existing ", page_type="entity", aliases=input_aliases)
    request = DedupRequest(candidates=[DedupCandidateRequest(candidate=candidate, allowed_targets=[target])])
    decision = DedupDecision(candidate_slug=" ENTITY/ACME ", canonical_slug=" entity/existing ")

    assert request.candidates[0].allowed_targets[0].aliases == ("Existing alias",)
    assert input_aliases == [" Existing alias "]
    assert decision.candidate_slug == "entity/acme"
    assert decision.canonical_slug == "entity/existing"
    with pytest.raises(ValidationError):
        DedupCandidateRequest(candidate=candidate, allowed_targets=[DedupPageCandidate(slug="concept/x", title="X", page_type="concept")])
    with pytest.raises(ValidationError):
        DedupOutput(decisions=[decision, decision])
    for aliases in ([" "], [" A ", "A"]):
        with pytest.raises(ValidationError):
            DedupPageCandidate(slug="entity/existing", title="Existing", page_type="entity", aliases=aliases)


def _record(
    *, slug: str = "entity/acme", knowledge_id: str = "knowledge-1", state: str = "active",
    tenant_id: int = 1, knowledge_base_id: UUID = KB_ID, page_type: str = "entity",
) -> StoredContributionRecord:
    return StoredContributionRecord(
        id=None, tenant_id=tenant_id, knowledge_base_id=knowledge_base_id, slug=slug, knowledge_id=knowledge_id,
        op_version="v1", page_type=page_type, state=state, title="Acme", content="Body", summary="Summary",
        aliases=[" Acme "], chunk_refs=[" c001 "],
    )


@pytest.mark.parametrize(
    ("action", "previous", "current"),
    [
        ("add", None, _record()),
        ("replace", _record(), _record()),
        ("retract_stale", _record(), None),
        ("retract", _record(state="retract_pending"), None),
    ],
)
def test_contribution_delta_accepts_each_action_contract(
    action: str, previous: StoredContributionRecord | None, current: StoredContributionRecord | None
) -> None:
    delta = ContributionDelta(pending_op_id=uuid4(), action=action, slug="entity/acme", knowledge_id="knowledge-1", previous=previous, current=current)
    assert delta.action == action


@pytest.mark.parametrize(
    ("action", "previous", "current"),
    [
        ("add", _record(), _record()),
        ("replace", None, _record()),
        ("retract_stale", _record(state="retract_pending"), None),
        ("retract", _record(), None),
    ],
)
def test_contribution_delta_rejects_invalid_action_states(
    action: str, previous: StoredContributionRecord | None, current: StoredContributionRecord | None
) -> None:
    with pytest.raises(ValidationError):
        ContributionDelta(pending_op_id=uuid4(), action=action, slug="entity/acme", knowledge_id="knowledge-1", previous=previous, current=current)


@pytest.mark.parametrize(
    "record",
    [
        _record(slug="entity/other"),
        _record(knowledge_id="other"),
    ],
)
def test_contribution_delta_rejects_mismatched_delta_identity(record: StoredContributionRecord) -> None:
    with pytest.raises(ValidationError):
        ContributionDelta(pending_op_id=uuid4(), action="add", slug="entity/acme", knowledge_id="knowledge-1", previous=None, current=record)


@pytest.mark.parametrize(
    "current",
    [
        _record(tenant_id=2),
        _record(knowledge_base_id=UUID("22222222-2222-2222-2222-222222222222")),
        _record(slug="concept/acme", page_type="concept"),
    ],
)
def test_contribution_delta_rejects_mismatched_previous_current_scope_or_type(current: StoredContributionRecord) -> None:
    with pytest.raises(ValidationError):
        ContributionDelta(pending_op_id=uuid4(), action="replace", slug="entity/acme", knowledge_id="knowledge-1", previous=_record(), current=current)


def test_contribution_delta_rejects_mismatched_previous_current_page_type() -> None:
    invalid_current = _record().model_copy(update={"page_type": "concept"})
    with pytest.raises(ValidationError):
        ContributionDelta(pending_op_id=uuid4(), action="replace", slug="entity/acme", knowledge_id="knowledge-1", previous=_record(), current=invalid_current)


@pytest.mark.parametrize("field", ["aliases", "chunk_refs"])
@pytest.mark.parametrize("values", [[" "], [" value ", "value"]])
def test_stored_contribution_record_rejects_empty_or_duplicate_arrays(field: str, values: list[str]) -> None:
    payload = _record().model_dump()
    payload[field] = values
    with pytest.raises(ValidationError):
        StoredContributionRecord(**payload)


def test_stored_contribution_record_strips_arrays_without_mutating_input() -> None:
    aliases = [" Acme alias "]
    chunk_refs = [" c001 "]
    payload = _record().model_dump()
    payload.update(aliases=aliases, chunk_refs=chunk_refs)
    record = StoredContributionRecord(**payload)

    assert record.aliases == ("Acme alias",)
    assert record.chunk_refs == ("c001",)
    assert aliases == [" Acme alias "]
    assert chunk_refs == [" c001 "]


def test_apply_request_enforces_operation_and_page_expectation_invariants() -> None:
    op_id, other_id = uuid4(), uuid4()
    page = ReducedPage(slug="entity/acme", title="Acme", page_type="entity", content="Body", summary="Summary")
    request = BatchApplyRequest(
        claim_token=uuid4(), pages=[page], contribution_deltas=[], completed_op_ids=[op_id],
        superseded_op_ids=[], failures=[OperationFailure(pending_op_id=other_id, error_code=" CODE ", error_summary=" broken\n now ")],
        expected_pages=[PageExpectation(slug="entity/acme", page_id=uuid4(), version=1)], operation_id=uuid4(),
    )
    assert request.failures[0].error_code == "CODE"
    assert request.failures[0].error_summary == "broken now"
    with pytest.raises(ValidationError):
        PageExpectation(slug="entity/acme", page_id=uuid4(), version=None)
    with pytest.raises(ValidationError):
        PageExpectation(slug="entity/acme", page_id=None, version=1)
    with pytest.raises(ValidationError):
        BatchApplyRequest(claim_token=uuid4(), pages=[page], contribution_deltas=[], completed_op_ids=[op_id], superseded_op_ids=[op_id], failures=[], expected_pages=[], operation_id=uuid4())


@pytest.mark.parametrize("field", ["completed_op_ids", "superseded_op_ids", "failures"])
def test_apply_request_rejects_duplicate_operation_ids(field: str) -> None:
    op_id = uuid4()
    payload = dict(claim_token=uuid4(), pages=[], contribution_deltas=[], completed_op_ids=[], superseded_op_ids=[], failures=[], expected_pages=[], operation_id=uuid4())
    payload[field] = ([op_id, op_id] if field != "failures" else [OperationFailure(pending_op_id=op_id, error_code="code", error_summary="failed")] * 2)
    with pytest.raises(ValidationError):
        BatchApplyRequest(**payload)


@pytest.mark.parametrize(
    ("completed", "superseded", "failures"),
    [(["shared"], ["shared"], []), (["shared"], [], ["shared"]), ([], ["shared"], ["shared"])],
)
def test_apply_request_rejects_cross_group_operation_id_overlap(
    completed: list[str], superseded: list[str], failures: list[str]
) -> None:
    op_id = uuid4()
    make_ids = lambda values: [op_id for _ in values]
    with pytest.raises(ValidationError):
        BatchApplyRequest(claim_token=uuid4(), pages=[], contribution_deltas=[], completed_op_ids=make_ids(completed), superseded_op_ids=make_ids(superseded), failures=[OperationFailure(pending_op_id=op_id, error_code="code", error_summary="failed") for _ in failures], expected_pages=[], operation_id=uuid4())


@pytest.mark.parametrize("field", ["pages", "expected_pages"])
def test_apply_request_rejects_duplicate_page_slugs(field: str) -> None:
    page = ReducedPage(slug="entity/acme", title="Acme", page_type="entity", content="Body", summary="Summary")
    expected = PageExpectation(slug="entity/acme")
    payload = dict(claim_token=uuid4(), pages=[], contribution_deltas=[], completed_op_ids=[], superseded_op_ids=[], failures=[], expected_pages=[], operation_id=uuid4())
    payload[field] = [page, page] if field == "pages" else [expected, expected]
    with pytest.raises(ValidationError):
        BatchApplyRequest(**payload)


@pytest.mark.parametrize(
    ("model", "field", "invalid_value"),
    [
        (
            CitationBatchRequest(
                knowledge_id="knowledge-1", batch_index=0, candidates=[],
                chunks=[CitationBatchChunk(alias="c001", text="Body")],
            ),
            "chunks",
            [CitationBatchChunk(alias="c001", text="A"), CitationBatchChunk(alias="c001", text="B")],
        ),
        (
            ContributionDelta(
                pending_op_id=uuid4(), action="add", slug="entity/acme", knowledge_id="knowledge-1",
                previous=None, current=_record(),
            ),
            "action",
            "replace",
        ),
        (PageExpectation(slug="entity/acme"), "page_id", uuid4()),
        (
            BatchApplyRequest(
                claim_token=uuid4(), pages=[], contribution_deltas=[], completed_op_ids=[uuid4()],
                superseded_op_ids=[], failures=[], expected_pages=[], operation_id=uuid4(),
            ),
            "superseded_op_ids",
            None,
        ),
    ],
)
def test_incremental_dto_assignment_failure_is_atomic(
    model: object, field: str, invalid_value: object
) -> None:
    if invalid_value is None:
        invalid_value = [model.completed_op_ids[0]]  # type: ignore[attr-defined]
    before = model.model_dump(mode="json")  # type: ignore[attr-defined]

    with pytest.raises(ValidationError):
        setattr(model, field, invalid_value)

    assert model.model_dump(mode="json") == before  # type: ignore[attr-defined]


def test_incremental_dto_collections_block_in_place_invariant_breaks() -> None:
    citation = CitationBatchRequest(
        knowledge_id="knowledge-1", batch_index=0, candidates=[],
        chunks=[CitationBatchChunk(alias="c001", text="Body")],
    )
    batch = BatchApplyRequest(
        claim_token=uuid4(), pages=[], contribution_deltas=[], completed_op_ids=[uuid4()],
        superseded_op_ids=[], failures=[], expected_pages=[], operation_id=uuid4(),
    )

    with pytest.raises((AttributeError, TypeError)):
        citation.chunks.append(CitationBatchChunk(alias="c001", text="Duplicate"))
    with pytest.raises((AttributeError, TypeError)):
        batch.superseded_op_ids.append(batch.completed_op_ids[0])
    assert isinstance(citation.model_dump(mode="json")["chunks"], list)
    assert isinstance(batch.model_dump(mode="json")["completed_op_ids"], list)


def test_page_expectation_failed_assignment_preserves_fields_set() -> None:
    expectation = PageExpectation(slug="entity/acme")
    before_fields_set = expectation.model_fields_set.copy()
    before_dump = expectation.model_dump(exclude_unset=True)

    with pytest.raises(ValidationError):
        expectation.page_id = uuid4()

    assert expectation.model_fields_set == before_fields_set
    assert expectation.model_dump(exclude_unset=True) == before_dump


def test_incremental_dto_nested_values_are_deeply_immutable() -> None:
    citation = CitationBatchRequest(
        knowledge_id="knowledge-1", batch_index=0, candidates=[],
        chunks=[CitationBatchChunk(alias="c001", text="A"), CitationBatchChunk(alias="c002", text="B")],
    )
    delta = ContributionDelta(
        pending_op_id=uuid4(), action="add", slug="entity/acme", knowledge_id="knowledge-1",
        previous=None, current=_record(),
    )
    batch = BatchApplyRequest(
        claim_token=uuid4(),
        pages=[
            ReducedPage(slug="entity/acme", title="Acme", page_type="entity", content="A", summary="A"),
            ReducedPage(slug="entity/other", title="Other", page_type="entity", content="B", summary="B"),
        ],
        contribution_deltas=[], completed_op_ids=[], superseded_op_ids=[], failures=[], expected_pages=[], operation_id=uuid4(),
    )

    with pytest.raises((AttributeError, TypeError, ValidationError)):
        citation.chunks[1].alias = "c001"
    with pytest.raises((AttributeError, TypeError, ValidationError)):
        delta.current.slug = "entity/other"  # type: ignore[union-attr]
    with pytest.raises((AttributeError, TypeError, ValidationError)):
        batch.pages[1].slug = "entity/acme"


def test_incremental_dto_collections_cannot_be_bypassed_by_builtin_mutators() -> None:
    citation = CitationBatchRequest(
        knowledge_id="knowledge-1", batch_index=0, candidates=[],
        chunks=[CitationBatchChunk(alias="c001", text="Body")],
    )
    output = CitationBatchOutput(refs_by_slug={"entity/acme": ["c001"]})

    with pytest.raises(TypeError):
        list.append(citation.chunks, CitationBatchChunk(alias="c002", text="Other"))
    with pytest.raises(TypeError):
        dict.__setitem__(output.refs_by_slug, "entity/other", ("c001",))


def test_incremental_frozen_dtos_keep_json_and_deep_copy_contracts() -> None:
    request = CitationBatchRequest(
        knowledge_id="knowledge-1", batch_index=0, candidates=[],
        chunks=[CitationBatchChunk(alias="c001", text="Body")],
    )
    output = CitationBatchOutput(refs_by_slug={"entity/acme": ["c001"]})
    copied = request.model_copy(deep=True)
    copied_output = output.model_copy(deep=True)

    assert copied is not request
    assert copied.chunks is not request.chunks
    assert copied_output.refs_by_slug is not output.refs_by_slug
    assert copied_output.model_dump() == output.model_dump()
    assert json.loads(output.model_dump_json()) == {"refs_by_slug": {"entity/acme": ["c001"]}, "supplemental_candidates": []}


def test_citation_mapping_items_preserve_pair_order_for_linear_dumps() -> None:
    output = CitationBatchOutput(
        refs_by_slug={
            "entity/first": ["c001"],
            "entity/second": ["c002"],
            "entity/third": ["c003"],
        }
    )

    assert isinstance(output.refs_by_slug.items(), ItemsView)
    assert tuple(output.refs_by_slug.items()) == (
        ("entity/first", ("c001",)),
        ("entity/second", ("c002",)),
        ("entity/third", ("c003",)),
    )
    assert dict(output.refs_by_slug) == {
        "entity/first": ("c001",),
        "entity/second": ("c002",),
        "entity/third": ("c003",),
    }
    assert output.refs_by_slug.items() & {("entity/second", ("c002",))} == {
        ("entity/second", ("c002",))
    }


def test_citation_mapping_supports_copy_and_pickle_round_trips() -> None:
    output = CitationBatchOutput(refs_by_slug={"entity/acme": ["c001"]})
    mapping = output.refs_by_slug

    copied_mapping = copy.copy(mapping)
    restored_mapping = pickle.loads(pickle.dumps(mapping))
    restored_output = pickle.loads(pickle.dumps(output))

    assert copied_mapping is mapping
    assert restored_mapping is not mapping
    assert restored_mapping == mapping
    assert restored_output is not output
    assert restored_output.refs_by_slug is not mapping
    assert restored_output.model_dump_json() == output.model_dump_json()
    with pytest.raises((AttributeError, TypeError)):
        restored_mapping._items = ()  # type: ignore[attr-defined]
