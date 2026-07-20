from __future__ import annotations

import asyncio
import json
from pathlib import Path
import re
from uuid import UUID

import pytest

from app.wiki.ingest.fakes import FakeDataset, load_fake_runtime_adapters
from app.wiki.ingest.schemas import EnqueueResult
from app.wiki.tasks import enqueue_fake, wiki_tasks


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
OTHER_KB_ID = UUID("33333333-3333-3333-3333-333333333333")
PENDING_OP_ID = UUID("22222222-2222-2222-2222-222222222222")


def _write_fake_fixture(
    path: Path,
    *,
    tenant_id: int = 23,
    op_version: str = "v1",
    status: str = "ready",
) -> None:
    path.write_text(
        json.dumps(
            {
                "knowledge_bases": [
                    {
                        "tenant_id": tenant_id,
                        "knowledge_base_id": str(KB_ID),
                        "config": {
                            "wiki_enabled": True,
                            "synthesis_model_id": "fake-synthesis",
                        },
                    }
                ],
                "knowledge": [
                    {
                        "id": "knowledge-1",
                        "tenant_id": tenant_id,
                        "knowledge_base_id": str(KB_ID),
                        "title": "Document One",
                        "op_version": op_version,
                        "status": status,
                        "chunks": [{"id": "chunk-1", "text": "Source text"}],
                    }
                ],
                "model_responses": {
                    "extract_candidates": {"knowledge-1": {}},
                    "summaries": {
                        "knowledge-1": {"headline": "Document One", "markdown": "Summary"}
                    },
                    "merges": {
                        "entity/example": {"headline": "Example", "markdown": "Body"}
                    },
                },
            }
        ),
        encoding="utf-8",
    )


def _args(*, knowledge_id: str = "knowledge-1", kb_id: str | None = None) -> list[str]:
    return [
        "--kb-id",
        kb_id or str(KB_ID),
        "--knowledge-id",
        knowledge_id,
    ]


def test_missing_fixture_exits_with_code_two_before_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GRAPH_WIKI_FAKE_DATA_FILE", raising=False)
    monkeypatch.setattr(
        wiki_tasks, "build_runtime", lambda: pytest.fail("不应启动 runtime")
    )

    with pytest.raises(SystemExit) as raised:
        enqueue_fake.main(_args())

    assert raised.value.code == 2


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["--kb-id", str(KB_ID)],
        ["--knowledge-id", "knowledge-1"],
        _args(kb_id="not-a-uuid"),
        _args(kb_id="AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"),
        _args(knowledge_id="   "),
    ],
)
def test_invalid_cli_arguments_exit_with_code_two_before_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    argv: list[str],
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture)
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))
    monkeypatch.setattr(
        wiki_tasks, "build_runtime", lambda: pytest.fail("不应启动 runtime")
    )

    with pytest.raises(SystemExit) as raised:
        enqueue_fake.main(argv)

    assert raised.value.code == 2


def test_fixture_mismatch_exits_before_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture)
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))
    monkeypatch.setattr(
        wiki_tasks, "build_runtime", lambda: pytest.fail("不应启动 runtime")
    )

    with pytest.raises(SystemExit) as raised:
        enqueue_fake.main(_args(knowledge_id="missing"))

    assert raised.value.code == 2


def test_duplicate_knowledge_id_reports_global_uniqueness_before_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture)
    data = json.loads(fixture.read_text(encoding="utf-8"))
    data["knowledge_bases"].append(
        {
            "tenant_id": 24,
            "knowledge_base_id": str(OTHER_KB_ID),
            "config": {
                "wiki_enabled": True,
                "synthesis_model_id": "fake-synthesis",
            },
        }
    )
    duplicate = dict(data["knowledge"][0])
    duplicate["tenant_id"] = 24
    duplicate["knowledge_base_id"] = str(OTHER_KB_ID)
    data["knowledge"].append(duplicate)
    fixture.write_text(json.dumps(data), encoding="utf-8")
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))
    monkeypatch.setattr(
        wiki_tasks, "build_runtime", lambda: pytest.fail("不应启动 runtime")
    )

    with pytest.raises(SystemExit) as raised:
        enqueue_fake.main(_args())

    assert raised.value.code == 2
    stderr = capsys.readouterr().err
    assert "knowledge_id" in stderr
    assert "全局唯一" in stderr


def test_success_uses_fixture_tenant_prints_public_json_and_closes_on_same_loop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture, tenant_id=23)
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))
    calls = []
    loops = []

    class Enqueue:
        async def enqueue_ingest(self, scope, knowledge_id):
            loops.append(asyncio.get_running_loop())
            calls.append(("ingest", scope, knowledge_id, None))
            return EnqueueResult(pending_op_id=PENDING_OP_ID)

    class Runtime:
        enqueue = Enqueue()

        async def aclose(self) -> None:
            loops.append(asyncio.get_running_loop())

    runtime = Runtime()
    monkeypatch.setattr(wiki_tasks, "build_runtime", lambda: runtime)

    enqueue_fake.main(_args())

    assert len(calls) == 1
    op, scope, knowledge_id, op_version = calls[0]
    assert op == "ingest"
    assert scope.tenant_id == 23
    assert scope.knowledge_base_id == KB_ID
    assert scope.actor_id == "wiki-fake-cli"
    assert scope.can_write is True
    assert knowledge_id == "knowledge-1"
    assert op_version is None
    assert loops[0] is loops[1]
    assert json.loads(capsys.readouterr().out) == {
        "op": "ingest",
        "pending_op_id": str(PENDING_OP_ID),
        "skipped_reason": None,
        "deduplicated": False,
    }


def test_retract_uses_fixture_version_and_prints_operation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture, op_version="fixture-delete-v7", status="deleted")
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))
    calls = []

    class Enqueue:
        async def enqueue_retract(self, scope, knowledge_id, op_version):
            calls.append((scope, knowledge_id, op_version))
            return EnqueueResult(pending_op_id=PENDING_OP_ID, deduplicated=True)

    class Runtime:
        enqueue = Enqueue()

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(wiki_tasks, "build_runtime", Runtime)

    enqueue_fake.main(["--op", "retract", *_args()])

    assert calls[0][1:] == ("knowledge-1", "fixture-delete-v7")
    assert json.loads(capsys.readouterr().out) == {
        "op": "retract",
        "pending_op_id": str(PENDING_OP_ID),
        "skipped_reason": None,
        "deduplicated": True,
    }


@pytest.mark.parametrize("status", ["deleting", "cancelled", "deleted"])
def test_ingest_rejects_inactive_fixture_status_before_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status: str,
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture, status=status)
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))
    monkeypatch.setattr(
        wiki_tasks, "build_runtime", lambda: pytest.fail("失活 ingest 不应启动 runtime")
    )

    with pytest.raises(SystemExit) as raised:
        enqueue_fake.main(_args())

    assert raised.value.code == 2


@pytest.mark.parametrize("status", ["ready", "deleting", "cancelled", "deleted"])
def test_retract_accepts_all_fixture_source_states(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    status: str,
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture, op_version=f"version-{status}", status=status)
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))
    calls = []

    class Enqueue:
        async def enqueue_retract(self, scope, knowledge_id, op_version):
            calls.append((scope, knowledge_id, op_version))
            return EnqueueResult(skipped_reason="queued")

    class Runtime:
        enqueue = Enqueue()

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(wiki_tasks, "build_runtime", Runtime)

    enqueue_fake.main(["--op", "retract", *_args()])

    assert calls[0][1:] == ("knowledge-1", f"version-{status}")
    assert json.loads(capsys.readouterr().out)["op"] == "retract"


def test_example_fixture_has_strict_incremental_model_responses() -> None:
    fixture_path = Path(__file__).parents[2] / "examples" / "wiki_fake_data.json"
    raw = json.loads(fixture_path.read_text(encoding="utf-8"))
    dataset = FakeDataset.model_validate(raw)
    source, chat_model, embedding_model = load_fake_runtime_adapters(fixture_path)

    assert source is not chat_model
    assert chat_model is not embedding_model
    assert embedding_model is not source
    assert embedding_model.calls == []
    assert dataset.model_responses.embeddings == {}
    assert {
        key: [
            (decision.slug, decision.new_segments) for decision in output.decisions
        ]
        for key, output in dataset.model_responses.taxonomies.items()
    } == {
        "concept/retrieval": [("concept/retrieval", ())],
        "entity/acme": [("entity/acme", ("Organizations", "Products"))],
        "concept/retrieval,entity/acme": [
            ("concept/retrieval", ()),
            ("entity/acme", ("Organizations", "Products")),
        ],
    }

    citations = dataset.model_responses.citations["knowledge-1"]
    assert len(citations) >= 2
    assert any(batch.supplemental_candidates for batch in citations)
    aliases = [
        alias
        for batch in citations
        for refs in batch.refs_by_slug.values()
        for alias in refs
    ]
    assert aliases and all(re.fullmatch(r"c\d{3}", alias) for alias in aliases)
    response_text = json.dumps(raw["model_responses"], ensure_ascii=False)
    chunk_ids = [chunk["id"] for item in raw["knowledge"] for chunk in item["chunks"]]
    assert all(chunk_id not in response_text for chunk_id in chunk_ids)
    decisions = dataset.model_responses.deduplications
    assert decisions
    assert all(decision.canonical_slug is not None for decision in decisions.values())
    assert all(
        decision.canonical_slug in dataset.model_responses.merges
        for decision in decisions.values()
    )


def test_enqueue_error_is_not_masked_by_close_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture)
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))
    enqueue_error = RuntimeError("enqueue failed")

    class Enqueue:
        async def enqueue_ingest(self, _scope, _knowledge_id):
            raise enqueue_error

    class Runtime:
        enqueue = Enqueue()
        close_count = 0

        async def aclose(self) -> None:
            self.close_count += 1
            raise OSError("close failed")

    runtime = Runtime()
    monkeypatch.setattr(wiki_tasks, "build_runtime", lambda: runtime)

    with pytest.raises(RuntimeError, match="enqueue failed") as raised:
        enqueue_fake.main(_args())

    assert raised.value is enqueue_error
    assert runtime.close_count == 1
    assert caplog.records[-1].wiki_runtime_error_type == "OSError"


def test_enqueue_cancellation_still_closes_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture)
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))

    class Enqueue:
        async def enqueue_ingest(self, _scope, _knowledge_id):
            raise asyncio.CancelledError()

    class Runtime:
        enqueue = Enqueue()
        close_count = 0

        async def aclose(self) -> None:
            self.close_count += 1

    runtime = Runtime()
    monkeypatch.setattr(wiki_tasks, "build_runtime", lambda: runtime)

    with pytest.raises(asyncio.CancelledError):
        enqueue_fake.main(_args())

    assert runtime.close_count == 1


def test_success_propagates_runtime_close_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture)
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))
    close_error = OSError("close failed")

    class Enqueue:
        async def enqueue_ingest(self, _scope, _knowledge_id):
            return EnqueueResult(skipped_reason="source_inactive")

    class Runtime:
        enqueue = Enqueue()

        async def aclose(self) -> None:
            raise close_error

    monkeypatch.setattr(wiki_tasks, "build_runtime", Runtime)

    with pytest.raises(OSError, match="close failed") as raised:
        enqueue_fake.main(_args())

    assert raised.value is close_error


def test_each_cli_call_builds_and_closes_a_new_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture)
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))
    runtimes = []

    class Enqueue:
        async def enqueue_ingest(self, _scope, _knowledge_id):
            return EnqueueResult(skipped_reason="source_inactive")

    class Runtime:
        def __init__(self) -> None:
            self.enqueue = Enqueue()
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True

    def build_runtime():
        runtime = Runtime()
        runtimes.append(runtime)
        return runtime

    monkeypatch.setattr(wiki_tasks, "build_runtime", build_runtime)

    enqueue_fake.main(_args())
    enqueue_fake.main(_args())
    capsys.readouterr()

    assert len(runtimes) == 2
    assert runtimes[0] is not runtimes[1]
    assert all(runtime.closed for runtime in runtimes)
