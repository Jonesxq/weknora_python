"""从共享 fake fixture 手动入队一条 Wiki 摄取操作。"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from collections.abc import Sequence
from pathlib import Path
from uuid import UUID

from pydantic import ValidationError

from app.wiki.ingest.fakes import FakeDataset
from app.wiki.ingest.schemas import EnqueueResult
from app.wiki.scope import WikiScope
from app.wiki.tasks import wiki_tasks


logger = logging.getLogger(__name__)


def _canonical_uuid(value: str) -> UUID:
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("必须是规范 UUID") from exc
    if str(parsed) != value:
        raise argparse.ArgumentTypeError("必须是规范 UUID")
    return parsed


def _non_empty(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise argparse.ArgumentTypeError("不能为空")
    return normalized


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="从 fake fixture 入队 Wiki 摄取操作")
    parser.add_argument("--kb-id", required=True, type=_canonical_uuid)
    parser.add_argument("--knowledge-id", required=True, type=_non_empty)
    return parser


def _fixture_tenant(
    parser: argparse.ArgumentParser,
    fixture_path: Path,
    knowledge_base_id: UUID,
    knowledge_id: str,
) -> int:
    try:
        dataset = FakeDataset.model_validate_json(fixture_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValidationError):
        parser.error("GRAPH_WIKI_FAKE_DATA_FILE 指向的 fixture 无法读取或格式无效")

    matches = [
        item
        for item in dataset.knowledge
        if item.knowledge_base_id == knowledge_base_id and item.id == knowledge_id
    ]
    if not matches:
        parser.error("fixture 中不存在与 --kb-id 和 --knowledge-id 匹配的知识条目")
    if len(matches) > 1:
        parser.error("fixture 中 --kb-id 和 --knowledge-id 匹配到多个租户")
    return matches[0].tenant_id


async def _enqueue(
    *,
    tenant_id: int,
    knowledge_base_id: UUID,
    knowledge_id: str,
) -> EnqueueResult:
    runtime = wiki_tasks.build_runtime()
    try:
        result = await runtime.enqueue.enqueue(
            WikiScope(
                tenant_id=tenant_id,
                knowledge_base_id=knowledge_base_id,
                actor_id="wiki-fake-cli",
                can_write=True,
            ),
            knowledge_id,
        )
        if not isinstance(result, EnqueueResult):
            raise TypeError("Wiki 入队服务必须返回 EnqueueResult")
    except BaseException:
        try:
            await runtime.aclose()
        except asyncio.CancelledError:
            raise
        except Exception as close_error:
            logger.error(
                "Wiki runtime 关闭失败",
                extra={"wiki_runtime_error_type": type(close_error).__name__},
            )
        raise
    await runtime.aclose()
    return result


def main(argv: Sequence[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    fixture_value = os.getenv("GRAPH_WIKI_FAKE_DATA_FILE", "").strip()
    if not fixture_value:
        parser.error("环境变量 GRAPH_WIKI_FAKE_DATA_FILE 必须配置为非空 fixture 路径")
    tenant_id = _fixture_tenant(
        parser,
        Path(fixture_value),
        args.kb_id,
        args.knowledge_id,
    )
    result = asyncio.run(
        _enqueue(
            tenant_id=tenant_id,
            knowledge_base_id=args.kb_id,
            knowledge_id=args.knowledge_id,
        )
    )
    print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False))


if __name__ == "__main__":
    main()
