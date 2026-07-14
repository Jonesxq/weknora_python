"""项目内文档解析适配层。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from app.docreader.parser import Parser


class DocumentParseError(RuntimeError):
    """文档内容无法被解析。"""


class UnsupportedDocumentTypeError(DocumentParseError):
    """文件格式未在当前解析注册表中启用。"""


@dataclass(frozen=True, slots=True)
class ParsedDocument:
    """与上游实现解耦的项目内解析结果。"""

    content: str
    images: Mapping[str, str]
    metadata: Mapping[str, Any]


class DocumentReaderService:
    """在当前 Python 进程内解析文档，不经过网络协议。"""

    def __init__(self, parser: Parser | None = None) -> None:
        self._parser = parser or Parser()

    def parse_bytes(
        self,
        *,
        file_name: str,
        content: bytes,
        file_type: str | None = None,
        parser_engine: str | None = None,
        engine_overrides: dict[str, Any] | None = None,
    ) -> ParsedDocument:
        """解析内存中的文件字节，返回 Markdown、图片和元数据。"""

        normalized_type = (file_type or Path(file_name).suffix).lstrip(".").lower()
        if not normalized_type:
            raise UnsupportedDocumentTypeError(
                "无法判断文件类型：请提供带扩展名的 file_name 或显式 file_type"
            )

        try:
            # 在真正解析前单独校验注册表，避免把解析器内部的 ValueError
            # （例如损坏的 Excel）误报成“不支持的文件格式”。
            self._parser.registry.get_parser_class(
                parser_engine or "", normalized_type
            )
        except ValueError as exc:
            raise UnsupportedDocumentTypeError(str(exc)) from exc

        try:
            result = self._parser.parse_file(
                file_name=file_name,
                file_type=normalized_type,
                content=content,
                parser_engine=parser_engine,
                engine_overrides=engine_overrides,
            )
        except Exception as exc:
            raise DocumentParseError(f"解析文档 {file_name!r} 失败: {exc}") from exc

        if not result or not result.content:
            raise DocumentParseError(f"解析文档 {file_name!r} 后未得到有效内容")

        return ParsedDocument(
            content=result.content,
            images=dict(result.images),
            metadata=dict(result.metadata),
        )

    def parse_path(
        self,
        path: str | Path,
        *,
        parser_engine: str | None = None,
        engine_overrides: dict[str, Any] | None = None,
    ) -> ParsedDocument:
        """从本地路径读取文件后解析，适用于脚本和后台任务。"""

        source = Path(path)
        try:
            content = source.read_bytes()
        except OSError as exc:
            raise DocumentParseError(f"读取文档 {source} 失败: {exc}") from exc

        return self.parse_bytes(
            file_name=source.name,
            content=content,
            parser_engine=parser_engine,
            engine_overrides=engine_overrides,
        )
