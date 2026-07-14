"""进程内文档解析能力。

解析内核复用自 Tencent WeKnora，并通过 :class:`DocumentReaderService`
向项目其他模块提供稳定接口。
"""

from .service import (
    DocumentParseError,
    DocumentReaderService,
    ParsedDocument,
    UnsupportedDocumentTypeError,
)

__all__ = [
    "DocumentParseError",
    "DocumentReaderService",
    "ParsedDocument",
    "UnsupportedDocumentTypeError",
]
