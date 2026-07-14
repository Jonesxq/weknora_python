"""应用依赖提供器。"""

from functools import lru_cache

from app.docreader import DocumentReaderService


@lru_cache(maxsize=1)
def get_document_reader() -> DocumentReaderService:
    """返回进程内共享的文档解析服务实例。"""

    return DocumentReaderService()
