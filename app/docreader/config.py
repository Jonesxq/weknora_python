"""文档解析器的进程内配置。"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


def _get_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        logger.warning("环境变量 %s=%r 不是整数，使用默认值 %d", name, raw, default)
        return default


@dataclass(frozen=True, slots=True)
class DocReaderConfig:
    """当前启用解析器需要的资源控制参数。"""

    max_file_size_mb: int
    docx_max_pages: int
    pdf_render_max_workers: int
    pdf_render_parallelism: int
    pdf_render_dpi: int
    pdf_jpeg_quality: int
    pdf_render_max_edge: int


def load_config() -> DocReaderConfig:
    cpu_count = os.cpu_count() or 1
    return DocReaderConfig(
        max_file_size_mb=_get_int("DOCREADER_MAX_FILE_SIZE_MB", 50),
        docx_max_pages=_get_int("DOCREADER_DOCX_MAX_PAGES", 0),
        pdf_render_max_workers=_get_int("DOCREADER_PDF_RENDER_MAX_WORKERS", 1),
        pdf_render_parallelism=_get_int(
            "DOCREADER_PDF_RENDER_PARALLELISM", max(1, min(4, cpu_count))
        ),
        pdf_render_dpi=_get_int("DOCREADER_PDF_RENDER_DPI", 200),
        pdf_jpeg_quality=_get_int("DOCREADER_PDF_JPEG_QUALITY", 85),
        pdf_render_max_edge=_get_int("DOCREADER_PDF_RENDER_MAX_EDGE", 2000),
    )


CONFIG = load_config()


def dump_config() -> dict[str, Any]:
    """返回便于日志记录和诊断的有效配置。"""

    return {
        "DOCREADER_MAX_FILE_SIZE_MB": CONFIG.max_file_size_mb,
        "DOCREADER_DOCX_MAX_PAGES": CONFIG.docx_max_pages,
        "DOCREADER_PDF_RENDER_MAX_WORKERS": CONFIG.pdf_render_max_workers,
        "DOCREADER_PDF_RENDER_PARALLELISM": CONFIG.pdf_render_parallelism,
        "DOCREADER_PDF_RENDER_DPI": CONFIG.pdf_render_dpi,
        "DOCREADER_PDF_JPEG_QUALITY": CONFIG.pdf_jpeg_quality,
        "DOCREADER_PDF_RENDER_MAX_EDGE": CONFIG.pdf_render_max_edge,
    }


def print_config() -> None:
    """把当前解析配置写入日志。"""

    for key, value in sorted(dump_config().items()):
        logger.info("%s=%s", key, value)
