"""
Document parser package adapted from WeKnora for in-process use.

This module provides document parsers for various file formats including:
- Microsoft Word documents (.docx)
- PDF documents
- Markdown files
- Plain text files
- Images with text content
- Excel workbooks and common image formats

The parsers extract Markdown and images. Chunking belongs to the RAG ingestion
layer and is intentionally not performed here.
"""

from .docx_parser import DocxParser
from .excel_parser import ExcelParser
from .image_parser import ImageParser
from .markdown_parser import MarkdownParser
from .parser import Parser
from .pdf_parser import PDFParser
from .registry import ParserEngineRegistry, registry

# Export public classes and modules
__all__ = [
    "DocxParser",
    "PDFParser",
    "MarkdownParser",
    "ImageParser",
    "Parser",
    "ExcelParser",
    "ParserEngineRegistry",
    "registry",
]
