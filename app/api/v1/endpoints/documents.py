"""文档解析 HTTP 接口。"""

from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status

from app.api.dependencies import get_document_reader
from app.docreader import (
    DocumentParseError,
    DocumentReaderService,
    UnsupportedDocumentTypeError,
)
from app.docreader.config import CONFIG
from app.schemas.document import ParsedDocumentResponse

router = APIRouter(prefix="/documents", tags=["文档解析"])

# 多读一个字节即可判断是否超限，避免把任意大小的上传全部放入内存。
MAX_UPLOAD_BYTES = CONFIG.max_file_size_mb * 1024 * 1024

ReaderDependency = Annotated[DocumentReaderService, Depends(get_document_reader)]
UploadedDocument = Annotated[
    UploadFile,
    File(description="待解析的 PDF、DOCX、Markdown、Excel 或图片文件"),
]


@router.post(
    "/parse",
    response_model=ParsedDocumentResponse,
    summary="解析上传文档",
    responses={
        status.HTTP_413_CONTENT_TOO_LARGE: {"description": "文件超过大小限制"},
        status.HTTP_415_UNSUPPORTED_MEDIA_TYPE: {"description": "文件扩展名不受支持"},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"description": "文件为空或内容无法解析"},
    },
)
def parse_document(
    file: UploadedDocument,
    reader: ReaderDependency,
) -> ParsedDocumentResponse:
    """读取上传内容并在 FastAPI 线程池中完成同步解析。"""

    file_name = (file.filename or "").strip()
    if not file_name:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="上传文件缺少文件名",
        )

    content = file.file.read(MAX_UPLOAD_BYTES + 1)
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=f"文件超过 {CONFIG.max_file_size_mb} MiB 限制",
        )
    if not content:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="上传文件为空",
        )

    try:
        result = reader.parse_bytes(file_name=file_name, content=content)
    except UnsupportedDocumentTypeError as exc:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=str(exc),
        ) from exc
    except DocumentParseError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc

    return ParsedDocumentResponse(
        file_name=file_name,
        content_type=file.content_type,
        size_bytes=len(content),
        content=result.content,
        images=dict(result.images),
        metadata=dict(result.metadata),
    )
