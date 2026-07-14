"""FastAPI 应用入口。"""

from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import JSONResponse

from app.api.v1.router import api_router
from app.wiki.errors import WikiError


app = FastAPI(
    title="Graph 文档解析 API",
    description="在当前 Python 进程内解析 PDF、DOCX、Markdown、Excel 和图片。",
    version="0.1.0",
)
app.include_router(api_router, prefix="/api/v1")


@app.exception_handler(WikiError)
async def handle_wiki_error(_request: Request, error: WikiError) -> JSONResponse:
    """返回稳定错误码，避免暴露 SQL 或内部服务细节。"""

    return JSONResponse(
        status_code=error.status_code,
        content={"detail": {"code": error.code, "message": error.message}},
    )


@app.get("/health", tags=["系统"], summary="健康检查")
def health_check() -> dict[str, str]:
    """供本地验证和部署探针检查应用是否已启动。"""

    return {"status": "ok"}
