"""FastAPI 应用入口。"""

from fastapi import FastAPI

from app.api.v1.router import api_router


app = FastAPI(
    title="Graph 文档解析 API",
    description="在当前 Python 进程内解析 PDF、DOCX、Markdown、Excel 和图片。",
    version="0.1.0",
)
app.include_router(api_router, prefix="/api/v1")


@app.get("/health", tags=["系统"], summary="健康检查")
def health_check() -> dict[str, str]:
    """供本地验证和部署探针检查应用是否已启动。"""

    return {"status": "ok"}
