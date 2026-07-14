"""文档解析接口模型。"""

from typing import Any

from pydantic import BaseModel, Field


class ParsedDocumentResponse(BaseModel):
    """上传文档解析成功后的响应。"""

    file_name: str = Field(description="原始文件名")
    content_type: str | None = Field(default=None, description="上传 MIME 类型")
    size_bytes: int = Field(ge=1, description="实际读取的文件字节数")
    content: str = Field(description="解析得到的 Markdown")
    images: dict[str, str] = Field(
        default_factory=dict,
        description="图片相对路径到 Base64 内容的映射",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="页数、扫描页数量等解析元数据",
    )
