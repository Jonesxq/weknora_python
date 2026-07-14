"""Wiki 服务可映射为 HTTP 响应的领域错误。"""


class WikiError(RuntimeError):
    def __init__(self, code: str, message: str, status_code: int) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


class WikiNotFoundError(WikiError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message, 404)


class WikiConflictError(WikiError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message, 409)


class WikiVersionConflictError(WikiConflictError):
    def __init__(self) -> None:
        super().__init__("VERSION_CONFLICT", "页面已被其他请求修改，请刷新后重试")


class WikiPermissionError(WikiError):
    def __init__(self) -> None:
        super().__init__("WIKI_WRITE_FORBIDDEN", "当前身份没有 Wiki 写权限", 403)


class WikiValidationError(WikiError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message, 422)
