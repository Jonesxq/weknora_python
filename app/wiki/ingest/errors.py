"""Wiki 摄取编排层共享异常。"""


class WikiBatchBusy(RuntimeError):
    """同一知识库已有批次持有执行锁。"""
