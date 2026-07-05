"""跨进程异常还原测试用的样本异常类。

as_exception() 跨进程按 exc_type 字符串 import 还原,需要一个模块级可 import 的类。
"""


class CliError(Exception):
    """带业务属性的异常:code + retryable,模拟 skill 自定义错误。"""

    def __init__(self, code: int, message: str, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
