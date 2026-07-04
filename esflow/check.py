"""启动预检:用户写检查函数,框架聚合执行。

skill 的 run.py 在 runner.run() 之前显式调用:

    from esflow import pass_check, CheckResult

    def check_port() -> str | None:
        '''返回 None=通过,返回字符串=失败原因'''
        import socket
        with socket.socket() as s:
            s.settimeout(1)
            try:
                s.connect(("localhost", 8080))
            except OSError:
                return "本地模型服务未监听 8080"
        return None

    def check_cli() -> CheckResult | None:
        import shutil
        if shutil.which("ffmpeg"):
            return None
        return CheckResult(
            reason="未安装 ffmpeg",
            fix="brew install ffmpeg          # macOS\n"
                "sudo apt install ffmpeg      # Debian/Ubuntu",
        )

    pass_check(check_port, check_cli)   # 任一失败 → 抛 FlowCheckError
    async for event in runner.run():
        ...

检查函数签名:() -> None | str | CheckResult(None 通过 / str 或 CheckResult 失败);抛异常也算失败。
str 视为只有 reason 的快捷;CheckResult 可带 fix 修复指引,聚合后随 reason 一起输出。
所有失败原因聚合后一次性抛 FlowCheckError,不逐个抛;reason 红色、fix 绿色高亮。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

_RED = "\033[31m"
_GREEN = "\033[32m"
_RESET = "\033[0m"


@dataclass
class CheckResult:
    """检查结果:reason=失败原因,fix=修复指引(多行字符串,可选)。"""

    reason: str
    fix: str | None = None


def _normalize(value: object) -> CheckResult:
    """str → CheckResult(reason=str);CheckResult 原样返回。"""
    if isinstance(value, CheckResult):
        return value
    return CheckResult(reason=str(value))


def _format_failure(name: str, result: CheckResult) -> str:
    """格式化单条失败:函数名 + 红色 reason,有 fix 则附绿色修复指引。"""
    head = f"- {name}: {_RED}{result.reason}{_RESET}"
    if not result.fix:
        return head
    fix_lines = "\n".join(f"    {_GREEN}{line}{_RESET}" for line in result.fix.splitlines())
    return f"{head}\n  修复:\n{fix_lines}"


class FlowCheckError(Exception):
    """预检失败:聚合所有失败原因与修复指引。"""

    def __init__(self, failures: list[str]) -> None:
        self.failures = failures
        super().__init__("预检失败:\n" + "\n".join(failures))


def pass_check(*checks: Callable[[], object]) -> None:
    """跑所有检查函数,全过才返回,否则聚合抛 FlowCheckError。

    每个检查函数返回 None 表示通过,返回 str/CheckResult 表示失败;抛异常也算失败(转成 CheckResult)。
    """
    failures: list[str] = []
    for fn in checks:
        try:
            result = fn()
        except Exception as exc:  # noqa: BLE001 预检要兜住用户函数任何异常
            result = CheckResult(reason=f"{type(exc).__name__}: {exc}")
        if result is None:
            continue
        failures.append(_format_failure(fn.__name__, _normalize(result)))
    if failures:
        raise FlowCheckError(failures)
