"""JobEvent 协议:Runner 产出的唯一事件流,借鉴当前 TS 项目 event.ts。

事件类型:
- trace:      节点状态变更(queued/running/done/error/skipped)
- delta:      节点产出增量文本
- checkpoint: 节点跑到暂停点(TO_HUMAN 等 resume/retry/abort,TO_AGENT 等外部 agent 写产物 + --resume)
- final:      节点最终 artifact
- error:      错误
- end:        job 结束
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


def _import_exception_class(exc_type: str) -> type[BaseException] | None:
    """按 'module.ClassName' 全名 import 异常类。失败返回 None(降级用)。"""
    if "." not in exc_type:
        return None
    module_name, _, cls_name = exc_type.rpartition(".")
    try:
        import importlib
        module = importlib.import_module(module_name)
        cls = getattr(module, cls_name, None)
    except Exception:
        return None
    if isinstance(cls, type) and issubclass(cls, BaseException):
        return cls
    return None


TraceStatus = Literal["queued", "running", "done", "error", "skipped"]


@dataclass
class JobEvent:
    """统一事件信封。type 决定哪些字段有效。"""

    type: Literal["trace", "delta", "checkpoint", "final", "error", "end"]
    at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    # 公共可选字段:run_id 标识一次节点运行(普通节点 = base id,副本 = replica_id)
    run_id: str | None = None
    # trace
    status: TraceStatus | None = None
    detail: str | None = None
    # delta
    text: str | None = None
    # checkpoint / final
    artifact: Any | None = None
    # TO_AGENT checkpoint:框架填好的介入指引(node_dir/upstream_artifact/job_dir),skill 直接印
    resume_hint: dict[str, Any] | None = None
    # error
    message: str | None = None
    # error 透传异常 __dict__(裸 Exception 为 None);跨进程降级用
    exc_attrs: dict[str, Any] | None = None
    # error 同进程:原异常引用,as_exception() 优先返回它;不进 json 序列化路径
    exc: BaseException | None = field(default=None, repr=False)
    # error 异常类全名(module.ClassName),供调试/日志/跨进程降级还原
    exc_type: str | None = None

    def as_exception(self) -> BaseException:
        """还原成原异常。

        - 同进程:优先返回 exc 引用(类型/属性/身份全保留)
        - 跨进程(无 exc,有 exc_type + exc_attrs):按 exc_type 全名 import 类,
          __new__ 绕过 __init__ 实例化,回填 __dict__ + 设 args=(message,)
          让 str() 输出原 message;import 失败降级 RuntimeError
        - 完全降级(无 exc_type):RuntimeError(message)
        """
        if self.exc is not None:
            return self.exc
        if self.exc_type:
            cls = _import_exception_class(self.exc_type)
            if cls is not None:
                inst = cls.__new__(cls)
                if self.exc_attrs:
                    inst.__dict__.update(self.exc_attrs)
                inst.args = (self.message or "",)
                return inst
        return RuntimeError(self.message or "未知错误")


# 构造便捷函数

def trace(run_id: str, status: TraceStatus, detail: str = "") -> JobEvent:
    return JobEvent(type="trace", run_id=run_id, status=status, detail=detail)


def delta(run_id: str, text: str) -> JobEvent:
    return JobEvent(type="delta", run_id=run_id, text=text)


def checkpoint(run_id: str, artifact: Any) -> JobEvent:
    return JobEvent(type="checkpoint", run_id=run_id, artifact=artifact)


def final(run_id: str, artifact: Any) -> JobEvent:
    return JobEvent(type="final", run_id=run_id, artifact=artifact)


def error(
    run_id: str | None,
    message: str,
    exc_attrs: dict[str, Any] | None = None,
    exc: BaseException | None = None,
    exc_type: str | None = None,
) -> JobEvent:
    """构造 error event。exc/exc_type 由 runner 透传,as_exception() 用。"""
    return JobEvent(
        type="error",
        run_id=run_id,
        message=message,
        exc_attrs=exc_attrs,
        exc=exc,
        exc_type=exc_type,
    )


def end() -> JobEvent:
    return JobEvent(type="end")


def esflow_event(event: JobEvent) -> None:
    """按事件类型打印一行,cli / skill run.py / view 共用的统一消费入口。

    所有输出走 stderr,保留 stdout 给管道数据。checkpoint 只打印 artifact,
    不打印交互提示(交互由调用方自行处理)。
    """
    if event.type == "trace":
        print(f"[{event.run_id}] {event.status}: {event.detail}", file=sys.stderr)
    elif event.type == "delta":
        print(f"[{event.run_id}] {event.text}", end="", file=sys.stderr)
    elif event.type == "checkpoint":
        print(f"\n[checkpoint] {event.run_id} artifact:", file=sys.stderr)
        print(json.dumps(event.artifact, ensure_ascii=False, indent=2, default=str), file=sys.stderr)
    elif event.type == "final":
        print(f"[{event.run_id}] artifact: {event.artifact}", file=sys.stderr)
    elif event.type == "error":
        print(f"[error] {event.run_id}: {event.message}", file=sys.stderr)
    elif event.type == "end":
        print("[end]", file=sys.stderr)
