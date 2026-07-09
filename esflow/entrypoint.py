"""skill 脚本入口:统一处理 --out/--resume、预检、断点提示与退出码。"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from .check import FlowCheckError, pass_check
from .event import esflow_event
from .runner import Runner


NodeArgsBuilder = Callable[[argparse.Namespace], dict[str, dict[str, Any]] | None]
Check = Callable[[], object]


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """给 skill 脚本补齐框架级产物目录参数。"""
    parser.add_argument(
        "--out",
        default=None,
        metavar="DIR",
        help="指定完整产物目录,节点产物写入 DIR/<node>/",
    )
    parser.add_argument(
        "--resume",
        default=None,
        metavar="DIR",
        help="从 job_dir 续跑 TO_AGENT 节点",
    )


async def run_flow_script(
    flow_dir: str | Path,
    *,
    node_args_builder: NodeArgsBuilder | None = None,
    checks: Sequence[Check] = (),
    parser: argparse.ArgumentParser | None = None,
    argv: Sequence[str] | None = None,
) -> int:
    """运行 skill flow,让脚本天然支持 --out 与 --resume。"""
    parser = parser or argparse.ArgumentParser()
    _add_common_args(parser)
    args = parser.parse_args(argv)

    # resume 默认只继承 job metadata,避免 parser 默认值覆盖首跑入参。
    node_args = (
        None if args.resume or node_args_builder is None
        else node_args_builder(args)
    )
    try:
        pass_check(*checks)
    except FlowCheckError as exc:
        print(exc, file=sys.stderr)
        return 1

    job_dir = Path(args.resume or args.out) if args.resume or args.out else None
    runner = Runner.load(str(flow_dir), job_dir=job_dir, node_args=node_args)
    events, kind, break_event = await runner.run_to_break(resume=bool(args.resume))
    for event in events:
        esflow_event(event)

    if kind == "to_agent" and break_event is not None:
        script = sys.argv[0] or "run.py"
        print(
            Runner.to_agent_hint(
                break_event,
                resume_cmd=f"python3 {script} --resume {{job_dir}}",
            ),
            file=sys.stderr,
        )
        return 2
    if kind == "error":
        return 1
    return 0 if runner.state.status != "error" else 1
