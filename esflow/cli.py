"""esflow CLI 调试便捷入口(非核心,主用法是库式 import)。

    esflow new my_skill                       # 生成 skill 模板(含可跑 demo flow)
    esflow run ./my_flow                      # 全跑,checkpoint TO_HUMAN 时 stdin 等命令
    esflow run ./my_flow --out ./runs/a       # 产物写入指定目录,用于后续续跑
    esflow run ./my_flow --out ./runs/a --from translate  # 从指定节点续跑到末端
    esflow run ./my_flow --out ./runs/a --from-depth 2    # 从拓扑深度 2 起续跑到末端
    esflow run ./my_flow --node worker#2      # 单调试指定副本及其上游
    esflow debug ./my_flow                    # 调试模式:产物固定到 /tmp/esflow/debug/<flow_id>/,持久化 artifact
    esflow debug ./my_flow --node worker#2    # 单调试复用上游持久化产物,只跑指定节点
    esflow view ./my_flow                     # Web 调试界面(浏览器,SSE 实时推送)

TO_AGENT(agent 介入)链路:
    esflow run ./agent_flow --out ./runs/a    # 跑到 TO_AGENT 节点退出,返回码 2
    # agent 读 stderr 拿上游产物,写文件到 ./runs/a/<to_agent 节点>/
    esflow run ./agent_flow --resume ./runs/a # 框架扫文件构造 artifact + deliver 校验,跑下游
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from .event import esflow_event
from .node import Checkpoint
from .runner import Runner
from .state import JobStatus


# CLI 返回码:end=0,error=1,待 agent 介入=2,Ctrl+C=130
EXIT_END = 0
EXIT_ERROR = 1
EXIT_TO_AGENT = 2
EXIT_INTERRUPTED = 130


def _handle_checkpoint_command(runner: Runner, command: str, run_id: str | None) -> None:
    """处理 checkpoint TO_HUMAN 短命令:c=继续,r=重试当前,a=中止。"""
    cmd = command.strip().lower()
    if not cmd or cmd == "c":
        runner.resume()
    elif cmd == "r":
        runner.retry(run_id or "")
    elif cmd == "a":
        runner.abort()
    else:
        print(f"未知命令:{command},默认 continue")
        runner.resume()


async def _run_cli(
    flow_dir: str,
    only: set[str] | None,
    out: str | None = None,
    from_node: str | None = None,
    from_depth: int | None = None,
) -> int:
    """跑 flow,checkpoint TO_HUMAN 走 stdin,checkpoint TO_AGENT 直接退出返回 2。"""
    runner = Runner.load(flow_dir, job_dir=Path(out) if out else None)
    async for event in runner.run(only=only, from_node=from_node, from_depth=from_depth):
        esflow_event(event)
        if event.type == "checkpoint":
            node = runner.runs.get(event.run_id)
            if node is not None and node.checkpoint == Checkpoint.TO_AGENT:
                # TO_AGENT:框架已填 resume_hint,cli 拼好自己的续跑命令打印
                resume_cmd = f"esflow run {flow_dir} --resume {{job_dir}}"
                print(Runner.to_agent_hint(event, resume_cmd=resume_cmd), file=sys.stderr)
                return EXIT_TO_AGENT
            # 普通 TO_HUMAN checkpoint:stdin 等人机命令
            print("(c) continue, (r) retry, (a) abort:", end=" ", flush=True)
            line = sys.stdin.readline().strip()
            _handle_checkpoint_command(runner, line, event.run_id)
        if event.type == "end":
            return EXIT_END if runner.state.status != JobStatus.ERROR else EXIT_ERROR
    return EXIT_END


async def _run_resume(job_dir: str) -> int:
    """--resume:从 _break_to_agent.json 续跑,TO_AGENT 节点扫文件构造 artifact。"""
    job_path = Path(job_dir)
    if not job_path.exists():
        print(f"job 目录不存在:{job_path}", file=sys.stderr)
        return EXIT_ERROR
    # job_dir 命名约定:output_root/<flow_id>/<job_id> 或用户 --out 指定
    # flow_id 是 job_dir 的父目录名,flow_dir 未知 → 用 flow.py 反查不可行
    # 简化:--resume 要求 job_dir 是 --out 指定的扁平目录,flow_dir 通过
    # job_dir 同目录的 _flow_dir 指针文件找回(首次跑时记录)
    flow_dir = _lookup_flow_dir(job_path)
    if flow_dir is None:
        print(f"找不到 flow 目录,首次跑请用 esflow run <flow> --out <path>", file=sys.stderr)
        return EXIT_ERROR
    runner = Runner.load(flow_dir, job_dir=job_path)
    if not runner.has_break_to_agent():
        print(f"无待完成的 TO_AGENT 节点:{job_path}", file=sys.stderr)
        return EXIT_ERROR
    resume_cmd = f"esflow run {flow_dir} --resume {{job_dir}}"
    events, kind, break_event = await runner.run_to_break(resume=True)
    for ev in events:
        esflow_event(ev)
    if kind == "to_agent" and break_event is not None:
        print(Runner.to_agent_hint(break_event, resume_cmd=resume_cmd), file=sys.stderr)
        return EXIT_TO_AGENT
    if kind == "error" and break_event is not None:
        return EXIT_ERROR
    return EXIT_END if runner.state.status != JobStatus.ERROR else EXIT_ERROR


def _lookup_flow_dir(job_dir: Path) -> str | None:
    """从 job_dir 读 _flow_dir.txt 找回 flow 目录绝对路径。"""
    pointer = job_dir / "_flow_dir.txt"
    if not pointer.exists():
        return None
    try:
        return pointer.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _record_flow_dir(job_dir: Path, flow_dir: str) -> None:
    """首次跑 --out 时记录 flow 目录绝对路径,供 --resume 找回。"""
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "_flow_dir.txt").write_text(
        str(Path(flow_dir).resolve()), encoding="utf-8"
    )


def cmd_run(args) -> int:
    # --resume:从 job_dir 续跑 TO_AGENT 节点,不需要 flow_dir 参数
    if args.resume:
        return asyncio.run(_run_resume(args.resume))

    if not args.flow_dir:
        print("需要 flow_dir 参数,或用 --resume <job_dir> 续跑", file=sys.stderr)
        return EXIT_ERROR

    only = set(args.node) if args.node else None
    from_node = args.from_node or None
    from_depth = args.from_depth if args.from_depth is not None else None
    if from_node and not args.out:
        print("--from 需要同时指定 --out", file=sys.stderr)
        return EXIT_ERROR
    if from_depth is not None and not args.out:
        print("--from-depth 需要同时指定 --out", file=sys.stderr)
        return EXIT_ERROR

    # 防误跑:job_dir 下有未完成 TO_AGENT 节点时,要求显式 --resume
    if args.out:
        out_path = Path(args.out)
        if out_path.exists() and (out_path / "_break_to_agent.json").exists():
            print(
                f"有未完成的 TO_AGENT 节点,先完成 agent 工作后续跑:\n"
                f"  esflow run {args.flow_dir} --resume {out_path}",
                file=sys.stderr,
            )
            return EXIT_ERROR
        _record_flow_dir(out_path, args.flow_dir)

    if from_node:
        pre = Runner.load(args.flow_dir, job_dir=Path(args.out))
        if from_node not in pre.runs:
            print(f"未知节点:{from_node}", file=sys.stderr)
            return EXIT_ERROR
        missing = pre.missing_upstream({from_node})
        if missing:
            print(f"上游产物缺失:{' '.join(missing)}", file=sys.stderr)
            print(
                f"先全跑落产物:esflow run {args.flow_dir} --out {args.out}",
                file=sys.stderr,
            )
            return EXIT_ERROR
    if from_depth is not None:
        pre = Runner.load(args.flow_dir, job_dir=Path(args.out))
        if from_depth < 0 or from_depth > pre.max_depth:
            print(
                f"--from-depth 越界:{from_depth},有效范围 [0, {pre.max_depth}]",
                file=sys.stderr,
            )
            return EXIT_ERROR
        target = pre._target_by_depth(from_depth)
        missing = pre.missing_upstream(target)
        if missing:
            print(f"上游产物缺失:{' '.join(missing)}", file=sys.stderr)
            print(
                f"先全跑落产物:esflow run {args.flow_dir} --out {args.out}",
                file=sys.stderr,
            )
            return EXIT_ERROR
    return asyncio.run(_run_cli(args.flow_dir, only, args.out, from_node, from_depth))


def cmd_debug(args) -> int:
    from .htmlview import run_html_view  # 延迟导入,debug 时才需要

    only = set(args.node) if args.node else None
    # 单点调试预检:上游产物必须已在 debug 目录,否则 view 打开无节点可跑
    if only:
        pre = Runner.load(args.flow_dir, debug=True)
        if args.clear:
            pre.clear_debug()
        missing = pre.missing_upstream(only)
        if missing:
            print(f"上游产物缺失:{' '.join(missing)}", file=sys.stderr)
            print(
                f"先全跑落产物:esflow debug {args.flow_dir}",
                file=sys.stderr,
            )
            return 1
    return asyncio.run(
        run_html_view(args.flow_dir, debug=True, only=only, clear=args.clear)
    )


def cmd_view(args) -> int:
    from .htmlview import run_html_view  # 延迟导入,view 时才需要

    return asyncio.run(run_html_view(args.flow_dir))


# —— esflow new:生成 skill 模板 ——

_SKILL_MD = """---
name: {name}
description: 一句话描述这个 skill 做什么
---

# {name}

## 使用

```bash
python3 scripts/run.py
```

## 节点

- `fetch`:生成简单 html,用 `self.depth` 决定标题层级,落盘到 output_dir
- `analyze`:读取上游 html,统计大小
- `report`:基于分析结果生成报告,落盘到 output_dir/report.md

## 预检

`run.py` 启动前用 `pass_check` 跑检查函数,任一失败聚合抛 `FlowCheckError` 不启动 runner。
新增检查:在 `run.py` 加 `() -> str | None` 函数,传给 `pass_check`。

## 扩展

在 `scripts/nodes/` 下加 Node 子类,在 `scripts/flow.py` 里声明 edges/replicas/dynamic。
节点产物文件写 `self.output_dir`,run 返回的 dict 登记路径供 view 展示与下游读取。
详见项目根 README。
"""

_FLOW_PY = '''"""{name}:抓取 → 分析 → 报告 最小示例。"""

from esflow import flow, edge


@flow(id="{name}", title="{title}")
class {cls}:
    nodes = ["fetch", "analyze", "report"]
    edges = [
        edge("fetch", "analyze"),
        edge("analyze", "report"),
    ]
'''

_FETCH_PY = '''"""fetch 节点:生成一个简单 html,用 self.depth 决定标题层级。"""

from esflow import Node


class Fetch(Node):
    id = "fetch"
    title = "生成 HTML"

    def run(self, ctx) -> dict:
        level = self.depth + 1
        html = f"<!doctype html><meta charset=utf-8><h{level}>Hello esflow</h{level}>"
        path = str(self.output_dir / "page.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        return {"html_path": path, "depth": self.depth}
'''

_ANALYZE_PY = '''"""analyze 节点:读取上游 html,统计大小。"""

from esflow import Node


class Analyze(Node):
    id = "analyze"
    title = "分析 HTML"

    def run(self, ctx) -> dict:
        upstream = ctx.get("fetch")
        path = upstream["html_path"]
        with open(path, encoding="utf-8") as f:
            content = f.read()
        return {
            "size": len(content),
            "depth": upstream["depth"],
            "html_path": path,
        }
'''

_REPORT_PY = '''"""report 节点:基于分析结果生成报告,落盘到 output_dir。"""

from esflow import Node


class Report(Node):
    id = "report"
    title = "生成报告"

    def run(self, ctx) -> dict:
        analysis = ctx.get("analyze")
        content = f"""# 分析报告

- html 大小: {analysis['size']} 字节
- 生成节点深度: {analysis['depth']}
- html 路径: {analysis['html_path']}"""
        path = str(self.output_dir / "report.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return {"report_path": path, "summary": content}
'''

_RUN_PY = '''#!/usr/bin/env python3
"""直接跑:python3 scripts/run.py

启动前 pass_check 预检,任一失败聚合抛 FlowCheckError,不启动 runner。
"""

import asyncio
import sys
from pathlib import Path

from esflow import Runner, esflow_event
from esflow.check import pass_check


def check_python_version() -> str | None:
    """Python >= 3.10(esflow 用了 X | Y 类型语法)。"""
    return None if sys.version_info >= (3, 10) else (
        f"需要 Python >= 3.10,当前 {sys.version_info.major}.{sys.version_info.minor}"
    )


async def main():
    pass_check(check_python_version)
    runner = Runner.load(str(Path(__file__).parent))
    async for event in runner.run():
        esflow_event(event)


if __name__ == "__main__":
    asyncio.run(main())
'''


def _pascal(name: str) -> str:
    """name 转 PascalCase(去掉非字母数字,分段首字母大写)。"""
    parts = [p for p in name.replace("-", "_").split("_") if p]
    return "".join(p[:1].upper() + p[1:] for p in parts) or "Flow"


def cmd_new(args) -> int:
    root = Path(args.name)              # 目录按完整路径创建(可含子目录)
    name = Path(args.name).name         # id/类名/SKILL 标题取末段,避免斜杠
    if not name:
        print(f"无效 name:{args.name}", file=sys.stderr)
        return 1
    if root.exists():
        print(f"已存在:{root}", file=sys.stderr)
        return 1

    scripts = root / "scripts"
    nodes = scripts / "nodes"
    nodes.mkdir(parents=True)

    cls = _pascal(name)
    (root / "SKILL.md").write_text(
        _SKILL_MD.format(name=name), encoding="utf-8"
    )
    (scripts / "flow.py").write_text(
        _FLOW_PY.format(name=name, title=name, cls=cls), encoding="utf-8"
    )
    (scripts / "run.py").write_text(_RUN_PY, encoding="utf-8")
    os.chmod(scripts / "run.py", 0o755)
    (nodes / "fetch.py").write_text(_FETCH_PY, encoding="utf-8")
    (nodes / "analyze.py").write_text(_ANALYZE_PY, encoding="utf-8")
    (nodes / "report.py").write_text(_REPORT_PY, encoding="utf-8")

    print(f"已创建:{root}")
    print(f"  {root / 'SKILL.md'}")
    print(f"  {scripts / 'flow.py'}")
    print(f"  {scripts / 'run.py'}")
    print(f"  {nodes / 'fetch.py'}")
    print(f"  {nodes / 'analyze.py'}")
    print(f"  {nodes / 'report.py'}")
    print(f"跑起来:python3 {scripts / 'run.py'}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="esflow", description="轻量 DAG workflow 调试入口")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_new = sub.add_parser("new", help="生成 skill 模板(含可跑 demo flow)")
    p_new.add_argument("name", help="skill 目录名")
    p_new.set_defaults(func=cmd_new)

    p_run = sub.add_parser("run", help="跑 flow;支持指定产物目录与定点续跑")
    p_run.add_argument(
        "flow_dir",
        nargs="?",
        default=None,
        help="flow 目录路径(--resume 模式可省略)",
    )
    p_run.add_argument(
        "--resume",
        dest="resume",
        default=None,
        metavar="DIR",
        help="从 job_dir 续跑 TO_AGENT 节点:框架扫产物文件构造 artifact + deliver 校验,跑下游",
    )
    run_scope = p_run.add_mutually_exclusive_group()
    run_scope.add_argument(
        "--node", "-n",
        nargs="+",
        default=None,
        metavar="NODE",
        help="只跑指定节点及其上游(空格分隔多个)",
    )
    run_scope.add_argument(
        "--from",
        dest="from_node",
        default=None,
        metavar="NODE",
        help="从指定节点续跑到末端,上游从 --out 目录复用",
    )
    run_scope.add_argument(
        "--from-depth",
        dest="from_depth",
        type=int,
        default=None,
        metavar="N",
        help="从拓扑深度 N 起重跑到末端,上游 depth<N 从 --out 目录复用",
    )
    p_run.add_argument(
        "--out",
        default=None,
        metavar="DIR",
        help="指定完整产物目录,节点产物写入 DIR/<node>/",
    )
    p_run.set_defaults(func=cmd_run)

    p_debug = sub.add_parser(
        "debug",
        help="调出 view 调试界面:产物固定到 /tmp/esflow/debug/<flow_id>/,持久化复用",
    )
    p_debug.add_argument("flow_dir", help="flow 目录路径")
    p_debug.add_argument(
        "--node", "-n",
        nargs="+",
        default=None,
        metavar="NODE",
        help="单点调试:上游从磁盘复用,在指定节点前暂停等 resume(空格分隔多个)",
    )
    p_debug.add_argument(
        "--clear",
        action="store_true",
        help="清空 debug 目录持久化产物后从头跑",
    )
    p_debug.set_defaults(func=cmd_debug)

    p_view = sub.add_parser("view", help="浏览器调试界面")
    p_view.add_argument("flow_dir", help="flow 目录路径")
    p_view.set_defaults(func=cmd_view)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        # Ctrl+C 静默退出(view/debug 长跑场景常见),不打印 traceback
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    sys.exit(main())
