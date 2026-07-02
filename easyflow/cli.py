"""easyflow CLI 调试便捷入口(非核心,主用法是库式 import)。

    easyflow new my_skill                       # 生成 skill 模板(含可跑 demo flow)
    easyflow run ./my_flow                      # 全跑,checkpoint 时 stdin 等命令
    easyflow run ./my_flow --node worker#2      # 单调试指定副本及其上游
    easyflow view ./my_flow                     # Web 调试界面(浏览器,SSE 实时推送)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from .runner import Runner


def _print_event(event) -> None:
    if event.type == "trace":
        print(f"[{event.step_id}] {event.status}: {event.detail}")
    elif event.type == "delta":
        print(f"[{event.step_id}] {event.text}", end="")
    elif event.type == "checkpoint":
        print(f"\n[checkpoint] {event.step_id} artifact:")
        print(json.dumps(event.artifact, ensure_ascii=False, indent=2, default=str))
        print("输入 resume / retry <step> / abort:", end=" ", flush=True)
    elif event.type == "final":
        pass  # final 已在 checkpoint 展示或随 trace
    elif event.type == "error":
        print(f"[error] {event.step_id}: {event.message}", file=sys.stderr)
    elif event.type == "end":
        print("[end]")


async def _run_cli(flow_dir: str, only: set[str] | None) -> int:
    runner = Runner.load(flow_dir)
    async for event in runner.run(only=only):
        _print_event(event)
        if event.type == "checkpoint":
            line = sys.stdin.readline().strip()
            if not line or line == "resume":
                runner.resume()
            elif line.startswith("retry"):
                parts = line.split()
                if len(parts) >= 2:
                    runner.retry(parts[1])
                else:
                    runner.retry(event.step_id)
            elif line == "abort":
                runner.abort()
            else:
                print(f"未知命令:{line},默认 resume")
                runner.resume()
        if event.type == "end":
            return 0 if runner.state.status != "error" else 1
    return 0


def cmd_run(args) -> int:
    only = set(args.node) if args.node else None
    return asyncio.run(_run_cli(args.flow_dir, only))


def cmd_view(args) -> int:
    from .htmlview import run_html_view  # 延迟导入,view 时才需要

    return asyncio.run(run_html_view(args.flow_dir))


# —— easyflow new:生成 skill 模板 ——

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

- `fetch`:抓取数据
- `analyze`:分析数据
- `report`:生成报告

## 扩展

在 `scripts/nodes/` 下加 Node 子类,在 `scripts/flow.py` 里声明 edges/replicas/dynamic。
详见项目根 README。
"""

_FLOW_PY = '''"""{name}:抓取 → 分析 → 报告 最小示例。"""

from easyflow import flow, edge


@flow(id="{name}", title="{title}")
class {cls}:
    nodes = ["fetch", "analyze", "report"]
    edges = [
        edge("fetch", "analyze"),
        edge("analyze", "report"),
    ]
'''

_FETCH_PY = '''"""fetch 节点:抓取数据。"""

from easyflow import Node


class Fetch(Node):
    id = "fetch"
    title = "抓取数据"

    def run(self, ctx) -> dict:
        return {"items": [1, 2, 3], "count": 3}
'''

_ANALYZE_PY = '''"""analyze 节点:分析上游数据。"""

from easyflow import Node


class Analyze(Node):
    id = "analyze"
    title = "分析数据"

    def run(self, ctx) -> dict:
        upstream = ctx.get("fetch")
        items = upstream["items"]
        return {"sum": sum(items), "count": upstream["count"]}
'''

_REPORT_PY = '''"""report 节点:基于分析结果生成报告。"""

from easyflow import Node


class Report(Node):
    id = "report"
    title = "生成报告"

    def run(self, ctx) -> dict:
        analysis = ctx.get("analyze")
        return {"report": f"共 {analysis['count']} 项,合计 {analysis['sum']}"}
'''

_RUN_PY = '''#!/usr/bin/env python3
"""直接跑:python3 scripts/run.py"""

import asyncio
from pathlib import Path

from easyflow import Runner


async def main():
    runner = Runner.load(str(Path(__file__).parent))
    async for event in runner.run():
        if event.type == "trace":
            print(f"[{event.step_id}] {event.status}: {event.detail}")
        elif event.type == "final":
            print(f"[{event.step_id}] artifact: {event.artifact}")
        elif event.type == "error":
            print(f"[error] {event.step_id}: {event.message}")
        elif event.type == "end":
            print("[end]")


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
    parser = argparse.ArgumentParser(prog="easyflow", description="轻量 DAG workflow 调试入口")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_new = sub.add_parser("new", help="生成 skill 模板(含可跑 demo flow)")
    p_new.add_argument("name", help="skill 目录名")
    p_new.set_defaults(func=cmd_new)

    p_run = sub.add_parser("run", help="跑 flow;传 --node 则只跑指定节点及其上游")
    p_run.add_argument("flow_dir", help="flow 目录路径")
    p_run.add_argument(
        "--node", "-n",
        nargs="+",
        default=None,
        metavar="NODE",
        help="只跑指定节点及其上游(空格分隔多个)",
    )
    p_run.set_defaults(func=cmd_run)

    p_view = sub.add_parser("view", help="浏览器调试界面")
    p_view.add_argument("flow_dir", help="flow 目录路径")
    p_view.set_defaults(func=cmd_view)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
