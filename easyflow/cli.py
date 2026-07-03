"""easyflow CLI 调试便捷入口(非核心,主用法是库式 import)。

    easyflow new my_skill                       # 生成 skill 模板(含可跑 demo flow)
    easyflow run ./my_flow                      # 全跑,checkpoint 时 stdin 等命令
    easyflow run ./my_flow --node worker#2      # 单调试指定副本及其上游
    easyflow debug ./my_flow                    # 调试模式:产物固定到 /tmp/easyflow/debug/<flow_id>/,持久化 artifact
    easyflow debug ./my_flow --node worker#2    # 单调试复用上游持久化产物,只跑指定节点
    easyflow view ./my_flow                     # Web 调试界面(浏览器,SSE 实时推送)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from .event import easyflow_event
from .runner import Runner


async def _run_cli(flow_dir: str, only: set[str] | None) -> int:
    runner = Runner.load(flow_dir)
    async for event in runner.run(only=only):
        easyflow_event(event)
        if event.type == "checkpoint":
            print("输入 resume / retry <step> / abort:", end=" ", flush=True)
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
                f"先全跑落产物:easyflow debug {args.flow_dir}",
                file=sys.stderr,
            )
            return 1
    return asyncio.run(
        run_html_view(args.flow_dir, debug=True, only=only, clear=args.clear)
    )


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

from easyflow import flow, edge


@flow(id="{name}", title="{title}")
class {cls}:
    nodes = ["fetch", "analyze", "report"]
    edges = [
        edge("fetch", "analyze"),
        edge("analyze", "report"),
    ]
'''

_FETCH_PY = '''"""fetch 节点:生成一个简单 html,用 self.depth 决定标题层级。"""

from easyflow import Node


class Fetch(Node):
    id = "fetch"
    title = "生成 HTML"

    def run(self, ctx) -> dict:
        level = self.depth + 1
        html = f"<!doctype html><meta charset=utf-8><h{level}>Hello EasyFlow</h{level}>"
        path = str(self.output_dir / "page.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        return {"html_path": path, "depth": self.depth}
'''

_ANALYZE_PY = '''"""analyze 节点:读取上游 html,统计大小。"""

from easyflow import Node


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

from easyflow import Node


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

from easyflow import Runner, easyflow_event
from easyflow.check import pass_check


def check_python_version() -> str | None:
    """Python >= 3.10(easyflow 用了 X | Y 类型语法)。"""
    return None if sys.version_info >= (3, 10) else (
        f"需要 Python >= 3.10,当前 {sys.version_info.major}.{sys.version_info.minor}"
    )


async def main():
    pass_check(check_python_version)
    runner = Runner.load(str(Path(__file__).parent))
    async for event in runner.run():
        easyflow_event(event)


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

    p_debug = sub.add_parser(
        "debug",
        help="调出 view 调试界面:产物固定到 /tmp/easyflow/debug/<flow_id>/,持久化复用",
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
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
