"""runner 核心测试:加载、执行、checkpoint resume、retry 复用、错误、DAG 校验。"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from easyflow import Runner, Checkpoint
from easyflow.event import WorkflowJobEvent
from easyflow.loader import load_flow, FlowLoadError


EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "quickstart_flow"


def _track_calls(runner: Runner) -> list[str]:
    """给每个 node.run 套一层计数,返回 calls 列表(引用,后续 append)。"""
    calls: list[str] = []

    def wrap(sd):
        orig = sd.node.run

        def wrapped(ctx):
            calls.append(sd.id)
            return orig(ctx)

        sd.node.run = wrapped

    for sd in runner.steps.values():
        wrap(sd)
    return calls


def _collect_events(runner: Runner, only: set[str] | None = None) -> list[WorkflowJobEvent]:
    """同步跑完 runner(自动 resume checkpoint),返回事件列表。"""
    events: list[WorkflowJobEvent] = []

    async def drive():
        async for ev in runner.run(only=only):
            events.append(ev)
            if ev.type == "checkpoint":
                runner.resume()

    asyncio.run(drive())
    return events


def test_load_quickstart_flow():
    flow, steps, _ = load_flow(str(EXAMPLE))
    assert flow.id == "quickstart_flow"
    assert set(steps.keys()) == {"fetch", "process", "review", "export"}
    assert steps["review"].checkpoint == Checkpoint.AFTER


def test_run_full_flow_with_auto_resume():
    runner = Runner.load(str(EXAMPLE))
    calls = _track_calls(runner)
    events = _collect_events(runner)

    types = [e.type for e in events]
    assert types[0] == "trace"
    assert types[-1] == "end"
    assert "checkpoint" in types
    assert calls == ["fetch", "process", "review", "export"]
    assert all(s.status == "done" for s in runner.state.steps.values())
    assert runner.artifacts["export"]["exported"] is True


def test_retry_reuses_upstream_artifacts():
    """retry review 后 fetch/process 不重跑,review/export 跑 2 次。"""
    runner = Runner.load(str(EXAMPLE))
    calls = _track_calls(runner)
    events: list[WorkflowJobEvent] = []

    async def drive():
        first_checkpoint = True
        async for ev in runner.run():
            events.append(ev)
            if ev.type == "checkpoint":
                if first_checkpoint:
                    runner.retry("review")
                    first_checkpoint = False
                else:
                    runner.resume()

    asyncio.run(drive())
    assert calls.count("fetch") == 1
    assert calls.count("process") == 1
    assert calls.count("review") == 2
    assert calls.count("export") == 1
    assert runner.state.status == "done"


def test_abort_at_checkpoint():
    runner = Runner.load(str(EXAMPLE))
    events: list[WorkflowJobEvent] = []

    async def drive():
        async for ev in runner.run():
            events.append(ev)
            if ev.type == "checkpoint":
                runner.abort()

    asyncio.run(drive())
    types = [e.type for e in events]
    assert "error" in types
    assert types[-1] == "end"
    assert runner.state.status == "error"


def test_step_error_propagates(tmp_path: Path):
    flow_dir = tmp_path / "bad"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from easyflow import flow, edge\n"
        "@flow(id='bad')\n"
        "class F:\n"
        "    nodes=['boom']\n"
        "    edges=[]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "boom.py").write_text(
        "from easyflow import Node\n"
        "class Boom(Node):\n"
        "    id='boom'\n"
        "    def run(self, ctx):\n"
        "        raise ValueError('炸了')\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(flow_dir))
    events = _collect_events(runner)
    assert any(e.type == "error" and "炸了" in (e.message or "") for e in events)
    assert runner.state.status == "error"


def test_cycle_detected(tmp_path: Path):
    flow_dir = tmp_path / "cyc"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from easyflow import flow, edge\n"
        "@flow(id='cyc')\n"
        "class F:\n"
        "    nodes=['a','b']\n"
        "    edges=[edge('a','b'), edge('b','a')]\n",
        encoding="utf-8",
    )
    for n in ("a", "b"):
        (flow_dir / "nodes" / f"{n}.py").write_text(
            f"from easyflow import Node\n"
            f"class N(Node):\n"
            f"    id='{n}'\n"
            f"    def run(self, ctx): return {{}}\n",
            encoding="utf-8",
        )
    with pytest.raises(FlowLoadError, match="有环"):
        load_flow(str(flow_dir))
