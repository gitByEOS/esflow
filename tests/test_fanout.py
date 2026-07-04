"""fanout 并行测试:副本展开、并行调度、accept/deliver 校验、only 单调试。"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from easyflow import Runner
from easyflow.event import JobEvent
from easyflow.loader import load_flow, FlowLoadError


FANOUT = Path(__file__).resolve().parent.parent / "examples" / "fanout_flow"


def _events(runner: Runner, only: set[str] | None = None) -> list[JobEvent]:
    out: list[JobEvent] = []

    async def drive():
        async for ev in runner.run(only=only):
            out.append(ev)

    asyncio.run(drive())
    return out


def test_load_fanout_expands_replicas():
    flow, runs, _ = load_flow(str(FANOUT))
    assert flow.id == "fanout_flow"
    # worker 展开成 5 个副本
    worker_ids = {rid for rid in runs if rid.startswith("worker#")}
    assert worker_ids == {f"worker#{i}" for i in range(5)}
    assert "fetch" in runs and "merge" in runs
    # 边扇出/扇入:fetch→5 worker,5 worker→merge
    from_ids = {e.from_ for e in flow.edges}
    to_ids = {e.to for e in flow.edges}
    assert "fetch" in from_ids and "merge" in to_ids
    assert all(f"worker#{i}" in from_ids for i in range(5))
    assert all(f"worker#{i}" in to_ids for i in range(5))


def test_run_fanout_parallel():
    runner = Runner.load(str(FANOUT))
    events = _events(runner)
    assert events[-1].type == "end"
    # 5 worker 都 done
    for i in range(5):
        assert runner.state.runs[f"worker#{i}"].status == "done"
    # merge 汇总 10 题
    assert runner.artifacts["merge"]["total"] == 10
    # 副本 index 注入正确,各分 2 题
    for i in range(5):
        assert runner.artifacts[f"worker#{i}"]["worker_index"] == i
        assert len(runner.artifacts[f"worker#{i}"]["results"]) == 2


def test_only_single_replica():
    """单调试:只跑 worker#2 及其上游 fetch,跳过其他副本和 merge。"""
    runner = Runner.load(str(FANOUT))
    events = _events(runner, only={"worker#2"})
    assert events[-1].type == "end"
    assert runner.state.runs["fetch"].status == "done"
    assert runner.state.runs["worker#2"].status == "done"
    # 其他副本未跑
    for i in (0, 1, 3, 4):
        assert runner.state.runs[f"worker#{i}"].status == "idle"
    # merge 是 worker 下游,不在 only target,未跑
    assert runner.state.runs["merge"].status == "idle"
    assert "merge" not in runner.artifacts


def test_accept_failure_emits_skip(tmp_path: Path):
    """accept 返回 False → emit skipped,节点不执行,artifact 为 None,下游可推进。"""
    d = tmp_path / "acc"
    (d / "nodes").mkdir(parents=True)
    (d / "flow.py").write_text(
        "from easyflow import flow, edge\n"
        "@flow(id='acc')\n"
        "class F:\n"
        "    nodes=['a','b']\n"
        "    edges=[edge('a','b')]\n",
        encoding="utf-8",
    )
    (d / "nodes" / "a.py").write_text(
        "from easyflow import Node\n"
        "class A(Node):\n"
        "    id='a'\n"
        "    def run(self, ctx): return {'ok': True}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "b.py").write_text(
        "from easyflow import Node\n"
        "class B(Node):\n"
        "    id='b'\n"
        "    def accept(self, ctx): return False\n"
        "    def run(self, ctx): return {'b': True}\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(d))
    events = _events(runner)
    assert any(
        e.type == "trace"
        and e.status == "skipped"
        and e.run_id == "b"
        for e in events
    )
    # b 未执行 run,artifact 为 None(占位),job 正常完成
    assert runner.state.runs["b"].status == "skipped"
    assert runner.artifacts.get("b") is None
    assert runner.state.status == "done"
    assert not any(e.type == "error" for e in events)


def test_deliver_failure_emits_error(tmp_path: Path):
    """deliver 返回 False → emit error,artifact 不入库。"""
    d = tmp_path / "del"
    (d / "nodes").mkdir(parents=True)
    (d / "flow.py").write_text(
        "from easyflow import flow, edge\n"
        "@flow(id='del')\n"
        "class F:\n"
        "    nodes=['x']\n"
        "    edges=[]\n",
        encoding="utf-8",
    )
    (d / "nodes" / "x.py").write_text(
        "from easyflow import Node\n"
        "class X(Node):\n"
        "    id='x'\n"
        "    def deliver(self, art): return False\n"
        "    def run(self, ctx): return {'x': 1}\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(d))
    events = _events(runner)
    assert any(
        e.type == "error" and "脱手确认失败" in (e.message or "") and e.run_id == "x"
        for e in events
    )
    assert runner.state.status == "error"
    assert "x" not in runner.artifacts


def test_replicas_count_invalid(tmp_path: Path):
    """replicas 数 <1 加载报错。"""
    d = tmp_path / "bad"
    (d / "nodes").mkdir(parents=True)
    (d / "flow.py").write_text(
        "from easyflow import flow, edge\n"
        "@flow(id='bad')\n"
        "class F:\n"
        "    nodes=['w']\n"
        "    edges=[]\n"
        "    replicas={'w': 0}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "w.py").write_text(
        "from easyflow import Node\n"
        "class W(Node):\n"
        "    id='w'\n"
        "    def run(self, ctx): return {}\n",
        encoding="utf-8",
    )
    with pytest.raises(FlowLoadError, match=">=1"):
        load_flow(str(d))
