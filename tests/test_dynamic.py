"""动态扇出测试:FanOut 运行时扩图、payload 注入、gather 收集、校验。"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from esflow import Runner
from esflow.event import JobEvent
from esflow.loader import load_flow, FlowLoadError


DYN = Path(__file__).resolve().parent.parent / "examples" / "fanout_dynamic"


def _events(runner: Runner, only: set[str] | None = None) -> list[JobEvent]:
    out: list[JobEvent] = []

    async def drive():
        async for ev in runner.run(only=only):
            out.append(ev)
            if ev.type == "checkpoint":
                runner.resume()

    asyncio.run(drive())
    return out


def test_load_dynamic_not_preinstantiated():
    """dynamic base 不预实例化,但参与 nodes 与无环校验。"""
    flow, runs, node_classes = load_flow(str(DYN))
    assert flow.id == "fanout_dynamic"
    assert "worker" in flow.dynamic
    # worker 未预实例化
    assert not any(rid.startswith("worker#") for rid in runs)
    assert "worker" not in runs
    # 其他节点正常
    assert {"ingest", "split", "merge"} <= set(runs)
    # node_classes 含 worker 类供 runner 实例化
    assert "worker" in node_classes


def test_run_dynamic_fanout():
    """split FanOut 按章节数展开 worker 副本,merge gather 汇总。"""
    runner = Runner.load(str(DYN))
    events = _events(runner)
    assert events[-1].type == "end"
    # 4 章 → 4 个 worker 副本
    worker_ids = [rid for rid in runner.state.runs if rid.startswith("worker#")]
    assert len(worker_ids) == 4
    for rid in worker_ids:
        assert runner.state.runs[rid].status == "done"
    # merge 汇总 4 章
    assert runner.artifacts["merge"]["total_chapters"] == 4


def test_payload_injected_per_replica():
    """每个 worker 副本拿到自己那章 payload,不用 index 切片。"""
    runner = Runner.load(str(DYN))
    _events(runner)
    chapters = runner.artifacts["ingest"]["chapters"]
    for i, chapter in enumerate(chapters):
        art = runner.artifacts[f"worker#{i}"]
        assert art["chapter"] == chapter
        assert art["translated"] == f"[译文]{chapter}"


def test_gather_sorted_by_index():
    """ctx.gather 按 index 排序返回。"""
    runner = Runner.load(str(DYN))
    _events(runner)
    results = runner.artifacts["merge"]["results"]
    chapters = runner.artifacts["ingest"]["chapters"]
    # gather 顺序即 index 顺序,章节按原序
    assert [r["chapter"] for r in results] == chapters


def test_split_accept_failure(tmp_path: Path):
    """split accept 失败 → emit error,不扇出。"""
    d = tmp_path / "acc"
    (d / "nodes").mkdir(parents=True)
    (d / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='acc')\n"
        "class F:\n"
        "    nodes=['ing','split','w','m']\n"
        "    edges=[edge('ing','split'),edge('split','w'),edge('w','m')]\n"
        "    dynamic={'w'}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "ing.py").write_text(
        "from esflow import Node\n"
        "class Ing(Node):\n"
        "    id='ing'\n"
        "    def run(self, ctx): return {'tasks': []}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "split.py").write_text(
        "from esflow import Node, FanOut\n"
        "class Split(Node):\n"
        "    id='split'\n"
        "    def accept(self, ctx): return bool(ctx.get('ing')['tasks'])\n"
        "    def run(self, ctx): return FanOut('w', ctx.get('ing')['tasks'])\n",
        encoding="utf-8",
    )
    (d / "nodes" / "w.py").write_text(
        "from esflow import Node\n"
        "class W(Node):\n"
        "    id='w'\n"
        "    def run(self, ctx): return {'r': self.fanout_payload}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "m.py").write_text(
        "from esflow import Node\n"
        "class M(Node):\n"
        "    id='m'\n"
        "    def run(self, ctx): return {'all': ctx.gather('w')}\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(d))
    events = _events(runner)
    # split accept False → 被 skip,不扇出,job 正常完成
    assert any(
        e.type == "trace" and e.status == "skipped" and e.run_id == "split"
        for e in events
    )
    assert runner.state.runs["split"].status == "skipped"
    # 未扇出
    assert not any(rid.startswith("w#") for rid in runner.state.runs)
    assert runner.state.status == "done"


def test_replicas_dynamic_overlap_rejected(tmp_path: Path):
    """同一 base 同时声明 replicas 和 dynamic → 加载报错。"""
    d = tmp_path / "ovl"
    (d / "nodes").mkdir(parents=True)
    (d / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='ovl')\n"
        "class F:\n"
        "    nodes=['w']\n"
        "    edges=[]\n"
        "    replicas={'w': 2}\n"
        "    dynamic={'w'}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "w.py").write_text(
        "from esflow import Node\n"
        "class W(Node):\n"
        "    id='w'\n"
        "    def run(self, ctx): return {}\n",
        encoding="utf-8",
    )
    with pytest.raises(FlowLoadError, match="replicas 和 dynamic"):
        load_flow(str(d))


def test_dynamic_base_missing_node(tmp_path: Path):
    """dynamic 引用了未定义的节点 → 加载报错。"""
    d = tmp_path / "miss"
    (d / "nodes").mkdir(parents=True)
    (d / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='miss')\n"
        "class F:\n"
        "    nodes=['ing']\n"
        "    edges=[]\n"
        "    dynamic={'ghost'}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "ing.py").write_text(
        "from esflow import Node\n"
        "class Ing(Node):\n"
        "    id='ing'\n"
        "    def run(self, ctx): return {}\n",
        encoding="utf-8",
    )
    with pytest.raises(FlowLoadError, match="ghost"):
        load_flow(str(d))


def test_retry_dynamic_replica(tmp_path: Path):
    """merge checkpoint 后 retry 某动态副本,只重跑该副本及下游。"""
    d = tmp_path / "rt"
    (d / "nodes").mkdir(parents=True)
    (d / "flow.py").write_text(
        "from esflow import flow, edge, Checkpoint\n"
        "@flow(id='rt')\n"
        "class F:\n"
        "    nodes=['ing','split','w','m']\n"
        "    edges=[edge('ing','split'),edge('split','w'),edge('w','m')]\n"
        "    dynamic={'w'}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "ing.py").write_text(
        "from esflow import Node\n"
        "class Ing(Node):\n"
        "    id='ing'\n"
        "    def run(self, ctx): return {'t': [1, 2]}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "split.py").write_text(
        "from esflow import Node, FanOut\n"
        "class Split(Node):\n"
        "    id='split'\n"
        "    def run(self, ctx): return FanOut('w', ctx.get('ing')['t'])\n",
        encoding="utf-8",
    )
    (d / "nodes" / "w.py").write_text(
        "from esflow import Node\n"
        "class W(Node):\n"
        "    id='w'\n"
        "    calls = []\n"
        "    def run(self, ctx):\n"
        "        W.calls.append(self.fanout_payload)\n"
        "        return {'r': self.fanout_payload}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "m.py").write_text(
        "from esflow import Node, Checkpoint\n"
        "class M(Node):\n"
        "    id='m'\n"
        "    checkpoint=Checkpoint.TO_HUMAN\n"
        "    def run(self, ctx): return {'all': ctx.gather('w')}\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(d))
    W_cls = runner.node_classes["w"]

    events: list[JobEvent] = []

    async def drive():
        first = True
        async for ev in runner.run():
            events.append(ev)
            if ev.type == "checkpoint":
                if first:
                    runner.retry("w#1")   # 重跑 w#1 及下游 m
                    first = False
                else:
                    runner.resume()

    asyncio.run(drive())
    # w#0(payload=1) 跑 1 次,w#1(payload=2) 被 retry 跑 2 次
    assert W_cls.calls.count(1) == 1
    assert W_cls.calls.count(2) == 2
    assert runner.state.status == "done"
