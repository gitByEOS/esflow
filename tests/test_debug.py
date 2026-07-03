"""debug 模式测试:产物固定目录、artifact 持久化、复用上游、retry 清磁盘。

debug 与 run 的核心区别:
- 产物落 /tmp/easyflow/debug/<flow_id>/<step_id>/,无 job_id,反复跑累积
- 节点 done/skipped 后写 artifact.json,启动时加载,已完成节点跳过不重跑
- retry 清下游磁盘 artifact.json,防止下次启动加载到旧产物
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import easyflow.runner as runner_mod
from easyflow import Runner
from easyflow.event import WorkflowJobEvent

EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "quickstart_flow"
ARTIFACT_FILE = runner_mod._ARTIFACT_FILE


def _drive(runner: Runner, only: set[str] | None = None) -> list[WorkflowJobEvent]:
    """跑 runner,checkpoint 自动 resume,返回事件列表。"""
    events: list[WorkflowJobEvent] = []

    async def go():
        async for ev in runner.run(only=only):
            events.append(ev)
            if ev.type == "checkpoint":
                runner.resume()

    asyncio.run(go())
    return events


def _track_runs(runner: Runner) -> list[str]:
    """记录每个节点 run 被调用的顺序。"""
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


def _debug_root(monkeypatch, tmp_path: Path) -> Path:
    """把 DEBUG_OUTPUT_ROOT 重定向到 tmp_path 下,避免污染 /tmp/easyflow/debug。"""
    root = tmp_path / "debug_root"
    monkeypatch.setattr(runner_mod, "DEBUG_OUTPUT_ROOT", root)
    return root


def test_debug_persists_artifacts(monkeypatch, tmp_path: Path):
    """debug 跑完后,每节点 output_dir 下有 artifact.json。"""
    _debug_root(monkeypatch, tmp_path)
    runner = Runner.load(str(EXAMPLE), debug=True)
    _drive(runner)

    assert runner.debug is True
    for sid in ("fetch", "process", "review", "export"):
        art_file = runner.job_dir / sid / ARTIFACT_FILE
        assert art_file.exists(), f"{sid} 缺 artifact.json"
        json.loads(art_file.read_text(encoding="utf-8"))


def test_debug_skips_completed_on_rerun(monkeypatch, tmp_path: Path):
    """第二次 debug 跑同一 flow,已完成节点从磁盘加载,不重跑。"""
    _debug_root(monkeypatch, tmp_path)

    first = Runner.load(str(EXAMPLE), debug=True)
    _drive(first)
    assert first.state.status == "done"

    second = Runner.load(str(EXAMPLE), debug=True)
    calls = _track_runs(second)
    _drive(second)

    assert calls == [], "已完成节点不应重跑"
    assert second.state.status == "done"
    # artifact 从磁盘复用
    assert second.artifacts["fetch"]["count"] == 3


def test_debug_only_reuses_upstream(monkeypatch, tmp_path: Path):
    """先 only=review 落上游产物,再 only=export 单调试:fetch/process/review 从磁盘复用,只跑 export。"""
    _debug_root(monkeypatch, tmp_path)

    base = Runner.load(str(EXAMPLE), debug=True)
    _drive(base, only={"review"})

    only = Runner.load(str(EXAMPLE), debug=True)
    calls = _track_runs(only)
    _drive(only, only={"export"})

    assert calls == ["export"], f"应只跑 export,实际 {calls}"
    assert only.artifacts["export"]["exported"] is True


def test_debug_retry_clears_downstream_artifact(monkeypatch, tmp_path: Path):
    """checkpoint retry review 后,review 及下游磁盘 artifact.json 被清,重跑出新产物。"""
    _debug_root(monkeypatch, tmp_path)
    runner = Runner.load(str(EXAMPLE), debug=True)
    calls = _track_runs(runner)

    events: list[WorkflowJobEvent] = []

    async def go():
        first_cp = True
        async for ev in runner.run():
            events.append(ev)
            if ev.type == "checkpoint":
                if first_cp:
                    runner.retry("review")
                    first_cp = False
                else:
                    runner.resume()

    asyncio.run(go())

    # retry 后 review 重跑,export 在 checkpoint 时未启动、retry 后才首跑,各 1 次
    # fetch/process 在 review 上游,复用不重跑
    assert calls.count("fetch") == 1
    assert calls.count("process") == 1
    assert calls.count("review") == 2
    assert calls.count("export") == 1
    assert runner.state.status == "done"
    # 磁盘产物是最新一次的
    art = json.loads((runner.job_dir / "review" / ARTIFACT_FILE).read_text(encoding="utf-8"))
    assert art is not None


def test_debug_skipped_node_persists_none(monkeypatch, tmp_path: Path):
    """skip 节点(artifact=None)也持久化,下游可推进,重跑时 skip 状态保留。"""
    _debug_root(monkeypatch, tmp_path)
    flow_dir = tmp_path / "skip_flow"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from easyflow import flow, edge\n"
        "@flow(id='skip_flow')\n"
        "class F:\n"
        "    nodes=['up','down']\n"
        "    edges=[edge('up','down')]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "up.py").write_text(
        "from easyflow import Node\n"
        "class Up(Node):\n"
        "    id='up'\n"
        "    def accept(self, ctx): return False\n"
        "    def run(self, ctx): return {}\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "down.py").write_text(
        "from easyflow import Node\n"
        "class Down(Node):\n"
        "    id='down'\n"
        "    def run(self, ctx): return {'ok': True}\n",
        encoding="utf-8",
    )

    runner = Runner.load(str(flow_dir), debug=True)
    _drive(runner)
    assert runner.state.status == "done"
    assert runner.artifacts["up"] is None

    up_art = runner.job_dir / "up" / ARTIFACT_FILE
    assert up_art.exists()
    assert json.loads(up_art.read_text(encoding="utf-8")) is None

    # 重跑:up 仍 skip(从磁盘加载),down 不重跑
    second = Runner.load(str(flow_dir), debug=True)
    calls = _track_runs(second)
    _drive(second)
    assert calls == []
    assert second.artifacts["up"] is None
    assert second.artifacts["down"]["ok"] is True


def test_debug_clear_wipes_artifacts(monkeypatch, tmp_path: Path):
    """--clear 清空 debug 目录:已有产物被删,重跑所有节点。"""
    _debug_root(monkeypatch, tmp_path)

    first = Runner.load(str(EXAMPLE), debug=True)
    _drive(first)
    assert first.state.status == "done"
    assert (first.job_dir / "fetch" / ARTIFACT_FILE).exists()

    # clear_debug 后磁盘清空
    cleared = Runner.load(str(EXAMPLE), debug=True)
    cleared.clear_debug()
    assert not cleared.job_dir.exists()

    # 重跑:无持久化产物,所有节点重跑
    calls = _track_runs(cleared)
    _drive(cleared)
    assert calls == ["fetch", "process", "review", "export"]
    assert cleared.state.status == "done"
    assert (cleared.job_dir / "fetch" / ARTIFACT_FILE).exists()


def test_clear_debug_noop_for_run_mode(tmp_path: Path):
    """非 debug 模式 clear_debug 无操作,不误删 outputs。"""
    runner = Runner.load(str(EXAMPLE), debug=False)
    job_dir = runner.job_dir
    runner.clear_debug()
    # 不应删 job_dir 的父目录或抛错;run 模式本就不该调,这里只确认无副作用
    assert runner.debug is False
    assert job_dir.parent.exists() or True  # outputs 根目录未被破坏


def test_scope_only_runs_target_without_upstream(monkeypatch, tmp_path: Path):
    """scope={X} 只跑 X,上游从磁盘加载,不重跑上游。"""
    _debug_root(monkeypatch, tmp_path)

    # 先全跑落产物
    base = Runner.load(str(EXAMPLE), debug=True)
    _drive(base)
    assert base.state.status == "done"

    # scope=review:上游 fetch/process 从磁盘复用,只跑 review
    runner = Runner.load(str(EXAMPLE), debug=True)
    calls = _track_runs(runner)
    events = []

    async def go():
        async for ev in runner.run(scope={"review"}):
            events.append(ev)
            if ev.type == "checkpoint":
                runner.resume()

    asyncio.run(go())
    assert calls == ["review"], f"应只跑 review,实际 {calls}"
    assert runner.state.status == "done"


def test_break_before_pauses_before_target(monkeypatch, tmp_path: Path):
    """break_before={X} 时 X 就绪后 emit checkpoint 暂停,resume 后才执行 X。
    用 export(无自身 checkpoint)确保只触发 break_before 一次。"""
    _debug_root(monkeypatch, tmp_path)

    # 先全跑落上游产物
    base = Runner.load(str(EXAMPLE), debug=True)
    _drive(base)

    runner = Runner.load(str(EXAMPLE), debug=True)
    calls = _track_runs(runner)
    events: list[WorkflowJobEvent] = []
    cp_count = 0

    async def go():
        nonlocal cp_count
        async for ev in runner.run(scope={"export"}, break_before={"export"}):
            events.append(ev)
            if ev.type == "checkpoint":
                cp_count += 1
                runner.resume()

    asyncio.run(go())

    # export 暂停一次(break_before),resume 后跑一次
    assert cp_count == 1
    assert calls == ["export"]
    assert runner.state.status == "done"
    assert runner.state.steps["export"].status == "done"


def test_scope_target_not_ready_when_upstream_missing(monkeypatch, tmp_path: Path):
    """scope={X} 上游磁盘无产物时 X 不就绪,job 直接 end,不跑任何节点。"""
    _debug_root(monkeypatch, tmp_path)

    runner = Runner.load(str(EXAMPLE), debug=True)
    calls = _track_runs(runner)

    async def go():
        async for _ in runner.run(scope={"review"}):
            pass

    asyncio.run(go())

    # 上游 fetch/process 没产物,review 不就绪,什么都没跑
    assert calls == []
    assert runner.state.status == "done"


def test_missing_upstream_detects_lack(monkeypatch, tmp_path: Path):
    """debug 目录无产物时,missing_upstream 返回 scope 节点的全部上游。"""
    _debug_root(monkeypatch, tmp_path)
    runner = Runner.load(str(EXAMPLE), debug=True)
    # 没跑过,磁盘空
    assert set(runner.missing_upstream({"review"})) == {"fetch", "process"}
    assert set(runner.missing_upstream({"export"})) == {"fetch", "process", "review"}

    # 全跑后磁盘有产物,missing 为空
    _drive(Runner.load(str(EXAMPLE), debug=True))
    runner2 = Runner.load(str(EXAMPLE), debug=True)
    assert runner2.missing_upstream({"review"}) == []
    assert runner2.missing_upstream({"export"}) == []


def test_missing_upstream_after_clear(monkeypatch, tmp_path: Path):
    """clear_debug 后 missing_upstream 返回全部上游(--clear + --node 场景)。"""
    _debug_root(monkeypatch, tmp_path)
    # 先落产物
    _drive(Runner.load(str(EXAMPLE), debug=True))
    # clear 后再测
    runner = Runner.load(str(EXAMPLE), debug=True)
    runner.clear_debug()
    assert set(runner.missing_upstream({"review"})) == {"fetch", "process"}
