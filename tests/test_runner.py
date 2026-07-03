"""runner 核心测试:加载、执行、checkpoint resume、retry 复用、错误、DAG 校验。"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

import easyflow.runner as runner_mod
from easyflow import Runner, Checkpoint
from easyflow.event import WorkflowJobEvent
from easyflow.loader import load_flow, FlowLoadError


EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "quickstart_flow"
OCR_EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "ocr_flow"
ARTIFACT_FILE = runner_mod._ARTIFACT_FILE


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


def _stub_ocr_flow(runner: Runner) -> list[str]:
    """把 ocr_flow 节点替换成无外部依赖的轻量实现。"""
    calls: list[str] = []

    def wrap(sd):
        def fake(ctx):
            calls.append(sd.id)
            if sd.id == "ingest":
                return {"image_path": "raw.png"}
            if sd.id == "preprocess":
                return {"image_path": ctx.get("ingest")["image_path"]}
            if sd.id == "ocr":
                return {"text": f"识别:{ctx.get('preprocess')['image_path']}"}
            if sd.id == "export":
                text = ctx.get("ocr")["text"]
                path = sd.node.output_dir / "result.txt"
                path.write_text(text + "\n", encoding="utf-8")
                return {"out_path": str(path), "chars": len(text)}
            return {}

        sd.node.run = fake

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


def _collect_events_from(runner: Runner, from_steps: set[str]) -> list[WorkflowJobEvent]:
    """同步跑 from_steps 续跑流程,返回事件列表。"""
    events: list[WorkflowJobEvent] = []

    async def drive():
        async for ev in runner.run(from_steps=from_steps):
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


def test_run_from_reuses_out_dir_upstream_artifacts(tmp_path: Path):
    """用 ocr_flow 验证:人工修正 preprocess 产物后,只从 ocr 续跑到 export。"""
    out_dir = tmp_path / "ocr_run"

    first = Runner.load(str(OCR_EXAMPLE), job_dir=out_dir)
    first_calls = _stub_ocr_flow(first)
    _collect_events(first)
    assert first_calls == ["ingest", "preprocess", "ocr", "export"]
    assert (out_dir / "preprocess" / ARTIFACT_FILE).exists()

    fixed_preprocess = {"image_path": "fixed.png"}
    (out_dir / "preprocess" / ARTIFACT_FILE).write_text(
        json.dumps(fixed_preprocess, ensure_ascii=False),
        encoding="utf-8",
    )
    stale_file = out_dir / "export" / "stale.txt"
    stale_file.write_text("旧产物", encoding="utf-8")

    second = Runner.load(str(OCR_EXAMPLE), job_dir=out_dir)
    second_calls = _stub_ocr_flow(second)
    events = _collect_events_from(second, {"ocr"})

    assert events[-1].type == "end"
    assert second_calls == ["ocr", "export"]
    assert second.artifacts["preprocess"] == fixed_preprocess
    assert second.artifacts["ocr"]["text"] == "识别:fixed.png"
    assert not stale_file.exists()

    ocr_artifact = json.loads((out_dir / "ocr" / ARTIFACT_FILE).read_text(encoding="utf-8"))
    export_artifact = json.loads((out_dir / "export" / ARTIFACT_FILE).read_text(encoding="utf-8"))
    assert ocr_artifact["text"] == "识别:fixed.png"
    assert export_artifact["chars"] == len("识别:fixed.png")


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
