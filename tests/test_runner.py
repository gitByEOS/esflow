"""runner 核心测试:加载、执行、checkpoint resume、retry 复用、错误、DAG 校验。"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

import esflow.runner as runner_mod
from esflow import Runner, Checkpoint
from esflow.event import JobEvent
from esflow.loader import load_flow, FlowLoadError


EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "quickstart_flow"
OCR_EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "ocr_flow"
ARTIFACT_FILE = runner_mod._ARTIFACT_FILE


def _track_calls(runner: Runner) -> list[str]:
    """给每个 node.run 套一层计数,返回 calls 列表(引用,后续 append)。"""
    calls: list[str] = []

    def wrap(sd):
        orig = sd.run

        def wrapped(ctx):
            calls.append(sd.replica_id)
            return orig(ctx)

        sd.run = wrapped

    for sd in runner.runs.values():
        wrap(sd)
    return calls


def _stub_ocr_flow(runner: Runner) -> list[str]:
    """把 ocr_flow 节点替换成无外部依赖的轻量实现。"""
    calls: list[str] = []

    def wrap(sd):
        def fake(ctx):
            calls.append(sd.replica_id)
            if sd.id == "ingest":
                return {"image_path": "raw.png"}
            if sd.id == "preprocess":
                return {"image_path": ctx.get("ingest")["image_path"]}
            if sd.id == "ocr":
                return {"text": f"识别:{ctx.get('preprocess')['image_path']}"}
            if sd.id == "export":
                text = ctx.get("ocr")["text"]
                path = sd.output_dir / "result.txt"
                path.write_text(text + "\n", encoding="utf-8")
                return {"out_path": str(path), "chars": len(text)}
            return {}

        sd.run = fake

    for sd in runner.runs.values():
        wrap(sd)
    return calls


def _collect_events(runner: Runner, **run_kwargs) -> list[JobEvent]:
    """同步跑 runner(自动 resume checkpoint),返回事件列表。run_kwargs 透传给 runner.run。"""
    events: list[JobEvent] = []

    async def drive():
        async for ev in runner.run(**run_kwargs):
            events.append(ev)
            if ev.type == "checkpoint":
                runner.resume()

    asyncio.run(drive())
    return events


def test_load_quickstart_flow():
    flow, runs, _ = load_flow(str(EXAMPLE))
    assert flow.id == "quickstart_flow"
    assert set(runs.keys()) == {"fetch", "process", "review", "export"}
    assert runs["review"].checkpoint == Checkpoint.TO_HUMAN


def test_run_full_flow_with_auto_resume():
    runner = Runner.load(str(EXAMPLE))
    calls = _track_calls(runner)
    events = _collect_events(runner)

    types = [e.type for e in events]
    assert types[0] == "trace"
    assert types[-1] == "end"
    assert "checkpoint" in types
    assert calls == ["fetch", "process", "review", "export"]
    assert all(s.status == "done" for s in runner.state.runs.values())
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
    events = _collect_events(second, from_node="ocr")

    assert events[-1].type == "end"
    assert second_calls == ["ocr", "export"]
    assert second.artifacts["preprocess"] == fixed_preprocess
    assert second.artifacts["ocr"]["text"] == "识别:fixed.png"
    assert not stale_file.exists()

    ocr_artifact = json.loads((out_dir / "ocr" / ARTIFACT_FILE).read_text(encoding="utf-8"))
    export_artifact = json.loads((out_dir / "export" / ARTIFACT_FILE).read_text(encoding="utf-8"))
    assert ocr_artifact["text"] == "识别:fixed.png"
    assert export_artifact["chars"] == len("识别:fixed.png")


def test_run_from_depth_reuses_upstream_layers(tmp_path: Path):
    """from_depth=2 重跑 depth>=2(ocr+export),上游 ingest+preprocess 复用不重跑。"""
    out_dir = tmp_path / "ocr_run"

    first = Runner.load(str(OCR_EXAMPLE), job_dir=out_dir)
    first_calls = _stub_ocr_flow(first)
    _collect_events(first)
    assert first_calls == ["ingest", "preprocess", "ocr", "export"]

    # 人工篡改 preprocess 产物,from_depth=2 不应感知(上游复用,不重跑)
    fixed_preprocess = {"image_path": "fixed.png"}
    (out_dir / "preprocess" / ARTIFACT_FILE).write_text(
        json.dumps(fixed_preprocess, ensure_ascii=False), encoding="utf-8"
    )

    second = Runner.load(str(OCR_EXAMPLE), job_dir=out_dir)
    second_calls = _stub_ocr_flow(second)
    events = _collect_events(second, from_depth=2)

    assert events[-1].type == "end"
    assert second_calls == ["ocr", "export"]
    assert second.artifacts["preprocess"] == fixed_preprocess
    assert second.artifacts["ocr"]["text"] == "识别:fixed.png"


def test_run_from_depth_zero_reruns_all(tmp_path: Path):
    """from_depth=0 重跑全部节点(target=所有,加载后全清)。"""
    out_dir = tmp_path / "ocr_run"
    first = Runner.load(str(OCR_EXAMPLE), job_dir=out_dir)
    _stub_ocr_flow(first)
    _collect_events(first)

    second = Runner.load(str(OCR_EXAMPLE), job_dir=out_dir)
    second_calls = _stub_ocr_flow(second)
    _collect_events(second, from_depth=0)
    assert second_calls == ["ingest", "preprocess", "ocr", "export"]


def test_run_from_depth_out_of_range_raises(tmp_path: Path):
    """from_depth 越界(>max_depth 或 <0)在 run() 迭代前抛 RuntimeError。"""
    out_dir = tmp_path / "ocr_run"
    runner = Runner.load(str(OCR_EXAMPLE), job_dir=out_dir)
    assert runner.max_depth == 3

    async def drive_bad():
        async for _ in runner.run(from_depth=4):
            pass

    with pytest.raises(RuntimeError, match="from_depth 越界"):
        asyncio.run(drive_bad())

    async def drive_neg():
        async for _ in runner.run(from_depth=-1):
            pass

    with pytest.raises(RuntimeError, match="from_depth 越界"):
        asyncio.run(drive_neg())


def test_retry_reuses_upstream_artifacts():
    """retry review 后 fetch/process 不重跑,review/export 跑 2 次。"""
    runner = Runner.load(str(EXAMPLE))
    calls = _track_calls(runner)
    events: list[JobEvent] = []

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
    events: list[JobEvent] = []

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


def test_node_error_propagates(tmp_path: Path):
    flow_dir = tmp_path / "bad"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='bad')\n"
        "class F:\n"
        "    nodes=['boom']\n"
        "    edges=[]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "boom.py").write_text(
        "from esflow import Node\n"
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


def test_node_error_attrs_propagated(tmp_path: Path):
    """节点抛带属性的自定义异常,error event 的 exc_attrs 透传 __dict__;裸 Exception 为 None。"""
    flow_dir = tmp_path / "err"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='err')\n"
        "class F:\n"
        "    nodes=['custom', 'plain']\n"
        "    edges=[]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "custom.py").write_text(
        "from esflow import Node\n"
        "class CliError(Exception):\n"
        "    def __init__(self, code, message, retryable=False):\n"
        "        super().__init__(message)\n"
        "        self.code = code\n"
        "        self.exit_code = code + 2\n"
        "        self.retryable = retryable\n"
        "class Custom(Node):\n"
        "    id='custom'\n"
        "    def run(self, ctx):\n"
        "        raise CliError(3, '必须传入 input', retryable=True)\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "plain.py").write_text(
        "from esflow import Node\n"
        "class Plain(Node):\n"
        "    id='plain'\n"
        "    def run(self, ctx):\n"
        "        raise ValueError('裸异常')\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(flow_dir))
    events = _collect_events(runner)

    custom_err = next(e for e in events if e.type == "error" and e.run_id == "custom")
    assert custom_err.exc_attrs == {"code": 3, "exit_code": 5, "retryable": True}

    plain_err = next(e for e in events if e.type == "error" and e.run_id == "plain")
    assert plain_err.exc_attrs is None


def test_cycle_detected(tmp_path: Path):
    flow_dir = tmp_path / "cyc"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='cyc')\n"
        "class F:\n"
        "    nodes=['a','b']\n"
        "    edges=[edge('a','b'), edge('b','a')]\n",
        encoding="utf-8",
    )
    for n in ("a", "b"):
        (flow_dir / "nodes" / f"{n}.py").write_text(
            f"from esflow import Node\n"
            f"class N(Node):\n"
            f"    id='{n}'\n"
            f"    def run(self, ctx): return {{}}\n",
            encoding="utf-8",
        )
    with pytest.raises(FlowLoadError, match="有环"):
        load_flow(str(flow_dir))


# —— TO_AGENT checkpoint:agent 介入链路 ——

def _build_agent_flow(tmp_path: Path) -> Path:
    """构造 fetch → agent_summary (TO_AGENT) → export 的临时 flow。"""
    flow_dir = tmp_path / "agent_flow"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='agent_flow')\n"
        "class F:\n"
        "    nodes=['fetch', 'agent_summary', 'export']\n"
        "    edges=[edge('fetch', 'agent_summary'), edge('agent_summary', 'export')]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "fetch.py").write_text(
        "from esflow import Node\n"
        "class Fetch(Node):\n"
        "    id='fetch'\n"
        "    def run(self, ctx):\n"
        "        return {'prompt': '总结以下内容', 'text': 'hello world'}\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "agent_summary.py").write_text(
        "from esflow import Node, Checkpoint\n"
        "class AgentSummary(Node):\n"
        "    id='agent_summary'\n"
        "    checkpoint=Checkpoint.TO_AGENT\n"
        "    def deliver(self, artifact) -> bool:\n"
        "        return 'summary.txt' in artifact.get('files', [])\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "export.py").write_text(
        "from esflow import Node\n"
        "class Export(Node):\n"
        "    id='export'\n"
        "    def run(self, ctx):\n"
        "        agent_art = ctx.get('agent_summary')\n"
        "        return {'exported': True, 'files': agent_art['files']}\n",
        encoding="utf-8",
    )
    return flow_dir


def _drive_no_resume(runner: Runner) -> list[JobEvent]:
    """跑 runner,checkpoint 时不 resume(TO_AGENT 路径用),返回事件列表。"""
    events: list[JobEvent] = []

    async def drive():
        async for ev in runner.run():
            events.append(ev)

    asyncio.run(drive())
    return events


def test_to_agent_first_run_emits_checkpoint_and_exits(tmp_path: Path):
    """首次跑到 TO_AGENT 节点:emit checkpoint,主循环退出,不 emit end。"""
    flow_dir = _build_agent_flow(tmp_path)
    out_dir = tmp_path / "run"
    runner = Runner.load(str(flow_dir), job_dir=out_dir)

    events = _drive_no_resume(runner)

    types = [e.type for e in events]
    assert "checkpoint" in types
    # TO_AGENT checkpoint 后主循环退出,不 emit end
    assert "end" not in types
    ckpt = next(e for e in events if e.type == "checkpoint")
    assert ckpt.run_id == "agent_summary"
    # checkpoint artifact = 上游产物集合
    assert ckpt.artifact == {"fetch": {"prompt": "总结以下内容", "text": "hello world"}}
    # agent_summary PAUSED,_break_to_agent.json 已写
    assert runner.state.runs["agent_summary"].status == "paused"
    assert runner.has_break_to_agent()
    assert runner.pending_break_to_agent() == ["agent_summary"]
    # agent_summary/artifact.json 不存在(产物由 agent 写)
    assert not (out_dir / "agent_summary" / ARTIFACT_FILE).exists()
    # fetch 已完成,export 未跑
    assert runner.state.runs["fetch"].status == "done"
    assert runner.state.runs["export"].status == "idle"


def test_to_agent_resume_after_agent_writes_files(tmp_path: Path):
    """agent 写 summary.txt 后 --resume:扫文件构造 artifact + deliver 通过 + 跑下游。"""
    flow_dir = _build_agent_flow(tmp_path)
    out_dir = tmp_path / "run"

    first = Runner.load(str(flow_dir), job_dir=out_dir)
    _drive_no_resume(first)

    # 模拟 agent 写产物文件
    (out_dir / "agent_summary").mkdir(parents=True, exist_ok=True)
    (out_dir / "agent_summary" / "summary.txt").write_text("这是摘要", encoding="utf-8")

    # --resume:新 Runner 实例,从 job_dir 加载 + 跑
    second = Runner.load(str(flow_dir), job_dir=out_dir)
    events: list[JobEvent] = []

    async def drive_second():
        async for ev in second.run(resume=True):
            events.append(ev)

    asyncio.run(drive_second())

    types = [e.type for e in events]
    assert types[-1] == "end"
    assert second.state.runs["agent_summary"].status == "done"
    assert second.state.runs["export"].status == "done"
    # _break_to_agent.json 被清
    assert not second.has_break_to_agent()
    # agent_summary artifact.json 已构造
    art = json.loads((out_dir / "agent_summary" / ARTIFACT_FILE).read_text(encoding="utf-8"))
    assert art["files"] == ["summary.txt"]
    assert "output_dir" in art
    # export 拿到 agent_summary artifact
    assert second.artifacts["export"]["files"] == ["summary.txt"]


def test_to_agent_deliver_rejects_wrong_files(tmp_path: Path):
    """agent 写了无关文件(deliver 不通过):emit error,节点标 error。"""
    flow_dir = _build_agent_flow(tmp_path)
    out_dir = tmp_path / "run"

    first = Runner.load(str(flow_dir), job_dir=out_dir)
    _drive_no_resume(first)

    # agent 写了 wrong.txt(不是 summary.txt)
    (out_dir / "agent_summary").mkdir(parents=True, exist_ok=True)
    (out_dir / "agent_summary" / "wrong.txt").write_text("写错了", encoding="utf-8")

    second = Runner.load(str(flow_dir), job_dir=out_dir)
    events: list[JobEvent] = []

    async def drive_second():
        async for ev in second.run(resume=True):
            events.append(ev)

    asyncio.run(drive_second())

    types = [e.type for e in events]
    assert "error" in types
    err = next(e for e in events if e.type == "error" and e.run_id == "agent_summary")
    assert "deliver" in (err.message or "")
    assert second.state.runs["agent_summary"].status == "error"
    # _break_to_agent.json 仍存在(未完成)
    assert second.has_break_to_agent()


def test_to_agent_resume_without_break_to_agent_errors(tmp_path: Path):
    """--resume 但 job_dir 无 _break_to_agent.json:has_break_to_agent 返回 False。"""
    flow_dir = _build_agent_flow(tmp_path)
    out_dir = tmp_path / "run"
    # 跑完整流程(模拟 agent 已写产物 + resume 完成),_break_to_agent.json 被清
    first = Runner.load(str(flow_dir), job_dir=out_dir)
    _drive_no_resume(first)
    (out_dir / "agent_summary").mkdir(parents=True, exist_ok=True)
    (out_dir / "agent_summary" / "summary.txt").write_text("摘要", encoding="utf-8")

    second = Runner.load(str(flow_dir), job_dir=out_dir)

    async def drive_second():
        async for _ in second.run(resume=True):
            pass

    asyncio.run(drive_second())
    assert not second.has_break_to_agent()

    # 第三次再 load:无 _break_to_agent.json,has_break_to_agent 应为 False
    third = Runner.load(str(flow_dir), job_dir=out_dir)
    assert not third.has_break_to_agent()
