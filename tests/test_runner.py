"""runner 核心测试:加载、执行、checkpoint resume、retry 复用、错误、DAG 校验。"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

import esflow.runner as runner_mod
from esflow import Runner, Checkpoint, BreakKind
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


def test_error_event_as_exception_restores_original(tmp_path: Path):
    """error event.as_exception() 返回原异常实例:类型/属性/同对象身份都保留。"""
    flow_dir = tmp_path / "err"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='err')\n"
        "class F:\n"
        "    nodes=['custom']\n"
        "    edges=[]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "custom.py").write_text(
        "from esflow import Node\n"
        "class CliError(Exception):\n"
        "    def __init__(self, code, message, retryable=False):\n"
        "        super().__init__(message)\n"
        "        self.code = code\n"
        "        self.retryable = retryable\n"
        "class Custom(Node):\n"
        "    id='custom'\n"
        "    def run(self, ctx):\n"
        "        raise CliError(3, '必须传入 input', retryable=True)\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(flow_dir))
    events = _collect_events(runner)

    err_event = next(e for e in events if e.type == "error" and e.run_id == "custom")
    # exc_type 透传类全名
    assert err_event.exc_type.endswith(".CliError")
    # as_exception 返回原异常:类型、属性、身份都保留
    restored = err_event.as_exception()
    assert type(restored).__name__ == "CliError"
    assert restored.code == 3
    assert restored.retryable is True
    # exc_attrs 仍兼容(裸 Exception 为 None,自定义异常带 __dict__)
    assert err_event.exc_attrs == {"code": 3, "retryable": True}


def test_error_event_as_exception_fallback_runtime_error():
    """无 exc 引用、无 exc_type(完全降级)时 as_exception 兜底 RuntimeError。"""
    from esflow.event import error
    ev = error("x", "节点炸了", exc_attrs=None, exc=None, exc_type=None)
    restored = ev.as_exception()
    assert isinstance(restored, RuntimeError)
    assert str(restored) == "节点炸了"


def test_as_exception_cross_process_restores_by_exc_type():
    """跨进程(无 exc 引用,只有 exc_type + exc_attrs):按类全名 import 还原。

    skill 拿到序列化后的 error event,raise event.as_exception() 应得到原异常类实例,
    类型/属性/str 都还原,不再被迫手拼 __dict__ 还原成 RuntimeError。
    """
    from esflow.event import error
    from tests.sample_exc import CliError

    ev = error(
        "custom",
        "必须传入 input",
        exc_attrs={"code": 3, "retryable": True},
        exc=None,
        exc_type=f"{CliError.__module__}.CliError",
    )
    restored = ev.as_exception()
    assert isinstance(restored, CliError)
    assert restored.code == 3
    assert restored.retryable is True
    assert str(restored) == "必须传入 input"


def test_as_exception_cross_process_unknown_type_falls_back():
    """exc_type 指向不存在的类:降级 RuntimeError(message),不抛 ImportError。"""
    from esflow.event import error
    ev = error(
        "x", "炸了",
        exc_attrs={"code": 1},
        exc=None,
        exc_type="no.such.module.CliError",
    )
    restored = ev.as_exception()
    assert isinstance(restored, RuntimeError)
    assert str(restored) == "炸了"


def test_to_envelope_end_kind(tmp_path: Path):
    """end 断点 → exit 0 + 最小 envelope。"""
    from esflow import Runner
    flow_dir = tmp_path / "pure"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='pure')\n"
        "class F:\n"
        "    nodes=['a']\n"
        "    edges=[]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "a.py").write_text(
        "from esflow import Node\n"
        "class A(Node):\n"
        "    id='a'\n"
        "    def run(self, ctx):\n"
        "        return {'v': 1}\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(flow_dir), output_root=tmp_path / "out")

    async def drive():
        return await runner.run_to_break()

    _, kind, break_event = asyncio.run(drive())
    code, envelope = Runner.to_envelope(kind, break_event)
    assert code == 0
    assert envelope == {"status": "end"}


def test_to_envelope_to_agent_kind(tmp_path: Path):
    """to_agent 断点 → exit 2 + node_id + resume_hint。"""
    from esflow import Runner
    flow_dir = _build_agent_flow(tmp_path)
    runner = Runner.load(str(flow_dir), job_dir=tmp_path / "run")

    async def drive():
        return await runner.run_to_break()

    _, kind, break_event = asyncio.run(drive())
    code, envelope = Runner.to_envelope(kind, break_event)
    assert code == 2
    assert envelope["status"] == "to_agent"
    assert envelope["node_id"] == "agent_summary"
    assert envelope["resume_hint"] == break_event.resume_hint


def test_to_envelope_error_kind(tmp_path: Path):
    """error 断点 → exit 1 + message + exc_type + exc_attrs。"""
    from esflow import Runner
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

    async def drive():
        return await runner.run_to_break()

    _, kind, break_event = asyncio.run(drive())
    code, envelope = Runner.to_envelope(kind, break_event)
    assert code == 1
    assert envelope["status"] == "error"
    assert "炸了" in envelope["message"]
    assert envelope["exc_type"].endswith(".ValueError")


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


def test_to_agent_persists_upstream_under_output_root(tmp_path: Path):
    """含 TO_AGENT 的 flow 在 output_root 模式下落盘上游产物,--resume 能跨进程加载。"""
    flow_dir = _build_agent_flow(tmp_path)
    jobs_root = tmp_path / "jobs"
    runner = Runner.load(str(flow_dir), output_root=jobs_root)

    events = _drive_no_resume(runner)

    # 跑到 TO_AGENT checkpoint 退出
    assert "checkpoint" in [e.type for e in events]
    assert "end" not in [e.type for e in events]
    # job_dir 由框架在 output_root 下自动生成
    assert runner.job_dir.parent == jobs_root / "agent_flow"
    # 上游 fetch 产物已落盘
    assert (runner.job_dir / "fetch" / ARTIFACT_FILE).exists()
    fetch_art = json.loads(
        (runner.job_dir / "fetch" / ARTIFACT_FILE).read_text(encoding="utf-8")
    )
    assert fetch_art["text"] == "hello world"


def test_to_agent_resume_under_output_root_no_keyerror(tmp_path: Path):
    """output_root 模式下 --resume:加载上游产物,ctx.get 不抛 KeyError,跑通到下游。"""
    flow_dir = _build_agent_flow(tmp_path)
    jobs_root = tmp_path / "jobs"

    first = Runner.load(str(flow_dir), output_root=jobs_root)
    _drive_no_resume(first)
    job_dir = first.job_dir

    # 模拟 agent 写产物
    (job_dir / "agent_summary").mkdir(parents=True, exist_ok=True)
    (job_dir / "agent_summary" / "summary.txt").write_text("摘要", encoding="utf-8")

    # --resume:新 Runner,不传 job_dir,靠 _flow_dir.txt 或显式 job_dir 找回
    # 这里直接传 job_dir 模拟 CLI --resume 已定位到具体 job_dir 的场景
    second = Runner.load(str(flow_dir), job_dir=job_dir)
    events: list[JobEvent] = []

    async def drive_second():
        async for ev in second.run(resume=True):
            events.append(ev)

    asyncio.run(drive_second())

    types = [e.type for e in events]
    assert types[-1] == "end"
    assert second.state.runs["agent_summary"].status == "done"
    assert second.state.runs["export"].status == "done"
    assert second.artifacts["export"]["files"] == ["summary.txt"]


def test_pure_flow_always_persists(tmp_path: Path):
    """全持久化:纯计算 flow(无 checkpoint)也落盘,from_node/from_depth 对所有 flow 开箱即用。"""
    flow_dir = tmp_path / "pure"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='pure')\n"
        "class F:\n"
        "    nodes=['a', 'b']\n"
        "    edges=[edge('a', 'b')]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "a.py").write_text(
        "from esflow import Node\n"
        "class A(Node):\n"
        "    id='a'\n"
        "    def run(self, ctx):\n"
        "        return {'v': 1}\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "b.py").write_text(
        "from esflow import Node\n"
        "class B(Node):\n"
        "    id='b'\n"
        "    def run(self, ctx):\n"
        "        return {'v': ctx.get('a')['v'] + 1}\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(flow_dir), output_root=tmp_path / "out")

    events = _collect_events(runner)
    assert [e.type for e in events][-1] == "end"
    assert runner.artifacts["b"]["v"] == 2
    # 纯计算 flow 也落盘 artifact.json(全持久化)
    assert (runner.job_dir / "a" / ARTIFACT_FILE).exists()
    assert (runner.job_dir / "b" / ARTIFACT_FILE).exists()


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
    # resume_hint 由框架填好:node_dir/upstream_artifact/job_dir/node_id
    assert ckpt.resume_hint is not None
    assert ckpt.resume_hint["node_id"] == "agent_summary"
    assert ckpt.resume_hint["node_dir"] == str(out_dir / "agent_summary")
    assert ckpt.resume_hint["job_dir"] == str(out_dir)
    assert ckpt.resume_hint["upstream_artifact"] == ckpt.artifact
    # agent_summary PAUSED,_break_to_agent.json 已写
    assert runner.state.runs["agent_summary"].status == "paused"
    assert runner.has_break_to_agent()
    assert runner.pending_break_to_agent() == ["agent_summary"]
    # agent_summary/artifact.json 不存在(产物由 agent 写)
    assert not (out_dir / "agent_summary" / ARTIFACT_FILE).exists()
    # fetch 已完成,export 未跑
    assert runner.state.runs["fetch"].status == "done"
    assert runner.state.runs["export"].status == "idle"


def test_to_agent_hint_formats_resume_command(tmp_path: Path):
    """Runner.to_agent_hint 把 event.resume_hint 格式化成指引字符串,填充 resume_cmd。"""
    flow_dir = _build_agent_flow(tmp_path)
    out_dir = tmp_path / "run"
    runner = Runner.load(str(flow_dir), job_dir=out_dir)
    events = _drive_no_resume(runner)
    ckpt = next(e for e in events if e.type == "checkpoint")

    # 带 {job_dir} 占位符:框架填充
    hint = Runner.to_agent_hint(ckpt, resume_cmd="python3 run.py --resume {job_dir}")
    assert f"写产物到:{out_dir / 'agent_summary'}" in hint
    assert f"上游产物:{{'fetch':" in hint
    assert f"完成后续跑:python3 run.py --resume {out_dir}" in hint

    # 不传 resume_cmd:只输出节点目录与上游产物
    hint3 = Runner.to_agent_hint(ckpt)
    assert "写产物到:" in hint3
    assert "完成后续跑:" not in hint3

    # 裸字符串不含 {job_dir} 占位符:raise,强制调用方显式写模板
    with pytest.raises(ValueError, match="必须含 \\{job_dir\\} 占位符"):
        Runner.to_agent_hint(ckpt, resume_cmd="esflow resume")


def test_run_to_break_returns_to_agent_kind(tmp_path: Path):
    """run_to_break 跑到 TO_AGENT 节点:返回 (events, 'to_agent'),不 emit end。"""
    flow_dir = _build_agent_flow(tmp_path)
    runner = Runner.load(str(flow_dir), job_dir=tmp_path / "run")

    async def drive():
        return await runner.run_to_break()

    events, kind, break_event = asyncio.run(drive())
    assert kind == "to_agent"
    assert break_event is not None
    assert break_event.type == "checkpoint"
    assert break_event.run_id == "agent_summary"
    assert break_event.resume_hint is not None
    assert [e.type for e in events][-1] == "checkpoint"
    assert "end" not in [e.type for e in events]


def test_run_to_break_returns_end_kind(tmp_path: Path):
    """纯计算 flow 跑完:返回 (events, 'end')。"""
    flow_dir = tmp_path / "pure"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='pure')\n"
        "class F:\n"
        "    nodes=['a', 'b']\n"
        "    edges=[edge('a', 'b')]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "a.py").write_text(
        "from esflow import Node\n"
        "class A(Node):\n"
        "    id='a'\n"
        "    def run(self, ctx):\n"
        "        return {'v': 1}\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "b.py").write_text(
        "from esflow import Node\n"
        "class B(Node):\n"
        "    id='b'\n"
        "    def run(self, ctx):\n"
        "        return {'v': ctx.get('a')['v'] + 1}\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(flow_dir), output_root=tmp_path / "out")

    async def drive():
        return await runner.run_to_break()

    events, kind, break_event = asyncio.run(drive())
    assert kind == "end"
    assert break_event is not None
    assert break_event.type == "end"
    assert [e.type for e in events][-1] == "end"
    assert runner.artifacts["b"]["v"] == 2


def test_run_to_break_returns_error_kind(tmp_path: Path):
    """节点抛异常:返回 (events, 'error'),最后事件是 error,as_exception 还原。"""
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

    async def drive():
        return await runner.run_to_break()

    events, kind, break_event = asyncio.run(drive())
    assert kind == "error"
    assert break_event is not None
    assert break_event.type == "error"
    assert break_event.run_id == "boom"
    restored = break_event.as_exception()
    assert isinstance(restored, ValueError)
    assert str(restored) == "炸了"


def test_node_args_injected_to_nodes(tmp_path: Path):
    """Runner.load(node_args=...) 把入参注入到节点 kwargs,skill 不再碰 runner.runs。"""
    flow_dir = tmp_path / "args_flow"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='args_flow')\n"
        "class F:\n"
        "    nodes=['resolve', 'worker']\n"
        "    edges=[edge('resolve', 'worker')]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "resolve.py").write_text(
        "from esflow import Node\n"
        "class Resolve(Node):\n"
        "    id='resolve'\n"
        "    def run(self, ctx):\n"
        "        return {'input': self.kwargs.get('input'), 'out': self.kwargs.get('out')}\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "worker.py").write_text(
        "from esflow import Node\n"
        "class Worker(Node):\n"
        "    id='worker'\n"
        "    def run(self, ctx):\n"
        "        return {'height': self.kwargs.get('height', 360)}\n",
        encoding="utf-8",
    )
    runner = Runner.load(
        str(flow_dir),
        output_root=tmp_path / "out",
        node_args={
            "resolve": {"input": "https://x", "out": "/tmp/x"},
            "worker": {"height": 720},
        },
    )
    # 注入后节点 kwargs 是独立 dict(不共享类属性)
    assert runner.runs["resolve"].kwargs == {"input": "https://x", "out": "/tmp/x"}
    assert runner.runs["worker"].kwargs == {"height": 720}

    events = _collect_events(runner)
    assert [e.type for e in events][-1] == "end"
    assert runner.artifacts["resolve"]["input"] == "https://x"
    assert runner.artifacts["worker"]["height"] == 720


def test_node_args_injected_to_static_replicas(tmp_path: Path):
    """node_args 注入到 base 与所有静态副本(base#i)。"""
    flow_dir = tmp_path / "rep_flow"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='rep_flow')\n"
        "class F:\n"
        "    nodes=['worker']\n"
        "    edges=[]\n"
        "    replicas={'worker': 3}\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "worker.py").write_text(
        "from esflow import Node\n"
        "class Worker(Node):\n"
        "    id='worker'\n"
        "    def run(self, ctx):\n"
        "        return {'cfg': self.kwargs.get('cfg')}\n",
        encoding="utf-8",
    )
    runner = Runner.load(
        str(flow_dir),
        output_root=tmp_path / "out",
        node_args={"worker": {"cfg": "X"}},
    )
    for rid in ("worker#0", "worker#1", "worker#2"):
        assert runner.runs[rid].kwargs == {"cfg": "X"}
        # 独立 dict,改一个不影响其他
        assert runner.runs[rid].kwargs is not runner.runs["worker#0"].kwargs or rid == "worker#0"


def test_node_args_inherited_by_dynamic_replicas(tmp_path: Path):
    """动态扇出副本继承 base 的 node_args。"""
    flow_dir = tmp_path / "dyn_flow"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge, FanOut\n"
        "@flow(id='dyn_flow')\n"
        "class F:\n"
        "    nodes=['split', 'worker', 'merge']\n"
        "    edges=[edge('split','worker'), edge('worker','merge')]\n"
        "    dynamic={'worker'}\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "split.py").write_text(
        "from esflow import Node, FanOut\n"
        "class Split(Node):\n"
        "    id='split'\n"
        "    def run(self, ctx):\n"
        "        return FanOut(base='worker', payload=[1, 2, 3])\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "worker.py").write_text(
        "from esflow import Node\n"
        "class Worker(Node):\n"
        "    id='worker'\n"
        "    def run(self, ctx):\n"
        "        return {'task': self.fanout_payload, 'cfg': self.kwargs.get('cfg')}\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "merge.py").write_text(
        "from esflow import Node\n"
        "class Merge(Node):\n"
        "    id='merge'\n"
        "    def run(self, ctx):\n"
        "        return {'all': ctx.gather('worker')}\n",
        encoding="utf-8",
    )
    runner = Runner.load(
        str(flow_dir),
        output_root=tmp_path / "out",
        node_args={"worker": {"cfg": "W"}},
    )
    events = _collect_events(runner)
    assert [e.type for e in events][-1] == "end"
    # 动态副本继承 base 的 kwargs
    merged = runner.artifacts["merge"]["all"]
    assert [m["cfg"] for m in merged] == ["W", "W", "W"]
    assert [m["task"] for m in merged] == [1, 2, 3]


def test_node_args_not_persisted(tmp_path: Path):
    """node_args 是输入非产物,不进 artifact.json;resume 时 skill 显式重传。"""
    flow_dir = tmp_path / "args_flow"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='args_flow')\n"
        "class F:\n"
        "    nodes=['a']\n"
        "    edges=[]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "a.py").write_text(
        "from esflow import Node\n"
        "class A(Node):\n"
        "    id='a'\n"
        "    def run(self, ctx):\n"
        "        return {'v': self.kwargs.get('v')}\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "run"
    runner = Runner.load(
        str(flow_dir), job_dir=out_dir, node_args={"a": {"v": 42}}
    )
    _collect_events(runner)
    # artifact.json 不含 kwargs
    art = json.loads((out_dir / "a" / ARTIFACT_FILE).read_text(encoding="utf-8"))
    assert art == {"v": 42}
    assert "kwargs" not in art


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


def test_to_agent_deliver_not_ready_emits_checkpoint(tmp_path: Path):
    """agent 未写好产物(deliver 不通过):emit checkpoint 让 agent 写,非 error。

    TO_AGENT 语义:deliver False = agent 未写完,checkpoint 让 agent 重写;
    deliver 抛异常才是 error。自定义 output_dir 时目录常有业务文件,
    deliver False 不代表 agent 写错,是 agent 还没写。
    """
    flow_dir = _build_agent_flow(tmp_path)
    out_dir = tmp_path / "run"

    first = Runner.load(str(flow_dir), job_dir=out_dir)
    _drive_no_resume(first)

    # agent 写了 wrong.txt(不是 summary.txt),deliver 不通过
    (out_dir / "agent_summary").mkdir(parents=True, exist_ok=True)
    (out_dir / "agent_summary" / "wrong.txt").write_text("写错了", encoding="utf-8")

    second = Runner.load(str(flow_dir), job_dir=out_dir)
    events: list[JobEvent] = []

    async def drive_second():
        async for ev in second.run(resume=True):
            events.append(ev)

    asyncio.run(drive_second())

    types = [e.type for e in events]
    # deliver 不通过 → checkpoint(让 agent 重写),不是 error
    assert "error" not in types
    assert "checkpoint" in types
    assert second.state.runs["agent_summary"].status == "paused"
    # _break_to_agent.json 仍存在(未完成)
    assert second.has_break_to_agent()


def test_to_agent_deliver_exception_is_error(tmp_path: Path):
    """deliver 抛异常:emit error,节点标 error(与 deliver False 的 checkpoint 路径区分)。"""
    flow_dir = tmp_path / "agent_exc"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='agent_exc')\n"
        "class F:\n"
        "    nodes=['agent_summary']\n"
        "    edges=[]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "agent_summary.py").write_text(
        "from esflow import Node, Checkpoint\n"
        "class AgentSummary(Node):\n"
        "    id='agent_summary'\n"
        "    checkpoint=Checkpoint.TO_AGENT\n"
        "    def deliver(self, artifact) -> bool:\n"
        "        raise ValueError('deliver 校验逻辑本身炸了')\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(flow_dir), job_dir=tmp_path / "run")
    events = _collect_events(runner)

    err = next(e for e in events if e.type == "error" and e.run_id == "agent_summary")
    assert "deliver 异常" in (err.message or "")
    assert runner.state.runs["agent_summary"].status == "error"


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


def _build_agent_flow_custom_output_dir(tmp_path: Path) -> tuple[Path, Path]:
    """构造 TO_AGENT 节点在 accept 里设 self.output_dir = work_dir 的 flow,返回 (flow_dir, work_dir)。"""
    flow_dir = tmp_path / "agent_flow_custom"
    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='agent_flow_custom')\n"
        "class F:\n"
        "    nodes=['resolve', 'agent_summary', 'export']\n"
        "    edges=[edge('resolve','agent_summary'), edge('agent_summary','export')]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "resolve.py").write_text(
        "from esflow import Node\n"
        "class Resolve(Node):\n"
        "    id='resolve'\n"
        "    def run(self, ctx):\n"
        "        return {'work_dir': self.kwargs.get('work_dir')}\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "agent_summary.py").write_text(
        "from pathlib import Path\n"
        "from esflow import Node, Checkpoint\n"
        "class AgentSummary(Node):\n"
        "    id='agent_summary'\n"
        "    checkpoint=Checkpoint.TO_AGENT\n"
        "    def accept(self, ctx):\n"
        "        # 在 accept 里设 output_dir 指向业务 work_dir,框架尊重不覆盖\n"
        "        work_dir = ctx.get('resolve')['work_dir']\n"
        "        self.output_dir = Path(work_dir)\n"
        "        return True\n"
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
        "        return {'exported': True, 'output_dir': agent_art['output_dir']}\n",
        encoding="utf-8",
    )
    return flow_dir, work_dir


def test_to_agent_custom_output_dir_respected(tmp_path: Path):
    """TO_AGENT 节点在 accept 里设 self.output_dir = work_dir,框架尊重,agent 写 work_dir。"""
    flow_dir, work_dir = _build_agent_flow_custom_output_dir(tmp_path)
    out_dir = tmp_path / "run"
    runner = Runner.load(
        str(flow_dir), job_dir=out_dir,
        node_args={"resolve": {"work_dir": str(work_dir)}},
    )

    events = _drive_no_resume(runner)
    ckpt = next(e for e in events if e.type == "checkpoint")
    # resume_hint.node_dir 指向 work_dir,不是 job_dir/agent_summary
    assert ckpt.resume_hint["node_dir"] == str(work_dir)
    # job_dir/agent_summary 目录不应被创建(框架没 fallback)
    assert not (out_dir / "agent_summary").exists()


def test_to_agent_custom_output_dir_resume_scans_work_dir(tmp_path: Path):
    """agent 写 work_dir/summary 后 --resume:框架扫 work_dir 构造 artifact + deliver + 跑下游。"""
    flow_dir, work_dir = _build_agent_flow_custom_output_dir(tmp_path)
    out_dir = tmp_path / "run"

    first = Runner.load(
        str(flow_dir), job_dir=out_dir,
        node_args={"resolve": {"work_dir": str(work_dir)}},
    )
    _drive_no_resume(first)

    # 模拟 agent 写产物到 work_dir(不是 job_dir)
    (work_dir / "summary.txt").write_text("业务摘要", encoding="utf-8")

    second = Runner.load(
        str(flow_dir), job_dir=out_dir,
        node_args={"resolve": {"work_dir": str(work_dir)}},
    )
    events: list[JobEvent] = []

    async def drive_second():
        async for ev in second.run(resume=True):
            events.append(ev)

    asyncio.run(drive_second())

    assert [e.type for e in events][-1] == "end"
    assert second.state.runs["agent_summary"].status == "done"
    # artifact.output_dir 指向 work_dir
    assert second.artifacts["agent_summary"]["output_dir"] == str(work_dir)
    assert second.artifacts["agent_summary"]["files"] == ["summary.txt"]
    # export 拿到 agent_summary artifact
    assert second.artifacts["export"]["output_dir"] == str(work_dir)


def test_to_agent_accept_return_false_skips_node(tmp_path: Path):
    """TO_AGENT 节点 accept 返回 False:跳过本节点,artifact None,下游可推进。"""
    flow_dir = tmp_path / "agent_skip"
    (flow_dir / "nodes").mkdir(parents=True)
    (flow_dir / "flow.py").write_text(
        "from esflow import flow, edge\n"
        "@flow(id='agent_skip')\n"
        "class F:\n"
        "    nodes=['agent_summary', 'export']\n"
        "    edges=[edge('agent_summary','export')]\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "agent_summary.py").write_text(
        "from esflow import Node, Checkpoint\n"
        "class AgentSummary(Node):\n"
        "    id='agent_summary'\n"
        "    checkpoint=Checkpoint.TO_AGENT\n"
        "    def accept(self, ctx):\n"
        "        return False\n"
        "    def deliver(self, artifact) -> bool:\n"
        "        return True\n",
        encoding="utf-8",
    )
    (flow_dir / "nodes" / "export.py").write_text(
        "from esflow import Node\n"
        "class Export(Node):\n"
        "    id='export'\n"
        "    def run(self, ctx):\n"
        "        return {'skipped_upstream': ctx.get('agent_summary')}\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(flow_dir), job_dir=tmp_path / "run")
    events = _collect_events(runner)

    types = [e.type for e in events]
    assert types[-1] == "end"
    # agent_summary 被 skip(artifact None),export 拿到 None 推进
    assert runner.state.runs["agent_summary"].status == "skipped"
    assert runner.state.runs["export"].status == "done"
    assert runner.artifacts["export"]["skipped_upstream"] is None
