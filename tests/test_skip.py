"""skip 机制测试:accept False → skipped,artifact None,下游可推进,fallback 兜底链。"""

from __future__ import annotations

import asyncio
from pathlib import Path

from easyflow import Runner
from easyflow.event import WorkflowJobEvent


SKIP = Path(__file__).resolve().parent.parent / "examples" / "skip_flow"


def _events(runner: Runner) -> list[WorkflowJobEvent]:
    out: list[WorkflowJobEvent] = []

    async def drive():
        async for ev in runner.run():
            out.append(ev)

    asyncio.run(drive())
    return out


def test_skip_flow_fallback_skipped_when_first_source_succeeds():
    """fetch_from_ssr 成功有产物 → fetch_from_wechat / fetch_from_bili accept False 被 skip,
    parse_to_html 成功 → parse_to_md skip,done 汇总最终产物。"""
    runner = Runner.load(str(SKIP))
    events = _events(runner)

    # fetch_from_wechat / fetch_from_bili 有 skipped,fetch_from_ssr 没有
    assert any(
        e.type == "trace" and e.status == "skipped" and e.step_id == "fetch_from_wechat"
        for e in events
    )
    assert any(
        e.type == "trace" and e.status == "skipped" and e.step_id == "fetch_from_bili"
        for e in events
    )
    assert not any(
        e.type == "trace" and e.status == "skipped" and e.step_id == "fetch_from_ssr"
        for e in events
    )
    # parse_to_md skip,parse_to_html 不 skip
    assert any(
        e.type == "trace" and e.status == "skipped" and e.step_id == "parse_to_md"
        for e in events
    )

    # 状态:fetch_from_wechat/bili skipped,其余 done
    assert runner.state.steps["fetch_from_ssr"].status == "done"
    assert runner.state.steps["fetch_from_wechat"].status == "skipped"
    assert runner.state.steps["fetch_from_bili"].status == "skipped"
    assert runner.state.steps["merge"].status == "done"
    assert runner.state.steps["parse_to_html"].status == "done"
    assert runner.state.steps["parse_to_md"].status == "skipped"
    assert runner.state.steps["done"].status == "done"

    # skip 节点 artifact 占位 None,fetch_from_ssr 有产物文件路径
    assert runner.artifacts["fetch_from_wechat"] is None
    assert runner.artifacts["fetch_from_bili"] is None
    assert Path(runner.artifacts["fetch_from_ssr"]["data_file"]).exists()
    assert Path(runner.artifacts["merge"]["merged_file"]).exists()
    assert Path(runner.artifacts["parse_to_html"]["html_file"]).exists()
    assert Path(runner.artifacts["done"]["final_file"]).exists()

    # 产物目录:skip 节点无目录,trigger 空目录,其余有产物文件
    assert not (runner.job_dir / "fetch_from_wechat").exists()
    assert not (runner.job_dir / "fetch_from_bili").exists()
    assert (runner.job_dir / "trigger").exists()
    assert (runner.job_dir / "fetch_from_ssr" / "raw.txt").exists()
    assert (runner.job_dir / "done" / "final.html").exists()

    # job 正常完成,无 error
    assert runner.state.status == "done"
    assert not any(e.type == "error" for e in events)


def test_skip_flow_worker_b_fallback_when_a_empty(tmp_path: Path):
    """worker_a 产物为空 → worker_b 接手兜底,两路都进 merge(同层 serial 结构)。"""
    d = tmp_path / "fallback"
    (d / "nodes").mkdir(parents=True)
    (d / "flow.py").write_text(
        "from easyflow import flow, edge\n"
        "@flow(id='fallback')\n"
        "class F:\n"
        "    nodes=['decide','worker_a','worker_b','merge']\n"
        "    edges=[edge('decide','worker_a'),edge('decide','worker_b'),"
        "edge('worker_a','merge'),edge('worker_b','merge')]\n"
        "    serial={'worker_a','worker_b'}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "decide.py").write_text(
        "from easyflow import Node\n"
        "class Decide(Node):\n"
        "    id='decide'\n"
        "    def run(self, ctx): return {'input': 'x'}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "worker_a.py").write_text(
        "from easyflow import Node\n"
        "class WorkerA(Node):\n"
        "    id='worker_a'\n"
        "    def accept(self, ctx): return True\n"
        "    def run(self, ctx): return {'result': None, 'ok': False}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "worker_b.py").write_text(
        "from easyflow import Node\n"
        "class WorkerB(Node):\n"
        "    id='worker_b'\n"
        "    def accept(self, ctx): return not bool(ctx.get('worker_a').get('result'))\n"
        "    def run(self, ctx): return {'result': 'B兜底产物', 'ok': True}\n",
        encoding="utf-8",
    )
    (d / "nodes" / "merge.py").write_text(
        "from easyflow import Node\n"
        "class Merge(Node):\n"
        "    id='merge'\n"
        "    def run(self, ctx):\n"
        "        results = [ctx.get(s) for s in ('worker_a','worker_b') if ctx.get(s)]\n"
        "        winner = next((r for r in results if r.get('ok')), None)\n"
        "        return {'total': len(results), 'winner': winner['result'] if winner else None}\n",
        encoding="utf-8",
    )
    runner = Runner.load(str(d))
    events = _events(runner)

    # worker_b 接手兜底,未 skip
    assert not any(
        e.type == "trace" and e.status == "skipped" and e.step_id == "worker_b"
        for e in events
    )
    assert runner.state.steps["worker_b"].status == "done"
    assert runner.artifacts["worker_b"]["result"] == "B兜底产物"
    # merge 汇总两路,worker_a 产物有(result=None 但 dict 非空),winner 取 B
    assert runner.artifacts["merge"]["winner"] == "B兜底产物"
    assert runner.state.status == "done"


def test_serial_starts_one_at_a_time():
    """serial 节点同层就绪时,fetch_from_ssr 先跑完,fetch_from_wechat 才 queued(不同轮)。"""
    runner = Runner.load(str(SKIP))
    events = _events(runner)
    # fetch_from_ssr 的 final 早于 fetch_from_wechat 的 queued(serial 保证依次启动)
    a_final = next(
        i for i, e in enumerate(events) if e.type == "final" and e.step_id == "fetch_from_ssr"
    )
    b_q = next(
        i
        for i, e in enumerate(events)
        if e.type == "trace" and e.step_id == "fetch_from_wechat" and e.status == "queued"
    )
    assert a_final < b_q


def test_parallel_not_broken_without_serial():
    """没有 serial 声明时,同层节点照常同轮并行(fanout_flow 验证)。"""
    from pathlib import Path as _P
    fanout = _P(__file__).resolve().parent.parent / "examples" / "fanout_flow"
    runner = Runner.load(str(fanout))
    events = _events(runner)
    # 5 个 worker 都进入 queued(顺序不限,因为并行),都在 merge queued 之前
    worker_queued = {
        e.step_id
        for e in events
        if e.type == "trace" and e.status == "queued" and e.step_id.startswith("worker#")
    }
    assert len(worker_queued) == 5
