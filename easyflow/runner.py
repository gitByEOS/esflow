"""Runner:DAG 拓扑执行 + 并行调度 + 静态副本/动态扇出 + 事件流 + 人机协作控制循环。

主用法:

    runner = Runner.load("./my_flow")
    async for event in runner.run():
        if event.type == "checkpoint":
            runner.resume()        # 或 retry("review") / abort()

    # 单调试:只跑指定副本及其必需上游
    async for event in runner.run(only={"worker#2"}):
        ...

并行:同一轮就绪节点 asyncio.gather 并行,同步 run 用 to_thread 包。
接手确认(accept)run 前校验,脱手确认(deliver)run 后校验,失败 emit error。
checkpoint 时整个 job 暂停,等控制信号。
动态扇出:节点 run 返回 FanOut,runner 运行时创建副本 + 动态连边。
retry 时不重跑已完成且无依赖变更的上游,只重跑 from_step 及其下游。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncGenerator

from .event import WorkflowJobEvent, trace, final, checkpoint, error, end
from .flow import Edge, FlowDefine
from .loader import load_flow
from .state import JobState, StepState, apply_event
from .step import Checkpoint, FanOut, Node, StepDefine, _instantiate


@dataclass
class _ControlSignal:
    kind: str  # resume / retry / abort
    from_step: str | None = None


class _Ctx:
    """运行时 StepContext 实现:取上游 artifact + 动态扇出 payload/gather。"""

    def __init__(
        self,
        artifacts: dict[str, Any],
        fanout_payload: Any = None,
    ) -> None:
        self._artifacts = artifacts
        self.fanout_payload = fanout_payload

    def get(self, upstream_id: str) -> Any:
        if upstream_id not in self._artifacts:
            raise KeyError(f"上游节点尚未完成:{upstream_id}")
        return self._artifacts[upstream_id]

    def upstream_ids(self) -> list[str]:
        return list(self._artifacts.keys())

    def gather(self, base_id: str) -> list[Any]:
        """收集某动态 base 的所有副本产物,按 index 排序。"""
        items: list[tuple[int, Any]] = []
        prefix = base_id + "#"
        for sid, art in self._artifacts.items():
            if sid.startswith(prefix):
                i = int(sid.rsplit("#", 1)[1])
                items.append((i, art))
        items.sort()
        return [v for _, v in items]


class Runner:
    """加载并执行一个 flow,产出事件流,支持并行/动态扇出/暂停/重试/中止/单调试。"""

    def __init__(
        self,
        flow: FlowDefine,
        steps: dict[str, StepDefine],
        node_classes: dict[str, type[Node]],
    ) -> None:
        self.flow = flow
        self.steps = steps
        self.node_classes = node_classes
        self.state = JobState(flow_id=flow.id)
        self.artifacts: dict[str, Any] = {}
        self._resume_event = asyncio.Event()
        self._control: _ControlSignal | None = None
        self._aborted = False
        self._target: set[str] | None = None

    @classmethod
    def load(cls, flow_dir: str) -> "Runner":
        flow, steps, node_classes = load_flow(flow_dir)
        return cls(flow, steps, node_classes)

    def _all_edges(self) -> list[Edge]:
        """静态边 + 运行时动态扩图改写的 self.flow.edges。"""
        return self.flow.edges

    def _downstream(self, from_step: str) -> set[str]:
        """from_step 及其所有下游(含自己)。"""
        result = {from_step}
        changed = True
        while changed:
            changed = False
            for e in self._all_edges():
                if e.from_ in result and e.to not in result:
                    result.add(e.to)
                    changed = True
        return result

    def _required(self, only: set[str]) -> set[str]:
        """only 集合及其全部上游(单调试时只跑这些节点)。"""
        target = set(only)
        changed = True
        while changed:
            changed = False
            for e in self._all_edges():
                if e.to in target and e.from_ not in target:
                    target.add(e.from_)
                    changed = True
        return target

    def _ready_nodes(self) -> list[str]:
        """上游全部 done 且自己未 done 的节点(单调试时限定 target 内)。"""
        done = {
            sid for sid, s in self.state.steps.items() if s.status == "done"
        }
        ready: list[str] = []
        for sid in self.steps:
            if sid in done:
                continue
            if self._target is not None and sid not in self._target:
                continue
            upstreams = [e.from_ for e in self._all_edges() if e.to == sid]
            if all(u in done for u in upstreams):
                ready.append(sid)
        return ready

    def _paused_nodes(self) -> list[str]:
        return [sid for sid, s in self.state.steps.items() if s.status == "paused"]

    def _expand_fanout(self, fanout_node: str, fanout: FanOut) -> None:
        """运行时动态扩图:创建 N 个副本,改写边(上游→副本→下游)。"""
        base = fanout.base
        if base not in self.flow.dynamic:
            raise RuntimeError(f"FanOut 指向非 dynamic 声明的 base:{base}")
        n = fanout.n
        # 原边:base 的上游与下游
        original_upstream = [e.from_ for e in self.flow.edges if e.to == base]
        original_downstream = [e.to for e in self.flow.edges if e.from_ == base]
        # 创建副本实例,注入 fanout_payload
        for i in range(n):
            rid = f"{base}#{i}"
            sd = _instantiate(self.node_classes[base], rid, i)
            sd.node.fanout_payload = fanout.payload[i]
            self.steps[rid] = sd
            self.state.steps[rid] = StepState(step_id=rid)
        # 改写边:移除原 base 边,加 上游→副本 / 副本→下游
        new_edges = [
            e for e in self.flow.edges if e.from_ != base and e.to != base
        ]
        for i in range(n):
            rid = f"{base}#{i}"
            for up in original_upstream:
                new_edges.append(Edge(from_=up, to=rid))
            for dn in original_downstream:
                new_edges.append(Edge(from_=rid, to=dn))
        self.flow.edges = new_edges

    async def _run_one(self, sid: str, queue: asyncio.Queue) -> None:
        """跑单个节点:accept → run → deliver / FanOut,事件推入 queue。完成推 None。"""
        sd = self.steps[sid]
        node = sd.node
        await queue.put(trace(sid, "queued", f"就绪:{sd.title}"))
        await queue.put(trace(sid, "running", f"开始:{sd.title}"))
        ctx = _Ctx(self.artifacts, fanout_payload=getattr(node, "fanout_payload", None))

        # 接手确认
        try:
            ok = node.accept(ctx)
        except Exception as exc:
            await queue.put(error(sid, f"接手确认异常:{type(exc).__name__}: {exc}"))
            await queue.put(None)
            return
        if not ok:
            await queue.put(error(sid, "接手确认失败:前置条件不满足"))
            await queue.put(None)
            return

        # 执行(同步 run 用 to_thread 并行)
        try:
            result = await asyncio.to_thread(node.run, ctx)
        except Exception as exc:
            await queue.put(error(sid, f"{type(exc).__name__}: {exc}"))
            await queue.put(None)
            return

        # 动态扇出:run 返回 FanOut,扩图,本节点不产 artifact
        if isinstance(result, FanOut):
            self.state.steps[sid].status = "done"
            self._expand_fanout(sid, result)
            await queue.put(
                trace(sid, "done", f"扇出 {result.n} 个 {result.base} 副本")
            )
            await queue.put(None)
            return

        # 脱手确认
        try:
            ok = node.deliver(result)
        except Exception as exc:
            await queue.put(error(sid, f"脱手确认异常:{type(exc).__name__}: {exc}"))
            await queue.put(None)
            return
        if not ok:
            await queue.put(error(sid, "脱手确认失败:产物校验不通过"))
            await queue.put(None)
            return

        self.artifacts[sid] = result
        await queue.put(final(sid, result))
        if sd.checkpoint == Checkpoint.AFTER:
            await queue.put(checkpoint(sid, result))
        await queue.put(None)

    async def _await_control(self) -> AsyncGenerator[WorkflowJobEvent, None]:
        """checkpoint 暂停后等控制信号,处理 resume/retry/abort。无事件产出。"""
        await self._resume_event.wait()
        self._resume_event.clear()
        ctrl = self._control
        self._control = None
        if ctrl is None or ctrl.kind == "resume":
            for sid in self._paused_nodes():
                self.state.steps[sid].status = "done"
            self.state.status = "running"
        elif ctrl.kind == "retry":
            ds = self._downstream(ctrl.from_step or "")
            for s2 in ds:
                self.artifacts.pop(s2, None)
                st = self.state.steps.get(s2)
                if st:
                    st.status = "idle"
                    st.artifact = None
            self.state.status = "running"
        elif ctrl.kind == "abort":
            self._aborted = True

    async def run(
        self, only: set[str] | None = None
    ) -> AsyncGenerator[WorkflowJobEvent, None]:
        """执行 flow,产出事件流。only 指定时只跑 only 及其必需上游(单调试)。"""
        self._target = self._required(only) if only else None
        for sid in self.steps:
            if sid not in self.state.steps:
                self.state.steps[sid] = StepState(step_id=sid)

        while True:
            if self._aborted:
                yield error(None, "aborted")
                apply_event(self.state, error(None, "aborted"))
                yield end()
                apply_event(self.state, end())
                return

            ready = self._ready_nodes()
            if not ready:
                if self._paused_nodes():
                    await self._await_control()
                    continue
                yield end()
                apply_event(self.state, end())
                return

            # 同轮就绪节点并行跑,事件经 queue 顺序产出
            queue: asyncio.Queue = asyncio.Queue()
            tasks = [
                asyncio.create_task(self._run_one(sid, queue)) for sid in ready
            ]
            active = len(tasks)
            errored = False
            while active > 0:
                ev = await queue.get()
                if ev is None:
                    active -= 1
                    continue
                yield ev
                apply_event(self.state, ev)
                if ev.type == "error":
                    errored = True
            await asyncio.gather(*tasks)

            if errored:
                yield end()
                apply_event(self.state, end())
                return

            # 有节点暂停则等信号,不继续跑下游
            if self._paused_nodes():
                await self._await_control()
                continue

    # —— 控制信号(外部在 checkpoint 时调用)——

    def resume(self) -> None:
        """确认继续,paused 节点转 done,跑下一轮就绪节点。"""
        self._control = _ControlSignal(kind="resume")
        self._resume_event.set()

    def retry(self, from_step: str) -> None:
        """从 from_step 重跑:清 from_step 及下游的 artifact 与状态,上游复用。"""
        self._control = _ControlSignal(kind="retry", from_step=from_step)
        self._resume_event.set()

    def abort(self) -> None:
        """中止 job。checkpoint 时立即生效,运行中等当前步结束。"""
        self._control = _ControlSignal(kind="abort")
        self._aborted = True
        self._resume_event.set()
