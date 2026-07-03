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
接手确认(accept)返回 False → 跳过本节点(emit skipped,artifact 置 None,下游可推进)。
脱手确认(deliver)run 后校验,失败 emit error。
checkpoint 时整个 job 暂停,等控制信号。
动态扇出:节点 run 返回 FanOut,runner 运行时创建副本 + 动态连边。
retry 时不重跑已完成且无依赖变更的上游,只重跑 from_step 及其下游。
"""

from __future__ import annotations

import asyncio
import json
import random
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncGenerator

from .event import WorkflowJobEvent, trace, final, checkpoint, error, end
from .flow import Edge, FlowDefine
from .loader import load_flow
from .state import JobState, StepState, apply_event
from .step import Checkpoint, FanOut, Node, StepDefine, _instantiate

DEFAULT_OUTPUT_ROOT = Path("/tmp/easyflow/outputs")
DEBUG_OUTPUT_ROOT = Path("/tmp/easyflow/debug")
_ARTIFACT_FILE = "artifact.json"


def _gen_job_id() -> str:
    """时间戳 + 4 位短 hash,人眼可读且不冲突。"""
    stamp = time.strftime("%Y%m%d-%H%M%S")
    suffix = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=4))
    return f"{stamp}-{suffix}"


@dataclass
class _ControlSignal:
    kind: str  # resume / retry / abort
    from_step: str | None = None


class _Ctx:
    """运行时 StepContext 实现:取上游 artifact + 动态扇出 payload/gather + 同层/跨层 layer。"""

    def __init__(
        self,
        artifacts: dict[str, Any],
        depths: dict[str, int],
        fanout_payload: Any = None,
    ) -> None:
        self._artifacts = artifacts
        self._depths = depths
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

    def layer(self, depth: int) -> list[Any]:
        """该拓扑深度的所有已完成节点产物 list,按声明顺序(含动态副本展开顺序)。
        skip 节点 artifact 为 None,一并返回便于枚举同层前序。"""
        return [
            self._artifacts.get(sid)
            for sid, d in self._depths.items()
            if d == depth and sid in self._artifacts
        ]


class Runner:
    """加载并执行一个 flow,产出事件流,支持并行/动态扇出/暂停/重试/中止/单调试。"""

    def __init__(
        self,
        flow: FlowDefine,
        steps: dict[str, StepDefine],
        node_classes: dict[str, type[Node]],
        output_root: Path = DEFAULT_OUTPUT_ROOT,
        debug: bool = False,
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
        self._break_before: set[str] | None = None
        self._break_triggered = False
        # 产物持久化:每节点 output_dir = job_dir / <step_id>,节点自己写文件
        # debug 模式:job_dir 固定(无 job_id),产物累积,artifact 持久化供单调试复用上游
        self.debug = debug
        self.output_root = output_root
        self.job_id = _gen_job_id()
        if debug:
            self.job_dir = DEBUG_OUTPUT_ROOT / flow.id
        else:
            self.job_dir = output_root / flow.id / self.job_id
        # 拓扑深度:depth(node) = 1 + max(depth(upstream)),入口 0;回填到 StepDefine 和 Node
        self._depths = self._compute_depths()
        for sid, sd in self.steps.items():
            d = self._depths.get(sid, 0)
            sd.depth = d
            sd.node.depth = d

    @classmethod
    def load(
        cls, flow_dir: str, output_root: Path = DEFAULT_OUTPUT_ROOT, debug: bool = False
    ) -> "Runner":
        flow, steps, node_classes = load_flow(flow_dir)
        return cls(flow, steps, node_classes, output_root=output_root, debug=debug)

    def clear_debug(self) -> None:
        """debug 模式:清空 job_dir 下持久化产物,下次 run 从头跑。非 debug 无操作。"""
        if not self.debug:
            return
        shutil.rmtree(self.job_dir, ignore_errors=True)

    def missing_upstream(self, target: set[str]) -> list[str]:
        """debug 单点调试预检:target 节点的全部上游中,缺 artifact.json 的 id 列表。
        cmd_debug 用它判断是否该提示用户先全跑,而非静默打开 view 后无节点可跑。"""
        required = set()
        for x in target:
            required |= self._required({x})
        required -= target
        return [
            sid for sid in required
            if not (self.job_dir / sid / _ARTIFACT_FILE).exists()
        ]

    def missing_upstream(self, scope: set[str]) -> list[str]:
        """返回 scope 节点的所有上游中磁盘无 artifact.json 的节点 id 列表。
        scope 单点调试前调用,缺失则上游无法复用,提示用户先全跑落产物。"""
        if not self.debug:
            return []
        needed: set[str] = set()
        frontier: set[str] = set(scope)
        while frontier:
            n = frontier.pop()
            for e in self.flow.edges:
                if e.to == n and e.from_ not in needed:
                    needed.add(e.from_)
                    frontier.add(e.from_)
        return [
            sid for sid in needed
            if not (self.job_dir / sid / _ARTIFACT_FILE).exists()
        ]

    def _compute_depths(self) -> dict[str, int]:
        """算所有节点拓扑深度。动态 base 以 base id 参与算,副本展开时继承 base depth。"""
        depth: dict[str, int] = {}

        def dfs(sid: str) -> int:
            if sid in depth:
                return depth[sid]
            ups = [e.from_ for e in self.flow.edges if e.to == sid]
            depth[sid] = 0 if not ups else 1 + max(dfs(u) for u in ups)
            return depth[sid]

        for sid in self.steps:
            dfs(sid)
        # 动态 base 没实例化但 _expand_fanout 要用其 depth
        for base in self.flow.dynamic:
            if base not in depth:
                dfs(base)
        return depth

    def _persist_artifact(self, sid: str, artifact: Any) -> None:
        """debug 模式:节点 done/skipped 后把 artifact 序列化到 output_dir/artifact.json。
        Path 等非 JSON 类型用 default=str 兜底;FanOut 不持久化(动态扩图指令非产物)。"""
        if not self.debug:
            return
        out = self.job_dir / sid
        out.mkdir(parents=True, exist_ok=True)
        (out / _ARTIFACT_FILE).write_text(
            json.dumps(artifact, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    def _load_persisted_artifacts(self, skip: set[str] | None = None) -> None:
        """debug 模式:启动时扫描 job_dir/<sid>/artifact.json,加载到 self.artifacts 并标 done/skipped。
        已完成节点被 _ready_nodes 自然跳过,单调试时上游产物直接复用,不重跑。
        skip 内的节点不加载(强制重跑)——scope 单点调试时跳过目标节点本身。"""
        if not self.debug or not self.job_dir.exists():
            return
        skip = skip or set()
        for sid in self.steps:
            if sid in skip:
                continue
            art_file = self.job_dir / sid / _ARTIFACT_FILE
            if not art_file.exists():
                continue
            try:
                artifact = json.loads(art_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            self.artifacts[sid] = artifact
            st = self.state.steps.setdefault(sid, StepState(step_id=sid))
            st.status = "skipped" if artifact is None else "done"
            st.artifact = artifact

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
        """上游全部 done/skipped 且自己未 done/skipped/paused 的节点(单调试时限定 target 内)。"""
        completed = {
            sid
            for sid, s in self.state.steps.items()
            if s.status in ("done", "skipped")
        }
        not_ready = completed | {
            sid for sid, s in self.state.steps.items() if s.status == "paused"
        }
        ready: list[str] = []
        for sid in self.steps:
            if sid in not_ready:
                continue
            if self._target is not None and sid not in self._target:
                continue
            upstreams = [e.from_ for e in self._all_edges() if e.to == sid]
            if all(u in completed for u in upstreams):
                ready.append(sid)
        return ready

    def _paused_nodes(self) -> list[str]:
        return [sid for sid, s in self.state.steps.items() if s.status == "paused"]

    def _apply_serial(self, ready: list[str]) -> list[str]:
        """serial 节点同层依次启动:ready 中属于 serial 集合的,只保留 flow.nodes 顺序最靠前的那个;
        非 serial 节点照常并行。返回实际本轮启动的节点列表。"""
        serial_ready = [sid for sid in ready if sid in self.flow.serial]
        if len(serial_ready) <= 1:
            return ready
        # 按 flow.nodes 声明顺序取第一个
        order = {sid: i for i, sid in enumerate(self.flow.nodes)}
        first = min(serial_ready, key=lambda s: order.get(s, len(order)))
        return [sid for sid in ready if sid not in self.flow.serial or sid == first]

    def _expand_fanout(self, fanout_node: str, fanout: FanOut) -> None:
        """运行时动态扩图:创建 N 个副本,改写边(上游→副本→下游)。"""
        base = fanout.base
        if base not in self.flow.dynamic:
            raise RuntimeError(f"FanOut 指向非 dynamic 声明的 base:{base}")
        n = fanout.n
        # 原边:base 的上游与下游
        original_upstream = [e.from_ for e in self.flow.edges if e.to == base]
        original_downstream = [e.to for e in self.flow.edges if e.from_ == base]
        # 副本继承 base 拓扑深度(替换 base 在 DAG 里的位置)
        base_depth = self._depths.get(base, 0)
        # 创建副本实例,注入 fanout_payload / depth / output_dir
        for i in range(n):
            rid = f"{base}#{i}"
            sd = _instantiate(self.node_classes[base], rid, i, depth=base_depth)
            sd.node.fanout_payload = fanout.payload[i]
            sd.node.output_dir = self.job_dir / rid
            self.steps[rid] = sd
            self.state.steps[rid] = StepState(step_id=rid)
            self._depths[rid] = base_depth
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
        # 注入产物目录路径(skip 节点不创建目录,accept 通过后才 mkdir)
        node.output_dir = self.job_dir / sid
        await queue.put(trace(sid, "queued", f"就绪:{sd.title}"))
        await queue.put(trace(sid, "running", f"开始:{sd.title}"))
        ctx = _Ctx(
            self.artifacts, depths=self._depths,
            fanout_payload=getattr(node, "fanout_payload", None),
        )

        # 接手确认:返回 False 表示不接手,跳过本节点(artifact 置 None,下游可推进)
        try:
            ok = node.accept(ctx)
        except Exception as exc:
            await queue.put(error(sid, f"接手确认异常:{type(exc).__name__}: {exc}"))
            await queue.put(None)
            return
        if not ok:
            self.artifacts[sid] = None
            self._persist_artifact(sid, None)
            await queue.put(trace(sid, "skipped", f"跳过:{sd.title}"))
            await queue.put(None)
            return

        # accept 通过,创建产物目录,节点自己往里写文件
        node.output_dir.mkdir(parents=True, exist_ok=True)

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
        self._persist_artifact(sid, result)
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
                st = self.state.steps[sid]
                # checkpoint AFTER 暂停:artifact 已写入,确认后转 done
                # break_before 暂停:artifact 为 None,转 idle 让下一轮重新就绪执行
                st.status = "done" if st.artifact is not None else "idle"
            self.state.status = "running"
        elif ctrl.kind == "retry":
            ds = self._downstream(ctrl.from_step or "")
            for s2 in ds:
                self.artifacts.pop(s2, None)
                st = self.state.steps.get(s2)
                if st:
                    st.status = "idle"
                    st.artifact = None
                # debug 模式:清磁盘 artifact.json,防止下次启动加载到旧产物
                if self.debug:
                    art_file = self.job_dir / s2 / _ARTIFACT_FILE
                    art_file.unlink(missing_ok=True)
            self.state.status = "running"
        elif ctrl.kind == "abort":
            self._aborted = True

    async def run(
        self,
        only: set[str] | None = None,
        break_before: set[str] | None = None,
        scope: set[str] | None = None,
    ) -> AsyncGenerator[WorkflowJobEvent, None]:
        """执行 flow,产出事件流。

        only 指定时只跑 only 及其必需上游(单调试,run 模式用,无持久化必须跑上游)。
        scope 指定时 _target 限定为 scope 本身(不含上游),上游必须已完成否则 scope
        节点不就绪——debug --node 用,上游从磁盘加载产物,只反复跑指定节点。
        break_before 指定时,这些节点就绪后不立即执行,先 emit checkpoint 暂停,
        等 resume 信号才转 idle 重新就绪执行——用于 view 在指定节点前停下来观察。
        """
        if scope is not None:
            self._target = set(scope)
        elif only is not None:
            self._target = self._required(only)
        else:
            self._target = None
        self._break_before = set(break_before) if break_before else None
        self._break_triggered = False
        for sid in self.steps:
            if sid not in self.state.steps:
                self.state.steps[sid] = StepState(step_id=sid)
        # debug 模式:加载上次持久化产物,已完成节点跳过,单调试复用上游
        # scope 模式:scope 内节点不加载产物,强制重跑(单点调试反复执行 X)
        self._load_persisted_artifacts(skip=self._target if scope is not None else None)

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

            # serial 节点同层依次启动:同轮只启动 nodes 顺序最靠前的那个 serial 节点,
            # 其他 serial 节点等下一轮(非 serial 节点照常并行)
            to_run = self._apply_serial(ready)

            # break_before:首次就绪时把目标节点从本轮移除,设 paused + emit checkpoint,
            # 其他节点照常跑。一次性触发,resume 后不再拦截
            if self._break_before and not self._break_triggered:
                hits = [sid for sid in to_run if sid in self._break_before]
                if hits:
                    self._break_triggered = True
                    to_run = [sid for sid in to_run if sid not in self._break_before]
                    for sid in hits:
                        self.state.steps[sid].status = "paused"
                        yield checkpoint(sid, None)
                        apply_event(self.state, checkpoint(sid, None))

            # 同轮就绪节点并行跑,事件经 queue 顺序产出
            queue: asyncio.Queue = asyncio.Queue()
            tasks = [
                asyncio.create_task(self._run_one(sid, queue)) for sid in to_run
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
