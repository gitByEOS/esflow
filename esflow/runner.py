"""Runner:DAG 拓扑执行 + 并行调度 + 静态副本/动态扇出 + 事件流 + 人机协作控制循环。

主用法:

    runner = Runner.load("./my_flow")
    async for event in runner.run():
        if event.type == "checkpoint":
            runner.resume()        # 或 retry("review") / abort()

    # 单调试:只跑指定副本及其必需上游
    async for event in runner.run(only={"worker#2"}):
        ...

    # TO_AGENT 续跑:agent 已写产物,加载所有产物 + 跑 pending + 下游
    async for event in runner.run(resume=True):
        ...

并行:同一轮就绪节点 asyncio.gather 并行,同步 run 用 to_thread 包。
接手确认(accept)返回 False → 跳过本节点(emit skipped,artifact 置 None,下游可推进)。
脱手确认(deliver)run 后校验,失败 emit error。
checkpoint TO_HUMAN 时整个 job 暂停 stdin 等控制信号;TO_AGENT 时 emit checkpoint 退出进程,
等外部 agent 写产物到 output_dir 后用 --resume 续跑(框架扫文件构造 artifact + deliver 校验)。
动态扇出:节点 run 返回 FanOut,runner 运行时创建副本 + 动态连边。
retry 时不重跑已完成且无依赖变更的上游,只重跑 from_node 及其下游。
from_depth 按拓扑深度续跑:重跑 depth >= N 的所有节点,上游 depth < N 复用。
"""

from __future__ import annotations

import asyncio
import json
import random
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Literal, NamedTuple

from .event import JobEvent, trace, final, checkpoint, error, end
from .flow import Edge, FlowDefine
from .loader import load_flow
from .state import JobState, JobStatus, RunState, NodeStatus, apply_event
from .node import Checkpoint, FanOut, Node, DepthScope, _instantiate

DEFAULT_OUTPUT_ROOT = Path("/tmp/esflow/outputs")
DEBUG_OUTPUT_ROOT = Path("/tmp/esflow/debug")
ESFLOW_META_DIR = ".esflow"
_ARTIFACT_FILE = "artifact.json"
_BREAK_TO_AGENT_FILE = "break_to_agent.json"
_FLOW_DIR_FILE = "flow_dir.txt"


def _error_from_exc(run_id: str, msg: str, exc: Exception) -> JobEvent:
    """从异常构造 error event:透传 exc 引用 + 类全名 + __dict__,as_exception() 直接还原。"""
    return error(
        run_id,
        msg,
        exc_attrs=getattr(exc, "__dict__", {}) or None,
        exc=exc,
        exc_type=f"{type(exc).__module__}.{type(exc).__name__}",
    )


def _gen_job_id() -> str:
    """时间戳 + 4 位短 hash,人眼可读且不冲突。"""
    stamp = time.strftime("%Y%m%d-%H%M%S")
    suffix = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=4))
    return f"{stamp}-{suffix}"


def _is_custom_output_dir(path: Path) -> bool:
    """节点是否显式设置了 output_dir(非默认空 Path)。"""
    return path is not None and path != Path()


@dataclass
class _ControlSignal:
    kind: str  # resume / retry / abort
    from_node: str | None = None


# run_to_break 断点类型:end=正常结束,to_agent=TO_AGENT 检查点,error=节点抛异常
BreakKind = Literal["end", "to_agent", "error"]


# run() 加载策略:决定启动前如何处理 job_dir 持久化产物
_LoadStrategy = Literal[
    "load_all",                       # 全量加载已完成产物,跳过已完成节点
    "load_skip_target",               # 加载但跳过 target,强制重跑 target(nodes 单点调试)
    "invalidate_all",                 # 清掉所有节点状态与产物(--out 全跑)
    "load_skip_target_then_invalidate",  # 加载跳过 target 后再清 target(from_node 续跑)
]


class _RunArgs(NamedTuple):
    """_parse_run_args 的返回:target 限定跑哪些节点 + 暂停点 + 加载策略。"""

    target: set[str] | None
    break_before: set[str] | None
    load_strategy: _LoadStrategy


def _parse_run_args(
    only: set[str] | None,
    break_before: set[str] | None,
    nodes: set[str] | None,
    from_node: str | None,
    from_depth: int | None,
    explicit_job_dir: bool,
    downstream: Callable[[str], set[str]],
    required: Callable[[set[str]], set[str]],
    target_by_depth: Callable[[int], set[str]],
    resume: bool = False,
) -> _RunArgs:
    """把 run() 的 5 个互斥入参解析成统一的 _RunArgs。

    优先级:resume > from_node > from_depth > nodes > only > 默认。各模式推导 target 与加载策略:
    - resume:     target = None,load_all(--resume 续跑:加载所有产物,跑 pending to_agent + 下游)
    - from_node:  target = from_node 下游,加载跳过 target 后再清 target(续跑)
    - from_depth: target = depth >= from_depth 的节点,加载跳过 target 后再清 target(按层续跑)
    - nodes:      target = nodes 本身,加载跳过 target(单点调试上游复用)
    - only:       target = only 及其上游,全量加载(跑必需闭包)
    - 默认:       target = None,explicit_job_dir 时清全部,否则全量加载
    """
    if resume:
        return _RunArgs(None, break_before, "load_all")
    if from_node is not None:
        target = downstream(from_node)
        return _RunArgs(target, break_before, "load_skip_target_then_invalidate")
    if from_depth is not None:
        target = target_by_depth(from_depth)
        return _RunArgs(target, break_before, "load_skip_target_then_invalidate")
    if nodes is not None:
        return _RunArgs(set(nodes), break_before, "load_skip_target")
    if only is not None:
        return _RunArgs(required(only), break_before, "load_all")
    if explicit_job_dir:
        return _RunArgs(None, break_before, "invalidate_all")
    return _RunArgs(None, break_before, "load_all")


_MISSING = object()


class _Ctx:
    """运行时 DepthScope 实现:取上游 artifact + 动态扇出 gather + 同层/跨层 layer。

    同 depth 的所有副本共享同一份 _artifacts / _depths(全局引用),没有 per-run
    私有状态。ctx 表达的是 depth 作用域,不是节点私有上下文。副本私有数据
    (动态扇出载荷)通过 Node.fanout_payload 访问,不进 ctx。
    """

    def __init__(
        self,
        artifacts: dict[str, Any],
        depths: dict[str, int],
    ) -> None:
        self._artifacts = artifacts
        self._depths = depths

    def get(self, upstream_id: str, default: Any = _MISSING) -> Any:
        """取上游 artifact。未完成时:未传 default 抛 KeyError,传了则返回 default。"""
        if upstream_id not in self._artifacts:
            if default is _MISSING:
                raise KeyError(f"上游节点尚未完成:{upstream_id}")
            return default
        return self._artifacts[upstream_id]

    def upstream_ids(self) -> list[str]:
        return list(self._artifacts.keys())

    def gather(self, base_id: str) -> list[Any]:
        """收集某动态 base 的所有副本产物,按 index 排序。"""
        items: list[tuple[int, Any]] = []
        prefix = base_id + "#"
        for rid, art in self._artifacts.items():
            if rid.startswith(prefix):
                i = int(rid.rsplit("#", 1)[1])
                items.append((i, art))
        items.sort()
        return [v for _, v in items]

    def layer(self, depth: int) -> list[Any]:
        """该拓扑深度的所有已完成节点产物 list,按声明顺序(含动态副本展开顺序)。
        skip 节点 artifact 为 None,一并返回便于枚举同层前序。"""
        return [
            self._artifacts.get(rid)
            for rid, d in self._depths.items()
            if d == depth and rid in self._artifacts
        ]


class Runner:
    """加载并执行一个 flow,产出事件流,支持并行/动态扇出/暂停/重试/中止/单调试。"""

    def __init__(
        self,
        flow: FlowDefine,
        runs: dict[str, Node],
        node_classes: dict[str, type[Node]],
        output_root: Path = DEFAULT_OUTPUT_ROOT,
        debug: bool = False,
        job_dir: Path | None = None,
        node_args: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.flow = flow
        self.runs = runs
        self.node_classes = node_classes
        self.state = JobState(flow_id=flow.id)
        self.artifacts: dict[str, Any] = {}
        self._resume_event = asyncio.Event()
        self._control: _ControlSignal | None = None
        self._aborted = False
        self._target: set[str] | None = None
        self._break_before: set[str] | None = None
        self._break_triggered = False
        # 邻接表缓存:从 self.flow.edges 派生,动态扩图后 _rebuild_adjacency 重建
        self._upstream_map: dict[str, list[str]] = {}
        self._downstream_map: dict[str, list[str]] = {}
        # 产物持久化:每节点 output_dir = job_dir / <run_id>,节点自己写文件
        # debug 模式:job_dir 固定(无 job_id),产物累积,artifact 持久化供单调试复用上游
        self.debug = debug
        self._explicit_job_dir = job_dir is not None
        # 全持久化:所有 flow 都落盘,默认 /tmp 享受系统自动清理。
        # 用户要长期保留就显式 --out 到持久目录。
        self.output_root = output_root
        self.job_id = _gen_job_id()
        if job_dir is not None:
            self.job_dir = Path(job_dir)
        elif debug:
            self.job_dir = DEBUG_OUTPUT_ROOT / flow.id
        else:
            self.job_dir = output_root / flow.id / self.job_id
        # 邻接表先建,_compute_depths 要用
        self._rebuild_adjacency()
        # 拓扑深度:depth(node) = 1 + max(depth(upstream)),入口 0;回填到 Node 实例
        self._depths = self._compute_depths()
        for rid, node in self.runs.items():
            d = self._depths.get(rid, 0)
            node.depth = d
        # 节点入参注入:node_args = {base_id: kwargs_dict},匹配 base 与所有副本(base#i)
        # 动态副本在 _expand_fanout 创建时从 self._node_args 继承 base 的入参
        self._node_args: dict[str, dict[str, Any]] = node_args or {}
        for base, kw in self._node_args.items():
            for rid, node in self.runs.items():
                if rid == base or rid.startswith(base + "#"):
                    node.kwargs = dict(kw)

    @classmethod
    def load(
        cls,
        flow_dir: str,
        output_root: Path = DEFAULT_OUTPUT_ROOT,
        debug: bool = False,
        job_dir: Path | str | None = None,
        node_args: dict[str, dict[str, Any]] | None = None,
    ) -> "Runner":
        flow, runs, node_classes = load_flow(flow_dir)
        resolved_job_dir = Path(job_dir) if job_dir is not None else None
        return cls(
            flow,
            runs,
            node_classes,
            output_root=output_root,
            debug=debug,
            job_dir=resolved_job_dir,
            node_args=node_args,
        )

    def _meta_path(self, *parts: str) -> Path:
        """框架元数据根:job_dir/.esflow/ 下拼接子路径。"""
        return self.job_dir.joinpath(ESFLOW_META_DIR, *parts)

    def clear_debug(self) -> None:
        """debug 模式:清空 job_dir 下持久化产物,下次 run 从头跑。非 debug 无操作。"""
        if not self.debug:
            return
        shutil.rmtree(self.job_dir, ignore_errors=True)

    def missing_upstream(self, nodes: set[str]) -> list[str]:
        """返回 nodes 节点的所有上游中磁盘无 artifact.json 的节点 id 列表。"""
        needed: set[str] = set()
        frontier: set[str] = set(nodes)
        while frontier:
            n = frontier.pop()
            for up in self._upstream_map.get(n, []):
                if up not in needed:
                    needed.add(up)
                    frontier.add(up)
        return [
            rid for rid in needed
            if not self._meta_path(rid, _ARTIFACT_FILE).exists()
        ]

    def _rebuild_adjacency(self) -> None:
        """从 self.flow.edges 重建邻接表。__init__ 与 _expand_fanout 改图后调用。"""
        self._upstream_map = {}
        self._downstream_map = {}
        for e in self.flow.edges:
            self._downstream_map.setdefault(e.from_, []).append(e.to)
            self._upstream_map.setdefault(e.to, []).append(e.from_)

    def _compute_depths(self) -> dict[str, int]:
        """算所有节点拓扑深度。动态 base 以 base id 参与算,副本展开时继承 base depth。"""
        depth: dict[str, int] = {}

        def dfs(rid: str) -> int:
            if rid in depth:
                return depth[rid]
            ups = self._upstream_map.get(rid, [])
            depth[rid] = 0 if not ups else 1 + max(dfs(u) for u in ups)
            return depth[rid]

        for rid in self.runs:
            dfs(rid)
        # 动态 base 没实例化但 _expand_fanout 要用其 depth
        for base in self.flow.dynamic:
            if base not in depth:
                dfs(base)
        return depth

    def _persist_artifact(self, rid: str, artifact: Any) -> None:
        """节点 done/skipped 后把 artifact 序列化到 .esflow/<rid>/artifact.json。
        Path 等非 JSON 类型用 default=str 兜底;FanOut 不持久化(动态扩图指令非产物)。"""
        out = self._meta_path(rid)
        out.mkdir(parents=True, exist_ok=True)
        (out / _ARTIFACT_FILE).write_text(
            json.dumps(artifact, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    def _load_persisted_artifacts(self, skip: set[str] | None = None) -> None:
        """启动时扫描 .esflow/<rid>/artifact.json,加载到 self.artifacts 并标 done/skipped。
        已完成节点被 _ready_nodes 自然跳过,单调试时上游产物直接复用,不重跑。
        skip 内的节点不加载(强制重跑)——nodes 单点调试时跳过目标节点本身。"""
        if not self.job_dir.exists():
            return
        skip = skip or set()
        for rid in self.runs:
            if rid in skip:
                continue
            art_file = self._meta_path(rid, _ARTIFACT_FILE)
            if not art_file.exists():
                continue
            try:
                artifact = json.loads(art_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            self.artifacts[rid] = artifact
            st = self.state.runs.setdefault(rid, RunState(run_id=rid))
            st.status = NodeStatus.SKIPPED if artifact is None else NodeStatus.DONE
            st.artifact = artifact

    def _invalidate_runs(self, nodes: set[str]) -> None:
        """清掉本次要重跑节点的内存状态与磁盘产物(框架元数据 + 业务产物)。"""
        for rid in nodes:
            self.artifacts.pop(rid, None)
            st = self.state.runs.get(rid)
            if st:
                st.status = NodeStatus.IDLE
                st.artifact = None
                st.detail = ""
                st.text = ""
            shutil.rmtree(self._meta_path(rid), ignore_errors=True)
            shutil.rmtree(self.job_dir / rid, ignore_errors=True)

    def _break_to_agent_path(self) -> Path:
        return self._meta_path(_BREAK_TO_AGENT_FILE)

    def _write_break_to_agent(self, rid: str) -> None:
        """首次跑到 TO_AGENT 节点:把 pending 节点 id 追加到 .esflow/break_to_agent.json。"""
        path = self._break_to_agent_path()
        pending: list[str] = []
        if path.exists():
            try:
                pending = json.loads(path.read_text(encoding="utf-8")).get("pending", [])
            except (OSError, json.JSONDecodeError):
                pending = []
        if rid not in pending:
            pending.append(rid)
        path.write_text(
            json.dumps({"pending": pending}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _clear_break_to_agent(self, rid: str) -> None:
        """--resume 完成 TO_AGENT 节点后:从 .esflow/break_to_agent.json 移除。空了删文件。"""
        path = self._break_to_agent_path()
        if not path.exists():
            return
        try:
            pending = json.loads(path.read_text(encoding="utf-8")).get("pending", [])
        except (OSError, json.JSONDecodeError):
            pending = []
        pending = [r for r in pending if r != rid]
        if pending:
            path.write_text(
                json.dumps({"pending": pending}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        else:
            path.unlink(missing_ok=True)

    def has_break_to_agent(self) -> bool:
        """启动时检测:job_dir 下是否有未完成的 TO_AGENT 节点。"""
        return self._break_to_agent_path().exists()

    def pending_break_to_agent(self) -> list[str]:
        """返回 .esflow/break_to_agent.json 里 pending 的 TO_AGENT 节点 id 列表。"""
        path = self._break_to_agent_path()
        if not path.exists():
            return []
        try:
            return json.loads(path.read_text(encoding="utf-8")).get("pending", [])
        except (OSError, json.JSONDecodeError):
            return []

    @staticmethod
    def to_agent_hint(event: JobEvent, resume_cmd: str | None = None) -> str:
        """把 TO_AGENT checkpoint event 格式化成介入指引字符串(skill 直接印)。

        event.resume_hint 由框架填好(node_dir/upstream_artifact/job_dir/node_id)。
        resume_cmd 是 skill 自己的续跑命令模板,必须含 {job_dir} 占位符,框架填充。
        不传 resume_cmd 则只输出节点目录与上游产物。
        """
        hint = event.resume_hint or {}
        node_dir = hint.get("node_dir", "")
        upstream = hint.get("upstream_artifact")
        job_dir = hint.get("job_dir", "")
        lines = [f"[to_agent] 写产物到:{node_dir}"]
        if upstream is not None:
            lines.append(f"[to_agent] 上游产物:{upstream}")
        if resume_cmd:
            if "{job_dir}" not in resume_cmd:
                raise ValueError("resume_cmd 必须含 {job_dir} 占位符")
            lines.append(f"[to_agent] 完成后续跑:{resume_cmd.format(job_dir=job_dir)}")
        return "\n".join(lines)

    @staticmethod
    def to_envelope(break_kind: BreakKind, break_event: JobEvent | None) -> tuple[int, dict[str, Any]]:
        """把 run_to_break 的断点翻译成 (exit_code, envelope),skill 入口一行调用消灭胶水。

        - end:      (0, {"status": "end"})
        - to_agent: (2, {"status": "to_agent", "node_id":..., "resume_hint":...})
        - error:    (1, {"status": "error", "message":..., "exc_type":..., "exc_attrs":...})

        envelope 是纯 dict,skill 直接 json.dumps 输出;error 时 raise break_event.as_exception()
        与本方法正交,由 skill 自行决定。
        """
        if break_kind == "end":
            return 0, {"status": "end"}
        if break_kind == "to_agent":
            assert break_event is not None, "to_agent 断点必须带 break_event"
            return 2, {
                "status": "to_agent",
                "node_id": break_event.run_id,
                "resume_hint": break_event.resume_hint,
            }
        if break_kind == "error":
            assert break_event is not None, "error 断点必须带 break_event"
            return 1, {
                "status": "error",
                "message": break_event.message,
                "exc_type": break_event.exc_type,
                "exc_attrs": break_event.exc_attrs,
            }
        raise ValueError(f"未知 break_kind: {break_kind}")

    def _downstream(self, from_node: str) -> set[str]:
        """from_node 及其所有下游(含自己)。"""
        result = {from_node}
        frontier = [from_node]
        while frontier:
            n = frontier.pop()
            for m in self._downstream_map.get(n, []):
                if m not in result:
                    result.add(m)
                    frontier.append(m)
        return result

    def _required(self, only: set[str]) -> set[str]:
        """only 集合及其全部上游(单调试时只跑这些节点)。"""
        target = set(only)
        frontier = list(only)
        while frontier:
            n = frontier.pop()
            for up in self._upstream_map.get(n, []):
                if up not in target:
                    target.add(up)
                    frontier.append(up)
        return target

    def _target_by_depth(self, n: int) -> set[str]:
        """from_depth 用:depth >= n 的所有运行实例 id。"""
        return {rid for rid, d in self._depths.items() if d >= n}

    @property
    def max_depth(self) -> int:
        """当前 DAG 最大拓扑深度,from_depth 越界校验用。"""
        return max(self._depths.values()) if self._depths else 0

    def _ready_nodes(self) -> list[str]:
        """上游全部 done/skipped 且自己未 done/skipped/paused 的节点(单调试时限定 target 内)。"""
        completed = {
            rid
            for rid, s in self.state.runs.items()
            if s.status in (NodeStatus.DONE, NodeStatus.SKIPPED)
        }
        not_ready = completed | {
            rid for rid, s in self.state.runs.items() if s.status == NodeStatus.PAUSED
        }
        ready: list[str] = []
        for rid in self.runs:
            if rid in not_ready:
                continue
            if self._target is not None and rid not in self._target:
                continue
            if all(u in completed for u in self._upstream_map.get(rid, [])):
                ready.append(rid)
        return ready

    def _paused_nodes(self) -> list[str]:
        return [rid for rid, s in self.state.runs.items() if s.status == NodeStatus.PAUSED]

    def _apply_serial(self, ready: list[str]) -> list[str]:
        """serial 节点同层依次启动:ready 中属于 serial 集合的,只保留 flow.nodes 顺序最靠前的那个;
        非 serial 节点照常并行。返回实际本轮启动的节点列表。"""
        serial_ready = [rid for rid in ready if rid in self.flow.serial]
        if len(serial_ready) <= 1:
            return ready
        # 按 flow.nodes 声明顺序取第一个
        order = {rid: i for i, rid in enumerate(self.flow.nodes)}
        first = min(serial_ready, key=lambda s: order.get(s, len(order)))
        return [rid for rid in ready if rid not in self.flow.serial or rid == first]

    def _expand_fanout(self, fanout_node: str, fanout: FanOut) -> None:
        """运行时动态扩图:创建 N 个副本,改写边(上游→副本→下游)。"""
        base = fanout.base
        if base not in self.flow.dynamic:
            raise RuntimeError(f"FanOut 指向非 dynamic 声明的 base:{base}")
        n = fanout.n
        # 原边:base 的上游与下游(从邻接表取,避免遍历 edges)
        original_upstream = self._upstream_map.get(base, [])
        original_downstream = self._downstream_map.get(base, [])
        # 副本继承 base 拓扑深度(替换 base 在 DAG 里的位置)
        base_depth = self._depths.get(base, 0)
        # 创建副本实例,注入 fanout_payload / depth / output_dir
        for i in range(n):
            rid = f"{base}#{i}"
            node = _instantiate(self.node_classes[base], rid, i, depth=base_depth)
            node.fanout_payload = fanout.payload[i]
            node.output_dir = self.job_dir / rid
            # 继承 base 的入参(若 node_args 有)
            if base in self._node_args:
                node.kwargs = dict(self._node_args[base])
            self.runs[rid] = node
            self.state.runs[rid] = RunState(run_id=rid)
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
        self._rebuild_adjacency()

    async def _run_one(self, rid: str, queue: asyncio.Queue) -> None:
        """跑单个节点:accept → run → deliver / FanOut,事件推入 queue。完成推 None。"""
        node = self.runs[rid]
        await queue.put(trace(rid, "queued", f"就绪:{node.title or node.id}"))
        await queue.put(trace(rid, "running", f"开始:{node.title or node.id}"))
        ctx = _Ctx(self.artifacts, depths=self._depths)

        # TO_AGENT 节点:不调 run,产物由外部 agent 写入 output_dir
        # 首次跑(无产物文件):emit checkpoint + 设 PAUSED + 写 .esflow/break_to_agent.json,进程由主循环退出
        # --resume(有产物文件):扫文件构造 artifact + deliver 校验 + 转 DONE,跑下游
        # TO_AGENT 也走 accept:校验前置 + 可在 accept 里设 self.output_dir 指向业务目录(work_dir),
        # 框架尊重节点自定义 output_dir,否则 fallback 到 job_dir/rid
        if node.checkpoint == Checkpoint.TO_AGENT:
            try:
                ok = node.accept(ctx)
            except Exception as exc:
                await queue.put(_error_from_exc(
                    rid, f"接手确认异常:{type(exc).__name__}: {exc}", exc))
                await queue.put(None)
                return
            if not ok:
                self.artifacts[rid] = None
                self._persist_artifact(rid, None)
                await queue.put(trace(rid, "skipped", f"跳过:{node.title or node.id}"))
                await queue.put(None)
                return
            node.output_dir = (
                node.output_dir if _is_custom_output_dir(node.output_dir)
                else self.job_dir / rid
            )
            node.output_dir.mkdir(parents=True, exist_ok=True)
            files = [
                f.name for f in node.output_dir.iterdir()
                if not f.name.startswith(".")
            ]
            artifact = {"output_dir": str(node.output_dir), "files": sorted(files)}
            # TO_AGENT 语义:deliver 校验 agent 是否已写产物
            # - deliver 通过 → agent 已写,转 DONE 跑下游
            # - deliver 不通过 → agent 未写或未写完,emit checkpoint 让 agent 写(非 error)
            # - deliver 抛异常 → error
            # 统一走 deliver 校验,不再用 files 是否为空分支(自定义 output_dir 时目录常有业务文件)
            try:
                ok = node.deliver(artifact)
            except Exception as exc:
                await queue.put(_error_from_exc(
                    rid, f"to_agent deliver 异常:{type(exc).__name__}: {exc}", exc))
                await queue.put(None)
                return
            if ok:
                self.artifacts[rid] = artifact
                self._persist_artifact(rid, artifact)
                self._clear_break_to_agent(rid)
                await queue.put(final(rid, artifact))
            else:
                upstream = self._upstream_map.get(rid, [])
                upstream_artifacts = {uid: self.artifacts.get(uid) for uid in upstream}
                self.state.runs[rid].status = NodeStatus.PAUSED
                self._write_break_to_agent(rid)
                ckpt = checkpoint(rid, upstream_artifacts)
                ckpt.resume_hint = {
                    "node_dir": str(node.output_dir),
                    "upstream_artifact": upstream_artifacts,
                    "job_dir": str(self.job_dir),
                    "node_id": rid,
                }
                await queue.put(ckpt)
            await queue.put(None)
            return

        # 接手确认:返回 False 表示不接手,跳过本节点(artifact 置 None,下游可推进)
        try:
            ok = node.accept(ctx)
        except Exception as exc:
            await queue.put(_error_from_exc(
                rid, f"接手确认异常:{type(exc).__name__}: {exc}", exc))
            await queue.put(None)
            return
        if not ok:
            self.artifacts[rid] = None
            self._persist_artifact(rid, None)
            await queue.put(trace(rid, "skipped", f"跳过:{node.title or node.id}"))
            await queue.put(None)
            return

        # accept 通过,respect 节点自定义 output_dir,否则 fallback job_dir/rid,创建产物目录
        node.output_dir = (
            node.output_dir if _is_custom_output_dir(node.output_dir)
            else self.job_dir / rid
        )
        node.output_dir.mkdir(parents=True, exist_ok=True)

        # 执行(同步 run 用 to_thread 并行)
        try:
            result = await asyncio.to_thread(node.run, ctx)
        except Exception as exc:
            await queue.put(_error_from_exc(rid, f"{type(exc).__name__}: {exc}", exc))
            await queue.put(None)
            return

        # 动态扇出:run 返回 FanOut,扩图,本节点不产 artifact
        if isinstance(result, FanOut):
            self.state.runs[rid].status = NodeStatus.DONE
            self._expand_fanout(rid, result)
            await queue.put(
                trace(rid, "done", f"扇出 {result.n} 个 {result.base} 副本")
            )
            await queue.put(None)
            return

        # 脱手确认
        try:
            ok = node.deliver(result)
        except Exception as exc:
            await queue.put(_error_from_exc(
                rid, f"脱手确认异常:{type(exc).__name__}: {exc}", exc))
            await queue.put(None)
            return
        if not ok:
            await queue.put(error(rid, "脱手确认失败:产物校验不通过"))
            await queue.put(None)
            return

        self.artifacts[rid] = result
        self._persist_artifact(rid, result)
        await queue.put(final(rid, result))
        if node.checkpoint == Checkpoint.TO_HUMAN:
            await queue.put(checkpoint(rid, result))
        await queue.put(None)

    async def _await_control(self) -> AsyncGenerator[JobEvent, None]:
        """checkpoint 暂停后等控制信号,处理 resume/retry/abort。无事件产出。"""
        await self._resume_event.wait()
        self._resume_event.clear()
        ctrl = self._control
        self._control = None
        if ctrl is None or ctrl.kind == "resume":
            for rid in self._paused_nodes():
                st = self.state.runs[rid]
                # checkpoint TO_HUMAN 暂停:artifact 已写入,确认后转 done
                # break_before 暂停:artifact 为 None,转 idle 让下一轮重新就绪执行
                st.status = NodeStatus.DONE if st.artifact is not None else NodeStatus.IDLE
            self.state.status = JobStatus.RUNNING
        elif ctrl.kind == "retry":
            ds = self._downstream(ctrl.from_node or "")
            for s2 in ds:
                self.artifacts.pop(s2, None)
                st = self.state.runs.get(s2)
                if st:
                    st.status = NodeStatus.IDLE
                    st.artifact = None
                # 清磁盘 artifact.json,防止下次启动加载到旧产物
                art_file = self._meta_path(s2, _ARTIFACT_FILE)
                art_file.unlink(missing_ok=True)
            self.state.status = JobStatus.RUNNING
        elif ctrl.kind == "abort":
            self._aborted = True

    def _apply_load_strategy(self, args: _RunArgs) -> None:
        """按 _RunArgs.load_strategy 处理 job_dir 持久化产物,run() 开头调用一次。

        load_all:                       全量加载,已完成节点跳过
        load_skip_target:               加载但跳过 target(nodes 单点调试反复执行 X)
        invalidate_all:                 清掉所有节点状态与产物(--out 全跑)
        load_skip_target_then_invalidate:加载跳过 target 后再清 target(from_node 续跑)
        """
        if args.load_strategy == "load_all":
            self._load_persisted_artifacts()
        elif args.load_strategy == "load_skip_target":
            self._load_persisted_artifacts(skip=args.target)
        elif args.load_strategy == "invalidate_all":
            self._invalidate_runs(set(self.runs))
        elif args.load_strategy == "load_skip_target_then_invalidate":
            self._load_persisted_artifacts(skip=args.target)
            self._invalidate_runs(args.target or set())

    async def run(
        self,
        only: set[str] | None = None,
        break_before: set[str] | None = None,
        nodes: set[str] | None = None,
        from_node: str | None = None,
        from_depth: int | None = None,
        resume: bool = False,
    ) -> AsyncGenerator[JobEvent, None]:
        """执行 flow,产出事件流。

        only 指定时只跑 only 及其必需上游(单调试,run 模式用,无持久化必须跑上游)。
        nodes 指定时 _target 限定为 nodes 本身(不含上游),上游必须已完成否则 nodes
        节点不就绪——debug --node 用,上游从磁盘加载产物,只反复跑指定节点。
        from_node 指定时只重跑该节点及下游,上游从 job_dir artifact.json 复用。
        from_depth 指定时重跑 depth >= from_depth 的所有节点,上游 depth < from_depth 复用。
        resume=True 时加载所有已有产物(--resume 续跑 TO_AGENT 节点),不 invalidate。
        break_before 指定时,这些节点就绪后不立即执行,先 emit checkpoint 暂停,
        等 resume 信号才转 idle 重新就绪执行——用于 view 在指定节点前停下来观察。

        TO_AGENT 节点(checkpoint=Checkpoint.TO_AGENT)就绪时不调 run,
        emit checkpoint(artifact=上游产物集合)+ 写 .esflow/break_to_agent.json + 主循环退出。
        外部 agent 写产物文件到 output_dir 后,用 --resume 续跑:
        框架扫文件构造 artifact={"output_dir", "files"},调 deliver 校验,通过则转 DONE。
        """
        if from_depth is not None and (from_depth < 0 or from_depth > self.max_depth):
            raise RuntimeError(
                f"from_depth 越界:{from_depth},有效范围 [0, {self.max_depth}]"
            )
        args = _parse_run_args(
            only, break_before, nodes, from_node, from_depth,
            self._explicit_job_dir,
            self._downstream, self._required, self._target_by_depth,
            resume=resume,
        )
        self._target = args.target
        self._break_before = set(args.break_before) if args.break_before else None
        self._break_triggered = False
        for rid in self.runs:
            if rid not in self.state.runs:
                self.state.runs[rid] = RunState(run_id=rid)
        self.state.status = JobStatus.RUNNING
        self.state.finished = False
        self._apply_load_strategy(args)

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
                hits = [rid for rid in to_run if rid in self._break_before]
                if hits:
                    self._break_triggered = True
                    to_run = [rid for rid in to_run if rid not in self._break_before]
                    for rid in hits:
                        self.state.runs[rid].status = NodeStatus.PAUSED
                        yield checkpoint(rid, None)
                        apply_event(self.state, checkpoint(rid, None))

            # 同轮就绪节点并行跑,事件经 queue 顺序产出
            queue: asyncio.Queue = asyncio.Queue()
            tasks = [
                asyncio.create_task(self._run_one(rid, queue)) for rid in to_run
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
                # TO_AGENT checkpoint:进程退出,等外部 agent 写产物 + --resume
                if self.has_break_to_agent():
                    return
                await self._await_control()
                continue

    # —— 控制信号(外部在 checkpoint 时调用)——

    async def run_to_break(
        self,
        only: set[str] | None = None,
        break_before: set[str] | None = None,
        nodes: set[str] | None = None,
        from_node: str | None = None,
        from_depth: int | None = None,
        resume: bool = False,
    ) -> tuple[list[JobEvent], BreakKind, JobEvent | None]:
        """高层 API:跑到断点停下,返回 (事件列表, 断点类型, 断点 event)。

        断点类型与 break_event:
        - "end":      job 正常结束(全跑完或被 abort),break_event = end event 或 None
        - "to_agent": TO_AGENT checkpoint,break_event = checkpoint event(含 resume_hint)
        - "error":    节点抛异常,break_event = error event,可调 as_exception() 还原

        不做任何 raise/exit 决策,skill 拿 break_kind + break_event 自己决定 envelope/退出码。
        直接 Runner.to_agent_hint(break_event, ...) / raise break_event.as_exception(),
        无需遍历 events 反查断点。
        TO_HUMAN checkpoint 不算断点(等 stdin 控制信号,本方法不处理,需用 run() 自己驱动)。
        view/debug 流式场景仍需直接 async for event in runner.run()。
        """
        events: list[JobEvent] = []
        async for ev in self.run(
            only=only, break_before=break_before, nodes=nodes,
            from_node=from_node, from_depth=from_depth, resume=resume,
        ):
            events.append(ev)
            if ev.type == "error":
                return events, "error", ev
            if ev.type == "checkpoint":
                node = self.runs.get(ev.run_id)
                if node is not None and node.checkpoint == Checkpoint.TO_AGENT:
                    return events, "to_agent", ev
            if ev.type == "end":
                return events, "end", ev
        return events, "end", None

    def resume(self) -> None:
        """确认继续,paused 节点转 done,跑下一轮就绪节点。"""
        self._control = _ControlSignal(kind="resume")
        self._resume_event.set()

    def retry(self, from_node: str) -> None:
        """从 from_node 重跑:清 from_node 及下游的 artifact 与状态,上游复用。"""
        self._control = _ControlSignal(kind="retry", from_node=from_node)
        self._resume_event.set()

    def abort(self) -> None:
        """中止 job。checkpoint 时立即生效,运行中等当前步结束。"""
        self._control = _ControlSignal(kind="abort")
        self._aborted = True
        self._resume_event.set()
