"""从目录加载 flow + 节点(含静态副本展开 + 动态扇出 base 声明)。

目录约定:

    my_flow/
      flow.py        # @flow 装饰的类,声明 nodes/edges/replicas/dynamic
      nodes/
        fetch.py     # 定义 Node 子类
        worker.py    # 定义 Node 子类(静态 replicas 或 dynamic 由 FanOut 展开)
        ...

load_flow(dir) 返回 (展开后的 FlowDefine, {run_id: Node 实例}, {base_id: Node子类}),校验:
- flow.py 里有且仅有一个 @flow
- nodes/*.py 每个有且仅有一个 Node 子类(id 非空)
- flow.nodes 的每个 base id 都能在 nodes 里找到对应 Node 子类
- replicas / dynamic 的 base 必须在 nodes 里,且二者不相交
- 静态副本展开后的 DAG(动态 base 以 base id 参与)无环

静态副本:replicas = {"worker": 5} → worker#0..worker#4 五个 Node 实例。
动态扇出:dynamic = {"worker"} → loader 不实例化,运行时由某节点 run 返回
FanOut(base="worker", payload=[...]) 创建副本,runner 动态扩图。
node_classes 全量返回,供 runner 实例化动态副本。
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from .flow import Edge, FlowDefine
from .node import Node, _instantiate


class FlowLoadError(Exception):
    """加载或校验失败。"""


def _load_module(py_path: Path, pkg_name: str):
    """从文件路径加载 python 模块。"""
    mod_name = f"_esflow_dyn_{pkg_name}_{py_path.stem}"
    spec = importlib.util.spec_from_file_location(mod_name, py_path)
    if spec is None or spec.loader is None:
        raise FlowLoadError(f"无法加载模块:{py_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


def _collect_node_classes(module) -> list[type[Node]]:
    """从模块收集 Node 子类(排除 Node 基类本身)。"""
    return [
        v
        for v in vars(module).values()
        if isinstance(v, type) and issubclass(v, Node) and v is not Node
    ]


def _collect_flow_define(module) -> FlowDefine | None:
    """从模块取唯一 FlowDefine 实例。"""
    flows = [v for v in vars(module).values() if isinstance(v, FlowDefine)]
    if not flows:
        return None
    if len(flows) > 1:
        raise FlowLoadError("flow.py 里只能有一个 @flow")
    return flows[0]


def _expand_ids(base: str, replicas: dict[str, int]) -> list[str]:
    """静态副本 base 按并行度展开为副本 id 列表;动态/普通 base 返回 [base]。"""
    if base in replicas:
        n = replicas[base]
        if n < 1:
            raise FlowLoadError(f"replicas 数必须 >=1:{base}={n}")
        return [f"{base}#{i}" for i in range(n)]
    return [base]


def _check_acyclic(nodes: set[str], edges: list[Edge]) -> None:
    """Kahn 拓扑校验 DAG 无环。"""
    adj: dict[str, list[str]] = {rid: [] for rid in nodes}
    indeg: dict[str, int] = {rid: 0 for rid in nodes}
    for e in edges:
        if e.from_ not in nodes or e.to not in nodes:
            raise FlowLoadError(f"边引用了未知节点:{e.from_} -> {e.to}")
        adj[e.from_].append(e.to)
        indeg[e.to] += 1
    queue = [rid for rid, d in indeg.items() if d == 0]
    seen = 0
    while queue:
        n = queue.pop()
        seen += 1
        for m in adj[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    if seen != len(nodes):
        raise FlowLoadError("DAG 有环,无法拓扑执行")


def load_flow(
    flow_dir: str | Path,
) -> tuple[FlowDefine, dict[str, Node], dict[str, type[Node]]]:
    """加载一个 flow 目录,展开静态副本,返回 (FlowDefine, runs, node_classes)。

    runs 是 {run_id: Node 实例},不含 dynamic base(运行时由 runner 实例化)。
    node_classes 全量 {base_id: Node子类},供 runner 创建动态副本。
    """
    root = Path(flow_dir)
    if not root.is_dir():
        raise FlowLoadError(f"目录不存在:{root}")

    flow_py = root / "flow.py"
    if not flow_py.exists():
        raise FlowLoadError(f"缺少 flow.py:{flow_py}")

    flow_mod = _load_module(flow_py, "flow")
    flow = _collect_flow_define(flow_mod)
    if flow is None:
        raise FlowLoadError("flow.py 里没有 @flow 装饰的类")

    # 加载 nodes/,收集 Node 子类(base id -> 类)
    nodes_dir = root / "nodes"
    node_classes: dict[str, type[Node]] = {}
    if nodes_dir.is_dir():
        for py in sorted(nodes_dir.glob("*.py")):
            if py.name.startswith("_"):
                continue
            mod = _load_module(py, "nodes")
            mod_nodes = _collect_node_classes(mod)
            if not mod_nodes:
                raise FlowLoadError(f"节点文件无 Node 子类:{py}")
            if len(mod_nodes) > 1:
                raise FlowLoadError(f"节点文件只能有一个 Node 子类:{py}")
            cls = mod_nodes[0]
            if not cls.id:
                raise FlowLoadError(f"Node 子类未设 id:{py}:{cls.__name__}")
            if cls.id in node_classes:
                raise FlowLoadError(f"节点 id 重复:{cls.id}")
            node_classes[cls.id] = cls

    # 校验 flow.nodes 的 base id 都有对应 Node 子类
    for base in flow.nodes:
        if base not in node_classes:
            raise FlowLoadError(f"flow.nodes 引用了未定义的节点:{base}")
    # replicas / dynamic 的 base 必须在 nodes 里
    for base in flow.replicas:
        if base not in node_classes:
            raise FlowLoadError(f"replicas 引用了未定义的节点:{base}")
    for base in flow.dynamic:
        if base not in node_classes:
            raise FlowLoadError(f"dynamic 引用了未定义的节点:{base}")
    for base in flow.serial:
        if base not in node_classes:
            raise FlowLoadError(f"serial 引用了未定义的节点:{base}")
    # 同一 base 不能同时静态副本和动态扇出
    overlap = set(flow.replicas) & flow.dynamic
    if overlap:
        raise FlowLoadError(f"base 不能同时声明 replicas 和 dynamic:{overlap}")

    # 展开静态副本 + 收集 runs;动态 base 不实例化(保留 base id 参与无环校验)
    runs: dict[str, Node] = {}
    expanded_nodes: list[str] = []
    for base in flow.nodes:
        if base in flow.dynamic:
            expanded_nodes.append(base)
            continue
        for rid in _expand_ids(base, flow.replicas):
            idx = int(rid.rsplit("#", 1)[1]) if "#" in rid else 0
            runs[rid] = _instantiate(node_classes[base], rid, idx)
            expanded_nodes.append(rid)

    # 展开边:静态副本 base 扇出/扇入,动态 base 保留 base id(运行时 runner 扩展)
    expanded_edges: list[Edge] = []
    for e in flow.edges:
        for f in _expand_ids(e.from_, flow.replicas):
            for t in _expand_ids(e.to, flow.replicas):
                expanded_edges.append(Edge(from_=f, to=t))

    # edge 引用的 base 必须可展开(对应 node 或副本 base)
    edge_bases = {e.from_ for e in flow.edges} | {e.to for e in flow.edges}
    for base in edge_bases:
        if base not in node_classes:
            raise FlowLoadError(f"边引用了未定义的节点:{base}")

    expanded_flow = FlowDefine(
        id=flow.id,
        title=flow.title,
        nodes=expanded_nodes,
        edges=expanded_edges,
        replicas={},  # 展开后不再有静态副本概念
        dynamic=flow.dynamic,  # 保留,runner 要用
        serial={rid for base in flow.serial for rid in _expand_ids(base, flow.replicas)},
    )

    # 无环校验:动态 base 以 base id 参与(运行时才展开成副本)
    _check_acyclic(set(expanded_nodes), expanded_edges)
    return expanded_flow, runs, node_classes
