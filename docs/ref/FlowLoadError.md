# FlowLoadError / load_flow

## 模块

`easyflow.loader` — `from easyflow import FlowLoadError, load_flow`

## 职责

从目录加载 flow + 节点（含静态副本展开 + 动态扇出 base 声明 + DAG 无环校验）。

## FlowLoadError

```python
class FlowLoadError(Exception):
    """加载或校验失败。"""
```

加载或 DAG 校验失败时抛。具体场景：

- 目录不存在 / 缺 `flow.py`
- `flow.py` 里没有 `@flow` 装饰的类，或有多个
- `nodes/*.py` 无 `Node` 子类，或有多个，或 `id` 为空，或 `id` 重复
- `flow.nodes` / `replicas` / `dynamic` / `serial` 引用了未定义的 base
- 同一 base 同时声明 `replicas` 和 `dynamic`
- `replicas` 数 `< 1`
- 展开静态副本后的 DAG 有环

## load_flow

```python
def load_flow(
    flow_dir: str | Path,
) -> tuple[FlowDefine, dict[str, Node], dict[str, type[Node]]]
```

加载一个 flow 目录，展开静态副本，返回三元组：

| 返回 | 类型 | 用途 |
|---|---|---|
| `flow` | [`FlowDefine`](FlowDefine.md) | 展开后的 flow 定义（`replicas` 清空，`nodes`/`edges`/`serial` 替换为副本 id，`dynamic` 保留） |
| `runs` | `dict[str, Node]` | `{run_id: Node 实例}`，不含 dynamic base（运行时由 runner 实例化） |
| `node_classes` | `dict[str, type[Node]]` | 全量 `{base_id: Node 子类}`，供 runner 创建动态副本 |

## 目录约定

```text
my_flow/
  flow.py        # @flow 装饰的类,声明 nodes/edges/replicas/dynamic/serial
  nodes/
    fetch.py     # 定义 Node 子类,一个文件一个节点
    worker.py    # 定义 Node 子类(静态 replicas 或 dynamic 由 FanOut 展开)
```

`flow.py` 里只能有一个 `@flow`，`nodes/*.py` 每个只能有一个 `Node` 子类。

## 校验链路

1. 加载 `flow.py`，取唯一 `FlowDefine`
2. 加载 `nodes/*.py`，收集 `Node` 子类，校验 `id` 唯一非空
3. 校验 `flow.nodes` / `replicas` / `dynamic` / `serial` 的 base 都有对应类
4. 校验 `replicas` 与 `dynamic` 不相交
5. 展开静态副本：`replicas = {"worker": 5}` → `worker#0..worker#4` 五个 `Node` 实例
6. 展开边：静态副本 base 扇出/扇入，动态 base 保留 base id（运行时 runner 扩展）
7. Kahn 拓扑校验 DAG 无环（动态 base 以 base id 参与）

## 用法

通常不直接调用，由 [`Runner.load()`](Runner.md#load) 内部使用。需要绕开 Runner 时：

```python
from easyflow import load_flow, FlowLoadError

try:
    flow, runs, node_classes = load_flow("./my_flow")
except FlowLoadError as exc:
    print(f"加载失败: {exc}", file=sys.stderr)
    raise
```

## 相关

- [`FlowDefine`](FlowDefine.md) — 加载产出
- [`Node`](Node.md) — 节点定义
- [`Runner`](Runner.md) — `Runner.load` 内部使用
