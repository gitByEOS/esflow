# 参考手册

easyflow 按"类"组织的 API 参考。每页一类，写明职责、定义、字段/方法、用法示例与相关类。

教程文档（怎么用）见 [../quickstart.md](../quickstart.md) 等主题页；本手册聚焦每个公共类/函数的精确语义。

## 模块导览

按使用链路组织：声明 → 节点 → 执行 → 事件/状态 → 周边。

| 类 / 函数 | 模块 | 一句话 |
|---|---|---|
| [`flow`](FlowDefine.md#flow-装饰器) / [`edge`](FlowDefine.md#edge) / [`FlowDefine`](FlowDefine.md) / [`Edge`](FlowDefine.md#Edge) | `easyflow.flow` | 声明 DAG：边、静态副本、动态扇出 base、serial 集合 |
| [`Node`](Node.md) | `easyflow.node` | 节点基类，子类设 `id`/`title`/`checkpoint`，实现 `run`，按需 override `accept`/`deliver` |
| [`DepthScope`](DepthScope.md) | `easyflow.node` | 运行时注入给 `Node.run`/`accept` 的上下文协议：取上游产物、gather 副本、layer 同层 |
| [`Checkpoint`](Checkpoint.md) | `easyflow.node` | 暂停点枚举：`NONE` / `AFTER` |
| [`FanOut`](FanOut.md) | `easyflow.node` | 动态扇出指令：节点 `run` 返回它，框架展开 N 个副本 |
| [`Runner`](Runner.md) | `easyflow.runner` | 加载并执行 flow，产出事件流，支持并行/扇出/暂停/重试/中止/单调试 |
| [`JobEvent`](JobEvent.md) | `easyflow.event` | 统一事件信封 + 构造函数 + `easyflow_event` 打印入口 |
| [`JobState`](JobState.md) | `easyflow.state` | 事件折叠状态：`JobState` / `RunState` / `NodeStatus` / `JobStatus` / `apply_event` |
| [`CheckResult`](CheckResult.md) | `easyflow.check` | 启动预检结果 + `pass_check` 聚合 + `FlowCheckError` |
| [`FlowLoadError`](FlowLoadError.md) | `easyflow.loader` | flow 目录加载或 DAG 校验失败异常 |
| `load_flow` | `easyflow.loader` | 加载 flow 目录，展开静态副本，返回 `(FlowDefine, runs, node_classes)` |

## 公共导出

`from easyflow import ...` 一览：

```python
from easyflow import (
    # flow 声明
    flow, edge, Edge, FlowDefine,
    # 节点
    Node, DepthScope, Checkpoint, FanOut,
    # 执行
    Runner,
    # 事件 / 状态
    JobEvent, trace, delta, checkpoint, final, error, end, easyflow_event,
    JobState, RunState, NodeStatus, apply_event,
    # 加载 / 预检
    load_flow, FlowLoadError,
)
from easyflow.check import pass_check, CheckResult, FlowCheckError
```

## 阅读约定

- 每页顶部给出 `模块` / `职责`，方便定位
- 字段/方法表格的"注入时机"列表示该字段由框架在何时写入，用户代码只读
- 示例代码最小化，仅演示当前类的用法；完整可跑示例见教程与 `examples/`
