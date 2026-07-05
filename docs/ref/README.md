# 参考手册

esflow 按"类"组织的 API 参考。每页一类,写明职责、字段/方法、用法示例。

教程(怎么用)见 [../quickstart.md](../quickstart.md) 等主题页;本手册聚焦每个公共类的精确语义。

## 模块导览

按使用链路:声明 → 节点 → 执行 → 事件/状态 → 周边。

| 类 / 函数 | 模块 | 一句话 |
|---|---|---|
| [`flow`](FlowDefine.md) / [`edge`](FlowDefine.md) / [`FlowDefine`](FlowDefine.md) / [`Edge`](FlowDefine.md) | `esflow.flow` | 声明 DAG |
| [`Node`](Node.md) | `esflow.node` | 节点基类 |
| [`DepthScope`](DepthScope.md) | `esflow.node` | 运行时上下文 |
| [`Checkpoint`](Checkpoint.md) | `esflow.node` | 暂停点枚举 |
| [`FanOut`](FanOut.md) | `esflow.node` | 动态扇出指令 |
| [`Runner`](Runner.md) | `esflow.runner` | 加载并执行 flow |
| [`JobEvent`](JobEvent.md) | `esflow.event` | 统一事件信封 |
| [`JobState`](JobState.md) | `esflow.state` | 事件折叠状态 |
| [`CheckResult`](CheckResult.md) | `esflow.check` | 启动预检结果 |
| [`FlowLoadError`](FlowLoadError.md) | `esflow.loader` | 加载/校验失败异常 |
| `load_flow` | `esflow.loader` | 加载 flow 目录 |

## 公共导出

`from esflow import ...` 一览:

```python
from esflow import (
    # flow 声明
    flow, edge, Edge, FlowDefine,
    # 节点
    Node, DepthScope, Checkpoint, FanOut,
    # 执行
    Runner,
    # 事件 / 状态
    JobEvent, trace, delta, checkpoint, final, error, end, esflow_event,
    JobState, RunState, NodeStatus, apply_event,
    # 加载 / 预检
    load_flow, FlowLoadError,
)
from esflow.check import pass_check, CheckResult, FlowCheckError
```

## 阅读约定

- 每页顶部给 `模块` / `职责`
- 字段表"注入时机"列表示框架何时写入,用户代码只读
- 示例最小化,仅演示当前类;完整可跑示例见教程与 `examples/`
