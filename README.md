# easyflow

## 定位

- Agent Skill时代下，轻量、好用的 workflow 框架
- 高效人机协作控制循环，支持：单步调试、流程编排、暂停确认、定点续跑
- 清晰化团队 Skill 规范，提高资产复用率，让单节点在多 Skill 中流转
- `easyflow` CLI 主要服务可视化和快速上手


## 功能一览

| 特性 | 说明 |
| --- | --- |
| DAG 拓扑执行 | 目录约定声明,Kahn 无环校验,就绪节点按拓扑推进 |
| 并行副本 | 一个 step 声明 N 个并行副本(扇出/扇入),`asyncio.gather` 并行 |
| 同层依次启动 | `serial` 集合同层按 `nodes` 顺序启动,用于 fallback 兜底链 |
| 接手 / 脱手确认 | `accept` False → skip;`deliver` 失败 → error(契约式设计) |
| skip 兜底链 | 上游有产物则 skip 当前,无产物则当前接手兜底 |
| 人机协作 | `checkpoint=AFTER` 暂停 job,等 `c` / `r` / `a` 控制 |
| retry 复用上游 | 已完成且无依赖变更的上游 artifact 复用,不重跑 |
| 定点续跑 | `run --out DIR --from NODE` 复用上游产物,只重跑指定节点及下游 |
| 单节点调试 | `--node worker#2` 只跑指定节点及其必需上游 |
| 统一事件流 | `WorkflowJobEvent` 折叠成 `JobState` 供视图消费 |
| 库 + CLI 双入口 | `async for event in runner.run()` 为主,CLI 调试 |
| 启动预检 | `pass_check` 在 `runner.run()` 前聚合检查,失败带 `fix` 修复指引 |

## 安装

```bash
pip install easyflow
```

## 快速开始

从仓库源码试用:

```bash
easyflow run examples/quickstart_flow
```

生成一个自己的 flow:

```bash
easyflow new my_skill
python my_skill/scripts/run.py
```

快速上手见 [docs/quickstart.md](docs/quickstart.md)。

## 常用命令

```bash
easyflow run ./my_flow
easyflow run ./my_flow --out ./runs/a
easyflow run ./my_flow --out ./runs/a --from translate
easyflow debug ./my_flow
easyflow view ./my_flow
```

checkpoint 时 stdin 命令:`c` continue / `r` retry / `a` abort。

人工修正某个节点产物后,从它的下一步继续跑:先用 `--out` 固定产物目录,再用 `--from` 指定重跑起点。详见 [docs/artifacts.md](docs/artifacts.md)。

## 最小用法

目录约定:

```
my_flow/
  flow.py        # @flow 装饰的类,声明 nodes + edges + replicas
  nodes/
    fetch.py     # 定义 Node 子类,一文件一节点
```

最小节点(`Node` 基类,`run` 必须实现):

```python
# nodes/fetch.py
from easyflow import Node

class Fetch(Node):
    id = "fetch"
    title = "抓取数据"

    def run(self, ctx) -> dict:
        return {"items": [1, 2, 3]}
```

更详细的节点开发(`accept`/`deliver`)、edge 用法、skill 模板生成见 [docs/quickstart.md](docs/quickstart.md)。

## 示例一览

| 示例 | 链路 | 演示特性 |
| --- | --- | --- |
| [`examples/quickstart_flow/`](examples/quickstart_flow) | `fetch → process → review → export` | 首屏推荐:4 节点线性 DAG + `checkpoint` |
| [`examples/skip_flow/`](examples/skip_flow) | `trigger → fetch_from_{ssr,wechat,bili} → merge → parse_to_{html,md} → done` | 两组 `serial` fallback 链:多源兜底抓取 + 解析格式降级 |
| [`examples/fanout_flow/`](examples/fanout_flow) | `fetch → worker#5 → merge` | 静态 `replicas` 扇出/扇入 |
| [`examples/fanout_dynamic/`](examples/fanout_dynamic) | `ingest → split → worker(动态) → merge` | `FanOut` 运行时展开副本 |
| [`examples/student_exam_flow/`](examples/student_exam_flow) | `register → publish_paper → student#3 → review → teacher_leave` | 多 `checkpoint` + `replicas` 综合 |
| [`examples/ocr_flow/`](examples/ocr_flow) | `ingest → preprocess → ocr → export` | `pass_check` 启动预检 + `output_dir` 落盘 |

## 文档

- [CHANGELOG.md](CHANGELOG.md) — 版本变化与已知限制
- [docs/quickstart.md](docs/quickstart.md) — 快速上手与第一个 flow
- [docs/artifacts.md](docs/artifacts.md) — output_dir、artifact.json、--out、--from
- [docs/semantics.md](docs/semantics.md) — 关键语义、静态 replicas vs 动态 FanOut、动态扇出限制
- [docs/pass_check.md](docs/pass_check.md) — 启动预检
- [docs/events.md](docs/events.md) — 事件协议
- [docs/debug.md](docs/debug.md) — debug/view 调试模式
