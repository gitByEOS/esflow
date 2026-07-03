# easyflow

## 定位

- 轻量 Python DAG workflow 框架,用 `xx.py` 文件声明节点,`flow.py` 声明边与并行度
- 方便人机协作控制循环(暂停确认 / 从某步重试 / 中止 / 调试)
- `easyflow` CLI 主要用于可视化和快速入手

## 功能一览

- **DAG 拓扑执行**:目录约定声明,Kahn 无环校验,就绪节点按拓扑推进
- **并行副本**:一个 step 声明 N 个并行副本(扇出/扇入),同轮就绪节点 `asyncio.gather` 并行
- **同层依次启动**:`serial` 集合声明同层节点按 `nodes` 顺序依次启动(不并行),用于 fallback 兜底链;非 serial 节点照常并行
- **接手确认 / 脱手确认**:节点 `accept` 返回 False → 跳过本节点(emit skipped,artifact 置 None,下游可推进);`deliver` 失败 emit error(契约式设计)
- **skip 兜底链**:`accept` 检查上游产物,上游有产物则 skip 当前,上游无产物则当前接手兜底,形成 fallback 链
- **人机协作**:节点 `checkpoint=AFTER` 暂停整 job,等 `resume` / `retry(from_step)` / `abort`
- **retry 复用上游**:从某步重跑时,已完成且无依赖变更的上游 artifact 复用,不重跑
- **单节点调试**:`easyflow run ./my_flow --node worker#2` 只跑指定节点(含副本)及其必需上游,跳过兄弟与下游
- **统一事件流**:Runner 产出 `WorkflowJobEvent`,可折叠成内存 `JobState` 供视图消费
- **库 + CLI 双入口**:库式 `async for event in runner.run()` 为主,`easyflow run/view` 调试

## 安装

```bash
pip install easyflow
```

## 命令行工具

```bash
easyflow new my_skill                  # 生成 skill 模板(含可跑 demo flow)
easyflow run ./my_flow                  # 全跑,checkpoint 时 stdin 等命令
easyflow run ./my_flow --node worker#2  # 单节点调试:只跑指定节点及其上游
easyflow view ./my_flow                 # 浏览器调试界面
```

checkpoint 时 stdin 命令:`resume` / `retry <step>` / `abort`。

### 生成 skill 模板

`easyflow new my_skill` 生成结构:

```
my_skill/
  SKILL.md              # skill 说明(含 frontmatter)
  scripts/
    flow.py             # 抓取 → 分析 → 报告 最小示例
    run.py              # 直接跑:python3 scripts/run.py
    nodes/
      fetch.py
      analyze.py
      report.py
```

生成后可直接跑:`python3 my_skill/scripts/run.py`。在 `scripts/nodes/` 下加 Node 子类,在 `scripts/flow.py` 里声明 edges/replicas/dynamic 扩展。

`name` 含路径时目录按完整路径创建,`id`/类名取末段:`easyflow new temp/okr` 创建 `temp/okr/`,flow id 为 `okr`。

## 用法

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

带接手/脱手确认的节点(`accept`/`deliver` 可选):

```python
# nodes/gen_srt.py
from pathlib import Path
from easyflow import Node, Checkpoint

class GenSrt(Node):
    id = "gen_srt"
    checkpoint = Checkpoint.AFTER

    def accept(self, ctx) -> bool:          # 接手:上游视频文件存在
        return Path(ctx.get("fetch")["video"]).exists()

    def deliver(self, artifact) -> bool:    # 脱手:srt 文件已生成
        return Path(artifact["srt"]).exists()

    def run(self, ctx) -> dict:
        video = ctx.get("fetch")["video"]
        # ... 生成 srt ...
        return {"srt": srt_path}
```

`edge()` 参数可以是节点 id 字符串,也可以是跨包 import 的 `StepDefine` 对象。

### 示例一览

| 示例 | 链路 | 演示特性 |
| --- | --- | --- |
| [`examples/quickstart_flow/`](examples/quickstart_flow) | `fetch → process → review → export` | 4 节点线性 DAG + `checkpoint` |
| [`examples/skip_flow/`](examples/skip_flow) | `trigger → fetch_from_{ssr,wechat,bili} → merge → parse_to_{html,md} → done` | 两组 `serial` fallback 链:多源兜底抓取 + 解析格式降级 |
| [`examples/fanout_flow/`](examples/fanout_flow) | `fetch → worker#5 → merge` | 静态 `replicas` 扇出/扇入 |
| [`examples/fanout_dynamic/`](examples/fanout_dynamic) | `ingest → split → worker(动态) → merge` | `FanOut` 运行时展开副本 |
| [`examples/student_exam_flow/`](examples/student_exam_flow) | `register → publish_paper → student#3 → review → teacher_leave` | 多 `checkpoint` + `replicas` 综合 |

### 关键语义

- **accept 语义**:返回 `False` 不是错误,而是"不接手"——框架跳过本节点(emit `skipped`,artifact 置 `None`,下游可推进)。`deliver` 失败才是错误(emit `error`)
- **serial 语义**:同轮就绪节点中属于 `serial` 集合的,只启动 `nodes` 声明顺序最靠前的那个,其他 serial 节点等下一轮;非 serial 节点照常并行
- **replicas**:loader 把 base id 展开成 `base#0..base#N-1`,边自动扇出/扇入;副本节点用 `self.index` 切片,扇入节点用 `ctx.upstream_ids()` 或 `ctx.gather("worker")` 收集
- **FanOut + dynamic**:节点 `run` 返回 `FanOut(base, payload)`,`payload` 必须是 list,`n = len(payload)`,第 i 个副本拿 `payload[i]`;flow 声明 `dynamic = {"worker"}` 让 loader 不预实例化,运行时由 `FanOut` 创建副本并动态连边;副本节点用 `ctx.fanout_payload` 取载荷
- **并行执行**:同轮就绪节点 `asyncio.gather` 并行(同步 `run` 用 `asyncio.to_thread` 包),checkpoint 时整 job 暂停
- **产物持久化**:每节点 `self.output_dir` 由框架注入(`/tmp/easyflow/outputs/<flow_id>/<job_id>/<step_id>/`),节点把大文件写到这里,`run` 返回的 dict 登记文件路径供下游读取;skip 节点不创建目录
- **拓扑深度**:`self.depth` 由框架注入(入口节点 0),同层节点 depth 相同;`ctx.layer(d)` 返回该深度所有已完成节点产物 list(按声明顺序,skip 的为 None),同层 fallback 用 `ctx.layer(self.depth)` 拿前序,跨层拿上游用 `ctx.layer(self.depth - 1)`

### 静态 replicas vs 动态 FanOut

- 副本数固定、要加载时无环校验 + view 静态预览 + `--node worker#2` 单调试 → 用 `replicas`
- 副本数依赖运行时产物 → 用 `dynamic` + `FanOut`
- 同一 flow 可混用:部分节点 `replicas`,部分节点 `dynamic`

### 动态扇出当前限制(最小模型)

- 动态副本节点(`split` / `worker#i`)不挂 checkpoint,扇入节点(`merge`)可以
- `only` 单调试暂不支持指定动态副本内部(展开前不存在),可指定到 `split` 级

## 事件协议

Runner 产出的唯一事件流(`WorkflowJobEvent`):

| type         | 含义                                                |
| ------------ | ------------------------------------------------- |
| `trace`      | 节点状态变更(queued / running / done / error / skipped) |
| `delta`      | 节点产出增量文本                                          |
| `checkpoint` | 节点到暂停点,等外部 resume / retry                         |
| `final`      | 节点最终 artifact                                     |
| `error`      | 错误(含接手/脱手确认失败)                                    |
| `end`        | job 结束                                            |

事件经 `apply_event` 折叠成内存 `JobState` 供视图消费。

## 后续

- 动态副本挂 checkpoint:支持 `split`/`worker#i` 暂停 + retry 重建副本
- `only` 单调试支持指定动态副本内部(展开后命中)
- `ctx.replicas_of(base)` 语义化枚举(静态副本也统一接口)
- LLM 节点:作为可插拔 callable,后补 provider 抽象
- 持久化恢复:`resume_from=<job_id>` 从磁盘加载 artifact,跳过已完成节点;store 接口 + SQLite 支持进程重启恢复(含动态副本图状态)
- view 适配并行多节点高亮 + 动态副本展开动画
