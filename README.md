# easyflow

## 定位

- 轻量 Python DAG workflow 框架,用 `xx.py` 文件声明节点,`flow.py` 声明边与并行度
- 方便人机协作控制循环(暂停确认 / 从某步重试 / 中止 / 调试)
- `easyflow` CLI 主要用于可视化和快速入手

## 功能一览

- **DAG 拓扑执行**:目录约定声明,Kahn 无环校验,就绪节点按拓扑推进
- **并行副本**:一个 step 声明 N 个并行副本(扇出/扇入),同轮就绪节点 `asyncio.gather` 并行
- **接手确认 / 脱手确认**:节点 run 前校验前置、run 后校验产物,失败 emit error(契约式设计)
- **人机协作**:节点 `checkpoint=AFTER` 暂停整 job,等 `resume` / `retry(from_step)` / `abort`
- **retry 复用上游**:从某步重跑时,已完成且无依赖变更的上游 artifact 复用,不重跑
- **单副本调试**:`runner.run(only={"worker#2"})` 只跑指定副本及其必需上游,跳过兄弟与下游
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
easyflow run ./my_flow --node worker#2  # 单调试指定副本及其上游
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

## 声明一个 workflow

目录约定:

```
my_flow/
  flow.py        # @flow 装饰的类,声明 nodes + edges + replicas
  nodes/
    fetch.py     # 定义 Node 子类,一文件一节点
    process.py
```

节点文件(`Node` 基类,`run` 必须实现,`accept`/`deliver` 可选):

```python
# nodes/fetch.py
from easyflow import Node

class Fetch(Node):
    id = "fetch"
    title = "抓取数据"

    def run(self, ctx) -> dict:
        return {"items": [1, 2, 3]}
```

带接手/脱手确认的节点(生成 srt 示例):

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

带暂停点的节点(`checkpoint=AFTER`:run 之后暂停等外部确认):

```python
# nodes/review.py
from easyflow import Node, Checkpoint

class Review(Node):
    id = "review"
    title = "人工复核"
    checkpoint = Checkpoint.AFTER

    def run(self, ctx) -> dict:
        upstream = ctx.get("process")       # 取上游 artifact
        return {"reviewed": upstream, "ok": True}
```

flow 文件:

```python
# flow.py
from easyflow import flow, edge

@flow(id="my_flow")
class MyFlow:
    nodes = ["fetch", "process", "review", "export"]
    edges = [
        edge("fetch", "process"),
        edge("process", "review"),
        edge("review", "export"),
    ]
```

`edge()` 的参数可以是节点 id 字符串,也可以是跨包 import 的 `StepDefine` 对象。

## 并行副本

`replicas` 声明并行度,loader 把 base id 展开成 `base#0..base#N-1` 个副本,边自动扇出/扇入:

```python
# examples/fanout_flow/flow.py
from easyflow import flow, edge

@flow(id="fanout_flow")
class FanoutFlow:
    nodes = ["fetch", "worker", "merge"]
    edges = [
        edge("fetch", "worker"),
        edge("worker", "merge"),
    ]
    replicas = {"worker": 5}   # worker 展开成 5 个并行副本
```

副本节点用 `self.index` 切片做自己那份:

```python
# nodes/worker.py
from easyflow import Node

class Worker(Node):
    id = "worker"

    def accept(self, ctx) -> bool:
        return bool(ctx.get("fetch")["tasks"])

    def run(self, ctx) -> dict:
        tasks = ctx.get("fetch")["tasks"]
        mine = [t for i, t in enumerate(tasks) if i % 5 == self.index]
        return {"worker_index": self.index, "results": [t * 10 for t in mine]}

    def deliver(self, artifact) -> bool:
        return len(artifact["results"]) > 0
```

扇入节点用 `ctx.upstream_ids()` 枚举各副本产物:

```python
# nodes/merge.py
from easyflow import Node

class Merge(Node):
    id = "merge"

    def run(self, ctx) -> dict:
        all_results = []
        for sid in ctx.upstream_ids():
            if sid.startswith("worker#"):
                all_results.extend(ctx.get(sid)["results"])
        return {"total": len(all_results), "results": sorted(all_results)}
```

同轮就绪节点并行跑(同步 `run` 用 `asyncio.to_thread` 包),checkpoint 时整 job 暂停。

## 动态扇出

副本数运行时由节点产物决定,不写死在 flow.py。以并行翻译一本书为例:读入章节 → 按章展开 worker 并行翻译 → 合并译本。节点 `run` 返回 `FanOut`,框架运行时展开副本:

```python
# examples/fanout_dynamic/flow.py
from easyflow import flow, edge

@flow(id="fanout_dynamic")
class FanoutDynamicFlow:
    nodes = ["ingest", "split", "worker", "merge"]
    edges = [
        edge("ingest", "split"),
        edge("split", "worker"),
        edge("worker", "merge"),
    ]
    dynamic = {"worker"}   # worker 由 FanOut 运行时实例化,loader 不预创建
```

```python
# nodes/ingest.py
from easyflow import Node

class Ingest(Node):
    id = "ingest"
    def run(self, ctx) -> dict:
        return {"chapters": ["第一章:清晨", "第二章:正午", "第三章:黄昏", "第四章:深夜"]}
```

```python
# nodes/split.py
from easyflow import Node, FanOut

class Split(Node):
    id = "split"
    def accept(self, ctx) -> bool:
        return bool(ctx.get("ingest")["chapters"])

    def run(self, ctx) -> FanOut:
        chapters = ctx.get("ingest")["chapters"]   # 运行时才知道几章
        return FanOut(base="worker", payload=chapters)   # n = len(payload)
```

```python
# nodes/worker.py
from easyflow import Node

class Worker(Node):
    id = "worker"
    def accept(self, ctx) -> bool:
        return ctx.fanout_payload is not None

    def run(self, ctx) -> dict:
        chapter = ctx.fanout_payload          # 框架注入第 i 章,不用 index 切片
        return {"chapter": chapter, "translated": f"[译文]{chapter}"}
```

```python
# nodes/merge.py
from easyflow import Node

class Merge(Node):
    def run(self, ctx) -> dict:
        results = ctx.gather("worker")         # 语义化收集所有副本产物(按 index 排序)
        book = "\n\n".join(r["translated"] for r in results)
        return {"total_chapters": len(results), "results": results, "book": book}
```

**FanOut 语义**:`FanOut(base, payload)`,`payload` 必须是 list,`n = len(payload)`,第 i 个副本拿 `payload[i]`,强制对齐消除切片约定。

**dynamic 声明**:`dynamic = {"worker"}` 告诉 loader 不预实例化 worker,运行时由某节点 run 返回 `FanOut(base="worker", ...)` 创建副本并动态连边(上游→副本→下游)。

**静态 replicas vs 动态 FanOut 怎么选**:

- 副本数固定、要加载时无环校验 + view 静态预览 + `--node worker#2` 单调试 → 用 `replicas`
- 副本数依赖运行时产物 → 用 `dynamic` + `FanOut`
- 同一 flow 可混用:部分节点 `replicas`,部分节点 `dynamic`

**动态扇出当前限制**(最小模型):

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
- 持久化:store 接口 + SQLite,支持进程重启恢复(含动态副本图状态)
- view 适配并行多节点高亮 + 动态副本展开动画

