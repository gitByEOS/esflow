# 用法

## 目录约定

```
my_flow/
  flow.py        # @flow 装饰的类,声明 nodes + edges + replicas
  nodes/
    fetch.py     # 定义 Node 子类,一文件一节点
```

## 节点开发

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

## edge

`edge()` 参数可以是节点 id 字符串,也可以是跨包 import 的 `StepDefine` 对象。

## skill 模板

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
