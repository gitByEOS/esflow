# Quickstart

这篇文档只做一件事:让你快速跑通第一个 flow,并知道下一步该改哪里。

## 跑通内置示例

```bash
easyflow run examples/quickstart_flow
```

`quickstart_flow` 是首屏推荐示例,链路很短:

```text
fetch -> process -> review -> export
```

其中 `review` 带 `checkpoint`,运行到暂停点后,CLI 会等待输入:

```text
(c) continue, (r) retry, (a) abort
```

## 生成自己的 flow

```bash
easyflow new my_skill
python my_skill/scripts/run.py
```

生成目录:

```text
my_skill/
  SKILL.md
  scripts/
    flow.py
    run.py
    nodes/
      fetch.py
      analyze.py
      report.py
```

## 最小节点

节点是 `Node` 子类,一个文件一个节点:

```python
from easyflow import Node


class Fetch(Node):
    id = "fetch"
    title = "抓取数据"

    def run(self, ctx) -> dict:
        return {"items": [1, 2, 3]}
```

## 声明 flow

`flow.py` 用 `nodes` 和 `edges` 声明 DAG:

```python
from easyflow import flow, edge


@flow(id="my_flow", title="我的流程")
class MyFlow:
    nodes = ["fetch", "process", "export"]
    edges = [
        edge("fetch", "process"),
        edge("process", "export"),
    ]
```

## 接手和脱手确认

`accept` 返回 `False` 表示跳过当前节点,下游仍可继续。`deliver` 返回 `False` 表示产物不合格,流程进入 error。

```python
from pathlib import Path

from easyflow import Node, Checkpoint


class GenSrt(Node):
    id = "gen_srt"
    checkpoint = Checkpoint.AFTER

    def accept(self, ctx) -> bool:
        return Path(ctx.get("fetch")["video"]).exists()

    def deliver(self, artifact) -> bool:
        return Path(artifact["srt"]).exists()

    def run(self, ctx) -> dict:
        video = ctx.get("fetch")["video"]
        srt_path = self.output_dir / "subtitle.srt"
        srt_path.write_text(f"subtitle for {video}\n", encoding="utf-8")
        return {"srt": str(srt_path)}
```

## 下一步

- 产物目录和续跑: [artifacts.md](artifacts.md)
- 调试模式: [debug.md](debug.md)
- 启动预检: [pass_check.md](pass_check.md)
- 参考手册（按类）: [ref/README.md](ref/README.md)
