# Quickstart

这篇文档只做一件事:让你快速跑通第一个 flow,并知道下一步该改哪里。

## 跑通内置示例

```bash
esflow run examples/quickstart_flow
```

`quickstart_flow` 是首屏推荐示例,链路很短:

```text
fetch -> process -> review -> export
```

其中 `review` 带 `checkpoint`,运行到暂停点后,CLI 会等待输入:

```text
(c) continue, (r) retry, (a) abort
```

`r` 会重跑当前节点及其下游(上游复用),详见 [ref/Checkpoint.md](ref/Checkpoint.md)。

## 生成自己的 flow

```bash
esflow new my_skill
python my_skill/scripts/run.py --out ./runs/a
python my_skill/scripts/run.py --resume ./runs/a
```

`esflow new` 生成的模板带 `run.py`,内含 `pass_check` 预检与 skill 专属配置,并默认支持 `--out` / `--resume`;纯 flow 目录(无 skill 包装)直接 `esflow run <dir>` 跑,详见 [cli.md](cli.md)。

生成目录(`scripts/` 那一层就是 `Runner.load` 的入口,loader 只认 `flow.py` + `nodes/` 同级):

```text
my_skill/
  SKILL.md
  scripts/        # ← Runner.load("./my_skill/scripts")
    flow.py
    run.py
    nodes/
      fetch.py
      analyze.py
      report.py
```

直接手写最小 flow 时,不必套 `scripts/` 这层,`flow.py` + `nodes/` 同级即可,详见 [ref/FlowLoadError.md](ref/FlowLoadError.md#目录约定)。

## 最小节点

节点是 `Node` 子类,一个文件一个节点:

```python
from esflow import Node


class Fetch(Node):
    id = "fetch"
    title = "抓取数据"

    def run(self, ctx) -> dict:
        return {"items": [1, 2, 3]}
```

## 声明 flow

`flow.py` 用 `nodes` 和 `edges` 声明 DAG:

```python
from esflow import flow, edge


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

from esflow import Node, Checkpoint


class GenSrt(Node):
    id = "gen_srt"
    checkpoint = Checkpoint.TO_HUMAN

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
