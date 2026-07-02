"""fanout_dynamic:并行翻译一本书。

ingest 读入原书章节 → split 按章 FanOut 展开多个 worker 并行翻译 → merge 合并译本。
副本数运行时由章节数决定,不写死在 flow.py。

跑:
    easyflow run examples/fanout_dynamic
"""

from easyflow import flow, edge


@flow(id="fanout_dynamic", title="并行翻译示范")
class FanoutDynamicFlow:
    nodes = ["ingest", "split", "worker", "merge"]
    edges = [
        edge("ingest", "split"),
        edge("split", "worker"),
        edge("worker", "merge"),
    ]
    dynamic = {"worker"}   # worker 由 split 的 FanOut 运行时实例化
