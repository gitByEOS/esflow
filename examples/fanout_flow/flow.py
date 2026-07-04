"""fanout_flow:并行副本 + 接手/脱手确认示范。

fetch 产出 10 道题 → 5 个 worker 副本并行各做 2 题 → merge 汇总。

跑:
    esflow run examples/fanout_flow
    esflow run examples/fanout_flow --node worker#2   # 单调试第 2 个副本
"""

from esflow import flow, edge


@flow(id="fanout_flow", title="并行扇出示范")
class FanoutFlow:
    nodes = ["fetch", "worker", "merge"]
    edges = [
        edge("fetch", "worker"),
        edge("worker", "merge"),
    ]
    replicas = {"worker": 5}
