"""quickstart_flow:4 节点线性 DAG,review 带 checkpoint。

跑:
    easyflow run examples/quickstart_flow
    easyflow view examples/quickstart_flow
"""

from easyflow import flow, edge


@flow(id="quickstart_flow", title="演示流程")
class QuickstartFlow:
    nodes = ["fetch", "process", "review", "export"]
    edges = [
        edge("fetch", "process"),
        edge("process", "review"),
        edge("review", "export"),
    ]
