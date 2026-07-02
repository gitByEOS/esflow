"""process 节点:加工上游数据。"""

from easyflow import Node


class Process(Node):
    id = "process"
    title = "加工数据"

    def run(self, ctx) -> dict:
        upstream = ctx.get("fetch")
        items = upstream["items"]
        return {"doubled": [x * 2 for x in items], "count": upstream["count"]}
