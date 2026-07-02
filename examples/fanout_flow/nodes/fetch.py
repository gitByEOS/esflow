"""fetch 节点:产出 10 道题(模拟抓取任务列表)。"""

from easyflow import Node


class Fetch(Node):
    id = "fetch"
    title = "抓取任务"

    def run(self, ctx) -> dict:
        return {"tasks": list(range(10))}
