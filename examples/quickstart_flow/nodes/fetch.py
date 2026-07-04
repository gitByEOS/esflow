"""fetch 节点:模拟抓取数据。"""

from esflow import Node


class Fetch(Node):
    id = "fetch"
    title = "抓取数据"

    def run(self, ctx) -> dict:
        return {"items": [1, 2, 3], "count": 3}
