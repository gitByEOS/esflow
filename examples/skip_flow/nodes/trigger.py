"""trigger:产出抓取目标。小数据只返回 dict,不写文件。"""

from esflow import Node


class Trigger(Node):
    id = "trigger"
    title = "抓取目标"

    def run(self, ctx) -> dict:
        return {"target": "https://example.com/article/1", "need": "html"}
