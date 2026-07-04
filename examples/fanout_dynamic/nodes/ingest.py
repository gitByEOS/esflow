"""ingest 节点:读入原书章节(数量运行时定)。"""

from esflow import Node


class Ingest(Node):
    id = "ingest"
    title = "读入原书"

    def run(self, ctx) -> dict:
        return {"chapters": ["第一章:清晨", "第二章:正午", "第三章:黄昏", "第四章:深夜"]}
