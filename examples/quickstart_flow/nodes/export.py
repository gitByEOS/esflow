"""export 节点:导出最终结果。"""

from easyflow import Node


class Export(Node):
    id = "export"
    title = "导出结果"

    def run(self, ctx) -> dict:
        reviewed = ctx.get("review")
        return {"output": reviewed, "exported": True}
