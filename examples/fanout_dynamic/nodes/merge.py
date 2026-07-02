"""merge 节点:用 ctx.gather 收集各章译文,按 index 排序合并成完整译本。"""

from easyflow import Node


class Merge(Node):
    id = "merge"
    title = "合并译本"

    def accept(self, ctx) -> bool:
        # 接手:worker 副本产物非空
        return len(ctx.gather("worker")) > 0

    def run(self, ctx) -> dict:
        results = ctx.gather("worker")
        book = "\n\n".join(r["translated"] for r in results)
        return {"total_chapters": len(results), "results": results, "book": book}

    def deliver(self, artifact) -> bool:
        # 脱手:章节数与副本数一致
        return artifact["total_chapters"] > 0
