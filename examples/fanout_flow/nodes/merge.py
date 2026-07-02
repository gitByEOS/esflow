"""merge 节点:汇总所有 worker 副本的结果。"""

from easyflow import Node


class Merge(Node):
    id = "merge"
    title = "汇总结果"

    def accept(self, ctx) -> bool:
        # 接手:至少有一个 worker 产物
        return any(sid.startswith("worker#") for sid in ctx.upstream_ids())

    def run(self, ctx) -> dict:
        all_results = []
        for sid in ctx.upstream_ids():
            if sid.startswith("worker#"):
                all_results.extend(ctx.get(sid)["results"])
        return {"total": len(all_results), "results": sorted(all_results)}

    def deliver(self, artifact) -> bool:
        # 脱手:汇总数 > 0
        return artifact["total"] > 0
