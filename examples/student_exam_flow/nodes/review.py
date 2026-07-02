"""review 节点:全部收卷后老师统一验收,checkpoint AFTER 等确认。

按 r = 验收通过,老师离场;e = 退回重做(从 review 重跑);a = 中止考试。
"""

from easyflow import Node, Checkpoint


class Review(Node):
    id = "review"
    title = "老师统一验收"
    checkpoint = Checkpoint.AFTER

    def accept(self, ctx) -> bool:
        # 接手:至少一个学生已交卷
        return any(sid.startswith("student#") for sid in ctx.upstream_ids())

    def run(self, ctx) -> dict:
        papers = []
        for sid in ctx.upstream_ids():
            if sid.startswith("student#"):
                art = ctx.get(sid)
                papers.append(
                    {"name": art["name"], "answers": art["answers"]}
                )
        papers.sort(key=lambda p: p["name"])
        print(f"[review] 收到 {len(papers)} 份答卷,等待验收确认")
        return {"papers": papers, "passed": True, "count": len(papers)}

    def deliver(self, artifact) -> bool:
        return artifact["count"] > 0 and artifact["passed"]
