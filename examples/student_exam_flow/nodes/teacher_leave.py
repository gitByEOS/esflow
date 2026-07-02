"""teacher_leave 节点:全部验收完毕,老师离场。"""

from easyflow import Node


class TeacherLeave(Node):
    id = "teacher_leave"
    title = "老师离场"

    def accept(self, ctx) -> bool:
        review = ctx.get("review")
        return review["passed"] and review["count"] > 0

    def run(self, ctx) -> dict:
        review = ctx.get("review")
        print(
            f"[teacher_leave] 验收 {review['count']} 份卷子完成,老师离场"
        )
        return {"finished": True, "papers": review["papers"]}
