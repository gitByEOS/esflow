"""publish_paper 节点:老师发布试卷,checkpoint AFTER 等待发卷动作。

按 r = 发布试卷,学生开始做题。
"""

from esflow import Node, Checkpoint


class PublishPaper(Node):
    id = "publish_paper"
    title = "老师发布试卷"
    checkpoint = Checkpoint.AFTER

    def accept(self, ctx) -> bool:
        roster = ctx.get("register")
        return roster["count"] > 0

    def run(self, ctx) -> dict:
        roster = ctx.get("register")
        questions = ["题1", "题2", "题3", "题4", "题5"]
        print(f"[publish_paper] 试卷已就绪,共 {len(questions)} 题,等待发卷指令")
        return {
            "questions": questions,
            "student_count": roster["count"],
            "students": roster["students"],
        }
