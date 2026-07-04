"""student 节点:并行副本,每个学生做题并交卷。

replicas=3,按 self.index 区分学生。
accept:上游已发卷;deliver:答卷非空(交卷确认,时间到自动收走)。
"""

import time

from esflow import Node


class Student(Node):
    id = "student"
    title = "学生做题"

    def accept(self, ctx) -> bool:
        paper = ctx.get("publish_paper")
        return bool(paper["questions"]) and paper["student_count"] > 0

    def run(self, ctx) -> dict:
        paper = ctx.get("publish_paper")
        name = paper["students"][self.index]
        questions = paper["questions"]
        # 模拟做题耗时:不同学生用时不同,view 能看到并行黄绿切换
        time.sleep(0.3 * (self.index + 1))
        answers = [f"{name}答:{q}" for q in questions]
        print(f"[student#{self.index}] {name} 做完 {len(answers)} 题,交卷")
        return {
            "student_index": self.index,
            "name": name,
            "answers": answers,
            "submitted": True,
        }

    def deliver(self, artifact) -> bool:
        # 交卷确认:答卷非空即收走
        return bool(artifact["answers"]) and artifact["submitted"]
