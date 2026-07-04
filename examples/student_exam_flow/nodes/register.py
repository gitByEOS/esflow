"""register 节点:学生入场签到,产出学生名单。"""

from esflow import Node


class Register(Node):
    id = "register"
    title = "学生入场签到"

    def run(self, ctx) -> dict:
        students = ["小明", "小红", "小刚"]
        print(f"[register] {len(students)} 名学生入场完毕:{students}")
        return {"students": students, "count": len(students)}
