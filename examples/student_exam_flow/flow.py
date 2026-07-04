"""student_exam_flow:学生考试场景,多 checkpoint + 副本示范。

链路:
    register(学生入场)
      → publish_paper[checkpoint](老师发卷,按 r 发布)
        → student#3(3 学生并行做题,做完即交卷)
          → review[checkpoint](全部收完,老师统一验收,按 r 通过 / e 重做 / a 中止)
            → teacher_leave(老师离场)

跑:
    esflow view examples/student_exam_flow
"""

from esflow import flow, edge


@flow(id="student_exam_flow", title="学生考试")
class StudentExamFlow:
    nodes = ["register", "publish_paper", "student", "review", "teacher_leave"]
    edges = [
        edge("register", "publish_paper"),
        edge("publish_paper", "student"),
        edge("student", "review"),
        edge("review", "teacher_leave"),
    ]
    replicas = {"student": 3}
