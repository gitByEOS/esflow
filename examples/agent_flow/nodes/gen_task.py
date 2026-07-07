"""gen_task 节点:生成给 agent 的任务卡(prompt + 待处理文本)。"""

from esflow import Node


class GenTask(Node):
    id = "gen_task"
    title = "生成任务"

    def run(self, ctx) -> dict:
        return {
            "prompt": "请把下面这段文本总结成一句话",
            "text": "esflow 是一个轻量 Python DAG workflow 框架,支持并行调度、动态扇出、人机协作控制循环",
        }
