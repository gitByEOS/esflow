"""split 节点:按章节 FanOut 展开 worker 副本,每章一个并行翻译。"""

from easyflow import Node, FanOut


class Split(Node):
    id = "split"
    title = "按章分发"

    def accept(self, ctx) -> bool:
        # 接手:上游有章节
        return bool(ctx.get("ingest")["chapters"])

    def run(self, ctx) -> FanOut:
        chapters = ctx.get("ingest")["chapters"]
        return FanOut(base="worker", payload=chapters)
