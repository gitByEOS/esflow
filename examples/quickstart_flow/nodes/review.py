"""review 节点:人工复核,run 后暂停等确认(checkpoint TO_HUMAN)。"""

from esflow import Node, Checkpoint


class Review(Node):
    id = "review"
    title = "人工复核"
    checkpoint = Checkpoint.TO_HUMAN

    def run(self, ctx) -> dict:
        upstream = ctx.get("process")
        return {"reviewed": upstream, "ok": True}
