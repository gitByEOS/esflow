"""worker 节点:并行翻译自己那章,读 self.fanout_payload 拿章节文本。

副本数由 split 的 FanOut 运行时决定(=章节数),不用 self.index 切片。
"""

from esflow import Node


class Worker(Node):
    id = "worker"
    title = "翻译章节"

    def accept(self, ctx) -> bool:
        # 接手:有自己那章
        return self.fanout_payload is not None

    def run(self, ctx) -> dict:
        chapter = self.fanout_payload
        return {"chapter": chapter, "translated": f"[译文]{chapter}"}

    def deliver(self, artifact) -> bool:
        # 脱手:有译文
        return bool(artifact["translated"])
